using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Service for recovering and rebuilding corrupted SQLite databases.
    /// </summary>
    public class DatabaseRecoveryService : IDatabaseRecoveryService
    {
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<DatabaseRecoveryService> _logger;
        private readonly string _metadataDirectory;
        private readonly string _quotaDirectory;
        private readonly IReadOnlyDictionary<string, string> _volumeIdByPath;
        private const int MetadataRebuildProgressCheckpoint = 1000;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseRecoveryService"/> class.
        /// </summary>
        public DatabaseRecoveryService(
            MetadataRepository metadataRepository,
            DirectoryQuotaRepository quotaRepository,
            IFileSystem fileSystem,
            ILogger<DatabaseRecoveryService> logger,
            string metadataDirectory,
            string quotaDirectory,
            IReadOnlyDictionary<string, string>? volumeIdByPath = null)
        {
            _metadataRepository = metadataRepository ?? throw new ArgumentNullException(nameof(metadataRepository));
            _quotaRepository = quotaRepository ?? throw new ArgumentNullException(nameof(quotaRepository));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _metadataDirectory = metadataDirectory ?? throw new ArgumentNullException(nameof(metadataDirectory));
            _quotaDirectory = quotaDirectory ?? throw new ArgumentNullException(nameof(quotaDirectory));
            _volumeIdByPath = NormalizeVolumePathMap(volumeIdByPath);
        }

        /// <summary>
        /// Checks if a database file is corrupted.
        /// </summary>
        /// <param name="dbPath">Path to the database file.</param>
        /// <returns>True if corrupted, false otherwise.</returns>
        public bool IsDatabaseCorrupted(string dbPath)
        {
            if (!_fileSystem.File.Exists(dbPath))
                return false;

            try
            {
                var connectionString = $"Data Source={dbPath};Cache=Shared;Mode=ReadOnly";
                using (var conn = new SqliteConnection(connectionString))
                {
                    conn.Open();
                    using (var cmd = conn.CreateCommand())
                    {
                        // integrity_check returns "ok" for a healthy database.
                        // Any other result (including multiple rows) means corruption.
                        cmd.CommandText = "PRAGMA integrity_check(1);";
                        var result = cmd.ExecuteScalar() as string;
                        if (result == "ok")
                            return false;

                        _logger.LogError(
                            "SQLite integrity_check failed for {DatabasePath}: {Result}",
                            dbPath, result);
                        return true;
                    }
                }
            }
            catch (SqliteException ex)
            {
                // SQLITE_CORRUPT (11), SQLITE_NOTADB (26), SQLITE_IOERR (10)
                if (ex.SqliteErrorCode == 11 || ex.SqliteErrorCode == 26 || ex.SqliteErrorCode == 10)
                {
                    _logger.LogError(ex,
                        "SQLite database corruption detected: {DatabasePath}. ErrorCode: {ErrorCode}",
                        dbPath, ex.SqliteErrorCode);
                    return true;
                }

                // SQLITE_BUSY (5) / SQLITE_LOCKED (6) -- temporarily unavailable, not corrupted
                if (ex.SqliteErrorCode == 5 || ex.SqliteErrorCode == 6)
                {
                    _logger.LogDebug(
                        "SQLite database is temporarily locked (not corrupted): {DatabasePath}",
                        dbPath);
                    return false;
                }

                _logger.LogWarning(ex,
                    "SqliteException when checking database: {DatabasePath}. ErrorCode: {ErrorCode}. Assuming corrupted.",
                    dbPath, ex.SqliteErrorCode);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Exception when checking database: {DatabasePath}. ExceptionType: {ExceptionType}. Assuming corrupted.",
                    dbPath, ex.GetType().Name);
                return true;
            }
        }

        /// <summary>
        /// Rebuilds a corrupted metadata database for a specific tenant by scanning physical files.
        /// Thread-safe: Acquires exclusive lock for this tenant, blocking all operations during rebuild.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="volumePaths">List of volume mount paths to scan.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Statistics about the rebuild operation.</returns>
        public async Task<DatabaseRebuildResult> RebuildMetadataDatabaseAsync(
            string tenantId,
            IEnumerable<string> volumePaths,
            CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var dbPath = _fileSystem.Path.Combine(_metadataDirectory, tenantId, "metadata.db");
            var result = new DatabaseRebuildResult
            {
                DatabaseType = "Metadata",
                TenantId = tenantId,
                DatabasePath = dbPath
            };

            _logger.LogWarning("Starting THREAD-SAFE metadata database rebuild for tenant {TenantId}. All operations will be BLOCKED.", tenantId);

            var rebuildLockHeld = false;
            try
            {
                // Step 1: Begin rebuild (acquires lock, backs up and deletes corrupted DB)
                result.BackupPath = await _metadataRepository.BeginDatabaseRebuildAsync(tenantId, ct);
                rebuildLockHeld = true;

                if (result.BackupPath == null)
                {
                    result.Success = true;
                    result.Errors.Add("No database file found to rebuild");
                    return result;
                }

                // Step 2: Scan physical files and rebuild metadata
                var rebuiltFiles = 0;
                var scannedFiles = 0;
                var failedFiles = 0;
                foreach (var volumePath in volumePaths)
                {
                    var tenantPath = _fileSystem.Path.Combine(volumePath, tenantId);
                    if (!_fileSystem.Directory.Exists(tenantPath))
                        continue;

                    var files = _fileSystem.Directory.EnumerateFiles(tenantPath, "*", SearchOption.AllDirectories);

                    foreach (var filePath in files)
                    {
                        ct.ThrowIfCancellationRequested();
                        scannedFiles++;

                        try
                        {
                            var fileInfo = _fileSystem.FileInfo.New(filePath);
                            var volumeId = ResolveVolumeId(volumePath);
                            var relativePath = filePath.Length > tenantPath.Length
                                ? filePath.Substring(tenantPath.Length).TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                                : string.Empty;
                            var directoryPath = Locus.Storage.DirectoryPathNormalizer.NormalizeFromRelativePath(
                                _fileSystem.Path.GetDirectoryName(relativePath));

                            // Use the physical file name as key so rebuild is deterministic.
                            var fileName = _fileSystem.Path.GetFileName(filePath);
                            var fileKey = _fileSystem.Path.GetFileNameWithoutExtension(fileName);
                            if (string.IsNullOrWhiteSpace(fileKey))
                            {
                                result.Errors.Add($"Failed to rebuild metadata for {filePath}: invalid file name");
                                failedFiles++;
                                continue;
                            }

                            var metadata = new FileMetadata
                            {
                                FileKey = fileKey,
                                TenantId = tenantId,
                                VolumeId = volumeId,
                                PhysicalPath = filePath,
                                DirectoryPath = directoryPath,
                                FileSize = fileInfo.Length,
                                Status = FileProcessingStatus.Pending, // Unknown status, mark as pending
                                CreatedAt = fileInfo.CreationTimeUtc,
                                RetryCount = 0,
                                AvailableForProcessingAt = DateTime.UtcNow,
                                OriginalFileName = fileName,
                                FileExtension = _fileSystem.Path.GetExtension(filePath)
                            };

                            // Use direct write — rebuild must be persisted synchronously so the
                            // new DB file exists and is fully populated when this method returns.
                            await _metadataRepository.AddOrUpdateDirectAsync(metadata, ct);
                            rebuiltFiles++;

                            if (scannedFiles % MetadataRebuildProgressCheckpoint == 0)
                            {
                                _logger.LogInformation(
                                    "Metadata rebuild progress for tenant {TenantId}: scanned={Scanned}, rebuilt={Rebuilt}, failed={Failed}",
                                    tenantId,
                                    scannedFiles,
                                    rebuiltFiles,
                                    failedFiles);
                            }
                        }
                        catch (Exception ex)
                        {
                            result.Errors.Add($"Failed to rebuild metadata for {filePath}: {ex.Message}");
                            failedFiles++;
                            _logger.LogWarning(ex, "Failed to rebuild metadata for file {FilePath}", filePath);
                        }
                    }
                }

                result.RecordsRebuilt = rebuiltFiles;
                result.Success = true;

                _logger.LogInformation(
                    "Metadata database rebuild completed for tenant {TenantId}. Scanned={Scanned}, Rebuilt={Rebuilt}, Failed={Failed}",
                    tenantId,
                    scannedFiles,
                    rebuiltFiles,
                    failedFiles);
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Errors.Add($"Rebuild failed: {ex.Message}");
                _logger.LogError(ex, "Failed to rebuild metadata database for tenant {TenantId}", tenantId);
            }
            finally
            {
                // Step 3: Release lock only when rebuild path actually acquired it.
                if (rebuildLockHeld)
                {
                    _metadataRepository.FinishDatabaseRebuild(tenantId);
                }
            }

            return result;
        }

        /// <summary>
        /// Rebuilds a corrupted quota database for a specific tenant by scanning directories.
        /// Thread-safe: Acquires exclusive lock for this tenant, blocking all operations during rebuild.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="volumePaths">List of volume mount paths to scan.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Statistics about the rebuild operation.</returns>
        public async Task<DatabaseRebuildResult> RebuildQuotaDatabaseAsync(
            string tenantId,
            IEnumerable<string> volumePaths,
            CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var dbPath = _fileSystem.Path.Combine(_quotaDirectory, tenantId, "quotas.db");
            var result = new DatabaseRebuildResult
            {
                DatabaseType = "Quota",
                TenantId = tenantId,
                DatabasePath = dbPath
            };

            _logger.LogWarning("Starting THREAD-SAFE quota database rebuild for tenant {TenantId}. All operations will be BLOCKED.", tenantId);

            var rebuildLockHeld = false;
            try
            {
                // Step 1: Begin rebuild (acquires lock, backs up and deletes corrupted DB)
                result.BackupPath = await _quotaRepository.BeginDatabaseRebuildAsync(tenantId, ct);
                rebuildLockHeld = true;

                if (result.BackupPath == null)
                {
                    result.Success = true;
                    result.Errors.Add("No database file found to rebuild");
                    return result;
                }

                // Step 2: Scan directories and rebuild quotas
                var directoryCounts = new Dictionary<string, int>();

                foreach (var volumePath in volumePaths)
                {
                    var tenantPath = _fileSystem.Path.Combine(volumePath, tenantId);
                    if (!_fileSystem.Directory.Exists(tenantPath))
                        continue;

                    ScanDirectoryRecursive(tenantPath, tenantPath, directoryCounts);
                }

                // Rebuild quota records
                var rebuiltQuotas = 0;
                foreach (var kvp in directoryCounts)
                {
                    ct.ThrowIfCancellationRequested();

                    try
                    {
                        // BeginDatabaseRebuildAsync already holds the tenant lock until FinishDatabaseRebuild.
                        // Use the rebuild-specific path to avoid re-acquiring the same lock and deadlocking.
                        await _quotaRepository.SetCurrentCountForRebuildAsync(tenantId, kvp.Key, kvp.Value, ct);
                        rebuiltQuotas++;

                        _logger.LogDebug("Rebuilt quota for directory: {DirectoryPath}, Count: {Count}",
                            kvp.Key, kvp.Value);
                    }
                    catch (Exception ex)
                    {
                        result.Errors.Add($"Failed to rebuild quota for {kvp.Key}: {ex.Message}");
                        _logger.LogWarning(ex, "Failed to rebuild quota for directory {DirectoryPath}", kvp.Key);
                    }
                }

                result.RecordsRebuilt = rebuiltQuotas;
                result.Success = true;

                _logger.LogInformation(
                    "Quota database rebuild completed for tenant {TenantId}. Rebuilt {Count} records",
                    tenantId, rebuiltQuotas);
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Errors.Add($"Rebuild failed: {ex.Message}");
                _logger.LogError(ex, "Failed to rebuild quota database for tenant {TenantId}", tenantId);
            }
            finally
            {
                // Step 3: Always release the lock
                if (rebuildLockHeld)
                {
                    _quotaRepository.FinishDatabaseRebuild(tenantId);
                }
            }

            return result;
        }

        /// <summary>
        /// Scans directories recursively and counts files in each directory.
        /// </summary>
        private void ScanDirectoryRecursive(
            string currentPath,
            string rootPath,
            Dictionary<string, int> directoryCounts)
        {
            if (!_fileSystem.Directory.Exists(currentPath))
                return;

            // Get relative path from root
            var relativePath = currentPath.Substring(rootPath.Length).TrimStart(Path.DirectorySeparatorChar);
            if (string.IsNullOrEmpty(relativePath))
                relativePath = "/";
            else
                relativePath = "/" + relativePath.Replace("\\", "/");

            // Count files in current directory (not recursive)
            var fileCount = _fileSystem.Directory.GetFiles(currentPath).Length;
            if (fileCount > 0 || directoryCounts.Count == 0) // Include root even if empty
            {
                directoryCounts[relativePath] = fileCount;
            }

            // Scan subdirectories
            var subdirectories = _fileSystem.Directory.GetDirectories(currentPath);
            foreach (var subdirectory in subdirectories)
            {
                ScanDirectoryRecursive(subdirectory, rootPath, directoryCounts);
            }
        }

        /// <summary>
        /// Checks all databases for corruption and returns a health report.
        /// </summary>
        public async Task<DatabaseHealthReport> CheckAllDatabasesAsync(CancellationToken ct)
        {
            var report = new DatabaseHealthReport();

            // Check metadata databases: each tenant has its own subdirectory {metadataDirectory}/{tenantId}/metadata.db
            if (_fileSystem.Directory.Exists(_metadataDirectory))
            {
                var tenantDirs = _fileSystem.Directory.GetDirectories(_metadataDirectory);
                foreach (var dir in tenantDirs)
                {
                    ct.ThrowIfCancellationRequested();

                    var tenantId = _fileSystem.Path.GetFileName(dir);
                    var dbPath = _fileSystem.Path.Combine(dir, "metadata.db");

                    if (!_fileSystem.File.Exists(dbPath))
                        continue;

                    var isCorrupted = IsDatabaseCorrupted(dbPath);

                    if (isCorrupted)
                    {
                        report.CorruptedDatabases.Add(new DatabaseHealthInfo
                        {
                            DatabaseType = "Metadata",
                            TenantId = tenantId,
                            DatabasePath = dbPath,
                            IsCorrupted = true
                        });
                    }
                    else
                    {
                        report.HealthyDatabases++;
                    }
                }
            }

            // Check quota databases: each tenant has its own subdirectory {quotaDirectory}/{tenantId}/quotas.db
            if (_fileSystem.Directory.Exists(_quotaDirectory))
            {
                var tenantDirs = _fileSystem.Directory.GetDirectories(_quotaDirectory);
                foreach (var dir in tenantDirs)
                {
                    ct.ThrowIfCancellationRequested();

                    var tenantId = _fileSystem.Path.GetFileName(dir);
                    var dbPath = _fileSystem.Path.Combine(dir, "quotas.db");

                    if (!_fileSystem.File.Exists(dbPath))
                        continue;

                    var isCorrupted = IsDatabaseCorrupted(dbPath);

                    if (isCorrupted)
                    {
                        report.CorruptedDatabases.Add(new DatabaseHealthInfo
                        {
                            DatabaseType = "Quota",
                            TenantId = tenantId,
                            DatabasePath = dbPath,
                            IsCorrupted = true
                        });
                    }
                    else
                    {
                        report.HealthyDatabases++;
                    }
                }
            }

            return await Task.FromResult(report);
        }

        private static IReadOnlyDictionary<string, string> NormalizeVolumePathMap(
            IReadOnlyDictionary<string, string>? volumeIdByPath)
        {
            if (volumeIdByPath == null || volumeIdByPath.Count == 0)
                return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var normalized = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in volumeIdByPath)
            {
                if (string.IsNullOrWhiteSpace(kvp.Key) || string.IsNullOrWhiteSpace(kvp.Value))
                    continue;

                var normalizedPath = Path.GetFullPath(kvp.Key)
                    .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                normalized[normalizedPath] = kvp.Value;
            }

            return normalized;
        }

        private string ResolveVolumeId(string volumePath)
        {
            var normalizedPath = Path.GetFullPath(volumePath)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

            if (_volumeIdByPath.TryGetValue(normalizedPath, out var configuredVolumeId))
                return configuredVolumeId;

            var fallback = _fileSystem.Path.GetFileName(volumePath);
            _logger.LogWarning(
                "Volume path {VolumePath} is not in the configured map, falling back to directory name {FallbackVolumeId}",
                volumePath, fallback);
            return fallback;
        }

    }
}
