using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Logging;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Service for recovering and rebuilding corrupted LiteDB databases.
    /// </summary>
    public class DatabaseRecoveryService : IDatabaseRecoveryService
    {
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<DatabaseRecoveryService> _logger;
        private readonly string _metadataDirectory;
        private readonly string _quotaDirectory;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseRecoveryService"/> class.
        /// </summary>
        public DatabaseRecoveryService(
            MetadataRepository metadataRepository,
            DirectoryQuotaRepository quotaRepository,
            IFileSystem fileSystem,
            ILogger<DatabaseRecoveryService> logger,
            string metadataDirectory,
            string quotaDirectory)
        {
            _metadataRepository = metadataRepository ?? throw new ArgumentNullException(nameof(metadataRepository));
            _quotaRepository = quotaRepository ?? throw new ArgumentNullException(nameof(quotaRepository));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _metadataDirectory = metadataDirectory ?? throw new ArgumentNullException(nameof(metadataDirectory));
            _quotaDirectory = quotaDirectory ?? throw new ArgumentNullException(nameof(quotaDirectory));
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
                var connectionString = $"Filename={dbPath};Mode=Shared";
                using (var db = new LiteDatabase(connectionString))
                {
                    // Try to read collections to verify database integrity
                    var collections = db.GetCollectionNames().ToList();
                    return false; // Database is accessible
                }
            }
            catch (LiteException ex)
            {
                // Distinguish between file locking and actual corruption
                var errorMessage = ex.Message.ToLowerInvariant();

                _logger.LogDebug(
                    "LiteException when checking database: {DatabasePath}. " +
                    "ErrorCode: {ErrorCode}, Message: {Message}",
                    dbPath, ex.ErrorCode, ex.Message);

                // Check for file locking issues (NOT corruption)
                if (errorMessage.Contains("being used by another process") ||
                    errorMessage.Contains("sharing violation") ||
                    ex.ErrorCode == 32 || // Win32: ERROR_SHARING_VIOLATION
                    ex.ErrorCode == 33)   // Win32: ERROR_LOCK_VIOLATION
                {
                    _logger.LogDebug(
                        "Database is temporarily locked (not corrupted): {DatabasePath}. " +
                        "This is normal during startup when multiple services initialize.",
                        dbPath);
                    return false; // NOT corrupted, just locked
                }

                // Any other LiteException indicates corruption or invalid database
                _logger.LogError(ex,
                    "Database corruption or invalid format detected: {DatabasePath}. " +
                    "ErrorCode: {ErrorCode}",
                    dbPath, ex.ErrorCode);
                return true; // Assume corruption for all other LiteDB errors
            }
            catch (Exception ex)
            {
                // Non-LiteDB exceptions (e.g., IOException) also indicate problems
                // This could be file corruption, permission issues, or file system errors
                _logger.LogWarning(ex,
                    "Exception when checking database: {DatabasePath}. " +
                    "Exception type: {ExceptionType}. Assuming corrupted.",
                    dbPath, ex.GetType().Name);
                return true; // If we can't open the database, assume it's corrupted
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

            var dbPath = _fileSystem.Path.Combine(_metadataDirectory, $"{tenantId}.db");
            var result = new DatabaseRebuildResult
            {
                DatabaseType = "Metadata",
                TenantId = tenantId,
                DatabasePath = dbPath
            };

            _logger.LogWarning("Starting THREAD-SAFE metadata database rebuild for tenant {TenantId}. All operations will be BLOCKED.", tenantId);

            try
            {
                // Step 1: Begin rebuild (acquires lock, backs up and deletes corrupted DB)
                result.BackupPath = await _metadataRepository.BeginDatabaseRebuildAsync(tenantId, ct);

                if (result.BackupPath == null)
                {
                    result.Success = true;
                    result.Errors.Add("No database file found to rebuild");
                    return result;
                }

                // Step 2: Scan physical files and rebuild metadata
                var rebuiltFiles = 0;
                foreach (var volumePath in volumePaths)
                {
                    var tenantPath = _fileSystem.Path.Combine(volumePath, tenantId);
                    if (!_fileSystem.Directory.Exists(tenantPath))
                        continue;

                    var files = _fileSystem.Directory.GetFiles(tenantPath, "*", SearchOption.AllDirectories);

                    foreach (var filePath in files)
                    {
                        ct.ThrowIfCancellationRequested();

                        try
                        {
                            var fileInfo = _fileSystem.FileInfo.New(filePath);
                            var volumeId = _fileSystem.Path.GetFileName(volumePath);
                            var relativePath = filePath.Substring(tenantPath.Length + 1);
                            var directoryPath = _fileSystem.Path.GetDirectoryName(relativePath) ?? "/";

                            // Generate a file key from the physical path
                            var fileKey = Guid.NewGuid().ToString("N");

                            var metadata = new FileMetadata
                            {
                                FileKey = fileKey,
                                TenantId = tenantId,
                                VolumeId = volumeId,
                                PhysicalPath = filePath,
                                DirectoryPath = directoryPath.Replace("\\", "/"),
                                FileSize = fileInfo.Length,
                                Status = FileProcessingStatus.Pending, // Unknown status, mark as pending
                                CreatedAt = fileInfo.CreationTimeUtc,
                                RetryCount = 0,
                                AvailableForProcessingAt = DateTime.UtcNow
                            };

                            await _metadataRepository.AddOrUpdateAsync(metadata, ct);
                            rebuiltFiles++;

                            _logger.LogDebug("Rebuilt metadata for file: {FilePath}", filePath);
                        }
                        catch (Exception ex)
                        {
                            result.Errors.Add($"Failed to rebuild metadata for {filePath}: {ex.Message}");
                            _logger.LogWarning(ex, "Failed to rebuild metadata for file {FilePath}", filePath);
                        }
                    }
                }

                result.RecordsRebuilt = rebuiltFiles;
                result.Success = true;

                _logger.LogInformation(
                    "Metadata database rebuild completed for tenant {TenantId}. Rebuilt {Count} records",
                    tenantId, rebuiltFiles);
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Errors.Add($"Rebuild failed: {ex.Message}");
                _logger.LogError(ex, "Failed to rebuild metadata database for tenant {TenantId}", tenantId);
            }
            finally
            {
                // Step 3: Always release the lock
                _metadataRepository.FinishDatabaseRebuild(tenantId);
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

            var dbPath = _fileSystem.Path.Combine(_quotaDirectory, $"{tenantId}-quotas.db");
            var result = new DatabaseRebuildResult
            {
                DatabaseType = "Quota",
                TenantId = tenantId,
                DatabasePath = dbPath
            };

            _logger.LogWarning("Starting THREAD-SAFE quota database rebuild for tenant {TenantId}. All operations will be BLOCKED.", tenantId);

            try
            {
                // Step 1: Begin rebuild (acquires lock, backs up and deletes corrupted DB)
                result.BackupPath = await _quotaRepository.BeginDatabaseRebuildAsync(tenantId, ct);

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
                        var quota = await _quotaRepository.GetOrCreateAsync(tenantId, kvp.Key, ct);
                        quota.CurrentCount = kvp.Value;
                        quota.LastUpdated = DateTime.UtcNow;
                        quota.Enabled = true;
                        quota.MaxCount = 0; // No limit by default, needs manual configuration

                        await _quotaRepository.UpdateAsync(tenantId, quota, ct);
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
                _quotaRepository.FinishDatabaseRebuild(tenantId);
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

            // Check metadata databases
            if (_fileSystem.Directory.Exists(_metadataDirectory))
            {
                var metadataFiles = _fileSystem.Directory.GetFiles(_metadataDirectory, "*.db");
                foreach (var dbPath in metadataFiles)
                {
                    ct.ThrowIfCancellationRequested();

                    var tenantId = _fileSystem.Path.GetFileNameWithoutExtension(dbPath);

                    // Skip backup files created by LiteDB Rebuild() or corruption recovery
                    // Examples: "tenant-001.db-backup-1", "tenant-001.db.corrupted.20240122120000"
                    if (IsBackupFile(tenantId))
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

            // Check quota databases
            if (_fileSystem.Directory.Exists(_quotaDirectory))
            {
                var quotaFiles = _fileSystem.Directory.GetFiles(_quotaDirectory, "*-quotas.db");
                foreach (var dbPath in quotaFiles)
                {
                    ct.ThrowIfCancellationRequested();

                    var fileName = _fileSystem.Path.GetFileNameWithoutExtension(dbPath);

                    // Skip backup files before extracting tenant ID
                    // Examples: "tenant-001-quotas.db-backup-1", "tenant-001-quotas.db.corrupted.20240122120000"
                    if (IsBackupFile(fileName))
                        continue;

                    var tenantId = fileName.EndsWith("-quotas")
                        ? fileName.Substring(0, fileName.Length - 7)
                        : fileName;

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

        /// <summary>
        /// Checks if a tenant ID represents a backup file rather than a real tenant.
        /// LiteDB Rebuild() creates temporary backup files like "tenant-001.db-backup-1".
        /// Corruption recovery creates backups like "tenant-001.db.corrupted.20240122120000".
        /// </summary>
        private static bool IsBackupFile(string tenantId)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                return true;

            // LiteDB Rebuild backup pattern: ends with "-backup" or "-backup-N"
            // Examples: "tenant-001.db-backup-1", "tenant-001.db-backup-2"
            if (tenantId.Contains("-backup", StringComparison.OrdinalIgnoreCase))
                return true;

            // Corruption recovery backup pattern: contains ".corrupted."
            // Examples: "tenant-001.db.corrupted.20240122120000"
            if (tenantId.Contains(".corrupted.", StringComparison.OrdinalIgnoreCase))
                return true;

            // LiteDB journal files
            if (tenantId.EndsWith("-journal", StringComparison.OrdinalIgnoreCase))
                return true;

            return false;
        }
    }
}
