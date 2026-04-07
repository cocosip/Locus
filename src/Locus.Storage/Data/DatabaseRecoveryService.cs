using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Service for recovering and rebuilding corrupted SQLite databases.
    /// </summary>
    public class DatabaseRecoveryService : IDatabaseRecoveryService
    {
        private readonly IQueueProjectionStore _projectionStore;
        private readonly IMetadataProjectionMaintenanceStore _metadataMaintenanceStore;
        private readonly IQuotaProjectionMaintenanceStore _quotaMaintenanceStore;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<DatabaseRecoveryService> _logger;
        private readonly string _metadataDirectory;
        private readonly string _quotaDirectory;
        private readonly IReadOnlyDictionary<string, string> _volumeIdByPath;
        private readonly IQueueProjectionMaintenanceService? _queueProjectionMaintenanceService;
        private readonly IStorageCleanupService? _storageCleanupService;
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
            IReadOnlyDictionary<string, string>? volumeIdByPath = null,
            IQueueProjectionMaintenanceService? queueProjectionMaintenanceService = null,
            IStorageCleanupService? storageCleanupService = null,
            IQueueProjectionStore? projectionStore = null,
            IMetadataProjectionMaintenanceStore? metadataMaintenanceStore = null,
            IQuotaProjectionMaintenanceStore? quotaMaintenanceStore = null)
        {
            if (metadataRepository == null)
                throw new ArgumentNullException(nameof(metadataRepository));
            if (quotaRepository == null)
                throw new ArgumentNullException(nameof(quotaRepository));

            _projectionStore = projectionStore ?? new MetadataRepositoryQueueProjectionStore(metadataRepository);
            _metadataMaintenanceStore = metadataMaintenanceStore ?? new MetadataRepositoryProjectionMaintenanceStore(metadataRepository);
            _quotaMaintenanceStore = quotaMaintenanceStore ?? new DirectoryQuotaRepositoryProjectionMaintenanceStore(quotaRepository);
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _metadataDirectory = metadataDirectory ?? throw new ArgumentNullException(nameof(metadataDirectory));
            _quotaDirectory = quotaDirectory ?? throw new ArgumentNullException(nameof(quotaDirectory));
            _volumeIdByPath = NormalizeVolumePathMap(volumeIdByPath);
            _queueProjectionMaintenanceService = queueProjectionMaintenanceService;
            _storageCleanupService = storageCleanupService;
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
                // Use Mode=ReadOnly with private cache (no Cache=Shared) and disable pooling
                // so integrity checks do not leave behind pooled file handles.
                // Cache=Shared would add this read-only connection to the process-wide SQLite
                // shared-cache pool; any concurrent write connection on the same file that also
                // uses Cache=Shared would then see the read-only flag and return SQLITE_READONLY
                // (Error 8), causing spurious write failures in DirectoryQuotaRepository or
                // MetadataRepository during startup races with DatabaseHealthCheckService.
                var connectionString = $"Data Source={dbPath};Mode=ReadOnly;Pooling=False";
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
        /// Rebuilds a corrupted metadata database for a specific tenant.
        /// Prefers queue-journal projection recovery when available and falls back to physical file scanning.
        /// Thread-safe: Acquires exclusive lock for this tenant, blocking all operations during rebuild.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="volumePaths">List of volume mount paths to scan.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Statistics about the rebuild operation.</returns>
        public async Task<DatabaseRebuildResult> RebuildMetadataDatabaseAsync(
            string tenantId,
            IEnumerable<string> volumePaths,
            CancellationToken ct = default)
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

            try
            {
                // BeginDatabaseRebuildAsync acquires the exclusive tenant lock and returns a handle
                // that releases the lock on Dispose. The using declaration inside this try block
                // guarantees the lock is released when the block exits (normally or via exception),
                // while the catch block below handles all failures (including cancellation) uniformly.
                using var rebuildHandle = await _metadataMaintenanceStore.BeginDatabaseRebuildAsync(tenantId, ct);
                result.BackupPath = rebuildHandle.BackupPath;

                if (result.BackupPath == null)
                {
                    _logger.LogInformation(
                        "No existing metadata database file was found for tenant {TenantId}; creating a new one during rebuild",
                        tenantId);
                }

                if (await TryRebuildMetadataFromQueueAsync(tenantId, volumePaths, result, ct).ConfigureAwait(false))
                    return result;

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
                            var directoryPath = DirectoryPathNormalizer.Normalize(null);

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

                            // Use direct write 鈥?rebuild must be persisted synchronously so the
                            // new DB file exists and is fully populated when this method returns.
                            QueueProjectionMetadataState.MarkAcceptedProjectionApplied(metadata);
                            await _projectionStore.UpsertProjectedFileAsync(metadata, ct).ConfigureAwait(false);
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

            return result;
        }

        /// <summary>
        /// Rebuilds a corrupted quota database for a specific tenant.
        /// Prefers metadata-based reconciliation when available and falls back to directory scanning.
        /// Thread-safe: Acquires exclusive lock for this tenant, blocking all operations during rebuild.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="volumePaths">List of volume mount paths to scan.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Statistics about the rebuild operation.</returns>
        public async Task<DatabaseRebuildResult> RebuildQuotaDatabaseAsync(
            string tenantId,
            IEnumerable<string> volumePaths,
            CancellationToken ct = default)
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
            var rebuiltFromMetadata = false;

            try
            {
                // BeginDatabaseRebuildAsync acquires the exclusive tenant lock and returns a handle
                // that releases the lock on Dispose. The using declaration inside this try block
                // guarantees the lock is released when the block exits (normally or via exception),
                // while the catch block below handles all failures (including cancellation) uniformly.
                using var rebuildHandle = await _quotaMaintenanceStore.BeginDatabaseRebuildAsync(tenantId, ct);
                result.BackupPath = rebuildHandle.BackupPath;

                if (result.BackupPath == null)
                {
                    _logger.LogInformation(
                        "No existing quota database file was found for tenant {TenantId}; creating a new one during rebuild",
                        tenantId);
                }

                if (await TryRebuildQuotaFromMetadataAsync(tenantId, volumePaths, result, ct).ConfigureAwait(false))
                {
                    rebuiltFromMetadata = true;
                }
                else
                {
                    var totalFileCount = CountPhysicalFilesForTenant(tenantId, volumePaths, ct);
                    var rebuiltQuotas = 0;

                    try
                    {
                        await _quotaMaintenanceStore.SetProjectedCountForRebuildAsync(
                            tenantId,
                            DirectoryPathNormalizer.Normalize(null),
                            totalFileCount,
                            ct).ConfigureAwait(false);
                        rebuiltQuotas++;

                        await _quotaMaintenanceStore.SetProjectedCountForRebuildAsync(
                            tenantId,
                            tenantId,
                            totalFileCount,
                            ct).ConfigureAwait(false);
                        rebuiltQuotas++;

                        result.RecordsRebuilt = rebuiltQuotas;
                        result.Success = true;

                        _logger.LogInformation(
                            "Quota database rebuild completed for tenant {TenantId} from physical scan. RootCount={RootCount}, TenantCount={TenantCount}",
                            tenantId,
                            totalFileCount,
                            totalFileCount);
                    }
                    catch (Exception ex)
                    {
                        result.Errors.Add($"Failed to rebuild logical root quota for tenant {tenantId}: {ex.Message}");
                        throw;
                    }
                }
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Errors.Add($"Rebuild failed: {ex.Message}");
                _logger.LogError(ex, "Failed to rebuild quota database for tenant {TenantId}", tenantId);
            }

            if (rebuiltFromMetadata && _storageCleanupService != null)
            {
                try
                {
                    await _storageCleanupService.ReconcileQuotaCountsAsync(tenantId, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Post-rebuild quota reconciliation callback failed for tenant {TenantId} after metadata-based recovery",
                        tenantId);
                }
            }

            return result;
        }

        private int CountPhysicalFilesForTenant(
            string tenantId,
            IEnumerable<string> volumePaths,
            CancellationToken ct)
        {
            var totalFileCount = 0;

            foreach (var volumePath in volumePaths)
            {
                ct.ThrowIfCancellationRequested();

                var tenantPath = _fileSystem.Path.Combine(volumePath, tenantId);
                if (!_fileSystem.Directory.Exists(tenantPath))
                    continue;

                totalFileCount += _fileSystem.Directory
                    .EnumerateFiles(tenantPath, "*", SearchOption.AllDirectories)
                    .Count();
            }

            return totalFileCount;
        }

        /// <summary>
        /// Checks all databases for corruption and returns a health report.
        /// </summary>
        public async Task<DatabaseHealthReport> CheckAllDatabasesAsync(CancellationToken ct = default)
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

        private async Task<bool> TryRebuildMetadataFromQueueAsync(
            string tenantId,
            IEnumerable<string> volumePaths,
            DatabaseRebuildResult result,
            CancellationToken ct)
        {
            if (_queueProjectionMaintenanceService == null)
                return false;

            try
            {
                var projectionState = await _queueProjectionMaintenanceService
                    .RebuildTenantAsync(tenantId, ct)
                    .ConfigureAwait(false);
                var rebuiltFiles = (await _projectionStore.GetProjectedFilesAsync(tenantId, ct).ConfigureAwait(false)).Count;
                if (rebuiltFiles == 0 && HasAnyPhysicalFilesForTenant(tenantId, volumePaths))
                {
                    _logger.LogWarning(
                        "Queue-based metadata recovery produced no records for tenant {TenantId} while physical files still exist; falling back to file scan",
                        tenantId);
                    return false;
                }

                result.RecordsRebuilt = rebuiltFiles;
                result.Success = true;
                _logger.LogInformation(
                    "Recovered metadata database for tenant {TenantId} from queue journal. RebuiltRecords={Records}, LagBytes={LagBytes}, Snapshot={HasSnapshot}",
                    tenantId,
                    rebuiltFiles,
                    projectionState.LagBytes,
                    projectionState.HasSnapshot);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Queue-based metadata recovery failed for tenant {TenantId}; falling back to physical scan",
                    tenantId);
                result.Errors.Add($"Queue-based metadata recovery failed: {ex.Message}");
                return false;
            }
        }

        private async Task<bool> TryRebuildQuotaFromMetadataAsync(
            string tenantId,
            IEnumerable<string> volumePaths,
            DatabaseRebuildResult result,
            CancellationToken ct)
        {
            try
            {
                var projectedMetadata = await _projectionStore.GetProjectedFilesAsync(tenantId, ct).ConfigureAwait(false);
                var metadataCount = projectedMetadata.Count;
                if (metadataCount == 0 && _queueProjectionMaintenanceService != null)
                {
                    var projectionState = await _queueProjectionMaintenanceService
                        .RebuildTenantAsync(tenantId, ct)
                        .ConfigureAwait(false);
                    projectedMetadata = await _projectionStore.GetProjectedFilesAsync(tenantId, ct).ConfigureAwait(false);
                    metadataCount = projectedMetadata.Count;

                    if (metadataCount > 0)
                    {
                        _logger.LogInformation(
                            "Recovered metadata from queue journal before quota rebuild for tenant {TenantId}. RebuiltRecords={Records}, LagBytes={LagBytes}, Snapshot={HasSnapshot}",
                            tenantId,
                            metadataCount,
                            projectionState.LagBytes,
                            projectionState.HasSnapshot);
                    }
                }

                if (metadataCount == 0)
                {
                    if (HasAnyPhysicalFilesForTenant(tenantId, volumePaths))
                    {
                        _logger.LogWarning(
                            "Quota rebuild for tenant {TenantId} could not use metadata or queue recovery while physical files still exist; falling back to directory scan",
                            tenantId);
                        return false;
                    }

                    result.Success = true;
                    result.RecordsRebuilt = 0;
                    _logger.LogInformation(
                        "Quota rebuild for tenant {TenantId} found no metadata and no physical files; treating as empty tenant",
                        tenantId);
                    return true;
                }

                var expectedDirectoryCounts = projectedMetadata
                    .GroupBy(
                        metadata => DirectoryPathNormalizer.Normalize(metadata.DirectoryPath),
                        StringComparer.Ordinal)
                    .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

                var existingDirectoryQuotas = (await _quotaMaintenanceStore.GetQuotaRowsAsync(tenantId, ct).ConfigureAwait(false))
                    .Where(quota => !string.IsNullOrWhiteSpace(quota.DirectoryPath)
                        && !string.Equals(quota.DirectoryPath, tenantId, StringComparison.Ordinal))
                    .ToDictionary(quota => quota.DirectoryPath, quota => quota, StringComparer.Ordinal);

                var knownDirectoryPaths = new HashSet<string>(existingDirectoryQuotas.Keys, StringComparer.Ordinal);
                foreach (var directoryPath in expectedDirectoryCounts.Keys)
                    knownDirectoryPaths.Add(directoryPath);

                foreach (var directoryPath in knownDirectoryPaths)
                {
                    ct.ThrowIfCancellationRequested();

                    var expectedDirectoryCount = expectedDirectoryCounts.TryGetValue(directoryPath, out var count)
                        ? count
                        : 0;

                    if (expectedDirectoryCount == 0
                        && existingDirectoryQuotas.TryGetValue(directoryPath, out var existingQuota)
                        && !HasExplicitQuotaLimit(existingQuota))
                    {
                        await _quotaMaintenanceStore.RemoveQuotaAsync(tenantId, directoryPath, ct).ConfigureAwait(false);
                        continue;
                    }

                    await _quotaMaintenanceStore.SetProjectedCountForRebuildAsync(
                        tenantId,
                        directoryPath,
                        expectedDirectoryCount,
                        ct).ConfigureAwait(false);
                }

                await _quotaMaintenanceStore.SetProjectedCountForRebuildAsync(tenantId, tenantId, metadataCount, ct).ConfigureAwait(false);
                result.RecordsRebuilt = (await _quotaMaintenanceStore.GetQuotaRowsAsync(tenantId, ct).ConfigureAwait(false)).Count;
                result.Success = true;
                _logger.LogInformation(
                    "Recovered quota database for tenant {TenantId} from projected metadata. RebuiltRecords={Records}, MetadataRecords={MetadataRecords}",
                    tenantId,
                    result.RecordsRebuilt,
                    metadataCount);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Metadata-based quota recovery failed for tenant {TenantId}; falling back to directory scan",
                    tenantId);
                result.Errors.Add($"Metadata-based quota recovery failed: {ex.Message}");
                return false;
            }
        }

        private static bool HasExplicitQuotaLimit(DirectoryQuota quota)
        {
            if (quota == null)
                throw new ArgumentNullException(nameof(quota));

            return quota.MaxCount > 0;
        }

        private bool HasAnyPhysicalFilesForTenant(string tenantId, IEnumerable<string> volumePaths)
        {
            foreach (var volumePath in volumePaths)
            {
                var tenantPath = _fileSystem.Path.Combine(volumePath, tenantId);
                if (!_fileSystem.Directory.Exists(tenantPath))
                    continue;

                if (_fileSystem.Directory.EnumerateFiles(tenantPath, "*", SearchOption.AllDirectories).Any())
                    return true;
            }

            return false;
        }

    }
}
