using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Provides automatic cleanup services for the storage pool.
    /// </summary>
    public class StorageCleanupService : IStorageCleanupService
    {
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<StorageCleanupService> _logger;
        private readonly ConcurrentDictionary<string, IStorageVolume> _volumes;
        private readonly CleanupStatistics _statistics;
        private readonly string _metadataDirectory;
        private readonly string _quotaDirectory;
        
        /// <summary>
        /// System files that should be ignored when checking if a directory is empty.
        /// If a directory contains only these files, they will be deleted and the directory removed.
        /// </summary>
        private static readonly HashSet<string> _ignoredFilenames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Thumbs.db",
            ".DS_Store",
            "desktop.ini"
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="StorageCleanupService"/> class.
        /// </summary>
        public StorageCleanupService(
            MetadataRepository metadataRepository,
            DirectoryQuotaRepository quotaRepository,
            IFileSystem fileSystem,
            ILogger<StorageCleanupService> logger,
            string metadataDirectory,
            string quotaDirectory)
        {
            _metadataRepository = metadataRepository ?? throw new ArgumentNullException(nameof(metadataRepository));
            _quotaRepository = quotaRepository ?? throw new ArgumentNullException(nameof(quotaRepository));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _volumes = new ConcurrentDictionary<string, IStorageVolume>();
            _statistics = new CleanupStatistics();

            if (string.IsNullOrWhiteSpace(metadataDirectory))
                throw new ArgumentException("Metadata directory cannot be empty", nameof(metadataDirectory));
            if (string.IsNullOrWhiteSpace(quotaDirectory))
                throw new ArgumentException("Quota directory cannot be empty", nameof(quotaDirectory));

            _metadataDirectory = metadataDirectory;
            _quotaDirectory = quotaDirectory;
        }

        /// <summary>
        /// Registers a storage volume with the cleanup service.
        /// This should be called for each volume that needs cleanup support.
        /// </summary>
        /// <param name="volume">The storage volume to register.</param>
        /// <exception cref="ArgumentNullException">Thrown when volume is null.</exception>
        public void RegisterVolume(IStorageVolume volume)
        {
            if (volume == null)
                throw new ArgumentNullException(nameof(volume));

            _volumes.TryAdd(volume.VolumeId, volume);

            _logger.LogDebug("Registered volume {VolumeId} with cleanup service, mount path: {MountPath}",
                volume.VolumeId, volume.MountPath);
        }

        /// <inheritdoc/>
        public async Task CleanupEmptyDirectoriesAsync(ITenantContext tenant, CancellationToken ct)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            await CleanupEmptyDirectoriesAsync(tenant.TenantId, ct);
        }

        /// <inheritdoc/>
        public async Task CleanupEmptyDirectoriesAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            _logger.LogInformation("Starting junk file cleanup for tenant: {TenantId}", tenantId);

            var removedCount = 0;

            foreach (var volume in _volumes.Values)
            {
                var tenantPath = Path.Combine(volume.MountPath, tenantId);
                if (_fileSystem.Directory.Exists(tenantPath))
                {
                    // isProtectedRoot=true: never delete the tenant root directory itself.
                    removedCount += await CleanupJunkFilesRecursiveAsync(tenantPath, ct, isProtectedRoot: true);
                }
            }

            // Note: We still use the metric name EmptyDirectoriesRemoved for compatibility, 
            // but it now represents the number of junk files removed.
            _statistics.EmptyDirectoriesRemoved += removedCount;
            _logger.LogInformation("Cleaned up {Count} junk files for tenant {TenantId}", removedCount, tenantId);
        }

        /// <inheritdoc/>
        public async Task CleanupAllEmptyDirectoriesAsync(CancellationToken ct)
        {
            _logger.LogInformation("Starting junk file cleanup for all tenants");

            var removedCount = 0;

            foreach (var volume in _volumes.Values)
            {
                if (_fileSystem.Directory.Exists(volume.MountPath))
                {
                    var subdirectories = _fileSystem.Directory.GetDirectories(volume.MountPath);
                    foreach (var subdirectory in subdirectories)
                    {
                        // Each subdirectory is a tenant root — protect it from deletion.
                        removedCount += await CleanupJunkFilesRecursiveAsync(subdirectory, ct, isProtectedRoot: true);
                    }
                }
            }

            _statistics.EmptyDirectoriesRemoved += removedCount;
            _logger.LogInformation("Cleaned up {Count} junk files across all tenants", removedCount);
        }

        /// <inheritdoc/>
        public async Task CleanupPermanentlyFailedFilesAsync(TimeSpan olderThan, CancellationToken ct)
        {
            _logger.LogInformation("Starting cleanup of permanently failed files older than {TimeSpan}", olderThan);

            var allMetadata = await _metadataRepository.GetAllAsync(ct);
            var cutoffTime = DateTime.UtcNow - olderThan;
            var removedCount = 0;
            long spaceFreed = 0;

            foreach (var metadata in allMetadata)
            {
                if (metadata.Status == FileProcessingStatus.PermanentlyFailed &&
                    metadata.LastFailedAt.HasValue &&
                    metadata.LastFailedAt.Value < cutoffTime)
                {
                    if (_volumes.TryGetValue(metadata.VolumeId, out var volume))
                    {
                        try
                        {
                            if (_fileSystem.File.Exists(metadata.PhysicalPath))
                            {
                                spaceFreed += metadata.FileSize;
                                await volume.DeleteAsync(metadata.PhysicalPath, ct);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to delete physical file {PhysicalPath}", metadata.PhysicalPath);
                        }
                    }

                    await _metadataRepository.RemoveAsync(metadata.TenantId, metadata.FileKey, ct);
                    await _quotaRepository.DecrementAsync(metadata.TenantId, metadata.DirectoryPath, ct);

                    removedCount++;
                }
            }

            _statistics.PermanentlyFailedFilesRemoved += removedCount;
            _statistics.SpaceFreed += spaceFreed;
            _logger.LogInformation("Cleaned up {Count} permanently failed files, freed {Size} bytes",
                removedCount, spaceFreed);
        }

        /// <inheritdoc/>
        public async Task CleanupOrphanedFilesAsync(ITenantContext tenant, CancellationToken ct)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            _logger.LogInformation("Starting orphaned file cleanup for tenant: {TenantId}", tenant.TenantId);

            var removedCount = 0;
            long spaceFreed = 0;

            // Build a HashSet of known physical paths once — O(N) — so each file lookup is O(1)
            // instead of calling GetAllAsync inside the per-file loop (which would be O(P × N)).
            var allMetadata = await _metadataRepository.GetAllAsync(ct);
            var knownPaths = new HashSet<string>(
                allMetadata.Select(m => m.PhysicalPath).Where(p => !string.IsNullOrEmpty(p)),
                StringComparer.Ordinal);

            foreach (var volume in _volumes.Values)
            {
                var tenantPath = Path.Combine(volume.MountPath, tenant.TenantId);
                if (!_fileSystem.Directory.Exists(tenantPath))
                    continue;

                // Get all physical files for this tenant
                var physicalFiles = _fileSystem.Directory.GetFiles(tenantPath, "*", SearchOption.AllDirectories);

                foreach (var physicalPath in physicalFiles)
                {
                    // O(1) HashSet lookup instead of O(N) linear scan per file
                    if (!knownPaths.Contains(physicalPath))
                    {
                        // Orphaned file - delete it
                        try
                        {
                            var fileInfo = _fileSystem.FileInfo.New(physicalPath);
                            spaceFreed += fileInfo.Length;
                            _fileSystem.File.Delete(physicalPath);
                            removedCount++;
                            _logger.LogDebug("Deleted orphaned file: {PhysicalPath}", physicalPath);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to delete orphaned file {PhysicalPath}", physicalPath);
                        }
                    }
                }
            }

            _statistics.OrphanedFilesRemoved += removedCount;
            _statistics.SpaceFreed += spaceFreed;
            _logger.LogInformation("Cleaned up {Count} orphaned files for tenant {TenantId}, freed {Size} bytes",
                removedCount, tenant.TenantId, spaceFreed);
        }

        /// <inheritdoc/>
        public async Task CleanupTimedOutProcessingFilesAsync(TimeSpan timeout, CancellationToken ct)
        {
            _logger.LogInformation("Starting cleanup of timed-out processing files (timeout: {Timeout})", timeout);

            var allMetadata = await _metadataRepository.GetAllAsync(ct);
            var cutoffTime = DateTime.UtcNow - timeout;
            var resetCount = 0;

            foreach (var metadata in allMetadata)
            {
                if (metadata.Status == FileProcessingStatus.Processing &&
                    metadata.ProcessingStartTime.HasValue &&
                    metadata.ProcessingStartTime.Value < cutoffTime)
                {
                    // Reset to Pending status
                    metadata.Status = FileProcessingStatus.Pending;
                    metadata.ProcessingStartTime = null;
                    metadata.AvailableForProcessingAt = DateTime.UtcNow; // Available immediately

                    await _metadataRepository.AddOrUpdateAsync(metadata, ct);
                    resetCount++;

                    _logger.LogDebug("Reset timed-out file {FileKey} from Processing to Pending", metadata.FileKey);
                }
            }

            _statistics.TimedOutFilesReset += resetCount;
            _logger.LogInformation("Reset {Count} timed-out processing files to Pending", resetCount);
        }

        /// <inheritdoc/>
        public async Task CleanupFilesByStatusAsync(
            TimeSpan? processingTimeout,
            TimeSpan? failedRetentionPeriod,
            CancellationToken ct)
        {
            // Skip the GetAllAsync call entirely when nothing needs doing.
            if (processingTimeout == null && failedRetentionPeriod == null)
                return;

            _logger.LogInformation(
                "Starting combined file status cleanup (timeout={Timeout}, failedRetention={Failed})",
                processingTimeout, failedRetentionPeriod);

            var allMetadata = await _metadataRepository.GetAllAsync(ct);
            var now = DateTime.UtcNow;

            var timeoutCutoff = processingTimeout.HasValue     ? now - processingTimeout.Value     : (DateTime?)null;
            var failedCutoff  = failedRetentionPeriod.HasValue ? now - failedRetentionPeriod.Value : (DateTime?)null;

            int resetCount = 0, removedFailed = 0;
            long spaceFreed = 0;

            foreach (var metadata in allMetadata)
            {
                ct.ThrowIfCancellationRequested();

                if (metadata.Status == FileProcessingStatus.Processing
                    && timeoutCutoff.HasValue
                    && metadata.ProcessingStartTime.HasValue
                    && metadata.ProcessingStartTime.Value < timeoutCutoff.Value)
                {
                    metadata.Status = FileProcessingStatus.Pending;
                    metadata.ProcessingStartTime = null;
                    metadata.AvailableForProcessingAt = now;
                    await _metadataRepository.AddOrUpdateAsync(metadata, ct);
                    resetCount++;
                    _logger.LogDebug("Reset timed-out file {FileKey} from Processing to Pending", metadata.FileKey);
                }
                else if (metadata.Status == FileProcessingStatus.PermanentlyFailed
                    && failedCutoff.HasValue
                    && metadata.LastFailedAt.HasValue
                    && metadata.LastFailedAt.Value < failedCutoff.Value)
                {
                    if (_volumes.TryGetValue(metadata.VolumeId, out var volume))
                    {
                        try
                        {
                            if (_fileSystem.File.Exists(metadata.PhysicalPath))
                            {
                                spaceFreed += metadata.FileSize;
                                await volume.DeleteAsync(metadata.PhysicalPath, ct);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to delete physical file {PhysicalPath}", metadata.PhysicalPath);
                        }
                    }
                    await _metadataRepository.RemoveAsync(metadata.TenantId, metadata.FileKey, ct);
                    await _quotaRepository.DecrementAsync(metadata.TenantId, metadata.DirectoryPath, ct);
                    removedFailed++;
                }
            }

            _statistics.TimedOutFilesReset += resetCount;
            _statistics.PermanentlyFailedFilesRemoved += removedFailed;
            _statistics.SpaceFreed += spaceFreed;

            _logger.LogInformation(
                "Combined cleanup completed: {TimedOut} timed-out reset, {Failed} permanently failed removed, {SpaceFreed} bytes freed",
                resetCount, removedFailed, spaceFreed);
        }

        /// <inheritdoc/>
        public Task<CleanupStatistics> GetCleanupStatisticsAsync(CancellationToken ct)
        {
            return Task.FromResult(new CleanupStatistics
            {
                EmptyDirectoriesRemoved = _statistics.EmptyDirectoriesRemoved,
                CompletedRecordsRemoved = _statistics.CompletedRecordsRemoved,
                PermanentlyFailedFilesRemoved = _statistics.PermanentlyFailedFilesRemoved,
                OrphanedFilesRemoved = _statistics.OrphanedFilesRemoved,
                TimedOutFilesReset = _statistics.TimedOutFilesReset,
                SpaceFreed = _statistics.SpaceFreed
            });
        }

        /// <inheritdoc/>
        public async Task<DatabaseOptimizationResult> OptimizeDatabasesAsync(CancellationToken ct)
        {
            _logger.LogWarning("Starting database optimization. This operation can be time-consuming for large databases.");
            _logger.LogWarning("Tenant operations will be BLOCKED during optimization. Run during low-activity periods.");

            var result = new DatabaseOptimizationResult
            {
                MetadataDatabasesOptimized = 0,
                QuotaDatabasesOptimized = 0,
                SpaceReclaimed = 0,
                SizeBefore = 0,
                SizeAfter = 0
            };

            // Get all tenant IDs from metadata databases
            var metadataTenantIds = await _metadataRepository.GetAllTenantIdsAsync(ct);

            // Optimize metadata databases (per-tenant, thread-safe)
            foreach (var tenantId in metadataTenantIds)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    var (sizeBefore, sizeAfter) = await _metadataRepository.OptimizeDatabaseAsync(tenantId, ct);

                    if (sizeBefore > 0) // Only count if database exists
                    {
                        result.SizeBefore += sizeBefore;
                        result.SizeAfter += sizeAfter;
                        result.SpaceReclaimed += (sizeBefore - sizeAfter);
                        result.MetadataDatabasesOptimized++;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to optimize metadata database for tenant {TenantId}", tenantId);
                }
            }

            // Get all tenant IDs from quota databases
            var quotaTenantIds = await _quotaRepository.GetAllTenantIdsAsync(ct);

            // Optimize quota databases (per-tenant, thread-safe)
            foreach (var tenantId in quotaTenantIds)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    var (sizeBefore, sizeAfter) = await _quotaRepository.OptimizeDatabaseAsync(tenantId, ct);

                    if (sizeBefore > 0) // Only count if database exists
                    {
                        result.SizeBefore += sizeBefore;
                        result.SizeAfter += sizeAfter;
                        result.SpaceReclaimed += (sizeBefore - sizeAfter);
                        result.QuotaDatabasesOptimized++;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to optimize quota database for tenant {TenantId}", tenantId);
                }
            }

            _logger.LogInformation("Database optimization completed. Total databases: {Total}, Metadata: {Metadata}, Quota: {Quota}, Space reclaimed: {SpaceMB:F2} MB ({Percentage:F1}%)",
                result.MetadataDatabasesOptimized + result.QuotaDatabasesOptimized,
                result.MetadataDatabasesOptimized,
                result.QuotaDatabasesOptimized,
                result.SpaceReclaimedMB,
                result.PercentageReclaimed);

            return result;
        }

        /// <summary>
        /// Cleans up incorrectly created database files that were mistakenly identified as tenants.
        /// This includes files created from LiteDB backup files like "tenant-001.db-backup-1.db".
        /// WARNING: This is a one-time cleanup operation. Only run if you have backup files incorrectly treated as tenants.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of invalid database files removed and space freed in bytes.</returns>
        public async Task<(int FilesRemoved, long SpaceFreed)> CleanupInvalidDatabaseFilesAsync(CancellationToken ct)
        {
            var filesRemoved = 0;
            long spaceFreed = 0;

            _logger.LogInformation("Starting cleanup of invalid database files (backup files mistakenly treated as tenants)...");

            // Cleanup metadata databases
            if (_fileSystem.Directory.Exists(_metadataDirectory))
            {
                var metadataFiles = _fileSystem.Directory.GetFiles(_metadataDirectory, "*.db");
                foreach (var dbPath in metadataFiles)
                {
                    ct.ThrowIfCancellationRequested();

                    var fileName = _fileSystem.Path.GetFileNameWithoutExtension(dbPath);

                    // Check if this is an invalid database file (backup file treated as tenant)
                    if (IsInvalidDatabaseFile(fileName))
                    {
                        try
                        {
                            var fileSize = _fileSystem.FileInfo.New(dbPath).Length;
                            _fileSystem.File.Delete(dbPath);
                            filesRemoved++;
                            spaceFreed += fileSize;
                            _logger.LogInformation("Deleted invalid metadata database: {FileName} ({SizeKB:F2} KB)", fileName, fileSize / 1024.0);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Failed to delete invalid metadata database: {FileName}", fileName);
                        }
                    }
                }
            }

            // Cleanup quota databases
            if (_fileSystem.Directory.Exists(_quotaDirectory))
            {
                var quotaFiles = _fileSystem.Directory.GetFiles(_quotaDirectory, "*-quotas.db");
                foreach (var dbPath in quotaFiles)
                {
                    ct.ThrowIfCancellationRequested();

                    var fileName = _fileSystem.Path.GetFileNameWithoutExtension(dbPath);

                    // Check if this is an invalid database file (backup file treated as tenant)
                    if (IsInvalidDatabaseFile(fileName))
                    {
                        try
                        {
                            var fileSize = _fileSystem.FileInfo.New(dbPath).Length;
                            _fileSystem.File.Delete(dbPath);
                            filesRemoved++;
                            spaceFreed += fileSize;
                            _logger.LogInformation("Deleted invalid quota database: {FileName} ({SizeKB:F2} KB)", fileName, fileSize / 1024.0);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Failed to delete invalid quota database: {FileName}", fileName);
                        }
                    }
                }
            }

            // Also cleanup LiteDB temporary backup files (*.db-backup-*)
            var (metadataBackupFiles, metadataBackupSpace) = await CleanupLiteDbBackupFilesAsync(_metadataDirectory, ct);
            filesRemoved += metadataBackupFiles;
            spaceFreed += metadataBackupSpace;

            var (quotaBackupFiles, quotaBackupSpace) = await CleanupLiteDbBackupFilesAsync(_quotaDirectory, ct);
            filesRemoved += quotaBackupFiles;
            spaceFreed += quotaBackupSpace;

            _logger.LogInformation("Invalid database cleanup completed. Files removed: {FilesRemoved}, Space freed: {SpaceMB:F2} MB",
                filesRemoved, spaceFreed / 1024.0 / 1024.0);

            return (filesRemoved, spaceFreed);
        }

        /// <summary>
        /// Checks if a database file name represents an invalid database (backup file mistakenly treated as tenant).
        /// Examples: "tenant-001.db-backup-1", "tenant-001-quotas.db-backup-2", etc.
        /// </summary>
        private static bool IsInvalidDatabaseFile(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
                return false;

            // LiteDB backup pattern: contains "-backup"
            if (fileName.IndexOf("-backup", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;

            // Corruption backup pattern: contains ".corrupted."
            if (fileName.IndexOf(".corrupted.", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;

            // LiteDB journal files
            if (fileName.EndsWith("-journal", StringComparison.OrdinalIgnoreCase))
                return true;

            return false;
        }

        /// <summary>
        /// Cleans up LiteDB temporary backup files (*.db-backup-*, *.db.corrupted.*) in a directory.
        /// </summary>
        private async Task<(int FilesRemoved, long SpaceFreed)> CleanupLiteDbBackupFilesAsync(string directory, CancellationToken ct)
        {
            var filesRemoved = 0;
            long spaceFreed = 0;

            if (!_fileSystem.Directory.Exists(directory))
                return (0, 0);

            var allFiles = _fileSystem.Directory.GetFiles(directory);
            foreach (var filePath in allFiles)
            {
                ct.ThrowIfCancellationRequested();

                var fileName = _fileSystem.Path.GetFileName(filePath);

                // Match LiteDB backup patterns:
                // - *.db-backup-* (e.g., "tenant-001.db-backup-1")
                // - *.db.corrupted.* (e.g., "tenant-001.db.corrupted.20240122120000")
                if (fileName.IndexOf("-backup-", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    fileName.IndexOf(".corrupted.", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    try
                    {
                        var fileSize = _fileSystem.FileInfo.New(filePath).Length;
                        _fileSystem.File.Delete(filePath);
                        filesRemoved++;
                        spaceFreed += fileSize;
                        _logger.LogDebug("Deleted LiteDB backup file: {FileName} ({SizeKB:F2} KB)", fileName, fileSize / 1024.0);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete LiteDB backup file: {FileName}", fileName);
                    }
                }
            }

            return await Task.FromResult((filesRemoved, spaceFreed));
        }

        /// <summary>
        /// Recursively removes junk files and empty directories, bottom-up.
        /// A directory is considered empty when it contains no files (other than junk files that are
        /// deleted) and no subdirectories survive after recursive processing.
        /// The <paramref name="isProtectedRoot"/> flag prevents the top-level tenant or volume directory
        /// from being deleted even when it is empty — only its children are cleaned up.
        /// </summary>
        /// <returns>Number of directories deleted.</returns>
        private async Task<int> CleanupJunkFilesRecursiveAsync(
            string directoryPath,
            CancellationToken ct,
            bool isProtectedRoot = false)
        {
            if (!_fileSystem.Directory.Exists(directoryPath))
                return 0;

            var removedCount = 0;

            // 1. Process subdirectories first (bottom-up so parents can be emptied).
            var subdirectories = _fileSystem.Directory.GetDirectories(directoryPath);
            foreach (var subdirectory in subdirectories)
            {
                removedCount += await CleanupJunkFilesRecursiveAsync(subdirectory, ct, isProtectedRoot: false);
            }

            // 2. Delete junk files in the current directory.
            var files = _fileSystem.Directory.GetFiles(directoryPath);
            foreach (var file in files)
            {
                ct.ThrowIfCancellationRequested();
                var fileName = _fileSystem.Path.GetFileName(file);
                if (_ignoredFilenames.Contains(fileName))
                {
                    try
                    {
                        _fileSystem.File.Delete(file);
                        _logger.LogDebug("Deleted junk file: {FilePath}", file);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete junk file {FilePath}", file);
                    }
                }
            }

            // 3. Delete the directory itself if it is now empty — but never delete protected roots
            //    (volume mount paths, tenant root directories) to avoid recreating them on next write.
            if (!isProtectedRoot && IsDirectoryEmpty(directoryPath))
            {
                try
                {
                    _fileSystem.Directory.Delete(directoryPath);
                    removedCount++;
                    _logger.LogDebug("Deleted empty directory: {DirectoryPath}", directoryPath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to delete empty directory {DirectoryPath}", directoryPath);
                }
            }

            return removedCount;
        }

        /// <summary>
        /// Returns true when a directory contains no files and no subdirectories.
        /// </summary>
        private bool IsDirectoryEmpty(string directoryPath)
        {
            try
            {
                return !_fileSystem.Directory.GetFileSystemEntries(directoryPath).Any();
            }
            catch
            {
                return false;
            }
        }
    }
}
