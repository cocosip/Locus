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
            _logger.LogDebug("Registered volume {VolumeId} with cleanup service", volume.VolumeId);
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

            _logger.LogInformation("Starting empty directory cleanup for tenant: {TenantId}", tenantId);

            var removedCount = 0;

            foreach (var volume in _volumes.Values)
            {
                var tenantPath = Path.Combine(volume.MountPath, tenantId);
                if (_fileSystem.Directory.Exists(tenantPath))
                {
                    removedCount += await CleanupEmptyDirectoriesRecursiveAsync(tenantPath, ct);
                }
            }

            _statistics.EmptyDirectoriesRemoved += removedCount;
            _logger.LogInformation("Cleaned up {Count} empty directories for tenant {TenantId}", removedCount, tenantId);
        }

        /// <inheritdoc/>
        public async Task CleanupAllEmptyDirectoriesAsync(CancellationToken ct)
        {
            _logger.LogInformation("Starting empty directory cleanup for all tenants");

            var removedCount = 0;

            foreach (var volume in _volumes.Values)
            {
                if (_fileSystem.Directory.Exists(volume.MountPath))
                {
                    removedCount += await CleanupEmptyDirectoriesRecursiveAsync(volume.MountPath, ct);
                }
            }

            _statistics.EmptyDirectoriesRemoved += removedCount;
            _logger.LogInformation("Cleaned up {Count} empty directories across all tenants", removedCount);
        }

        /// <inheritdoc/>
        public async Task CleanupCompletedFileRecordsAsync(TimeSpan olderThan, CancellationToken ct)
        {
            _logger.LogInformation("Starting cleanup of completed file records older than {TimeSpan}", olderThan);

            var allMetadata = await _metadataRepository.GetAllAsync(ct);
            var cutoffTime = DateTime.UtcNow - olderThan;
            var removedCount = 0;

            foreach (var metadata in allMetadata)
            {
                if (metadata.Status == FileProcessingStatus.Completed &&
                    metadata.CreatedAt < cutoffTime)
                {
                    await _metadataRepository.RemoveAsync(metadata.TenantId, metadata.FileKey, ct);
                    removedCount++;
                }
            }

            _statistics.CompletedRecordsRemoved += removedCount;
            _logger.LogInformation("Cleaned up {Count} completed file records", removedCount);
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
                    // Delete physical file
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

                    // Remove metadata
                    await _metadataRepository.RemoveAsync(metadata.TenantId, metadata.FileKey, ct);

                    // Decrement quota
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

            foreach (var volume in _volumes.Values)
            {
                var tenantPath = Path.Combine(volume.MountPath, tenant.TenantId);
                if (!_fileSystem.Directory.Exists(tenantPath))
                    continue;

                // Get all physical files for this tenant
                var physicalFiles = _fileSystem.Directory.GetFiles(tenantPath, "*", SearchOption.AllDirectories);

                foreach (var physicalPath in physicalFiles)
                {
                    // Check if metadata exists
                    var allMetadata = await _metadataRepository.GetAllAsync(ct);
                    var hasMetadata = allMetadata.Any(m => m.PhysicalPath == physicalPath);

                    if (!hasMetadata)
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
        /// Recursively cleans up empty directories.
        /// </summary>
        private async Task<int> CleanupEmptyDirectoriesRecursiveAsync(string directoryPath, CancellationToken ct)
        {
            if (!_fileSystem.Directory.Exists(directoryPath))
                return 0;

            var removedCount = 0;

            // First, recursively clean up subdirectories
            var subdirectories = _fileSystem.Directory.GetDirectories(directoryPath);
            foreach (var subdirectory in subdirectories)
            {
                removedCount += await CleanupEmptyDirectoriesRecursiveAsync(subdirectory, ct);
            }

            // Then check if this directory is now empty
            var hasFiles = _fileSystem.Directory.GetFiles(directoryPath).Any();
            var hasSubdirs = _fileSystem.Directory.GetDirectories(directoryPath).Any();

            if (!hasFiles && !hasSubdirs)
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
    }
}
