using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Exceptions;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// File scheduler that manages concurrent file processing with retry logic.
    /// Ensures that different threads receive different files.
    /// </summary>
    public class FileScheduler : IFileScheduler
    {
        private const string CompleteDeleteFailedPrefix = "COMPLETE_DELETE_FAILED";
        private readonly MetadataRepository _repository;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<FileScheduler> _logger;
        private readonly FileRetryPolicy _retryPolicy;
        private readonly StorageVolumeRegistry? _volumeRegistry;
        private readonly ITenantQuotaManager? _tenantQuotaManager;
        private readonly IDirectoryQuotaManager? _directoryQuotaManager;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileScheduler"/> class.
        /// </summary>
        public FileScheduler(
            MetadataRepository repository,
            IFileSystem fileSystem,
            ILogger<FileScheduler> logger,
            FileRetryPolicy? retryPolicy = null,
            StorageVolumeRegistry? volumeRegistry = null,
            ITenantQuotaManager? tenantQuotaManager = null,
            IDirectoryQuotaManager? directoryQuotaManager = null)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _retryPolicy = retryPolicy ?? new FileRetryPolicy();
            _volumeRegistry = volumeRegistry;
            _tenantQuotaManager = tenantQuotaManager;
            _directoryQuotaManager = directoryQuotaManager;
        }

        /// <inheritdoc/>
        public async Task<FileLocation?> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            var metadata = await _repository.GetNextPendingFileAsync(tenant.TenantId, ct);

            if (metadata == null)
                return null;

            // NOTE: File.Exists is intentionally omitted from this hot path.
            // The cleanup service (CleanupOrphanedFilesAsync) periodically rebuilds metadata
            // for any orphaned physical files, so the in-memory cache stays consistent.
            // Doing a syscall on every allocation would add latency proportional to inode
            // lookup cost (significant on network volumes) and is not necessary at runtime.
            return MapToFileLocation(metadata);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<FileLocation>> GetNextBatchForProcessingAsync(
            ITenantContext tenant,
            int batchSize,
            CancellationToken ct)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (batchSize <= 0)
                throw new ArgumentException("Batch size must be greater than zero", nameof(batchSize));

            var metadataList = await _repository.GetNextPendingBatchAsync(tenant.TenantId, batchSize, ct);

            // NOTE: File.Exists is intentionally omitted — same rationale as GetNextFileForProcessingAsync.
            var locations = metadataList.Select(MapToFileLocation).ToList();

            if (locations.Count == 0)
                _logger.LogDebug("No pending files available for tenant: {TenantId}", tenant.TenantId);

            return locations;
        }

        /// <inheritdoc/>
        public async Task MarkAsProcessingAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Direct cross-tenant lookup — O(tenants) instead of O(total files)
            var metadata = await _repository.GetByFileKeyAsync(fileKey, ct);

            if (metadata == null)
            {
                throw new System.IO.FileNotFoundException($"File not found: {fileKey}");
            }

            if (metadata.Status == FileProcessingStatus.Processing)
            {
                throw new FileAlreadyProcessingException($"File is already being processed: {fileKey}");
            }

            if (metadata.Status != FileProcessingStatus.Pending)
            {
                throw new InvalidOperationException(
                    $"File {fileKey} cannot be marked as processing from status {metadata.Status}. Only Pending files can transition to Processing.");
            }

            // Clone before mutating — GetByFileKeyAsync returns a shared cache reference.
            // Mutating it in-place would expose a partial (inconsistent) state to concurrent readers.
            var updated = metadata.Clone();
            updated.Status = FileProcessingStatus.Processing;
            updated.ProcessingStartTime = DateTime.UtcNow;

            await _repository.AddOrUpdateAsync(updated, ct);

            _logger.LogDebug("Marked file as processing: {FileKey}", fileKey);
        }

        /// <inheritdoc/>
        public async Task MarkAsCompletedAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Direct cross-tenant lookup — O(tenants) instead of O(total files)
            var metadata = await _repository.GetByFileKeyAsync(fileKey, ct);

            if (metadata == null)
            {
                _logger.LogWarning("Attempted to mark non-existent file as completed: {FileKey}", fileKey);
                return;
            }

            // Delete physical file if it exists. If deletion fails for reasons other than
            // not-found, keep metadata and mark the file PermanentlyFailed so maintenance
            // cleanup can handle it and quota is not decremented.
            Exception? deleteException = null;
            if (!string.IsNullOrWhiteSpace(metadata.PhysicalPath))
            {
                try
                {
                    await DeletePhysicalFileAsync(metadata, ct);
                    _logger.LogDebug("Deleted physical file: {PhysicalPath}", metadata.PhysicalPath);
                }
                catch (Exception ex) when (ex is System.IO.FileNotFoundException || ex is System.IO.DirectoryNotFoundException)
                {
                    // File already gone — idempotent, proceed to metadata removal
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to delete physical file: {PhysicalPath}", metadata.PhysicalPath);
                    deleteException = ex;
                }
            }

            if (deleteException != null)
            {
                // Clone before mutating so readers never see partial state.
                var updated = metadata.Clone();
                updated.Status = FileProcessingStatus.PermanentlyFailed;
                updated.LastFailedAt = DateTime.UtcNow;
                updated.ProcessingStartTime = null;
                updated.AvailableForProcessingAt = null;
                updated.LastError = $"{CompleteDeleteFailedPrefix}: {deleteException.GetType().Name}: {deleteException.Message}";

                await _repository.AddOrUpdateAsync(updated, ct);

                _logger.LogWarning(
                    "Marked file as PermanentlyFailed because physical delete failed: {FileKey}, Path: {PhysicalPath}",
                    fileKey, metadata.PhysicalPath);

                throw new IOException($"Failed to delete physical file for completion: {fileKey}", deleteException);
            }

            // Remove metadata (Write-Behind: memory removed immediately, SQLite delete queued)
            await _repository.RemoveAsync(metadata.TenantId, fileKey, ct);

            _logger.LogDebug("Completed and deleted file: {FileKey}", fileKey);
        }

        /// <inheritdoc/>
        public async Task MarkAsFailedAsync(string fileKey, string errorMessage, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Direct cross-tenant lookup — O(tenants) instead of O(total files)
            var metadata = await _repository.GetByFileKeyAsync(fileKey, ct);

            if (metadata == null)
            {
                _logger.LogWarning("Attempted to mark non-existent file as failed: {FileKey}", fileKey);
                return;
            }

            // Clone before mutating — GetByFileKeyAsync returns a shared cache reference.
            var updated = metadata.Clone();
            updated.RetryCount++;
            updated.LastError = errorMessage;
            updated.LastFailedAt = DateTime.UtcNow;
            updated.ProcessingStartTime = null;

            if (updated.RetryCount >= _retryPolicy.MaxRetryCount)
            {
                // Exceeded max retries, mark as permanently failed
                updated.Status = FileProcessingStatus.PermanentlyFailed;
                updated.AvailableForProcessingAt = null;

                _logger.LogError("File permanently failed after {RetryCount} retries: {FileKey}, Error: {Error}",
                    updated.RetryCount, fileKey, errorMessage);
            }
            else
            {
                // Return to pending status for retry
                updated.Status = FileProcessingStatus.Pending;

                // Calculate retry delay with exponential backoff
                var delay = CalculateRetryDelay(updated.RetryCount);
                updated.AvailableForProcessingAt = DateTime.UtcNow.Add(delay);

                _logger.LogWarning("File marked as failed (retry {RetryCount}/{MaxRetries}), will retry after {Delay}: {FileKey}, Error: {Error}",
                    updated.RetryCount, _retryPolicy.MaxRetryCount, delay, fileKey, errorMessage);
            }

            await _repository.AddOrUpdateAsync(updated, ct);
        }

        /// <inheritdoc/>
        public async Task ResetProcessingStatusAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Direct cross-tenant lookup — O(tenants) instead of O(total files)
            var metadata = await _repository.GetByFileKeyAsync(fileKey, ct);

            if (metadata == null)
            {
                _logger.LogWarning("Attempted to reset non-existent file: {FileKey}", fileKey);
                return;
            }

            // Clone before mutating — GetByFileKeyAsync returns a shared cache reference.
            var updated = metadata.Clone();
            updated.Status = FileProcessingStatus.Pending;
            updated.ProcessingStartTime = null;
            updated.AvailableForProcessingAt = null;
            updated.RetryCount = 0;
            updated.LastError = null;
            updated.LastFailedAt = null;

            await _repository.AddOrUpdateAsync(updated, ct);

            _logger.LogInformation("Reset processing status for file: {FileKey}", fileKey);
        }

        /// <inheritdoc/>
        public async Task<FileProcessingStatus> GetFileStatusAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Direct cross-tenant lookup — O(tenants) instead of O(total files)
            var metadata = await _repository.GetByFileKeyAsync(fileKey, ct);

            if (metadata == null)
            {
                throw new System.IO.FileNotFoundException($"File not found: {fileKey}");
            }

            return metadata.Status;
        }

        /// <summary>
        /// Calculates the retry delay using exponential backoff.
        /// </summary>
        private TimeSpan CalculateRetryDelay(int retryCount)
        {
            if (!_retryPolicy.UseExponentialBackoff)
            {
                return _retryPolicy.InitialRetryDelay;
            }

            // Cap the exponent before computing to prevent double.PositiveInfinity.
            // 2^62 ≈ 4.6e18 ms — far above any practical MaxRetryDelay, so the clamp below
            // catches it. TimeSpan.FromMilliseconds(Infinity) throws OverflowException, which
            // would escape the caller without this guard.
            const int maxExponent = 62;
            var exponent = Math.Max(0, Math.Min(retryCount - 1, maxExponent));
            var delayMs = _retryPolicy.InitialRetryDelay.TotalMilliseconds * Math.Pow(2, exponent);

            // Apply MaxRetryDelay cap using double comparison to stay safe before constructing TimeSpan.
            var maxMs = _retryPolicy.MaxRetryDelay.TotalMilliseconds;
            return TimeSpan.FromMilliseconds(Math.Min(delayMs, maxMs));
        }

        /// <inheritdoc/>
        public async Task<int> CleanupOrphanedMetadataAsync(CancellationToken ct)
        {
            var removedCount = 0;

            // Process one tenant at a time to avoid allocating a single list of all files
            // across every tenant (O(total_files) memory spike → O(max_tenant_files) peak).
            foreach (var tenantId in _repository.GetActiveTenantIds())
            {
                ct.ThrowIfCancellationRequested();

                var tenantMetadata = await _repository.GetByTenantAsync(tenantId, ct);

                foreach (var metadata in tenantMetadata)
                {
                    // Check if physical file exists
                    if (!string.IsNullOrWhiteSpace(metadata.PhysicalPath) &&
                        !_fileSystem.File.Exists(metadata.PhysicalPath))
                    {
                        _logger.LogInformation("Removing orphaned metadata for missing file: {FileKey}, Path: {PhysicalPath}",
                            metadata.FileKey, metadata.PhysicalPath);

                        // Decrement quota counters before removing metadata so they stay consistent.
                        // Use CancellationToken.None for the full cleanup sequence once a candidate
                        // orphan has been selected; otherwise a mid-flight cancellation could leave
                        // quotas decremented while the metadata row remains present.
                        if (_tenantQuotaManager != null)
                        {
                            try
                            {
                                await _tenantQuotaManager.DecrementFileCountAsync(metadata.TenantId, CancellationToken.None);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Failed to decrement tenant quota for orphaned file: {FileKey}", metadata.FileKey);
                            }
                        }

                        if (_directoryQuotaManager != null && !string.IsNullOrWhiteSpace(metadata.DirectoryPath))
                        {
                            try
                            {
                                await _directoryQuotaManager.DecrementFileCountAsync(metadata.TenantId, metadata.DirectoryPath, CancellationToken.None);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Failed to decrement directory quota for orphaned file: {FileKey}", metadata.FileKey);
                            }
                        }

                        await _repository.RemoveAsync(metadata.TenantId, metadata.FileKey, CancellationToken.None);
                        removedCount++;
                    }
                }
            }

            if (removedCount > 0)
            {
                _logger.LogInformation("Cleaned up {Count} orphaned metadata records", removedCount);
            }

            return removedCount;
        }

        /// <summary>
        /// Maps FileMetadata to FileLocation.
        /// </summary>
        private FileLocation MapToFileLocation(FileMetadata metadata)
        {
            return new FileLocation
            {
                FileKey = metadata.FileKey,
                TenantId = metadata.TenantId,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                CreatedAt = metadata.CreatedAt,
                Status = metadata.Status,
                RetryCount = metadata.RetryCount,
                LastFailedAt = metadata.LastFailedAt,
                LastError = metadata.LastError
            };
        }

        private Task DeletePhysicalFileAsync(FileMetadata metadata, CancellationToken ct)
        {
            if (_volumeRegistry != null
                && !string.IsNullOrWhiteSpace(metadata.VolumeId)
                && _volumeRegistry.TryGetVolume(metadata.VolumeId, out var volume))
            {
                return volume.DeleteAsync(metadata.PhysicalPath, ct);
            }

            _fileSystem.File.Delete(metadata.PhysicalPath);
            return Task.CompletedTask;
        }
    }
}
