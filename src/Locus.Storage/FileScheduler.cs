using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Runtime.InteropServices;
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
        private static readonly StringComparer PhysicalPathComparer =
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal;
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
        public async Task<FileLocation?> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            var metadata = await _repository.GetNextPendingFileAsync(tenant.TenantId, ct);

            if (metadata == null)
                return null;

            // NOTE: File.Exists is intentionally omitted from this hot path.
            // The recovery service (RecoverOrphanedFilesAsync) periodically rebuilds metadata
            // for any orphaned physical files, so the in-memory cache stays consistent.
            // Doing a syscall on every allocation would add latency proportional to inode
            // lookup cost (significant on network volumes) and is not necessary at runtime.
            return MapToFileLocation(metadata);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<FileLocation>> GetNextBatchForProcessingAsync(
            ITenantContext tenant,
            int batchSize,
            CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (batchSize <= 0)
                throw new ArgumentException("Batch size must be greater than zero", nameof(batchSize));

            var metadataList = await _repository.GetNextPendingBatchAsync(tenant.TenantId, batchSize, ct);

            // NOTE: File.Exists is intentionally omitted - same rationale as GetNextFileForProcessingAsync.
            var locations = metadataList.Select(MapToFileLocation).ToList();

            if (locations.Count == 0)
                _logger.LogDebug("No pending files available for tenant: {TenantId}", tenant.TenantId);

            return locations;
        }

        /// <inheritdoc/>
        public async Task MarkAsProcessingAsync(string fileKey, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (!_repository.TryResolveTenantIdByFileKey(fileKey, out var tenantId))
                throw new System.IO.FileNotFoundException($"File not found: {fileKey}");

            const int maxAttempts = 3;
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                var updated = await _repository.TryMarkPendingFileAsProcessingAsync(
                    tenantId,
                    fileKey,
                    DateTime.UtcNow,
                    ct);
                if (updated != null)
                {
                    _logger.LogDebug("Marked file as processing: {FileKey}", fileKey);
                    return;
                }
            }

            var latest = await _repository.GetByFileKeyAsync(fileKey, ct);
            if (latest == null)
                throw new System.IO.FileNotFoundException($"File not found: {fileKey}");

            if (latest.Status == FileProcessingStatus.Processing)
                throw new FileAlreadyProcessingException($"File is already being processed: {fileKey}");

            throw new InvalidOperationException(
                $"File {fileKey} changed state concurrently and is now {latest.Status}.");
        }

        /// <inheritdoc/>
        public async Task MarkAsCompletedAsync(string fileKey, DateTime expectedProcessingStartTimeUtc, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (!_repository.TryResolveTenantIdByFileKey(fileKey, out var tenantId))
            {
                _logger.LogWarning("Attempted to mark non-existent file as completed: {FileKey}", fileKey);
                return;
            }

            var removedMetadata = await _repository.TryRemoveProcessingFileAsync(
                tenantId,
                fileKey,
                expectedProcessingStartTimeUtc,
                ct);
            var metadata = removedMetadata == null
                ? await _repository.GetByFileKeyAsync(fileKey, ct)
                : null;
            if (removedMetadata == null)
                throw CreateLeaseMismatchException(fileKey, expectedProcessingStartTimeUtc, metadata);

            // Delete physical file if it exists. If deletion fails for reasons other than
            // not-found, recreate metadata as PermanentlyFailed so maintenance cleanup can handle it.
            Exception? deleteException = null;
            if (!string.IsNullOrWhiteSpace(removedMetadata.PhysicalPath))
            {
                try
                {
                    await DeletePhysicalFileAsync(removedMetadata, ct);
                    _logger.LogDebug("Deleted physical file: {PhysicalPath}", removedMetadata.PhysicalPath);
                }
                catch (Exception ex) when (ex is System.IO.FileNotFoundException || ex is System.IO.DirectoryNotFoundException)
                {
                    // File already gone - idempotent, proceed to metadata removal
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to delete physical file: {PhysicalPath}", removedMetadata.PhysicalPath);
                    deleteException = ex;
                }
            }

            if (deleteException != null)
            {
                // Clone before mutating so readers never see partial state.
                var updated = removedMetadata.Clone();
                updated.Status = FileProcessingStatus.PermanentlyFailed;
                updated.LastFailedAt = DateTime.UtcNow;
                updated.ProcessingStartTime = null;
                updated.AvailableForProcessingAt = null;
                updated.LastError = $"{CompleteDeleteFailedPrefix}: {deleteException.GetType().Name}: {deleteException.Message}";

                await _repository.AddOrUpdateAsync(updated, ct);

                _logger.LogWarning(
                    "Marked file as PermanentlyFailed because physical delete failed: {FileKey}, Path: {PhysicalPath}",
                    fileKey, removedMetadata.PhysicalPath);

                throw new IOException($"Failed to delete physical file for completion: {fileKey}", deleteException);
            }

            _logger.LogDebug("Completed and deleted file: {FileKey}", fileKey);
        }

        /// <inheritdoc/>
        public async Task MarkAsFailedAsync(string fileKey, DateTime expectedProcessingStartTimeUtc, string errorMessage, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (!_repository.TryResolveTenantIdByFileKey(fileKey, out var tenantId))
            {
                _logger.LogWarning("Attempted to mark non-existent file as failed: {FileKey}", fileKey);
                return;
            }

            var updated = await _repository.TryUpdateProcessingFileAsync(
                tenantId,
                fileKey,
                expectedProcessingStartTimeUtc,
                current =>
                {
                    current.RetryCount++;
                    current.LastError = errorMessage;
                    current.LastFailedAt = DateTime.UtcNow;
                    current.ProcessingStartTime = null;

                    if (current.RetryCount >= _retryPolicy.MaxRetryCount)
                    {
                        current.Status = FileProcessingStatus.PermanentlyFailed;
                        current.AvailableForProcessingAt = null;
                    }
                    else
                    {
                        current.Status = FileProcessingStatus.Pending;
                        current.AvailableForProcessingAt = DateTime.UtcNow.Add(CalculateRetryDelay(current.RetryCount));
                    }

                    return current;
                },
                ct);
            var metadata = updated == null
                ? await _repository.GetByFileKeyAsync(fileKey, ct)
                : null;
            if (updated == null)
                throw CreateLeaseMismatchException(fileKey, expectedProcessingStartTimeUtc, metadata);

            if (updated.Status == FileProcessingStatus.PermanentlyFailed)
            {
                _logger.LogError("File permanently failed after {RetryCount} retries: {FileKey}, Error: {Error}",
                    updated.RetryCount, fileKey, errorMessage);
            }
            else
            {
                var delay = updated.AvailableForProcessingAt!.Value - DateTime.UtcNow;
                _logger.LogWarning("File marked as failed (retry {RetryCount}/{MaxRetries}), will retry after {Delay}: {FileKey}, Error: {Error}",
                    updated.RetryCount, _retryPolicy.MaxRetryCount, delay, fileKey, errorMessage);
            }
        }

        /// <inheritdoc/>
        public async Task ResetProcessingStatusAsync(string fileKey, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (!_repository.TryResolveTenantIdByFileKey(fileKey, out var tenantId))
            {
                _logger.LogWarning("Attempted to reset non-existent file: {FileKey}", fileKey);
                return;
            }

            const int maxAttempts = 3;
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                var updated = await _repository.TryResetFileToPendingAsync(tenantId, fileKey, ct);
                if (updated == null)
                    continue;

                if (updated.Status == FileProcessingStatus.Processing)
                    throw new FileAlreadyProcessingException($"File is already being processed: {fileKey}");

                _logger.LogInformation("Reset processing status for file: {FileKey}", fileKey);
                return;
            }

            var latest = await _repository.GetByFileKeyAsync(fileKey, ct);
            if (latest == null)
            {
                _logger.LogWarning("Attempted to reset non-existent file: {FileKey}", fileKey);
                return;
            }

            if (latest.Status == FileProcessingStatus.Processing)
                throw new FileAlreadyProcessingException($"File is already being processed: {fileKey}");

            throw new InvalidOperationException(
                $"File {fileKey} changed state concurrently and is now {latest.Status}.");
        }

        /// <inheritdoc/>
        public async Task<FileProcessingStatus> GetFileStatusAsync(string fileKey, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

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
            // 2^62 is about 4.6e18 ms - far above any practical MaxRetryDelay, so the clamp below
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
        public async Task<int> CleanupOrphanedMetadataAsync(CancellationToken ct = default)
        {
            var removedCount = 0;
            var tenantIds = await _repository.GetAllTenantIdsAsync(ct).ConfigureAwait(false);

            // Process one tenant at a time to avoid allocating a single list of all files
            // across every tenant (O(total_files) memory spike -> O(max_tenant_files) peak).
            // EnumerateTenantMetadataRaw returns the live cache values without cloning - we
            // only read stable fields (PhysicalPath, TenantId, FileKey) so this is safe.
            // Use all known tenant IDs, not only hot in-memory tenants, so cold SQLite-only
            // tenants also have stale metadata reconciled before they are leased again.
            foreach (var tenantId in tenantIds)
            {
                ct.ThrowIfCancellationRequested();

                var tenantMetadata = _repository.SnapshotTenantMetadataRaw(tenantId);
                var physicalPathExistsCache = new Dictionary<string, bool>(PhysicalPathComparer);

                foreach (var metadata in tenantMetadata)
                {
                    if (string.IsNullOrWhiteSpace(metadata.PhysicalPath))
                        continue;

                    if (!TryConfirmPhysicalFileMissing(metadata, physicalPathExistsCache, out var physicalFileMissing))
                        continue;

                    if (physicalFileMissing)
                    {
                        _logger.LogInformation("Removing orphaned metadata for missing file: {FileKey}, Path: {PhysicalPath}",
                            metadata.FileKey, metadata.PhysicalPath);

                        // Use default for the full cleanup sequence once a candidate orphan has
                        // been selected; otherwise a mid-flight cancellation could leave cleanup
                        // work half-applied.
                        var removed = await _repository.RemoveAsync(metadata.TenantId, metadata.FileKey, default);
                        if (!removed)
                        {
                            _logger.LogDebug(
                                "Skipped orphaned metadata cleanup for {FileKey} because the metadata row changed concurrently",
                                metadata.FileKey);
                            continue;
                        }

                        if (_tenantQuotaManager != null)
                        {
                            try
                            {
                                await _tenantQuotaManager.DecrementFileCountAsync(metadata.TenantId, default);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Failed to decrement tenant quota for orphaned file: {FileKey}", metadata.FileKey);
                            }
                        }

                        if (_directoryQuotaManager != null)
                        {
                            try
                            {
                                var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(metadata.DirectoryPath);
                                await _directoryQuotaManager.DecrementFileCountAsync(
                                    metadata.TenantId,
                                    normalizedDirectoryPath,
                                    default);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Failed to decrement directory quota for orphaned file: {FileKey}", metadata.FileKey);
                            }
                        }

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

        private bool TryConfirmPhysicalFileMissing(
            FileMetadata metadata,
            IDictionary<string, bool> physicalPathExistsCache,
            out bool physicalFileMissing)
        {
            physicalFileMissing = false;

            if (physicalPathExistsCache.TryGetValue(metadata.PhysicalPath, out var physicalFileExists))
            {
                physicalFileMissing = !physicalFileExists;
                return true;
            }

            physicalFileExists = _fileSystem.File.Exists(metadata.PhysicalPath);
            if (physicalFileExists)
            {
                physicalPathExistsCache[metadata.PhysicalPath] = true;
                return true;
            }

            if (_volumeRegistry != null
                && !string.IsNullOrWhiteSpace(metadata.VolumeId)
                && !_volumeRegistry.TryGetVolume(metadata.VolumeId, out _))
            {
                _logger.LogWarning(
                    "Skipping orphaned metadata cleanup because volume {VolumeId} is unavailable for file {FileKey}",
                    metadata.VolumeId,
                    metadata.FileKey);
                return false;
            }

            try
            {
                using (var stream = _fileSystem.File.Open(
                    metadata.PhysicalPath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete))
                {
                }

                physicalPathExistsCache[metadata.PhysicalPath] = true;
                return true;
            }
            catch (System.IO.FileNotFoundException)
            {
                physicalPathExistsCache[metadata.PhysicalPath] = false;
                physicalFileMissing = true;
                return true;
            }
            catch (System.IO.DirectoryNotFoundException)
            {
                physicalPathExistsCache[metadata.PhysicalPath] = false;
                physicalFileMissing = true;
                return true;
            }
            catch (UnauthorizedAccessException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Skipping orphaned metadata cleanup because physical-path access could not be verified for {FileKey}",
                    metadata.FileKey);
                return false;
            }
            catch (IOException ex)
            {
                _logger.LogWarning(
                    ex,
                    "Skipping orphaned metadata cleanup because physical-path access could not be verified for {FileKey}",
                    metadata.FileKey);
                return false;
            }
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
                LastError = metadata.LastError,
                ProcessingStartTime = metadata.ProcessingStartTime
            };
        }

        private static FileProcessingLeaseMismatchException CreateLeaseMismatchException(
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            FileMetadata? currentMetadata)
        {
            return new FileProcessingLeaseMismatchException(
                fileKey,
                expectedProcessingStartTimeUtc,
                currentMetadata?.Status == FileProcessingStatus.Processing
                    ? currentMetadata.ProcessingStartTime
                    : null);
        }

        private Task DeletePhysicalFileAsync(FileMetadata metadata, CancellationToken ct = default)
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
