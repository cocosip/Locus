using System;
using System.Collections.Concurrent;
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
    public class FileScheduler : IFileScheduler, IQueueEventManagedFileScheduler
    {
        private static readonly StringComparer PhysicalPathComparer =
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal;
        private readonly IQueueProjectionStore _projectionStore;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<FileScheduler> _logger;
        private readonly FileRetryPolicy _retryPolicy;
        private readonly StorageVolumeRegistry? _volumeRegistry;
        private readonly ITenantQuotaManager? _tenantQuotaManager;
        private readonly IDirectoryQuotaManager? _directoryQuotaManager;
        private readonly IQueueEventJournal? _queueEventJournal;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _transitionGuards;
        private readonly bool _allowLegacyNonJournalMode;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileScheduler"/> class.
        /// </summary>
        public FileScheduler(
            IQueueProjectionStore projectionStore,
            IFileSystem fileSystem,
            ILogger<FileScheduler> logger,
            FileRetryPolicy? retryPolicy = null,
            StorageVolumeRegistry? volumeRegistry = null,
            ITenantQuotaManager? tenantQuotaManager = null,
            IDirectoryQuotaManager? directoryQuotaManager = null,
            IQueueEventJournal? queueEventJournal = null,
            bool allowLegacyNonJournalMode = false)
        {
            _projectionStore = projectionStore ?? throw new ArgumentNullException(nameof(projectionStore));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _retryPolicy = retryPolicy ?? new FileRetryPolicy();
            _volumeRegistry = volumeRegistry;
            _tenantQuotaManager = tenantQuotaManager;
            _directoryQuotaManager = directoryQuotaManager;
            _queueEventJournal = queueEventJournal;
            _allowLegacyNonJournalMode = allowLegacyNonJournalMode;
            _transitionGuards = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.Ordinal);
            ValidateLegacyNonJournalMode();
        }

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
            IDirectoryQuotaManager? directoryQuotaManager = null,
            IQueueEventJournal? queueEventJournal = null,
            IQueueProjectionStore? projectionStore = null,
            bool allowLegacyNonJournalMode = false)
            : this(
                projectionStore ?? new MetadataRepositoryQueueProjectionStore(repository ?? throw new ArgumentNullException(nameof(repository))),
                fileSystem,
                logger,
                retryPolicy,
                volumeRegistry,
                tenantQuotaManager,
                directoryQuotaManager,
                queueEventJournal,
                allowLegacyNonJournalMode)
        {
        }

        /// <inheritdoc/>
        public bool HandlesQueueJournal => _queueEventJournal != null;

        private void ValidateLegacyNonJournalMode()
        {
            if (_queueEventJournal != null)
                return;

            if (!_allowLegacyNonJournalMode)
            {
                throw new InvalidOperationException(
                    "FileScheduler requires IQueueEventJournal unless legacy non-journal mode is explicitly allowed.");
            }

            _logger.LogWarning(
                "FileScheduler is running without IQueueEventJournal. This legacy non-journal mode should only be used for explicit compatibility or tests.");
        }

        /// <inheritdoc/>
        public async Task<FileLocation?> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            var metadata = await _projectionStore.LeaseNextPendingFileAsync(tenant.TenantId, ct).ConfigureAwait(false);

            if (metadata == null)
                return null;

            if (_queueEventJournal != null)
            {
                try
                {
                    await _queueEventJournal.AppendAsync(CreateProcessingStartedEvent(metadata), ct).ConfigureAwait(false);
                }
                catch
                {
                    await TryRollbackLeasedFilesAsync(new[] { metadata }).ConfigureAwait(false);
                    throw;
                }
            }

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

            var metadataList = (await _projectionStore.LeaseNextPendingBatchAsync(tenant.TenantId, batchSize, ct).ConfigureAwait(false)).ToList();

            if (_queueEventJournal != null && metadataList.Count > 0)
            {
                try
                {
                    await _queueEventJournal.AppendBatchAsync(
                        metadataList.Select(CreateProcessingStartedEvent).ToArray(),
                        ct).ConfigureAwait(false);
                }
                catch
                {
                    await TryRollbackLeasedFilesAsync(
                        metadataList).ConfigureAwait(false);
                    throw;
                }
            }

            // NOTE: File.Exists is intentionally omitted - same rationale as GetNextFileForProcessingAsync.
            var locations = metadataList.Select(MapToFileLocation).ToList();

            if (locations.Count == 0)
                _logger.LogDebug("No pending files available for tenant: {TenantId}", tenant.TenantId);

            return locations;
        }

        /// <inheritdoc/>
        public async Task MarkAsProcessingAsync(string tenantId, string fileKey, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            const int maxAttempts = 3;
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                var updated = await _projectionStore.TryMarkProjectedPendingFileAsProcessingAsync(
                    tenantId,
                    fileKey,
                    DateTime.UtcNow,
                    ct).ConfigureAwait(false);
                if (updated != null)
                {
                    _logger.LogDebug("Marked file as processing: {FileKey}", fileKey);
                    return;
                }
            }

            var latest = await _projectionStore.GetProjectedFileAsync(tenantId, fileKey, ct).ConfigureAwait(false);
            if (latest == null)
                throw new System.IO.FileNotFoundException($"File not found: {fileKey}");

            if (latest.Status == FileProcessingStatus.Processing)
                throw new FileAlreadyProcessingException($"File is already being processed: {fileKey}");

            if (latest.Status == FileProcessingStatus.Pending
                && latest.AvailableForProcessingAt.HasValue
                && latest.AvailableForProcessingAt.Value > DateTime.UtcNow)
            {
                throw new InvalidOperationException(
                    $"File {fileKey} is not available for processing until {latest.AvailableForProcessingAt.Value:O}.");
            }

            throw new InvalidOperationException(
                $"File {fileKey} changed state concurrently and is now {latest.Status}.");
        }

        /// <inheritdoc/>
        public async Task MarkAsCompletedAsync(FileProcessingLease lease, CancellationToken ct = default)
        {
            ValidateLease(lease);

            SemaphoreSlim? transitionGuard = null;
            if (_queueEventJournal != null)
            {
                transitionGuard = GetTransitionGuard(lease.FileKey);
                await transitionGuard.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    var current = await _projectionStore.GetProjectedFileAsync(lease.TenantId, lease.FileKey, ct).ConfigureAwait(false);
                    if (current == null)
                        return;

                    if (IsCompletionCommittedStatus(current.Status))
                        return;

                    if (current.Status != FileProcessingStatus.Processing
                        || !current.ProcessingStartTime.HasValue
                        || current.ProcessingStartTime.Value != lease.ProcessingStartTimeUtc)
                    {
                        throw CreateLeaseMismatchException(lease.FileKey, lease.ProcessingStartTimeUtc, current);
                    }

                    var completedAtUtc = DateTime.UtcNow;
                    await _queueEventJournal.AppendBatchAsync(
                        new[]
                        {
                            CreateProcessingCompletedEvent(current, lease.ProcessingStartTimeUtc, completedAtUtc),
                            CreateDeleteRequestedEvent(current, completedAtUtc),
                        },
                        ct).ConfigureAwait(false);
                }
                catch
                {
                    transitionGuard.Release();
                    throw;
                }
            }

            try
            {
                var completedAtUtc = DateTime.UtcNow;
                var updated = await _projectionStore.TryUpdateProjectedProcessingFileAsync(
                    lease.TenantId,
                    lease.FileKey,
                    lease.ProcessingStartTimeUtc,
                    current =>
                    {
                        current.Status = FileProcessingStatus.Completed;
                        current.ProcessingStartTime = null;
                        current.CompletedAt = completedAtUtc;
                        current.DeleteSucceededAt = null;
                        current.DeadLetteredAt = null;
                        current.AvailableForProcessingAt = null;
                        current.LastError = null;
                        QueueProjectionMetadataState.ClearDeadLetterProjection(current);
                        return current;
                    },
                    ct).ConfigureAwait(false);
                if (updated != null)
                {
                    _logger.LogDebug("Marked file as completed: {FileKey}", lease.FileKey);
                    return;
                }

                var metadata = await _projectionStore.GetProjectedFileAsync(lease.TenantId, lease.FileKey, ct).ConfigureAwait(false);
                if (metadata != null && IsCompletionCommittedStatus(metadata.Status))
                    return;

                if (updated == null)
                    throw CreateLeaseMismatchException(lease.FileKey, lease.ProcessingStartTimeUtc, metadata);
            }
            finally
            {
                transitionGuard?.Release();
            }
        }

        /// <inheritdoc/>
        public async Task MarkAsFailedAsync(FileProcessingLease lease, string errorMessage, CancellationToken ct = default)
        {
            ValidateLease(lease);

            SemaphoreSlim? transitionGuard = null;
            if (_queueEventJournal != null)
            {
                transitionGuard = GetTransitionGuard(lease.FileKey);
                await transitionGuard.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    var current = await _projectionStore.GetProjectedFileAsync(lease.TenantId, lease.FileKey, ct).ConfigureAwait(false);
                    if (current == null)
                        throw CreateLeaseMismatchException(lease.FileKey, lease.ProcessingStartTimeUtc, null);

                    if (current.Status != FileProcessingStatus.Processing
                        || !current.ProcessingStartTime.HasValue
                        || current.ProcessingStartTime.Value != lease.ProcessingStartTimeUtc)
                    {
                        throw CreateLeaseMismatchException(lease.FileKey, lease.ProcessingStartTimeUtc, current);
                    }

                    var projected = BuildFailedMetadata(current, errorMessage);
                    await _queueEventJournal.AppendAsync(
                        CreateProcessingFailedEvent(projected, errorMessage, lease.ProcessingStartTimeUtc),
                        ct).ConfigureAwait(false);
                }
                catch
                {
                    transitionGuard.Release();
                    throw;
                }
            }

            try
            {
                var updated = await _projectionStore.TryUpdateProjectedProcessingFileAsync(
                    lease.TenantId,
                    lease.FileKey,
                    lease.ProcessingStartTimeUtc,
                    current =>
                    {
                        current.RetryCount++;
                        current.LastError = errorMessage;
                        current.LastFailedAt = DateTime.UtcNow;
                        current.ProcessingStartTime = null;
                        current.DeleteSucceededAt = null;
                        current.DeadLetteredAt = null;
                        QueueProjectionMetadataState.ClearDeadLetterProjection(current);

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
                    ct).ConfigureAwait(false);
                var metadata = updated == null
                    ? await _projectionStore.GetProjectedFileAsync(lease.TenantId, lease.FileKey, ct).ConfigureAwait(false)
                    : null;
                if (updated == null)
                    throw CreateLeaseMismatchException(lease.FileKey, lease.ProcessingStartTimeUtc, metadata);

                if (updated.Status == FileProcessingStatus.PermanentlyFailed)
                {
                    _logger.LogError("File permanently failed after {RetryCount} retries: {FileKey}, Error: {Error}",
                        updated.RetryCount, lease.FileKey, errorMessage);
                }
                else
                {
                    var delay = updated.AvailableForProcessingAt!.Value - DateTime.UtcNow;
                    _logger.LogWarning("File marked as failed (retry {RetryCount}/{MaxRetries}), will retry after {Delay}: {FileKey}, Error: {Error}",
                        updated.RetryCount, _retryPolicy.MaxRetryCount, delay, lease.FileKey, errorMessage);
                }
            }
            finally
            {
                transitionGuard?.Release();
            }
        }

        /// <inheritdoc/>
        public async Task ResetProcessingStatusAsync(string tenantId, string fileKey, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            const int maxAttempts = 3;
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                var updated = await _projectionStore.TryResetProjectedFileToPendingAsync(tenantId, fileKey, ct).ConfigureAwait(false);
                if (updated == null)
                    continue;

                if (updated.Status == FileProcessingStatus.Processing)
                    throw new FileAlreadyProcessingException($"File is already being processed: {fileKey}");

                _logger.LogInformation("Reset processing status for file: {FileKey}", fileKey);
                return;
            }

            var latest = await _projectionStore.GetProjectedFileAsync(tenantId, fileKey, ct).ConfigureAwait(false);
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
        public async Task<FileProcessingStatus> GetFileStatusAsync(string tenantId, string fileKey, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var metadata = await _projectionStore.GetProjectedFileAsync(tenantId, fileKey, ct).ConfigureAwait(false);

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

        private SemaphoreSlim GetTransitionGuard(string fileKey)
        {
            return _transitionGuards.GetOrAdd(fileKey, _ => new SemaphoreSlim(1, 1));
        }

        private async Task TryRollbackLeasedFilesAsync(IEnumerable<FileMetadata> leasedFiles)
        {
            foreach (var leasedFile in leasedFiles
                .Where(file => file.ProcessingStartTime.HasValue)
                .GroupBy(file => file.FileKey, StringComparer.Ordinal)
                .Select(group => group.First()))
            {
                try
                {
                    await _projectionStore.TryUpdateProjectedProcessingFileAsync(
                        leasedFile.TenantId,
                        leasedFile.FileKey,
                        leasedFile.ProcessingStartTime!.Value,
                        current =>
                        {
                            current.Status = FileProcessingStatus.Pending;
                            current.ProcessingStartTime = null;
                            current.AvailableForProcessingAt = null;
                            return current;
                        },
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to rollback leased file after queue journal append failure: Tenant={TenantId}, FileKey={FileKey}",
                        leasedFile.TenantId,
                        leasedFile.FileKey);
                }
            }
        }

        private FileMetadata BuildFailedMetadata(FileMetadata current, string errorMessage)
        {
            var updated = current.Clone();
            updated.RetryCount++;
            updated.LastError = errorMessage;
            updated.LastFailedAt = DateTime.UtcNow;
            updated.ProcessingStartTime = null;
            updated.DeleteSucceededAt = null;
            updated.DeadLetteredAt = null;
            QueueProjectionMetadataState.ClearDeadLetterProjection(updated);

            if (updated.RetryCount >= _retryPolicy.MaxRetryCount)
            {
                updated.Status = FileProcessingStatus.PermanentlyFailed;
                updated.AvailableForProcessingAt = null;
            }
            else
            {
                updated.Status = FileProcessingStatus.Pending;
                updated.AvailableForProcessingAt = DateTime.UtcNow.Add(CalculateRetryDelay(updated.RetryCount));
            }

            return updated;
        }

        private static QueueEventRecord CreateProcessingStartedEvent(FileMetadata metadata)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = metadata.ProcessingStartTime ?? DateTime.UtcNow,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = metadata.ProcessingStartTime,
                RetryCount = metadata.RetryCount,
                AvailableForProcessingAtUtc = null,
                ErrorMessage = metadata.LastError,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension,
            };
        }

        private static QueueEventRecord CreateProcessingCompletedEvent(
            FileMetadata metadata,
            DateTime processingStartTimeUtc,
            DateTime completedAtUtc)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.ProcessingCompleted,
                OccurredAtUtc = completedAtUtc,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.Completed,
                ProcessingStartTimeUtc = processingStartTimeUtc,
                RetryCount = metadata.RetryCount,
                AvailableForProcessingAtUtc = null,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension,
            };
        }

        private static QueueEventRecord CreateDeleteRequestedEvent(FileMetadata metadata, DateTime occurredAtUtc)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.DeleteRequested,
                OccurredAtUtc = occurredAtUtc,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.DeleteRequested,
                RetryCount = metadata.RetryCount,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension,
            };
        }

        private static bool IsCompletionCommittedStatus(FileProcessingStatus status)
        {
            return status == FileProcessingStatus.Completed
                || status == FileProcessingStatus.DeleteRequested
                || status == FileProcessingStatus.DeleteSucceeded;
        }

        private static QueueEventRecord CreateProcessingFailedEvent(
            FileMetadata metadata,
            string errorMessage,
            DateTime expectedProcessingStartTimeUtc)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.ProcessingFailed,
                OccurredAtUtc = metadata.LastFailedAt ?? DateTime.UtcNow,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = metadata.Status,
                ProcessingStartTimeUtc = expectedProcessingStartTimeUtc,
                RetryCount = metadata.RetryCount,
                AvailableForProcessingAtUtc = metadata.AvailableForProcessingAt,
                ErrorMessage = errorMessage,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension,
            };
        }

        /// <inheritdoc/>
        public async Task<int> CleanupOrphanedMetadataAsync(CancellationToken ct = default)
        {
            var removedCount = 0;
            var tenantIds = await _projectionStore.GetProjectedTenantIdsAsync(ct).ConfigureAwait(false);

            // Process one tenant at a time to avoid allocating a single list of all files
            // across every tenant (O(total_files) memory spike -> O(max_tenant_files) peak).
            // Use all projected tenant IDs, not only hot in-memory tenants, so cold SQLite-only
            // tenants also have stale metadata reconciled before they are leased again.
            foreach (var tenantId in tenantIds)
            {
                ct.ThrowIfCancellationRequested();

                var tenantMetadata = await _projectionStore.GetProjectedFilesAsync(tenantId, ct).ConfigureAwait(false);
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

                        var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(metadata.DirectoryPath);
                        var quotaApplied = false;

                        // Use default for the full cleanup sequence once a candidate orphan has
                        // been selected; otherwise a mid-flight cancellation could leave cleanup
                        // work half-applied.
                        if (ActiveQuotaMetadata.CountsTowardActiveQuota(metadata))
                        {
                            await ApplyOrphanedMetadataDeleteQuotaAsync(
                                metadata.TenantId,
                                metadata.FileKey,
                                normalizedDirectoryPath).ConfigureAwait(false);
                            quotaApplied = true;
                        }

                        bool removed;
                        try
                        {
                            removed = await _projectionStore.RemoveProjectedFileAsync(
                                metadata.TenantId,
                                metadata.FileKey,
                                default).ConfigureAwait(false);
                        }
                        catch
                        {
                            if (quotaApplied)
                            {
                                await RollbackOrphanedMetadataDeleteQuotaAsync(
                                    metadata.TenantId,
                                    metadata.FileKey,
                                    normalizedDirectoryPath).ConfigureAwait(false);
                            }

                            throw;
                        }

                        if (!removed)
                        {
                            if (quotaApplied)
                            {
                                await RollbackOrphanedMetadataDeleteQuotaAsync(
                                    metadata.TenantId,
                                    metadata.FileKey,
                                    normalizedDirectoryPath).ConfigureAwait(false);
                            }

                            _logger.LogDebug(
                                "Skipped orphaned metadata cleanup for {FileKey} because the metadata row changed concurrently",
                                metadata.FileKey);
                            continue;
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

        private async Task ApplyOrphanedMetadataDeleteQuotaAsync(
            string tenantId,
            string fileKey,
            string normalizedDirectoryPath)
        {
            var tenantQuotaApplied = false;

            try
            {
                if (_tenantQuotaManager != null)
                {
                    await ApplyDeleteSucceededTenantProjectionAsync(tenantId).ConfigureAwait(false);
                    tenantQuotaApplied = true;
                }

                if (_directoryQuotaManager != null)
                {
                    try
                    {
                        await ApplyDeleteSucceededDirectoryProjectionAsync(tenantId, normalizedDirectoryPath).ConfigureAwait(false);
                    }
                    catch
                    {
                        if (tenantQuotaApplied)
                        {
                            await RollbackDeleteSucceededTenantProjectionAsync(tenantId).ConfigureAwait(false);
                        }

                        throw;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to apply orphaned metadata quota cleanup for tenant {TenantId}, file {FileKey}",
                    tenantId,
                    fileKey);
                throw;
            }
        }

        private async Task RollbackOrphanedMetadataDeleteQuotaAsync(
            string tenantId,
            string fileKey,
            string normalizedDirectoryPath)
        {
            Exception? firstException = null;

            if (_directoryQuotaManager != null)
            {
                try
                {
                    await RollbackDeleteSucceededDirectoryProjectionAsync(tenantId, normalizedDirectoryPath).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    firstException = ex;
                    _logger.LogError(
                        ex,
                        "Failed to rollback orphaned directory quota cleanup for tenant {TenantId}, file {FileKey}, directory {DirectoryPath}",
                        tenantId,
                        fileKey,
                        normalizedDirectoryPath);
                }
            }

            if (_tenantQuotaManager != null)
            {
                try
                {
                    await RollbackDeleteSucceededTenantProjectionAsync(tenantId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (firstException == null)
                        firstException = ex;

                    _logger.LogError(
                        ex,
                        "Failed to rollback orphaned tenant quota cleanup for tenant {TenantId}, file {FileKey}",
                        tenantId,
                        fileKey);
                }
            }

            if (firstException != null)
            {
                throw new InvalidOperationException(
                    $"Failed to rollback orphaned metadata quota cleanup for tenant '{tenantId}', file '{fileKey}'.",
                    firstException);
            }
        }

        private async Task ApplyDeleteSucceededTenantProjectionAsync(string tenantId)
        {
            await _tenantQuotaManager!.DecrementFileCountAsync(tenantId, default).ConfigureAwait(false);
        }

        private async Task ApplyDeleteSucceededDirectoryProjectionAsync(string tenantId, string directoryPath)
        {
            await _directoryQuotaManager!.DecrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
        }

        private async Task RollbackDeleteSucceededTenantProjectionAsync(string tenantId)
        {
            if (_tenantQuotaManager is ITenantQuotaCompensationManager tenantQuotaCompensationManager)
            {
                await tenantQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, default).ConfigureAwait(false);
                return;
            }

            await _tenantQuotaManager!.IncrementFileCountAsync(tenantId, default).ConfigureAwait(false);
        }

        private async Task RollbackDeleteSucceededDirectoryProjectionAsync(string tenantId, string directoryPath)
        {
            if (_directoryQuotaManager is IDirectoryQuotaCompensationManager directoryQuotaCompensationManager)
            {
                await directoryQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
                return;
            }

            await _directoryQuotaManager!.IncrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
        }

        private bool TryConfirmPhysicalFileMissing(
            FileMetadata metadata,
            IDictionary<string, bool> physicalPathExistsCache,
            out bool physicalFileMissing)
        {
            physicalFileMissing = false;

            if (_volumeRegistry != null && !string.IsNullOrWhiteSpace(metadata.VolumeId))
            {
                if (!_volumeRegistry.TryGetVolume(metadata.VolumeId, out var volume))
                {
                    _logger.LogWarning(
                        "Skipping orphaned metadata cleanup because volume {VolumeId} is unavailable for file {FileKey}",
                        metadata.VolumeId,
                        metadata.FileKey);
                    return false;
                }

                if (!volume.IsHealthy)
                {
                    _logger.LogWarning(
                        "Skipping orphaned metadata cleanup because volume {VolumeId} is unhealthy for file {FileKey}",
                        metadata.VolumeId,
                        metadata.FileKey);
                    return false;
                }
            }

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
            var lease = metadata.ProcessingStartTime.HasValue
                ? new FileProcessingLease
                {
                    TenantId = metadata.TenantId,
                    FileKey = metadata.FileKey,
                    ProcessingStartTimeUtc = metadata.ProcessingStartTime.Value
                }
                : null;

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
                ProcessingStartTime = metadata.ProcessingStartTime,
                Lease = lease
            };
        }

        private static void ValidateLease(FileProcessingLease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            if (string.IsNullOrWhiteSpace(lease.TenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(lease));

            if (string.IsNullOrWhiteSpace(lease.FileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(lease));
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
    }
}
