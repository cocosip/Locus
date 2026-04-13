using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
    /// Manages file storage operations across multiple storage volumes with multi-tenant support.
    /// Provides unified API for both basic storage operations and queue-based processing workflow.
    /// Volumes are configured at startup and managed internally.
    /// </summary>
    public class StoragePool : IStoragePool
    {
        private readonly ConcurrentDictionary<string, IStorageVolume> _volumes;
        private readonly IQueueProjectionStore _projectionStore;
        private readonly IQueueProjectionWriteStore _projectionWriteStore;
        private readonly ITenantQuotaManager _tenantQuotaManager;
        private readonly IDirectoryQuotaManager _directoryQuotaManager;
        private readonly ITenantManager _tenantManager;
        private readonly ILogger<StoragePool> _logger;
        private readonly IFileScheduler _fileScheduler;
        private readonly StorageVolumeRegistry _volumeRegistry;
        private readonly IQueueEventJournal? _queueEventJournal;
        private readonly bool _allowLegacyNonJournalMode;

        // Cache a writable-volume snapshot and refresh periodically.
        // Selection then uses power-of-two choices to avoid pinning traffic to one volume.
        private volatile IStorageVolume[] _cachedWritableVolumes = Array.Empty<IStorageVolume>();
        private long _lastVolumeSnapshotTicks;
        private int _volumeSelectionCounter;
        // Singleflight guard: only the CAS winner rebuilds the snapshot; losers reuse the stale cache.
        private int _volumeSnapshotRefreshInProgress;
        private static readonly long VolumeSnapshotRefreshTicks = Stopwatch.Frequency; // 1 second
        private static readonly long CapacitySnapshotRefreshTicks = Stopwatch.Frequency; // 1 second
        private long _lastCapacitySnapshotTicks;
        private long _cachedTotalCapacity;
        private long _cachedAvailableCapacity;

        // Serialize completion for the same file key to keep quota decrement idempotent.
        private readonly SemaphoreSlim[] _completionGuards;

        /// <summary>
        /// Default number of completion-guard stripes used to reduce lock collisions.
        /// </summary>
        public const int DefaultCompletionGuardStripeCount = 256;

        /// <summary>
        /// Initializes a new instance of the <see cref="StoragePool"/> class.
        /// </summary>
        public StoragePool(
            MetadataRepository metadataRepository,
            ITenantQuotaManager tenantQuotaManager,
            IDirectoryQuotaManager directoryQuotaManager,
            ITenantManager tenantManager,
            IFileScheduler fileScheduler,
            ILogger<StoragePool> logger,
            StorageVolumeRegistry? volumeRegistry = null,
            IQueueEventJournal? queueEventJournal = null,
            IQueueProjectionStore? projectionStore = null,
            IQueueProjectionWriteStore? projectionWriteStore = null,
            bool allowLegacyNonJournalMode = false)
            : this(
                metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                tenantManager,
                fileScheduler,
                logger,
                DefaultCompletionGuardStripeCount,
                volumeRegistry,
                queueEventJournal,
                projectionStore,
                projectionWriteStore,
                allowLegacyNonJournalMode)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StoragePool"/> class.
        /// </summary>
        public StoragePool(
            MetadataRepository metadataRepository,
            ITenantQuotaManager tenantQuotaManager,
            IDirectoryQuotaManager directoryQuotaManager,
            ITenantManager tenantManager,
            IFileScheduler fileScheduler,
            ILogger<StoragePool> logger,
            int completionGuardStripeCount,
            StorageVolumeRegistry? volumeRegistry = null,
            IQueueEventJournal? queueEventJournal = null,
            IQueueProjectionStore? projectionStore = null,
            IQueueProjectionWriteStore? projectionWriteStore = null,
            bool allowLegacyNonJournalMode = false)
        {
            if (metadataRepository == null)
                throw new ArgumentNullException(nameof(metadataRepository));

            _projectionStore = projectionStore ?? new MetadataRepositoryQueueProjectionStore(metadataRepository);
            _projectionWriteStore = projectionWriteStore ?? new MetadataRepositoryQueueProjectionWriteStore(metadataRepository);
            _tenantQuotaManager = tenantQuotaManager ?? throw new ArgumentNullException(nameof(tenantQuotaManager));
            _directoryQuotaManager = directoryQuotaManager ?? throw new ArgumentNullException(nameof(directoryQuotaManager));
            _tenantManager = tenantManager ?? throw new ArgumentNullException(nameof(tenantManager));
            _fileScheduler = fileScheduler ?? throw new ArgumentNullException(nameof(fileScheduler));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _volumeRegistry = volumeRegistry ?? new StorageVolumeRegistry();
            _queueEventJournal = queueEventJournal;
            _allowLegacyNonJournalMode = allowLegacyNonJournalMode;
            if (completionGuardStripeCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(completionGuardStripeCount), "Completion guard stripe count must be greater than zero.");

            _volumes = new ConcurrentDictionary<string, IStorageVolume>();
            _completionGuards = Enumerable.Range(0, completionGuardStripeCount)
                .Select(_ => new SemaphoreSlim(1, 1))
                .ToArray();
            ValidateLegacyNonJournalMode();
        }

        /// <summary>
        /// Adds a storage volume to the pool asynchronously.
        /// Performs multiple health check attempts with async delays to handle transient failures
        /// (especially important for network storage in Kubernetes/Docker environments).
        /// This method must be called from an async context (e.g., IHostedService.StartAsync)
        /// to avoid blocking the thread pool.
        /// </summary>
        /// <param name="volume">The storage volume to add.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <param name="initialDelayMs">
        /// Delay before the first health check in milliseconds.
        /// Used to allow K8s PVC mount and file system synchronization.
        /// Pass 0 to skip the initial delay (e.g., in tests or local file system scenarios).
        /// Default: 2000.
        /// </param>
        /// <param name="healthCheckDelayMs">
        /// Delay between health check retries in milliseconds.
        /// Pass 0 to disable inter-check delays.
        /// Default: 500.
        /// </param>
        /// <exception cref="ArgumentNullException">Thrown when volume is null.</exception>
        /// <exception cref="StorageVolumeUnavailableException">Thrown when the volume fails health checks after all retries.</exception>
        /// <exception cref="InvalidOperationException">Thrown when the volume is already mounted.</exception>
        public async Task AddVolumeAsync(
            IStorageVolume volume,
            int initialDelayMs = 2000,
            int healthCheckDelayMs = 500,
            CancellationToken ct = default)
        {
            if (volume == null)
                throw new ArgumentNullException(nameof(volume));

            const int maxHealthCheckAttempts = 10;
            int healthyAttempts = 0;

            if (initialDelayMs > 0)
                await Task.Delay(initialDelayMs, ct);

            for (int attempt = 1; attempt <= maxHealthCheckAttempts; attempt++)
            {
                ct.ThrowIfCancellationRequested();

                if (ProbeVolumeHealth(volume, forceRefresh: true))
                {
                    healthyAttempts++;
                    _logger.LogDebug("Volume {VolumeId} health check passed (attempt {Attempt}/{MaxAttempts})",
                        volume.VolumeId, attempt, maxHealthCheckAttempts);

                    if (healthyAttempts >= 2)
                        break;
                }
                else
                {
                    healthyAttempts = 0;
                    _logger.LogDebug("Volume {VolumeId} health check failed (attempt {Attempt}/{MaxAttempts})",
                        volume.VolumeId, attempt, maxHealthCheckAttempts);
                }

                if (attempt < maxHealthCheckAttempts && healthCheckDelayMs > 0)
                    await Task.Delay(healthCheckDelayMs, ct);
            }

            if (healthyAttempts < 2)
            {
                _logger.LogWarning("Volume {VolumeId} is not healthy after {MaxAttempts} attempts, skipping mount",
                    volume.VolumeId, maxHealthCheckAttempts);
                throw new StorageVolumeUnavailableException(
                    $"Volume {volume.VolumeId} is not healthy after {maxHealthCheckAttempts} attempts");
            }

            if (!_volumes.TryAdd(volume.VolumeId, volume))
            {
                _logger.LogWarning("Volume {VolumeId} is already mounted", volume.VolumeId);
                throw new InvalidOperationException($"Volume {volume.VolumeId} is already mounted");
            }

            _volumeRegistry.Register(volume);

            _cachedWritableVolumes = Array.Empty<IStorageVolume>();
            Interlocked.Exchange(ref _lastVolumeSnapshotTicks, 0);
            Interlocked.Exchange(ref _lastCapacitySnapshotTicks, 0);

            _logger.LogInformation("Mounted volume {VolumeId} at {MountPath} (healthy checks: {HealthyAttempts}/{MaxAttempts})",
                volume.VolumeId, volume.MountPath, healthyAttempts, maxHealthCheckAttempts);
        }

        /// <inheritdoc/>
        public async Task<string> WriteFileAsync(ITenantContext tenant, Stream content, string? originalFileName, CancellationToken ct = default)
        {
            return await WriteFileAsync(tenant, content, originalFileName, null, ct).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<string> WriteFileAsync(
            ITenantContext tenant,
            Stream content,
            string? originalFileName,
            string? logicalDirectoryPath,
            CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (content == null)
                throw new ArgumentNullException(nameof(content));

            // 1. Validate tenant status
            await ValidateTenantAsync(tenant.TenantId, ct);

            // 2. Check tenant quota
            await _tenantQuotaManager.IncrementFileCountAsync(tenant.TenantId, ct);
            var tenantQuotaIncremented = true;
            var normalizedLogicalDirectoryPath = DirectoryPathNormalizer.Normalize(logicalDirectoryPath);

            string? physicalPath = null;
            IStorageVolume? volume = null;
            bool fileWritten = false;
            bool directoryQuotaIncremented = false;
            bool acceptanceCompleted = false;
            CountingReadStream? countingStream = null;
            var initialContentPosition = content.CanSeek ? content.Position : 0;
            var expectedFileSize = content.CanSeek ? content.Length - content.Position : (long?)null;

            try
            {
                // 4. Generate unique file key
                var fileKey = GenerateFileKey();

                // 5. Extract file extension from original file name
                var fileExtension = string.Empty;
                if (!string.IsNullOrWhiteSpace(originalFileName))
                {
                    fileExtension = Path.GetExtension(originalFileName);
                }

                // 6. Reserve directory quota after tenant quota succeeds.
                await _directoryQuotaManager.IncrementFileCountAsync(tenant.TenantId, normalizedLogicalDirectoryPath, ct);
                directoryQuotaIncremented = true;

                // 7. Capture the bytes that will be written: from current position to end.
                // Must be read before WriteAsync because CopyToAsync advances Position to the end.
                // Using content.Length alone is wrong for streams not at position 0 because it
                // reports the total stream size rather than the bytes actually written.
                var fileSize = expectedFileSize ?? 0;
                var contentToWrite = content;
                if (!content.CanSeek)
                {
                    countingStream = new CountingReadStream(content);
                    contentToWrite = countingStream;
                }

                // 8. Write file to volume. Seekable streams can be retried on another
                // healthy volume because we can restore the original stream position.
                var candidateVolumes = SelectVolumeCandidatesForWrite(expectedFileSize);
                Exception? lastWriteException = null;
                for (var attemptIndex = 0; attemptIndex < candidateVolumes.Length; attemptIndex++)
                {
                    volume = candidateVolumes[attemptIndex];
                    physicalPath = volume.BuildPhysicalPath(tenant.TenantId, fileKey, fileExtension);

                    if (content.CanSeek)
                        content.Position = initialContentPosition;

                    try
                    {
                        await volume.WriteAsync(physicalPath, contentToWrite, ct).ConfigureAwait(false);
                        fileWritten = true;
                        lastWriteException = null;
                        break;
                    }
                    catch (Exception ex) when (content.CanSeek
                        && attemptIndex + 1 < candidateVolumes.Length
                        && IsRetryableWriteFailure(ex))
                    {
                        lastWriteException = ex;
                        await TryCleanupFailedWriteAsync(volume, physicalPath).ConfigureAwait(false);
                        InvalidateWritableVolumeSnapshot(volume);
                        _logger.LogWarning(
                            ex,
                            "Write failed on volume {VolumeId}; retrying another healthy volume for tenant {TenantId}",
                            volume.VolumeId,
                            tenant.TenantId);
                    }
                }

                if (!fileWritten)
                {
                    if (lastWriteException != null)
                        throw lastWriteException;

                    throw new InsufficientStorageException("No healthy storage volumes accepted the file write");
                }

                if (volume == null || physicalPath == null)
                    throw new InvalidOperationException("Write completed without selecting a storage volume.");

                if (countingStream != null)
                    fileSize = countingStream.BytesRead;

                // 10. Create file metadata. Write-behind keeps memory current and persists SQLite asynchronously.
                // QueueProjectedFileAsync never throws from the caller's perspective. If SQLite is unavailable,
                // the physical file stays safe on disk and will be recovered by the cleanup service on restart.
                var metadata = new FileMetadata
                {
                    FileKey = fileKey,
                    TenantId = tenant.TenantId,
                    VolumeId = volume.VolumeId,
                    PhysicalPath = physicalPath,
                    DirectoryPath = normalizedLogicalDirectoryPath,
                    FileSize = fileSize,
                    CreatedAt = DateTime.UtcNow,
                    Status = FileProcessingStatus.Pending,
                    RetryCount = 0,
                    ProcessingStartTime = null,
                    AvailableForProcessingAt = null,
                    LastError = null,
                    LastFailedAt = null,
                    OriginalFileName = originalFileName,
                    FileExtension = fileExtension
                };

                if (_queueEventJournal != null)
                {
                    QueueProjectionMetadataState.MarkAcceptedProjectionPending(metadata);
                    await AppendAcceptedQueueEventAsync(metadata).ConfigureAwait(false);
                    acceptanceCompleted = true;
                }
                else
                {
                    var tenantReservationConsumed = false;
                    var directoryReservationConsumed = false;
                    try
                    {
                        tenantReservationConsumed = await ApplyAcceptedTenantProjectionAsync(metadata.TenantId, ct).ConfigureAwait(false);
                        directoryReservationConsumed = await ApplyAcceptedDirectoryProjectionAsync(metadata.TenantId, metadata.DirectoryPath, ct).ConfigureAwait(false);
                        QueueProjectionMetadataState.MarkAcceptedProjectionApplied(metadata);
                        acceptanceCompleted = true;
                    }
                    catch
                    {
                        await RollbackAcceptedProjectionAsync(
                            metadata.TenantId,
                            metadata.DirectoryPath,
                            tenantReservationConsumed,
                            directoryReservationConsumed).ConfigureAwait(false);
                        throw;
                    }
                }

                await _projectionWriteStore.QueueProjectedFileAsync(metadata, ct);

                _logger.LogDebug("File written successfully: {FileKey} for tenant {TenantId} at {PhysicalPath}",
                    fileKey, tenant.TenantId, physicalPath);

                return fileKey;
            }
            catch
            {
                var physicalWriteSucceeded = fileWritten;
                var rollbackQuota = !physicalWriteSucceeded;

                if (!acceptanceCompleted && physicalPath != null)
                {
                    try
                    {
                        await DeleteWrittenFileAsync(volume, physicalPath).ConfigureAwait(false);
                        rollbackQuota = true;
                        fileWritten = false;
                        if (physicalWriteSucceeded)
                        {
                            _logger.LogWarning(
                                "Deleted physical file after queue journal append failed: Tenant={TenantId}, Path={PhysicalPath}",
                                tenant.TenantId,
                                physicalPath);
                        }
                        else
                        {
                            _logger.LogWarning(
                                "Deleted residual physical file after write failure: Tenant={TenantId}, Path={PhysicalPath}",
                                tenant.TenantId,
                                physicalPath);
                        }
                    }
                    catch (Exception cleanupEx)
                    {
                        rollbackQuota = false;
                        _logger.LogError(
                            cleanupEx,
                            physicalWriteSucceeded
                                ? "Failed to delete physical file after queue journal append failure: Tenant={TenantId}, Path={PhysicalPath}"
                                : "Failed to delete residual physical file after write failure: Tenant={TenantId}, Path={PhysicalPath}",
                            tenant.TenantId,
                            physicalPath);
                    }
                }

                // Only roll back the quota when the physical write never reached disk.
                // If fileWritten==true the file exists on disk; the cleanup service will
                // recover it as a Pending orphan on restart, so its quota slot is still needed.
                // Guard with !fileWritten so that a future exception after the write
                // (e.g. in QueueProjectedFileAsync) does not incorrectly decrement the quota.
                if (rollbackQuota)
                {
                    if (directoryQuotaIncremented)
                    {
                        try
                        {
                            await _directoryQuotaManager.DecrementFileCountAsync(
                                tenant.TenantId,
                                normalizedLogicalDirectoryPath,
                                default);
                        }
                        catch (Exception rollbackEx)
                        {
                            _logger.LogError(
                                rollbackEx,
                                "Failed to rollback directory quota after write failure: Tenant={TenantId}, Directory={DirectoryPath}",
                                tenant.TenantId,
                                normalizedLogicalDirectoryPath);
                        }
                    }

                    if (tenantQuotaIncremented)
                    {
                        await _tenantQuotaManager.DecrementFileCountAsync(tenant.TenantId, default);
                    }
                }

                // If the write to the selected volume failed, invalidate the volume-selection cache
                // so the next caller re-evaluates all volumes instead of reusing a stale (unhealthy)
                // cached entry.  This is cheap: the next SelectVolumeForWrite() will re-probe IsHealthy
                // (itself cached at 30 s) and pick the best available volume.
                if (!physicalWriteSucceeded && volume != null)
                {
                    InvalidateWritableVolumeSnapshot(volume);
                }
                else if (physicalWriteSucceeded && !acceptanceCompleted && physicalPath != null)
                {
                    _logger.LogError(
                        "Physical file was written but acceptance did not complete, leaving a manual recovery candidate at {PhysicalPath}",
                        physicalPath);
                }
                else if (physicalWriteSucceeded && physicalPath != null && volume != null)
                {
                    _logger.LogWarning("Transaction failed with physical file potentially orphaned at {PhysicalPath}", physicalPath);
                }

                throw;
            }
        }

        /// <inheritdoc/>
        public async Task<Stream> ReadFileAsync(ITenantContext tenant, string fileKey, CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // 1. Validate tenant status
            await ValidateTenantAsync(tenant.TenantId, ct);

            // 2. Get file metadata
            var metadata = await _projectionStore.GetProjectedFileAsync(tenant.TenantId, fileKey, ct).ConfigureAwait(false);
            if (metadata == null)
                throw new FileNotFoundException($"File not found: {fileKey}");

            // 3. Validate tenant ownership
            if (metadata.TenantId != tenant.TenantId)
                throw new UnauthorizedAccessException($"File {fileKey} does not belong to tenant {tenant.TenantId}");

            // 4. Get storage volume
            if (!_volumes.TryGetValue(metadata.VolumeId, out var volume))
                throw new StorageVolumeUnavailableException($"Volume {metadata.VolumeId} is not mounted");

            // 5. Read file from volume
            var stream = await ReadFileFromVolumeAsync(metadata, volume, ct).ConfigureAwait(false);

            _logger.LogDebug("File read successfully: {FileKey} for tenant {TenantId}", fileKey, tenant.TenantId);

            return stream;
        }

        private async Task<Stream> ReadFileFromVolumeAsync(FileMetadata metadata, IStorageVolume volume, CancellationToken ct)
        {
            try
            {
                return await volume.ReadAsync(metadata.PhysicalPath, ct).ConfigureAwait(false);
            }
            catch (FileNotFoundException)
            {
                if (await TryCorrectMetadataPhysicalPathAsync(metadata, volume, ct).ConfigureAwait(false))
                    return await volume.ReadAsync(metadata.PhysicalPath, ct).ConfigureAwait(false);

                throw;
            }
            catch (DirectoryNotFoundException)
            {
                if (await TryCorrectMetadataPhysicalPathAsync(metadata, volume, ct).ConfigureAwait(false))
                    return await volume.ReadAsync(metadata.PhysicalPath, ct).ConfigureAwait(false);

                throw;
            }
        }

        private async Task<bool> TryCorrectMetadataPhysicalPathAsync(FileMetadata metadata, IStorageVolume volume, CancellationToken ct)
        {
            var correctedPath = volume.BuildPhysicalPath(metadata.TenantId, metadata.FileKey, metadata.FileExtension);
            if (PathsEqual(metadata.PhysicalPath, correctedPath))
                return false;

            try
            {
                using (await volume.ReadAsync(correctedPath, ct).ConfigureAwait(false))
                {
                }
            }
            catch (FileNotFoundException)
            {
                return false;
            }
            catch (DirectoryNotFoundException)
            {
                return false;
            }

            metadata.PhysicalPath = correctedPath;
            await _projectionStore.UpsertProjectedFileAsync(metadata, ct).ConfigureAwait(false);
            return true;
        }

        private static bool PathsEqual(string left, string right)
        {
            return string.Equals(
                Path.GetFullPath(left),
                Path.GetFullPath(right),
                RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                    ? StringComparison.OrdinalIgnoreCase
                    : StringComparison.Ordinal);
        }

        /// <inheritdoc/>
        public async Task<Core.Models.FileInfo?> GetFileInfoAsync(ITenantContext tenant, string fileKey, CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // Validate tenant status before exposing metadata.
            await ValidateTenantAsync(tenant.TenantId, ct);

            // Get file metadata
            var metadata = await _projectionStore.GetProjectedFileAsync(tenant.TenantId, fileKey, ct).ConfigureAwait(false);
            if (metadata == null)
                return null;

            // Validate tenant ownership
            if (metadata.TenantId != tenant.TenantId)
                return null;

            return new Core.Models.FileInfo
            {
                FileKey = metadata.FileKey,
                TenantId = metadata.TenantId,
                FileSize = metadata.FileSize,
                CreatedAt = metadata.CreatedAt,
                Status = metadata.Status,
                RetryCount = metadata.RetryCount
            };
        }

        /// <inheritdoc/>
        public async Task<FileLocation?> GetFileLocationAsync(ITenantContext tenant, string fileKey, CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // Validate tenant status before exposing physical location.
            await ValidateTenantAsync(tenant.TenantId, ct);

            // Get file metadata
            var metadata = await _projectionStore.GetProjectedFileAsync(tenant.TenantId, fileKey, ct).ConfigureAwait(false);
            if (metadata == null)
                return null;

            // Validate tenant ownership
            if (metadata.TenantId != tenant.TenantId)
                return null;

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


        /// <inheritdoc/>
        public async Task<FileLocation?> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            // Validate tenant status
            await ValidateTenantAsync(tenant.TenantId, ct);

            // Delegate to file scheduler
            var location = await _fileScheduler.GetNextFileForProcessingAsync(tenant, ct);
            if (location != null && ShouldAppendQueueEventsInStoragePool())
            {
                EnsureLease(location);
                try
                {
                    await AppendQueueEventAsync(CreateProcessingStartedEvent(location), ct).ConfigureAwait(false);
                }
                catch
                {
                    await RollbackFallbackProcessingStartAsync(tenant.TenantId, location.FileKey).ConfigureAwait(false);
                    throw;
                }
            }

            return location;
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

            // Validate tenant status
            await ValidateTenantAsync(tenant.TenantId, ct);

            // Delegate to file scheduler
            var locations = (await _fileScheduler.GetNextBatchForProcessingAsync(tenant, batchSize, ct)).ToList();
            if (locations.Count > 0 && ShouldAppendQueueEventsInStoragePool())
            {
                foreach (var location in locations)
                    EnsureLease(location);

                try
                {
                    await AppendQueueEventsAsync(
                        locations.Select(CreateProcessingStartedEvent).ToArray(),
                        ct).ConfigureAwait(false);
                }
                catch
                {
                    await RollbackFallbackProcessingStartBatchAsync(
                        tenant.TenantId,
                        locations.Select(location => location.FileKey)).ConfigureAwait(false);
                    throw;
                }
            }

            return locations;
        }

        /// <inheritdoc/>
        public async Task MarkAsCompletedAsync(FileProcessingLease lease, CancellationToken ct = default)
        {
            ValidateLease(lease);

            // Concurrent duplicate completion calls for the same key must be idempotent.
            // Without this guard, two callers can both read metadata before scheduler update
            // and both try to move the same file into Completed.
            var completionGuard = GetCompletionGuard(lease.FileKey);
            await completionGuard.WaitAsync(ct);
            try
            {
                // Read current metadata before delegating so we can validate the active lease once.
                var metadata = await _projectionStore.GetProjectedFileAsync(lease.TenantId, lease.FileKey, ct).ConfigureAwait(false);
                if (metadata == null)
                    return;

                if (IsCompletionCommittedStatus(metadata.Status))
                    return;

                if (metadata.Status != FileProcessingStatus.Processing
                    || !metadata.ProcessingStartTime.HasValue
                    || metadata.ProcessingStartTime.Value != lease.ProcessingStartTimeUtc)
                {
                    throw new FileProcessingLeaseMismatchException(
                        lease.FileKey,
                        lease.ProcessingStartTimeUtc,
                        metadata.Status == FileProcessingStatus.Processing ? metadata.ProcessingStartTime : null);
                }

                await _fileScheduler.MarkAsCompletedAsync(lease, ct);

                if (!ShouldAppendQueueEventsInStoragePool())
                    return;

                var completed = await _projectionStore.GetProjectedFileAsync(lease.TenantId, lease.FileKey, ct).ConfigureAwait(false);
                if (completed == null)
                    return;

                try
                {
                    await AppendQueueEventsAsync(
                        new[]
                        {
                            CreateProcessingCompletedEvent(completed, lease.ProcessingStartTimeUtc),
                            CreateDeleteRequestedEvent(completed),
                        },
                        ct).ConfigureAwait(false);
                }
                catch
                {
                    await RestoreProjectedMetadataAsync(metadata).ConfigureAwait(false);
                    throw;
                }
            }
            finally
            {
                completionGuard.Release();
            }
        }

        /// <inheritdoc/>
        public async Task MarkAsFailedAsync(FileProcessingLease lease, string errorMessage, CancellationToken ct = default)
        {
            ValidateLease(lease);

            var previousMetadata = ShouldAppendQueueEventsInStoragePool()
                ? await _projectionStore.GetProjectedFileAsync(lease.TenantId, lease.FileKey, ct).ConfigureAwait(false)
                : null;

            // Delegate to file scheduler
            await _fileScheduler.MarkAsFailedAsync(lease, errorMessage, ct);

            if (ShouldAppendQueueEventsInStoragePool())
            {
                var updated = await _projectionStore.GetProjectedFileAsync(lease.TenantId, lease.FileKey, ct).ConfigureAwait(false);
                if (updated != null)
                {
                    try
                    {
                        await AppendQueueEventAsync(
                            CreateProcessingFailedEvent(updated, errorMessage, lease.ProcessingStartTimeUtc),
                            ct).ConfigureAwait(false);
                    }
                    catch
                    {
                        if (previousMetadata != null)
                            await RestoreProjectedMetadataAsync(previousMetadata).ConfigureAwait(false);

                        throw;
                    }
                }
            }
        }

        /// <inheritdoc/>
        public async Task<FileProcessingStatus> GetFileStatusAsync(ITenantContext tenant, string fileKey, CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            await ValidateTenantAsync(tenant.TenantId, ct);

            // Delegate to file scheduler
            return await _fileScheduler.GetFileStatusAsync(tenant.TenantId, fileKey, ct);
        }

        /// <inheritdoc/>
        public Task<long> GetTotalCapacityAsync(CancellationToken ct = default)
        {
            RefreshCapacitySnapshotIfNeeded(Stopwatch.GetTimestamp());
            return Task.FromResult(Volatile.Read(ref _cachedTotalCapacity));
        }

        /// <inheritdoc/>
        public Task<long> GetAvailableSpaceAsync(CancellationToken ct = default)
        {
            RefreshCapacitySnapshotIfNeeded(Stopwatch.GetTimestamp());
            return Task.FromResult(Volatile.Read(ref _cachedAvailableCapacity));
        }

        /// <summary>
        /// Validates that a tenant exists and is enabled.
        /// Auto-creation is handled by TenantManager if configured.
        /// </summary>
        private async Task ValidateTenantAsync(string tenantId, CancellationToken ct = default)
        {
            // Check if tenant exists (auto-creation handled by TenantManager)
            var tenant = await _tenantManager.GetTenantAsync(tenantId, ct);

            // Check if tenant is enabled
            if (tenant.Status != TenantStatus.Enabled)
            {
                throw new TenantDisabledException(tenantId);
            }
        }

        private void RefreshCapacitySnapshotIfNeeded(long nowTicks)
        {
            var lastRefresh = Interlocked.Read(ref _lastCapacitySnapshotTicks);
            if (lastRefresh != 0 && (nowTicks - lastRefresh) < CapacitySnapshotRefreshTicks)
                return;

            var totalCapacity = 0L;
            var availableCapacity = 0L;

            foreach (var volume in _volumes.Values)
            {
                if (!volume.IsHealthy)
                    continue;

                totalCapacity += volume.TotalCapacity;
                availableCapacity += volume.AvailableSpace;
            }

            Volatile.Write(ref _cachedTotalCapacity, totalCapacity);
            Volatile.Write(ref _cachedAvailableCapacity, availableCapacity);
            Interlocked.Exchange(ref _lastCapacitySnapshotTicks, nowTicks);
        }

        /// <summary>
        /// Selects a storage volume for writing.
        /// Uses a periodically refreshed healthy-volume snapshot + "power of two choices"
        /// to balance load while still favoring volumes with more free space.
        /// </summary>
        private IStorageVolume[] SelectVolumeCandidatesForWrite(long? requiredBytes)
        {
            var now = Stopwatch.GetTimestamp();
            var writableVolumes = GetWritableVolumeSnapshot(now, requiredBytes);

            if (writableVolumes.Length == 0)
            {
                var healthyCount = _volumes.Values.Count(v => ProbeVolumeHealth(v));
                if (healthyCount == 0)
                    throw new InsufficientStorageException("No healthy storage volumes available");

                if (requiredBytes.HasValue)
                {
                    throw new InsufficientStorageException(
                        $"No healthy storage volume has enough free space for {requiredBytes.Value} bytes");
                }

                throw new InsufficientStorageException("All storage volumes are full");
            }

            if (writableVolumes.Length == 1)
                return writableVolumes;

            // Pick two pseudo-random distinct candidates and choose the one with more free space.
            var ticket = (uint)Interlocked.Increment(ref _volumeSelectionCounter);
            var firstIndex = (int)(ticket % (uint)writableVolumes.Length);
            var secondTicket = ticket * 1103515245u + 12345u;
            var secondIndex = (int)(secondTicket % (uint)writableVolumes.Length);

            if (secondIndex == firstIndex)
                secondIndex = (secondIndex + 1) % writableVolumes.Length;

            var first = writableVolumes[firstIndex];
            var second = writableVolumes[secondIndex];
            var preferred = first.AvailableSpace >= second.AvailableSpace ? first : second;
            var alternate = ReferenceEquals(preferred, first) ? second : first;

            return writableVolumes
                .Where(v => !ReferenceEquals(v, preferred) && !ReferenceEquals(v, alternate))
                .Prepend(alternate)
                .Prepend(preferred)
                .ToArray();
        }

        private IStorageVolume[] GetWritableVolumeSnapshot(long nowTicks, long? requiredBytes = null)
        {
            var cached = _cachedWritableVolumes;
            var lastRefresh = Interlocked.Read(ref _lastVolumeSnapshotTicks);

            if (lastRefresh != 0 && (nowTicks - lastRefresh) < VolumeSnapshotRefreshTicks)
                return FilterWritableVolumesByRequiredSpace(cached, requiredBytes);

            // Singleflight: only the CAS winner rebuilds the snapshot.
            // Concurrent callers return the slightly stale cached value, which is acceptable since
            // the snapshot is refreshed at most every VolumeSnapshotRefreshTicks (1 s) anyway.
            if (Interlocked.CompareExchange(ref _volumeSnapshotRefreshInProgress, 1, 0) != 0)
                return FilterWritableVolumesByRequiredSpace(cached, requiredBytes);

            try
            {
                var refreshed = _volumes.Values
                    .Where(v => ProbeVolumeHealth(v) && v.AvailableSpace > 0)
                    .ToArray();

                _cachedWritableVolumes = refreshed;
                Interlocked.Exchange(ref _lastVolumeSnapshotTicks, nowTicks);
                return FilterWritableVolumesByRequiredSpace(refreshed, requiredBytes);
            }
            finally
            {
                Volatile.Write(ref _volumeSnapshotRefreshInProgress, 0);
            }
        }

        private static IStorageVolume[] FilterWritableVolumesByRequiredSpace(IStorageVolume[] volumes, long? requiredBytes)
        {
            if (!requiredBytes.HasValue || requiredBytes.Value <= 0)
                return volumes;

            return volumes
                .Where(v => v.AvailableSpace >= requiredBytes.Value)
                .ToArray();
        }

        private static bool ProbeVolumeHealth(IStorageVolume volume, bool forceRefresh = false)
        {
            if (forceRefresh && volume is IStorageVolumeHealthProbe healthProbe)
                return healthProbe.ProbeHealth();

            return volume.IsHealthy;
        }

        private static bool IsRetryableWriteFailure(Exception ex)
        {
            return ex is IOException || ex is UnauthorizedAccessException;
        }

        private async Task TryCleanupFailedWriteAsync(IStorageVolume volume, string physicalPath)
        {
            try
            {
                await DeleteWrittenFileAsync(volume, physicalPath).ConfigureAwait(false);
            }
            catch (Exception cleanupEx)
            {
                _logger.LogWarning(
                    cleanupEx,
                    "Failed to cleanup partial write at {PhysicalPath} on volume {VolumeId}",
                    physicalPath,
                    volume.VolumeId);
            }
        }

        private void InvalidateWritableVolumeSnapshot(IStorageVolume volume)
        {
            _cachedWritableVolumes = Array.Empty<IStorageVolume>();
            Interlocked.Exchange(ref _lastVolumeSnapshotTicks, 0);
            _logger.LogWarning("Write failed on volume {VolumeId}; volume-selection cache invalidated", volume.VolumeId);
        }

        private SemaphoreSlim GetCompletionGuard(string fileKey)
        {
            var hash = StringComparer.Ordinal.GetHashCode(fileKey) & int.MaxValue;
            return _completionGuards[hash % _completionGuards.Length];
        }

        private static void EnsureLease(FileLocation location)
        {
            if (location == null)
                throw new ArgumentNullException(nameof(location));

            if (location.Lease != null || !location.ProcessingStartTime.HasValue)
                return;

            location.Lease = new FileProcessingLease
            {
                TenantId = location.TenantId,
                FileKey = location.FileKey,
                ProcessingStartTimeUtc = location.ProcessingStartTime.Value
            };
        }

        private static void ValidateLease(FileProcessingLease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            if (string.IsNullOrWhiteSpace(lease.TenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(lease));

            if (string.IsNullOrWhiteSpace(lease.FileKey))
                throw new ArgumentException("File key cannot be empty", nameof(lease));
        }

        private async Task<bool> ApplyAcceptedTenantProjectionAsync(string tenantId, CancellationToken ct)
        {
            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
                return await tenantQuotaProjectionManager.ApplyAcceptedProjectionAsync(tenantId, ct).ConfigureAwait(false);

            return false;
        }

        private async Task<bool> ApplyAcceptedDirectoryProjectionAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                return await directoryQuotaProjectionManager
                    .ApplyAcceptedProjectionAsync(tenantId, directoryPath, ct)
                    .ConfigureAwait(false);
            }

            return false;
        }

        private async Task RollbackAcceptedProjectionAsync(
            string tenantId,
            string directoryPath,
            bool tenantRestoreReservation,
            bool directoryRestoreReservation)
        {
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                await directoryQuotaProjectionManager
                    .RollbackAcceptedProjectionAsync(tenantId, directoryPath, directoryRestoreReservation, default)
                    .ConfigureAwait(false);
            }
            else
            {
                await _directoryQuotaManager.DecrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
            }

            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
            {
                await tenantQuotaProjectionManager
                    .RollbackAcceptedProjectionAsync(tenantId, tenantRestoreReservation, default)
                    .ConfigureAwait(false);
            }
            else
            {
                await _tenantQuotaManager.DecrementFileCountAsync(tenantId, default).ConfigureAwait(false);
            }
        }

        private async Task AppendAcceptedQueueEventAsync(FileMetadata metadata)
        {
            if (_queueEventJournal == null)
                return;

            await _queueEventJournal.AppendAsync(
                new QueueEventRecord
                {
                    TenantId = metadata.TenantId,
                    FileKey = metadata.FileKey,
                    EventType = QueueEventType.Accepted,
                    OccurredAtUtc = metadata.CreatedAt,
                    VolumeId = metadata.VolumeId,
                    PhysicalPath = metadata.PhysicalPath,
                    DirectoryPath = metadata.DirectoryPath,
                    FileSize = metadata.FileSize,
                    Status = metadata.Status,
                    RetryCount = metadata.RetryCount,
                    OriginalFileName = metadata.OriginalFileName,
                    FileExtension = metadata.FileExtension
                },
                default).ConfigureAwait(false);
        }

        private bool ShouldAppendQueueEventsInStoragePool()
        {
            return _queueEventJournal != null
                && !(_fileScheduler is IQueueEventManagedFileScheduler managedScheduler
                    && managedScheduler.HandlesQueueJournal);
        }

        private void ValidateLegacyNonJournalMode()
        {
            if (_queueEventJournal != null)
                return;

            if (!_allowLegacyNonJournalMode)
            {
                throw new InvalidOperationException(
                    "StoragePool requires IQueueEventJournal unless legacy non-journal mode is explicitly allowed.");
            }

            _logger.LogWarning(
                "StoragePool is running without IQueueEventJournal. This legacy non-journal mode should only be used for explicit compatibility or tests.");
        }

        private async Task AppendQueueEventAsync(QueueEventRecord record, CancellationToken ct)
        {
            if (_queueEventJournal == null)
                return;

            await _queueEventJournal.AppendAsync(record, ct).ConfigureAwait(false);
        }

        private async Task AppendQueueEventsAsync(IReadOnlyList<QueueEventRecord> records, CancellationToken ct)
        {
            if (_queueEventJournal == null || records.Count == 0)
                return;

            await _queueEventJournal.AppendBatchAsync(records, ct).ConfigureAwait(false);
        }

        private async Task RollbackFallbackProcessingStartAsync(string tenantId, string fileKey)
        {
            try
            {
                await _fileScheduler.ResetProcessingStatusAsync(tenantId, fileKey, default).ConfigureAwait(false);
            }
            catch (Exception rollbackEx)
            {
                _logger.LogError(
                    rollbackEx,
                    "Failed to rollback processing lease after queue journal append failure: Tenant={TenantId}, FileKey={FileKey}",
                    tenantId,
                    fileKey);
                throw new InvalidOperationException(
                    $"Failed to rollback processing lease after queue journal append failure for file {fileKey}.",
                    rollbackEx);
            }
        }

        private async Task RollbackFallbackProcessingStartBatchAsync(string tenantId, IEnumerable<string> fileKeys)
        {
            Exception? rollbackFailure = null;
            foreach (var fileKey in fileKeys.Distinct(StringComparer.Ordinal))
            {
                try
                {
                    await _fileScheduler.ResetProcessingStatusAsync(tenantId, fileKey, default).ConfigureAwait(false);
                }
                catch (Exception rollbackEx)
                {
                    rollbackFailure ??= rollbackEx;
                    _logger.LogError(
                        rollbackEx,
                        "Failed to rollback processing lease after batch queue journal append failure: Tenant={TenantId}, FileKey={FileKey}",
                        tenantId,
                        fileKey);
                }
            }

            if (rollbackFailure != null)
            {
                throw new InvalidOperationException(
                    $"Failed to rollback one or more processing leases after queue journal append failure for tenant {tenantId}.",
                    rollbackFailure);
            }
        }

        private async Task RestoreProjectedMetadataAsync(FileMetadata metadata)
        {
            try
            {
                await _projectionWriteStore.QueueProjectedFileAsync(metadata.Clone(), default).ConfigureAwait(false);
            }
            catch (Exception rollbackEx)
            {
                _logger.LogError(
                    rollbackEx,
                    "Failed to restore projected metadata after queue journal append failure: Tenant={TenantId}, FileKey={FileKey}",
                    metadata.TenantId,
                    metadata.FileKey);
                throw new InvalidOperationException(
                    $"Failed to restore projected metadata after queue journal append failure for file {metadata.FileKey}.",
                    rollbackEx);
            }
        }

        private static QueueEventRecord CreateProcessingStartedEvent(FileLocation location)
        {
            return new QueueEventRecord
            {
                TenantId = location.TenantId,
                FileKey = location.FileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = location.ProcessingStartTime ?? DateTime.UtcNow,
                VolumeId = location.VolumeId,
                PhysicalPath = location.PhysicalPath,
                DirectoryPath = location.DirectoryPath,
                FileSize = location.FileSize,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = location.ProcessingStartTime,
                RetryCount = location.RetryCount,
                AvailableForProcessingAtUtc = null,
                ErrorMessage = location.LastError
            };
        }

        private static QueueEventRecord CreateProcessingCompletedEvent(
            FileMetadata metadata,
            DateTime processingStartTimeUtc)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.ProcessingCompleted,
                OccurredAtUtc = metadata.CompletedAt ?? DateTime.UtcNow,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.Completed,
                ProcessingStartTimeUtc = processingStartTimeUtc,
                RetryCount = metadata.RetryCount,
                AvailableForProcessingAtUtc = metadata.AvailableForProcessingAt,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension
            };
        }

        private static QueueEventRecord CreateDeleteRequestedEvent(FileMetadata metadata)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.DeleteRequested,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.DeleteRequested,
                RetryCount = metadata.RetryCount,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension
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
                FileExtension = metadata.FileExtension
            };
        }

        private static Task DeleteWrittenFileAsync(IStorageVolume? volume, string physicalPath)
        {
            if (volume != null)
                return volume.DeleteAsync(physicalPath, default);

            File.Delete(physicalPath);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Generates a unique file key using GUID.
        /// </summary>
        private string GenerateFileKey()
        {
            return Guid.NewGuid().ToString("N"); // 32-character hex string without dashes
        }

        private sealed class CountingReadStream : Stream
        {
            private readonly Stream _inner;

            public CountingReadStream(Stream inner)
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            }

            public long BytesRead { get; private set; }

            public override bool CanRead => _inner.CanRead;

            public override bool CanSeek => _inner.CanSeek;

            public override bool CanWrite => _inner.CanWrite;

            public override long Length => _inner.Length;

            public override long Position
            {
                get => _inner.Position;
                set => _inner.Position = value;
            }

            public override void Flush()
            {
                _inner.Flush();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                var read = _inner.Read(buffer, offset, count);
                BytesRead += read;
                return read;
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
            {
                return ReadAsyncInternal(buffer, offset, count, cancellationToken);
            }

            private async Task<int> ReadAsyncInternal(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
            {
                var read = await _inner.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
                BytesRead += read;
                return read;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                return _inner.Seek(offset, origin);
            }

            public override void SetLength(long value)
            {
                _inner.SetLength(value);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                _inner.Write(buffer, offset, count);
            }

            protected override void Dispose(bool disposing)
            {
                // The caller owns the wrapped stream lifetime.
                base.Dispose(disposing);
            }
        }
    }
}
