using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
    /// Manages file storage operations across multiple storage volumes with multi-tenant support.
    /// Provides unified API for both basic storage operations and queue-based processing workflow.
    /// Volumes are configured at startup and managed internally.
    /// </summary>
    public class StoragePool : IStoragePool
    {
        private readonly ConcurrentDictionary<string, IStorageVolume> _volumes;
        private readonly MetadataRepository _metadataRepository;
        private readonly ITenantQuotaManager _tenantQuotaManager;
        private readonly IDirectoryQuotaManager _directoryQuotaManager;
        private readonly ITenantManager _tenantManager;
        private readonly ILogger<StoragePool> _logger;
        private readonly IFileScheduler _fileScheduler;
        private readonly StorageVolumeRegistry _volumeRegistry;

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
            StorageVolumeRegistry? volumeRegistry = null)
            : this(
                metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                tenantManager,
                fileScheduler,
                logger,
                DefaultCompletionGuardStripeCount,
                volumeRegistry)
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
            StorageVolumeRegistry? volumeRegistry = null)
        {
            _metadataRepository = metadataRepository ?? throw new ArgumentNullException(nameof(metadataRepository));
            _tenantQuotaManager = tenantQuotaManager ?? throw new ArgumentNullException(nameof(tenantQuotaManager));
            _directoryQuotaManager = directoryQuotaManager ?? throw new ArgumentNullException(nameof(directoryQuotaManager));
            _tenantManager = tenantManager ?? throw new ArgumentNullException(nameof(tenantManager));
            _fileScheduler = fileScheduler ?? throw new ArgumentNullException(nameof(fileScheduler));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _volumeRegistry = volumeRegistry ?? new StorageVolumeRegistry();
            if (completionGuardStripeCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(completionGuardStripeCount), "Completion guard stripe count must be greater than zero.");

            _volumes = new ConcurrentDictionary<string, IStorageVolume>();
            _completionGuards = Enumerable.Range(0, completionGuardStripeCount)
                .Select(_ => new SemaphoreSlim(1, 1))
                .ToArray();
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

                if (volume.IsHealthy)
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
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (content == null)
                throw new ArgumentNullException(nameof(content));

            // 1. Validate tenant status
            await ValidateTenantAsync(tenant.TenantId, ct);

            // 2. Check tenant quota
            await _tenantQuotaManager.IncrementFileCountAsync(tenant.TenantId, ct);
            var tenantQuotaIncremented = true;

            string? physicalPath = null;
            string? directoryPath = null;
            IStorageVolume? volume = null;
            bool fileWritten = false;
            bool directoryQuotaIncremented = false;
            CountingReadStream? countingStream = null;

            try
            {
                // 3. Select storage volume (prioritize volume with most available space)
                volume = SelectVolumeForWrite();

                // 4. Generate unique file key
                var fileKey = GenerateFileKey();

                // 5. Extract file extension from original file name
                var fileExtension = string.Empty;
                if (!string.IsNullOrWhiteSpace(originalFileName))
                {
                    fileExtension = Path.GetExtension(originalFileName);
                }

                // 6. Build physical file path using the volume's layout strategy
                physicalPath = volume.BuildPhysicalPath(tenant.TenantId, fileKey, fileExtension);

                // Derive the shard directory from the physical path so directory-level quotas
                // are tracked per actual shard directory rather than a single root "/".
                var tenantRootPath = Path.Combine(volume.MountPath, tenant.TenantId);
                directoryPath = DirectoryPathNormalizer.NormalizeFromPhysicalPath(
                    tenantRootPath,
                    Path.GetDirectoryName(physicalPath));

                // 7. Reserve directory quota after tenant quota succeeds.
                await _directoryQuotaManager.IncrementFileCountAsync(tenant.TenantId, directoryPath, ct);
                directoryQuotaIncremented = true;

                // 8. Capture the bytes that will be written: from current position to end.
                // Must be read BEFORE WriteAsync because CopyToAsync advances Position to the end.
                // Using content.Length alone is wrong for streams not at position 0 �?it would
                // report the total stream size rather than the bytes actually written.
                var fileSize = content.CanSeek ? content.Length - content.Position : 0;
                var contentToWrite = content;
                if (!content.CanSeek)
                {
                    countingStream = new CountingReadStream(content);
                    contentToWrite = countingStream;
                }

                // 9. Write file to volume
                await volume.WriteAsync(physicalPath, contentToWrite, ct);
                fileWritten = true; // Mark that physical file was written

                if (countingStream != null)
                    fileSize = countingStream.BytesRead;

                // 10. Create file metadata �?Write-Behind: memory is updated immediately, SQLite write is async.
                // AddOrUpdateAsync never throws from the caller's perspective. If SQLite is unavailable,
                // the physical file stays safe on disk and will be recovered by the cleanup service on restart.
                var metadata = new FileMetadata
                {
                    FileKey = fileKey,
                    TenantId = tenant.TenantId,
                    VolumeId = volume.VolumeId,
                    PhysicalPath = physicalPath,
                    DirectoryPath = directoryPath,
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

                await _metadataRepository.AddOrUpdateAsync(metadata, ct);

                _logger.LogDebug("File written successfully: {FileKey} for tenant {TenantId} at {PhysicalPath}",
                    fileKey, tenant.TenantId, physicalPath);

                return fileKey;
            }
            catch
            {
                // Only roll back the quota when the physical write never reached disk.
                // If fileWritten==true the file exists on disk; the cleanup service will
                // recover it as a Pending orphan on restart �?its quota slot is still needed.
                // Guard with !fileWritten so that a future exception after the write
                // (e.g. in AddOrUpdateAsync) does not incorrectly decrement the quota.
                if (!fileWritten)
                {
                    if (directoryQuotaIncremented && !string.IsNullOrWhiteSpace(directoryPath))
                    {
                        var rollbackDirectoryPath = directoryPath!;
                        try
                        {
                            await _directoryQuotaManager.DecrementFileCountAsync(
                                tenant.TenantId,
                                rollbackDirectoryPath,
                                default);
                        }
                        catch (Exception rollbackEx)
                        {
                            _logger.LogError(
                                rollbackEx,
                                "Failed to rollback directory quota after write failure: Tenant={TenantId}, Directory={DirectoryPath}",
                                tenant.TenantId,
                                rollbackDirectoryPath);
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
                if (!fileWritten && volume != null)
                {
                    _cachedWritableVolumes = Array.Empty<IStorageVolume>();
                    Interlocked.Exchange(ref _lastVolumeSnapshotTicks, 0);
                    _logger.LogWarning("Write failed on volume {VolumeId}; volume-selection cache invalidated", volume.VolumeId);
                }
                else if (fileWritten && physicalPath != null && volume != null)
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
            var metadata = await _metadataRepository.GetAsync(tenant.TenantId, fileKey, ct);
            if (metadata == null)
                throw new FileNotFoundException($"File not found: {fileKey}");

            // 3. Validate tenant ownership
            if (metadata.TenantId != tenant.TenantId)
                throw new UnauthorizedAccessException($"File {fileKey} does not belong to tenant {tenant.TenantId}");

            // 4. Get storage volume
            if (!_volumes.TryGetValue(metadata.VolumeId, out var volume))
                throw new StorageVolumeUnavailableException($"Volume {metadata.VolumeId} is not mounted");

            // 5. Read file from volume
            var stream = await volume.ReadAsync(metadata.PhysicalPath, ct);

            _logger.LogDebug("File read successfully: {FileKey} for tenant {TenantId}", fileKey, tenant.TenantId);

            return stream;
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
            var metadata = await _metadataRepository.GetAsync(tenant.TenantId, fileKey, ct);
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
            var metadata = await _metadataRepository.GetAsync(tenant.TenantId, fileKey, ct);
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
            return await _fileScheduler.GetNextFileForProcessingAsync(tenant, ct);
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
            return await _fileScheduler.GetNextBatchForProcessingAsync(tenant, batchSize, ct);
        }

        private async Task CompensateTenantQuotaIncrementAsync(string tenantId)
        {
            if (_tenantQuotaManager is ITenantQuotaCompensationManager tenantQuotaCompensationManager)
            {
                await tenantQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, default).ConfigureAwait(false);
                return;
            }

            // Custom ITenantQuotaManager has no force-increment API; fall back to the normal
            // increment but suppress quota-exceeded exceptions so compensation never masks the
            // original error or leaves the counter permanently under-counted.
            try
            {
                await _tenantQuotaManager.IncrementFileCountAsync(tenantId, default).ConfigureAwait(false);
            }
            catch (TenantQuotaExceededException)
            {
                _logger.LogWarning(
                    "Could not compensate tenant quota for {TenantId}: limit reached during rollback. " +
                    "Quota counter may be under-counted by 1.",
                    tenantId);
            }
        }

        private async Task CompensateDirectoryQuotaIncrementAsync(string tenantId, string directoryPath)
        {
            if (_directoryQuotaManager is IDirectoryQuotaCompensationManager directoryQuotaCompensationManager)
            {
                await directoryQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
                return;
            }

            // Same rationale as CompensateTenantQuotaIncrementAsync: suppress quota-exceeded
            // exceptions so the rollback path never throws due to limit enforcement.
            try
            {
                await _directoryQuotaManager.IncrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
            }
            catch (DirectoryQuotaExceededException)
            {
                _logger.LogWarning(
                    "Could not compensate directory quota for {TenantId}/{DirectoryPath}: limit reached during rollback. " +
                    "Quota counter may be under-counted by 1.",
                    tenantId, directoryPath);
            }
        }

        /// <inheritdoc/>
        public async Task MarkAsCompletedAsync(string fileKey, DateTime expectedProcessingStartTimeUtc, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // Concurrent duplicate completion calls for the same key must be idempotent.
            // Without this guard, two callers can both read metadata before scheduler removal
            // and both decrement quota.
            var completionGuard = GetCompletionGuard(fileKey);
            await completionGuard.WaitAsync(ct);
            try
            {
                // Read tenant before delegating �?scheduler removes metadata record.
                var metadata = await _metadataRepository.GetByFileKeyAsync(fileKey, ct);
                if (metadata == null)
                    return; // Already completed by another caller.

                if (metadata.Status != FileProcessingStatus.Processing
                    || !metadata.ProcessingStartTime.HasValue
                    || metadata.ProcessingStartTime.Value != expectedProcessingStartTimeUtc)
                {
                    throw new FileProcessingLeaseMismatchException(
                        fileKey,
                        expectedProcessingStartTimeUtc,
                        metadata.Status == FileProcessingStatus.Processing ? metadata.ProcessingStartTime : null);
                }

                var normalizedDirectoryPath = NormalizeDirectoryPathForQuota(metadata.DirectoryPath);

                // Decrement quotas BEFORE physical deletion + metadata removal.
                // If quota decrement fails, abort completion so the file can be retried later
                // without creating a "file gone but quota not decremented" split-brain state.
                await _directoryQuotaManager.DecrementFileCountAsync(
                    metadata.TenantId,
                    normalizedDirectoryPath,
                    default);

                var tenantQuotaDecremented = false;
                try
                {
                    await _tenantQuotaManager.DecrementFileCountAsync(metadata.TenantId, default);
                    tenantQuotaDecremented = true;
                }
                catch
                {
                    // Compensate directory quota on partial quota-success.
                    try
                    {
                        await CompensateDirectoryQuotaIncrementAsync(
                            metadata.TenantId,
                            normalizedDirectoryPath);
                    }
                    catch (Exception compensationEx)
                    {
                        _logger.LogError(
                            compensationEx,
                            "Failed to compensate directory quota after tenant quota decrement failure: Tenant={TenantId}, Directory={DirectoryPath}",
                            metadata.TenantId,
                            normalizedDirectoryPath);
                    }

                    throw;
                }

                try
                {
                    // Delegate physical deletion + metadata removal to file scheduler.
                    await _fileScheduler.MarkAsCompletedAsync(fileKey, expectedProcessingStartTimeUtc, ct);
                }
                catch
                {
                    // Completion failed after quota decrement; compensate both quotas best-effort.
                    if (tenantQuotaDecremented)
                    {
                        try
                        {
                            await CompensateTenantQuotaIncrementAsync(metadata.TenantId);
                        }
                        catch (Exception compensationEx)
                        {
                            _logger.LogError(
                                compensationEx,
                                "Failed to compensate tenant quota after completion failure: Tenant={TenantId}, FileKey={FileKey}",
                                metadata.TenantId,
                                fileKey);
                        }
                    }

                    try
                    {
                        await CompensateDirectoryQuotaIncrementAsync(
                            metadata.TenantId,
                            normalizedDirectoryPath);
                    }
                    catch (Exception compensationEx)
                    {
                        _logger.LogError(
                            compensationEx,
                            "Failed to compensate directory quota after completion failure: Tenant={TenantId}, Directory={DirectoryPath}, FileKey={FileKey}",
                            metadata.TenantId,
                            normalizedDirectoryPath,
                            fileKey);
                    }

                    throw;
                }
            }
            finally
            {
                completionGuard.Release();
            }
        }

        /// <inheritdoc/>
        public async Task MarkAsFailedAsync(string fileKey, DateTime expectedProcessingStartTimeUtc, string errorMessage, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // Delegate to file scheduler
            await _fileScheduler.MarkAsFailedAsync(fileKey, expectedProcessingStartTimeUtc, errorMessage, ct);
        }

        /// <inheritdoc/>
        public async Task<FileProcessingStatus> GetFileStatusAsync(string fileKey, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // Delegate to file scheduler
            return await _fileScheduler.GetFileStatusAsync(fileKey, ct);
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
        private IStorageVolume SelectVolumeForWrite()
        {
            var now = Stopwatch.GetTimestamp();
            var writableVolumes = GetWritableVolumeSnapshot(now);

            if (writableVolumes.Length == 0)
            {
                var healthyCount = _volumes.Values.Count(v => v.IsHealthy);
                if (healthyCount == 0)
                    throw new InsufficientStorageException("No healthy storage volumes available");

                throw new InsufficientStorageException("All storage volumes are full");
            }

            if (writableVolumes.Length == 1)
                return writableVolumes[0];

            // Pick two pseudo-random distinct candidates and choose the one with more free space.
            var ticket = (uint)Interlocked.Increment(ref _volumeSelectionCounter);
            var firstIndex = (int)(ticket % (uint)writableVolumes.Length);
            var secondTicket = ticket * 1103515245u + 12345u;
            var secondIndex = (int)(secondTicket % (uint)writableVolumes.Length);

            if (secondIndex == firstIndex)
                secondIndex = (secondIndex + 1) % writableVolumes.Length;

            var first = writableVolumes[firstIndex];
            var second = writableVolumes[secondIndex];

            return first.AvailableSpace >= second.AvailableSpace ? first : second;
        }

        private IStorageVolume[] GetWritableVolumeSnapshot(long nowTicks)
        {
            var cached = _cachedWritableVolumes;
            var lastRefresh = Interlocked.Read(ref _lastVolumeSnapshotTicks);

            if (lastRefresh != 0 && (nowTicks - lastRefresh) < VolumeSnapshotRefreshTicks)
                return cached;

            // Singleflight: only the CAS winner rebuilds the snapshot.
            // Concurrent callers return the (slightly stale) cached value �?acceptable since
            // the snapshot is refreshed at most every VolumeSnapshotRefreshTicks (1 s) anyway.
            if (Interlocked.CompareExchange(ref _volumeSnapshotRefreshInProgress, 1, 0) != 0)
                return cached;

            try
            {
                var refreshed = _volumes.Values
                    .Where(v => v.IsHealthy && v.AvailableSpace > 0)
                    .ToArray();

                _cachedWritableVolumes = refreshed;
                Interlocked.Exchange(ref _lastVolumeSnapshotTicks, nowTicks);
                return refreshed;
            }
            finally
            {
                Volatile.Write(ref _volumeSnapshotRefreshInProgress, 0);
            }
        }

        private SemaphoreSlim GetCompletionGuard(string fileKey)
        {
            var hash = StringComparer.Ordinal.GetHashCode(fileKey) & int.MaxValue;
            return _completionGuards[hash % _completionGuards.Length];
        }

        private static string NormalizeDirectoryPathForQuota(string? directoryPath)
        {
            return DirectoryPathNormalizer.Normalize(directoryPath);
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
