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
        private readonly ITenantManager _tenantManager;
        private readonly ILogger<StoragePool> _logger;
        private readonly IFileScheduler _fileScheduler;

        // --- Optimization 2: Volume selection cache ---
        // SelectVolumeForWrite() previously iterated all volumes and called IsHealthy+AvailableSpace
        // on every write. Cache the selected volume for a short window.
        private volatile IStorageVolume? _cachedSelectedVolume;
        private long _lastVolumeSelectionTicks;
        private static readonly long VolumeSelectionCacheTicks = 3L * Stopwatch.Frequency; // 3 seconds

        /// <summary>
        /// Initializes a new instance of the <see cref="StoragePool"/> class.
        /// </summary>
        public StoragePool(
            MetadataRepository metadataRepository,
            ITenantQuotaManager tenantQuotaManager,
            ITenantManager tenantManager,
            IFileScheduler fileScheduler,
            ILogger<StoragePool> logger)
        {
            _metadataRepository = metadataRepository ?? throw new ArgumentNullException(nameof(metadataRepository));
            _tenantQuotaManager = tenantQuotaManager ?? throw new ArgumentNullException(nameof(tenantQuotaManager));
            _tenantManager = tenantManager ?? throw new ArgumentNullException(nameof(tenantManager));
            _fileScheduler = fileScheduler ?? throw new ArgumentNullException(nameof(fileScheduler));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _volumes = new ConcurrentDictionary<string, IStorageVolume>();
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

            _cachedSelectedVolume = null;
            Interlocked.Exchange(ref _lastVolumeSelectionTicks, 0);

            _logger.LogInformation("Mounted volume {VolumeId} at {MountPath} (healthy checks: {HealthyAttempts}/{MaxAttempts})",
                volume.VolumeId, volume.MountPath, healthyAttempts, maxHealthCheckAttempts);
        }

        /// <inheritdoc/>
        public async Task<string> WriteFileAsync(ITenantContext tenant, Stream content, string? originalFileName, CancellationToken ct)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (content == null)
                throw new ArgumentNullException(nameof(content));

            // 1. Validate tenant status
            await ValidateTenantAsync(tenant.TenantId, ct);

            // 2. Check tenant quota
            await _tenantQuotaManager.IncrementFileCountAsync(tenant.TenantId, ct);

            string? physicalPath = null;
            IStorageVolume? volume = null;
            bool fileWritten = false;

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
                var directoryPath = Path.GetDirectoryName(physicalPath) ?? "/";

                // 7. Write file to volume
                await volume.WriteAsync(physicalPath, content, ct);
                fileWritten = true; // Mark that physical file was written

                // 8. Get file size
                var fileSize = content.CanSeek ? content.Length : 0;

                // 9. Create file metadata — Write-Behind: memory is updated immediately, LiteDB write is async.
                // AddOrUpdateAsync never throws from the caller's perspective. If LiteDB is unavailable,
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
                // Rollback: decrement quota if file write failed
                await _tenantQuotaManager.DecrementFileCountAsync(tenant.TenantId, ct);

                // If the write to the selected volume failed, invalidate the volume-selection cache
                // so the next caller re-evaluates all volumes instead of reusing a stale (unhealthy)
                // cached entry.  This is cheap: the next SelectVolumeForWrite() will re-probe IsHealthy
                // (itself cached at 30 s) and pick the best available volume.
                if (!fileWritten && volume != null)
                {
                    _cachedSelectedVolume = null;
                    Interlocked.Exchange(ref _lastVolumeSelectionTicks, 0);
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
        public async Task<Stream> ReadFileAsync(ITenantContext tenant, string fileKey, CancellationToken ct)
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
        public async Task<Core.Models.FileInfo?> GetFileInfoAsync(ITenantContext tenant, string fileKey, CancellationToken ct)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

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
        public async Task<FileLocation?> GetFileLocationAsync(ITenantContext tenant, string fileKey, CancellationToken ct)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

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
                LastError = metadata.LastError
            };
        }

        /// <summary>
        /// Deletes a file from the storage pool asynchronously (internal use).
        /// Removes both the physical file and metadata, and decrements tenant quota count.
        /// </summary>
        internal async Task DeleteFileAsync(ITenantContext tenant, string fileKey, CancellationToken ct)
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
            {
                _logger.LogWarning("File not found for deletion: {FileKey}", fileKey);
                return; // Idempotent: already deleted
            }

            // 3. Validate tenant ownership
            if (metadata.TenantId != tenant.TenantId)
                throw new UnauthorizedAccessException($"File {fileKey} does not belong to tenant {tenant.TenantId}");

            // 4. Delete physical file
            if (_volumes.TryGetValue(metadata.VolumeId, out var volume))
            {
                await volume.DeleteAsync(metadata.PhysicalPath, ct);
            }
            else
            {
                _logger.LogWarning("Volume {VolumeId} not found for file deletion, will remove metadata only", metadata.VolumeId);
            }

            // 5. Remove metadata
            await _metadataRepository.RemoveAsync(tenant.TenantId, fileKey, ct);

            // 6. Decrement tenant quota
            await _tenantQuotaManager.DecrementFileCountAsync(tenant.TenantId, ct);

            _logger.LogInformation("File deleted successfully: {FileKey} for tenant {TenantId}", fileKey, tenant.TenantId);
        }

        /// <inheritdoc/>
        public async Task<FileLocation?> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct)
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
            CancellationToken ct)
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

        /// <inheritdoc/>
        public async Task MarkAsCompletedAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // Delegate to file scheduler
            await _fileScheduler.MarkAsCompletedAsync(fileKey, ct);
        }

        /// <inheritdoc/>
        public async Task MarkAsFailedAsync(string fileKey, string errorMessage, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // Delegate to file scheduler
            await _fileScheduler.MarkAsFailedAsync(fileKey, errorMessage, ct);
        }

        /// <inheritdoc/>
        public async Task<FileProcessingStatus> GetFileStatusAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // Delegate to file scheduler
            return await _fileScheduler.GetFileStatusAsync(fileKey, ct);
        }

        /// <inheritdoc/>
        public Task<long> GetTotalCapacityAsync(CancellationToken ct)
        {
            var totalCapacity = _volumes.Values
                .Where(v => v.IsHealthy)
                .Sum(v => v.TotalCapacity);

            return Task.FromResult(totalCapacity);
        }

        /// <inheritdoc/>
        public Task<long> GetAvailableSpaceAsync(CancellationToken ct)
        {
            var availableSpace = _volumes.Values
                .Where(v => v.IsHealthy)
                .Sum(v => v.AvailableSpace);

            return Task.FromResult(availableSpace);
        }

        /// <summary>
        /// Validates that a tenant exists and is enabled.
        /// Auto-creation is handled by TenantManager if configured.
        /// </summary>
        private async Task ValidateTenantAsync(string tenantId, CancellationToken ct)
        {
            // Check if tenant exists (auto-creation handled by TenantManager)
            var tenant = await _tenantManager.GetTenantAsync(tenantId, ct);

            // Check if tenant is enabled
            if (tenant.Status != TenantStatus.Enabled)
            {
                throw new TenantDisabledException(tenantId);
            }
        }

        /// <summary>
        /// Selects the best storage volume for writing a file.
        /// Prioritizes volumes with the most available space.
        /// Result is cached for 3 seconds to avoid calling IsHealthy (which probes disk) on every write.
        /// Cache is invalidated when volumes are added or when a write to the cached volume fails.
        /// </summary>
        private IStorageVolume SelectVolumeForWrite()
        {
            // Fast path: return cached volume if still within TTL and still healthy.
            // IsHealthy is now itself cached (30 s TTL), so this check is very cheap.
            var now = Stopwatch.GetTimestamp();
            var last = Interlocked.Read(ref _lastVolumeSelectionTicks);
            var cached = _cachedSelectedVolume;

            if (cached != null && last != 0 && (now - last) < VolumeSelectionCacheTicks && cached.IsHealthy)
                return cached;

            // Slow path: re-evaluate all volumes.
            // AvailableSpace is cached per-volume (30 s TTL), so this is still cheap.
            var healthyVolumes = _volumes.Values
                .Where(v => v.IsHealthy)
                .OrderByDescending(v => v.AvailableSpace)
                .ToList();

            if (healthyVolumes.Count == 0)
                throw new InsufficientStorageException("No healthy storage volumes available");

            var selectedVolume = healthyVolumes.First();

            if (selectedVolume.AvailableSpace <= 0)
                throw new InsufficientStorageException("All storage volumes are full");

            // Update cache (not CAS — last-writer-wins is fine here)
            _cachedSelectedVolume = selectedVolume;
            Interlocked.Exchange(ref _lastVolumeSelectionTicks, now);

            return selectedVolume;
        }

        /// <summary>
        /// Generates a unique file key using GUID.
        /// </summary>
        private string GenerateFileKey()
        {
            return Guid.NewGuid().ToString("N"); // 32-character hex string without dashes
        }
    }
}
