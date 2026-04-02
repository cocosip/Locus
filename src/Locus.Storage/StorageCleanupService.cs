using System;
using System.Collections.Concurrent;
using System.Diagnostics;
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
    /// Provides automatic cleanup services for the storage pool.
    /// </summary>
    public class StorageCleanupService : IStorageCleanupService
    {
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly ITenantQuotaManager _tenantQuotaManager;
        private readonly ITenantManager? _tenantManager;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<StorageCleanupService> _logger;
        private readonly ConcurrentDictionary<string, IStorageVolume> _volumes;
        private readonly StorageVolumeRegistry _volumeRegistry;
        private readonly string _metadataDirectory;
        private readonly string _quotaDirectory;
        private readonly int _statusCleanupBatchSizePerTenant;
        private readonly int _databaseOptimizationTenantBatchSize;
        private readonly TimeSpan _databaseOptimizationPauseBetweenBatches;
        private readonly int _maxOrphanFilesPerRun;
        private readonly int _orphanRebuildLookupCacheSize;
        private readonly StringComparer _pathComparer;
        private readonly StringComparison _pathComparison;
        private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> _orphanScanQueues;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _orphanScanLocks;
        private readonly ConcurrentDictionary<string, DateTime> _orphanScanLastRunUtc;
        private long _statusCleanupIterationCount;
        private long _statusCleanupIterationStopwatchTicks;
        private int _emptyDirectoriesRemoved;
        private int _completedRecordsRemoved;
        private int _permanentlyFailedFilesRemoved;
        private int _orphanedFilesRemoved;
        private int _timedOutFilesReset;
        private long _spaceFreed;
        
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
            ITenantQuotaManager tenantQuotaManager,
            IFileSystem fileSystem,
            ILogger<StorageCleanupService> logger,
            string metadataDirectory,
            string quotaDirectory,
            CleanupOptions? cleanupOptions = null,
            StorageVolumeRegistry? volumeRegistry = null,
            ITenantManager? tenantManager = null)
        {
            _metadataRepository = metadataRepository ?? throw new ArgumentNullException(nameof(metadataRepository));
            _quotaRepository = quotaRepository ?? throw new ArgumentNullException(nameof(quotaRepository));
            _tenantQuotaManager = tenantQuotaManager ?? throw new ArgumentNullException(nameof(tenantQuotaManager));
            _tenantManager = tenantManager;
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _volumes = new ConcurrentDictionary<string, IStorageVolume>();
            _volumeRegistry = volumeRegistry ?? new StorageVolumeRegistry();
            _orphanScanQueues = new ConcurrentDictionary<string, ConcurrentQueue<string>>();
            _orphanScanLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
            _orphanScanLastRunUtc = new ConcurrentDictionary<string, DateTime>();

            if (string.IsNullOrWhiteSpace(metadataDirectory))
                throw new ArgumentException("Metadata directory cannot be empty", nameof(metadataDirectory));
            if (string.IsNullOrWhiteSpace(quotaDirectory))
                throw new ArgumentException("Quota directory cannot be empty", nameof(quotaDirectory));

            _metadataDirectory = metadataDirectory;
            _quotaDirectory = quotaDirectory;

            var options = cleanupOptions ?? new CleanupOptions();
            _statusCleanupBatchSizePerTenant = options.CleanupBatchSizePerTenant > 0
                ? options.CleanupBatchSizePerTenant
                : 500;
            _databaseOptimizationTenantBatchSize = options.DatabaseOptimizationTenantBatchSize > 0
                ? options.DatabaseOptimizationTenantBatchSize
                : 10;
            _databaseOptimizationPauseBetweenBatches = options.DatabaseOptimizationPauseBetweenBatches >= TimeSpan.Zero
                ? options.DatabaseOptimizationPauseBetweenBatches
                : TimeSpan.Zero;
            _maxOrphanFilesPerRun = options.MaxOrphanFilesPerRun > 0
                ? options.MaxOrphanFilesPerRun
                : int.MaxValue;
            _orphanRebuildLookupCacheSize = options.OrphanRebuildLookupCacheSize > 0
                ? options.OrphanRebuildLookupCacheSize
                : 0;
            _pathComparer = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal;
            _pathComparison = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;
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
            _volumeRegistry.Register(volume);

            _logger.LogDebug("Registered volume {VolumeId} with cleanup service, mount path: {MountPath}",
                volume.VolumeId, volume.MountPath);
        }

        /// <inheritdoc/>
        public async Task CleanupEmptyDirectoriesAsync(ITenantContext tenant, CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            await CleanupEmptyDirectoriesAsync(tenant.TenantId, ct);
        }

        /// <inheritdoc/>
        public async Task CleanupEmptyDirectoriesAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            _logger.LogInformation("Starting junk-file sweep for tenant: {TenantId}", tenantId);

            var removedCount = 0;

            foreach (var volume in _volumes.Values)
            {
                var tenantPath = _fileSystem.Path.Combine(volume.MountPath, tenantId);
                if (_fileSystem.Directory.Exists(tenantPath))
                {
                    // isProtectedRoot=true: never delete the tenant root directory itself.
                    // Pass shardingDepth so shard directories are also protected.
                    removedCount += await CleanupJunkFilesRecursiveAsync(tenantPath, ct, isProtectedRoot: true, shardingDepth: volume.ShardingDepth);
                }
            }

            Interlocked.Add(ref _emptyDirectoriesRemoved, removedCount);
            _logger.LogInformation(
                "Junk-file sweep completed for tenant {TenantId}: removed {Count} empty directories",
                tenantId,
                removedCount);
        }

        /// <inheritdoc/>
        public async Task CleanupAllEmptyDirectoriesAsync(CancellationToken ct = default)
        {
            _logger.LogInformation("Starting junk-file sweep for all tenants");

            var removedCount = 0;

            foreach (var volume in _volumes.Values)
            {
                if (_fileSystem.Directory.Exists(volume.MountPath))
                {
                    var subdirectories = _fileSystem.Directory.GetDirectories(volume.MountPath);
                    foreach (var subdirectory in subdirectories)
                    {
                        // Each subdirectory is a tenant root -- protect it from deletion.
                        // Pass shardingDepth so shard directories are also protected.
                        removedCount += await CleanupJunkFilesRecursiveAsync(subdirectory, ct, isProtectedRoot: true, shardingDepth: volume.ShardingDepth);
                    }
                }
            }

            Interlocked.Add(ref _emptyDirectoriesRemoved, removedCount);
            _logger.LogInformation(
                "Junk-file sweep completed across all tenants: removed {Count} empty directories",
                removedCount);
        }

        /// <inheritdoc/>
        public async Task CleanupPermanentlyFailedFilesAsync(TimeSpan olderThan, CancellationToken ct = default)
        {
            _logger.LogInformation("Starting cleanup of permanently failed files older than {TimeSpan}", olderThan);

            var cutoffTime = DateTime.UtcNow - olderThan;
            var removedCount = 0;
            long spaceFreed = 0;

            var tenantIds = await _metadataRepository.GetAllTenantIdsAsync(ct);
            foreach (var tenantId in tenantIds)
            {
                ct.ThrowIfCancellationRequested();
                var tenantResult = await CleanupPermanentlyFailedForTenantAsync(tenantId, cutoffTime, ct);
                removedCount += tenantResult.RemovedCount;
                spaceFreed += tenantResult.SpaceFreed;
            }

            Interlocked.Add(ref _permanentlyFailedFilesRemoved, removedCount);
            Interlocked.Add(ref _spaceFreed, spaceFreed);
            _logger.LogInformation("Cleaned up {Count} permanently failed files, freed {Size} bytes",
                removedCount, spaceFreed);
        }

        /// <inheritdoc/>
        public async Task RecoverOrphanedFilesAsync(ITenantContext tenant, CancellationToken ct = default)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            _logger.LogInformation("Starting orphaned file metadata rebuild for tenant: {TenantId}", tenant.TenantId);

            var rebuiltCount = 0;

            foreach (var volume in _volumes.Values)
            {
                var tenantPath = _fileSystem.Path.Combine(volume.MountPath, tenant.TenantId);
                if (!_fileSystem.Directory.Exists(tenantPath))
                    continue;

                var scanKey = GetOrphanScanKey(tenant.TenantId, volume.VolumeId);
                var scanLock = _orphanScanLocks.GetOrAdd(scanKey, _ => new SemaphoreSlim(1, 1));
                var existenceCache = _orphanRebuildLookupCacheSize > 0
                    ? new Dictionary<string, bool>(_orphanRebuildLookupCacheSize, _pathComparer)
                    : null;

                await scanLock.WaitAsync(ct);
                try
                {
                    var scanQueue = GetOrphanScanQueue(scanKey, tenantPath);
                    var scannedThisRun = 0;
                    var budgetReached = false;

                    while (scannedThisRun < _maxOrphanFilesPerRun && scanQueue.TryDequeue(out var directory))
                    {
                        ct.ThrowIfCancellationRequested();

                        if (!_fileSystem.Directory.Exists(directory))
                            continue;

                        try
                        {
                            foreach (var physicalPath in _fileSystem.Directory.EnumerateFiles(directory))
                            {
                                ct.ThrowIfCancellationRequested();
                                scannedThisRun++;

                                var normalizedPhysical = NormalizePath(physicalPath);
                                var metadataExists = false;
                                if (normalizedPhysical != null)
                                {
                                    if (existenceCache != null
                                        && existenceCache.TryGetValue(normalizedPhysical, out var cachedExists))
                                    {
                                        metadataExists = cachedExists;
                                    }
                                    else
                                    {
                                        metadataExists = await _metadataRepository.ExistsByNormalizedPhysicalPathAsync(
                                            tenant.TenantId,
                                            normalizedPhysical,
                                            ct);

                                        if (existenceCache != null
                                            && existenceCache.Count < _orphanRebuildLookupCacheSize)
                                        {
                                            existenceCache[normalizedPhysical] = metadataExists;
                                        }
                                    }
                                }

                                if (metadataExists)
                                {
                                    if (scannedThisRun >= _maxOrphanFilesPerRun)
                                    {
                                        scanQueue.Enqueue(directory);
                                        budgetReached = true;
                                        break;
                                    }

                                    continue;
                                }

                                // Orphaned physical file -- reconstruct metadata and re-queue for processing.
                                // DO NOT delete the file: it contains real data that was uploaded but whose
                                // metadata record was lost (e.g. process crash during write-behind flush, or
                                // persistence queue overflow).  Rebuilding brings it back into the Pending
                                // queue so that normal consumers can process it on the next scheduling cycle.
                                try
                                {
                                    var metadata = RebuildMetadataFromPath(physicalPath, volume, tenant.TenantId);

                                    if (metadata == null)
                                    {
                                        _logger.LogWarning(
                                            "Skipping orphaned file whose path does not match the expected storage layout: {PhysicalPath}",
                                            physicalPath);
                                        continue;
                                    }

                                    var tenantQuotaCompensated = false;
                                    var directoryQuotaCompensated = false;
                                    try
                                    {
                                        await CompensateRebuiltTenantQuotaAsync(metadata.TenantId, default);
                                        tenantQuotaCompensated = true;

                                        var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(metadata.DirectoryPath);
                                        await _quotaRepository.ForceIncrementAsync(metadata.TenantId, normalizedDirectoryPath, default);
                                        directoryQuotaCompensated = true;

                                        // Persist rebuilt metadata only after quota compensation succeeds so we never
                                        // re-queue a file whose quota counters still under-report real usage.
                                        await _metadataRepository.AddOrUpdateDirectAsync(metadata, ct);
                                    }
                                    catch
                                    {
                                        await RollbackRebuiltFileQuotaCompensationAsync(
                                            metadata,
                                            tenantQuotaCompensated,
                                            directoryQuotaCompensated);
                                        throw;
                                    }

                                    if (normalizedPhysical != null && existenceCache != null)
                                    {
                                        if (existenceCache.Count < _orphanRebuildLookupCacheSize
                                            || existenceCache.ContainsKey(normalizedPhysical))
                                        {
                                            existenceCache[normalizedPhysical] = true;
                                        }
                                    }

                                    rebuiltCount++;
                                    _logger.LogInformation(
                                        "Rebuilt metadata and compensated quota for orphaned file: fileKey={FileKey}, tenant={TenantId}, path={PhysicalPath}",
                                        metadata.FileKey, metadata.TenantId, physicalPath);
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogWarning(ex, "Failed to rebuild metadata for orphaned file {PhysicalPath}", physicalPath);
                                }

                                if (scannedThisRun >= _maxOrphanFilesPerRun)
                                {
                                    scanQueue.Enqueue(directory);
                                    budgetReached = true;
                                    break;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to enumerate files in directory {DirectoryPath}", directory);
                        }

                        if (budgetReached)
                            break;

                        try
                        {
                            foreach (var subdirectory in _fileSystem.Directory.EnumerateDirectories(directory))
                                scanQueue.Enqueue(subdirectory);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to enumerate subdirectories in directory {DirectoryPath}", directory);
                        }
                    }

                    if (budgetReached)
                    {
                        _logger.LogInformation(
                            "Orphaned file scan budget reached for tenant {TenantId} on volume {VolumeId}; remaining directories will be scanned next run",
                            tenant.TenantId, volume.VolumeId);
                    }
                }
                finally
                {
                    scanLock.Release();
                }

                _orphanScanLastRunUtc[scanKey] = DateTime.UtcNow;
            }

            Interlocked.Add(ref _orphanedFilesRemoved, rebuiltCount);
            _logger.LogInformation(
                "Orphaned file rebuild complete for tenant {TenantId}: {Count} file(s) re-queued as Pending",
                tenant.TenantId, rebuiltCount);
        }

        /// <inheritdoc/>
        public async Task RecoverAllOrphanedFilesAsync(CancellationToken ct = default)
        {
            _logger.LogInformation("Starting orphaned file metadata rebuild across all registered volumes");

            var tenantIds = new HashSet<string>(StringComparer.Ordinal);
            foreach (var volume in _volumes.Values)
            {
                ct.ThrowIfCancellationRequested();

                if (!_fileSystem.Directory.Exists(volume.MountPath))
                    continue;

                string[] tenantDirectories;
                try
                {
                    tenantDirectories = _fileSystem.Directory.GetDirectories(volume.MountPath);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to enumerate tenant directories under volume {VolumeId}", volume.VolumeId);
                    continue;
                }

                foreach (var tenantDirectory in tenantDirectories)
                {
                    ct.ThrowIfCancellationRequested();

                    var tenantId = _fileSystem.Path.GetFileName(tenantDirectory);
                    if (!string.IsNullOrWhiteSpace(tenantId))
                        tenantIds.Add(tenantId);
                }
            }

            foreach (var tenantId in tenantIds)
            {
                ct.ThrowIfCancellationRequested();
                var tenant = await ResolveCleanupTenantAsync(tenantId, ct);
                if (tenant == null)
                    continue;

                await RecoverOrphanedFilesAsync(tenant, ct);
            }
        }

        private async Task<ITenantContext?> ResolveCleanupTenantAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                return null;

            if (_tenantManager == null)
                return new CleanupTenantContext(tenantId);

            try
            {
                var tenant = await _tenantManager.GetTenantAsync(tenantId, ct);
                if (tenant.Status != TenantStatus.Enabled)
                {
                    _logger.LogInformation(
                        "Skipping orphaned-file rebuild for tenant {TenantId} because tenant status is {Status}",
                        tenantId,
                        tenant.Status);
                    return null;
                }

                return tenant;
            }
            catch (TenantNotFoundException)
            {
                _logger.LogInformation(
                    "Skipping orphaned-file rebuild for tenant {TenantId} because tenant metadata was not found",
                    tenantId);
                return null;
            }
        }

        private string GetOrphanScanKey(string tenantId, string volumeId)
        {
            return $"{tenantId}\u001F{volumeId}";
        }

        private ConcurrentQueue<string> GetOrphanScanQueue(string scanKey, string tenantPath)
        {
            var queue = _orphanScanQueues.GetOrAdd(scanKey, _ => new ConcurrentQueue<string>());
            if (queue.IsEmpty)
                queue.Enqueue(tenantPath);
            return queue;
        }

        private string? NormalizePath(string? path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return null;

            try
            {
                return _fileSystem.Path.GetFullPath(path!);
            }
            catch
            {
                return path;
            }
        }

        /// <summary>
        /// Reconstructs <see cref="FileMetadata"/> from the physical path of an orphaned file.
        /// Returns null if the path does not conform to the expected storage layout:
        ///   {mountPath}/{tenantId}/[shardDirs.../{fileKey}{extension}
        /// </summary>
        private FileMetadata? RebuildMetadataFromPath(string physicalPath, IStorageVolume volume, string expectedTenantId)
        {
            var normalizedPhysical = _fileSystem.Path.GetFullPath(physicalPath);
            var normalizedMount   = _fileSystem.Path.GetFullPath(volume.MountPath);

            // Ensure the file is actually inside this volume's mount path.
            var mountPrefix = normalizedMount.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                              + Path.DirectorySeparatorChar;
            if (!normalizedPhysical.StartsWith(mountPrefix, _pathComparison))
                return null;

            // Relative path segments after the mount: [tenantId, shard?, shard?, ..., filename]
            var relativePath = normalizedPhysical.Substring(mountPrefix.Length);
            var segments = relativePath.Split(
                new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar },
                StringSplitOptions.RemoveEmptyEntries);

            // Minimum viable path: tenantId/fileKey (2 segments)
            if (segments.Length < 2)
                return null;

            var tenantId = segments[0];
            if (!string.Equals(tenantId, expectedTenantId, StringComparison.Ordinal))
                return null;

            // Last segment is the file name: {fileKey}{extension}
            var filename      = segments[segments.Length - 1];
            var fileExtension = Path.GetExtension(filename);
            var fileKey       = Path.GetFileNameWithoutExtension(filename);

            if (string.IsNullOrWhiteSpace(fileKey))
                return null;

            var fileInfo = _fileSystem.FileInfo.New(physicalPath);
            var tenantRootPath = _fileSystem.Path.Combine(volume.MountPath, tenantId);
            var directoryPath = DirectoryPathNormalizer.NormalizeFromPhysicalPath(
                tenantRootPath,
                _fileSystem.Path.GetDirectoryName(physicalPath));

            return new FileMetadata
            {
                FileKey       = fileKey,
                TenantId      = tenantId,
                VolumeId      = volume.VolumeId,
                PhysicalPath  = physicalPath,
                DirectoryPath = directoryPath,
                FileSize      = fileInfo.Length,
                // Prefer the file's own creation time; fall back to now if unavailable.
                CreatedAt     = fileInfo.CreationTimeUtc > DateTime.MinValue
                                    ? fileInfo.CreationTimeUtc
                                    : DateTime.UtcNow,
                Status        = FileProcessingStatus.Pending,
                RetryCount    = 0,
                FileExtension = string.IsNullOrEmpty(fileExtension) ? null : fileExtension,
                // Original file name is unknown for orphaned files.
                OriginalFileName = null,
            };
        }

        private async Task CompensateRebuiltTenantQuotaAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (_tenantQuotaManager is ITenantQuotaCompensationManager tenantQuotaCompensationManager)
            {
                await tenantQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, ct);
                return;
            }

            await _tenantQuotaManager.IncrementFileCountAsync(tenantId, ct);
        }

        private async Task RollbackRebuiltFileQuotaCompensationAsync(
            FileMetadata metadata,
            bool tenantQuotaCompensated,
            bool directoryQuotaCompensated)
        {
            var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(metadata.DirectoryPath);

            if (directoryQuotaCompensated)
            {
                try
                {
                    await _quotaRepository.DecrementAsync(metadata.TenantId, normalizedDirectoryPath, default);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to rollback directory quota compensation for orphaned file rebuild. Tenant={TenantId}, Directory={DirectoryPath}, FileKey={FileKey}",
                        metadata.TenantId,
                        normalizedDirectoryPath,
                        metadata.FileKey);
                }
            }

            if (tenantQuotaCompensated)
            {
                try
                {
                    await _tenantQuotaManager.DecrementFileCountAsync(metadata.TenantId, default);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to rollback tenant quota compensation for orphaned file rebuild. Tenant={TenantId}, FileKey={FileKey}",
                        metadata.TenantId,
                        metadata.FileKey);
                }
            }
        }

        /// <inheritdoc/>
        public async Task CleanupTimedOutProcessingFilesAsync(TimeSpan timeout, CancellationToken ct = default)
        {
            _logger.LogInformation("Starting cleanup of timed-out processing files (timeout: {Timeout})", timeout);

            var cutoffTime = DateTime.UtcNow - timeout;
            var resetCount = 0;
            var tenantIds = await _metadataRepository.GetAllTenantIdsAsync(ct);

            foreach (var tenantId in tenantIds)
            {
                ct.ThrowIfCancellationRequested();
                resetCount += await ResetTimedOutForTenantAsync(tenantId, cutoffTime, DateTime.UtcNow, ct);
            }

            Interlocked.Add(ref _timedOutFilesReset, resetCount);
            _logger.LogInformation("Reset {Count} timed-out processing files to Pending", resetCount);
        }

        /// <inheritdoc/>
        public async Task CleanupFilesByStatusAsync(
            TimeSpan? processingTimeout,
            TimeSpan? failedRetentionPeriod,
            CancellationToken ct = default)
        {
            if (processingTimeout == null && failedRetentionPeriod == null)
                return;

            _logger.LogInformation(
                "Starting combined file status cleanup (timeout={Timeout}, failedRetention={Failed})",
                processingTimeout, failedRetentionPeriod);

            var now = DateTime.UtcNow;
            var timeoutCutoff = processingTimeout.HasValue ? now - processingTimeout.Value : (DateTime?)null;
            var failedCutoff = failedRetentionPeriod.HasValue ? now - failedRetentionPeriod.Value : (DateTime?)null;

            int resetCount = 0, removedFailed = 0;
            long spaceFreed = 0;

            var tenantIds = await _metadataRepository.GetAllTenantIdsAsync(ct);
            foreach (var tenantId in tenantIds)
            {
                ct.ThrowIfCancellationRequested();
                if (timeoutCutoff.HasValue)
                    resetCount += await ResetTimedOutForTenantAsync(tenantId, timeoutCutoff.Value, now, ct);

                if (failedCutoff.HasValue)
                {
                    var tenantResult = await CleanupPermanentlyFailedForTenantAsync(tenantId, failedCutoff.Value, ct);
                    removedFailed += tenantResult.RemovedCount;
                    spaceFreed += tenantResult.SpaceFreed;
                }
            }

            Interlocked.Add(ref _timedOutFilesReset, resetCount);
            Interlocked.Add(ref _permanentlyFailedFilesRemoved, removedFailed);
            Interlocked.Add(ref _spaceFreed, spaceFreed);

            _logger.LogInformation(
                "Combined cleanup completed: {TimedOut} timed-out reset, {Failed} permanently failed removed, {SpaceFreed} bytes freed",
                resetCount, removedFailed, spaceFreed);
        }

        private async Task<int> ResetTimedOutForTenantAsync(
            string tenantId,
            DateTime timeoutCutoffUtc,
            DateTime nowUtc,
            CancellationToken ct = default)
        {
            int resetCount = 0;

            while (true)
            {
                var iterationStartedAt = Stopwatch.GetTimestamp();
                var batch = await _metadataRepository.GetProcessingTimedOutAsync(
                    tenantId,
                    timeoutCutoffUtc,
                    _statusCleanupBatchSizePerTenant,
                    ct);
                if (batch.Count == 0)
                {
                    RecordStatusCleanupIteration("timeout_reset", tenantId, 0, iterationStartedAt);
                    break;
                }

                foreach (var metadata in batch)
                {
                    if (!metadata.ProcessingStartTime.HasValue)
                        continue;

                    var reset = await _metadataRepository.TryResetTimedOutFileAsync(
                        tenantId,
                        metadata.FileKey,
                        metadata.ProcessingStartTime.Value,
                        nowUtc,
                        ct);
                    if (!reset)
                        continue;

                    resetCount++;
                    _logger.LogDebug("Reset timed-out file {FileKey} from Processing to Pending", metadata.FileKey);
                }

                RecordStatusCleanupIteration("timeout_reset", tenantId, batch.Count, iterationStartedAt);

                if (batch.Count < _statusCleanupBatchSizePerTenant)
                    break;
            }

            return resetCount;
        }

        private async Task<(int RemovedCount, long SpaceFreed)> CleanupPermanentlyFailedForTenantAsync(
            string tenantId,
            DateTime failedCutoffUtc,
            CancellationToken ct = default)
        {
            int removedCount = 0;
            long spaceFreed = 0;

            while (true)
            {
                var iterationStartedAt = Stopwatch.GetTimestamp();
                var batch = await _metadataRepository.GetPermanentlyFailedOlderThanAsync(
                    tenantId,
                    failedCutoffUtc,
                    _statusCleanupBatchSizePerTenant,
                    ct);
                if (batch.Count == 0)
                {
                    RecordStatusCleanupIteration("failed_cleanup", tenantId, 0, iterationStartedAt);
                    break;
                }

                var removedThisIteration = 0;

                foreach (var metadata in batch)
                {
                    var physicalFileRemoved = false;
                    if (_volumes.TryGetValue(metadata.VolumeId, out var volume))
                    {
                        try
                        {
                            if (_fileSystem.File.Exists(metadata.PhysicalPath))
                            {
                                await volume.DeleteAsync(metadata.PhysicalPath, ct);
                                physicalFileRemoved = !_fileSystem.File.Exists(metadata.PhysicalPath);
                                if (physicalFileRemoved)
                                    spaceFreed += metadata.FileSize;
                            }
                            else
                            {
                                physicalFileRemoved = true;
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to delete physical file {PhysicalPath}", metadata.PhysicalPath);
                        }
                    }
                    else if (!_fileSystem.File.Exists(metadata.PhysicalPath))
                    {
                        physicalFileRemoved = true;
                    }
                    else
                    {
                        _logger.LogWarning(
                            "Skipping permanently failed cleanup because volume {VolumeId} is unavailable and file still exists: {PhysicalPath}",
                            metadata.VolumeId,
                            metadata.PhysicalPath);
                    }

                    if (!physicalFileRemoved)
                        continue;

                    if (!metadata.LastFailedAt.HasValue)
                        continue;

                    var removed = await _metadataRepository.TryRemovePermanentlyFailedFileAsync(
                        metadata.TenantId,
                        metadata.FileKey,
                        metadata.LastFailedAt.Value,
                        ct);
                    if (!removed)
                        continue;

                    var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(metadata.DirectoryPath);
                    if (string.IsNullOrWhiteSpace(metadata.DirectoryPath))
                    {
                        _logger.LogWarning(
                            "Encountered empty directory path in metadata during cleanup. Falling back to root '/'. Tenant={TenantId}, FileKey={FileKey}",
                            metadata.TenantId,
                            metadata.FileKey);
                    }

                    // Decrement quotas in independent try-catch blocks.
                    // Metadata and physical file are already removed at this point; if a quota
                    // decrement fails the file is permanently gone but the counter is not updated.
                    // We log a warning and continue rather than re-throwing, which would halt the
                    // cleanup loop and leave subsequent permanently-failed files unprocessed.
                    try
                    {
                        await _quotaRepository.DecrementAsync(metadata.TenantId, normalizedDirectoryPath, default);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex,
                            "Failed to decrement directory quota after removing permanently-failed file. " +
                            "Tenant={TenantId}, Directory={DirectoryPath}, FileKey={FileKey}",
                            metadata.TenantId, normalizedDirectoryPath, metadata.FileKey);
                    }

                    try
                    {
                        await _tenantQuotaManager.DecrementFileCountAsync(metadata.TenantId, default);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex,
                            "Failed to decrement tenant quota after removing permanently-failed file. " +
                            "Tenant={TenantId}, FileKey={FileKey}",
                            metadata.TenantId, metadata.FileKey);
                    }

                    removedCount++;
                    removedThisIteration++;
                }

                RecordStatusCleanupIteration("failed_cleanup", tenantId, batch.Count, iterationStartedAt);

                if (removedThisIteration == 0)
                {
                    _logger.LogWarning(
                        "Stopping permanently-failed cleanup for tenant {TenantId} because the current batch made no progress",
                        tenantId);
                    break;
                }

                if (batch.Count < _statusCleanupBatchSizePerTenant)
                    break;
            }

            return (removedCount, spaceFreed);
        }

        private void RecordStatusCleanupIteration(
            string operation,
            string tenantId,
            int batchSize,
            long iterationStartedAt)
        {
            var elapsedStopwatchTicks = Stopwatch.GetTimestamp() - iterationStartedAt;
            var iteration = Interlocked.Increment(ref _statusCleanupIterationCount);
            Interlocked.Add(ref _statusCleanupIterationStopwatchTicks, elapsedStopwatchTicks);

            if (iteration % 128 != 1)
                return;

            var totalTicks = Interlocked.Read(ref _statusCleanupIterationStopwatchTicks);
            var avgMs = iteration > 0
                ? (totalTicks * 1000.0 / Stopwatch.Frequency) / iteration
                : 0;
            var elapsedMs = elapsedStopwatchTicks * 1000.0 / Stopwatch.Frequency;

            _logger.LogInformation(
                "Status cleanup iteration metrics: Operation={Operation}, Tenant={TenantId}, BatchSize={BatchSize}, IterationMs={IterationMs:F3}, AvgIterationMs={AvgIterationMs:F3}, TotalIterations={TotalIterations}",
                operation,
                tenantId,
                batchSize,
                elapsedMs,
                avgMs,
                iteration);
        }

        /// <inheritdoc/>
        public Task<CleanupStatistics> GetCleanupStatisticsAsync(CancellationToken ct = default)
        {
            return Task.FromResult(new CleanupStatistics
            {
                EmptyDirectoriesRemoved = Volatile.Read(ref _emptyDirectoriesRemoved),
                CompletedRecordsRemoved = Volatile.Read(ref _completedRecordsRemoved),
                PermanentlyFailedFilesRemoved = Volatile.Read(ref _permanentlyFailedFilesRemoved),
                OrphanedFilesRemoved = Volatile.Read(ref _orphanedFilesRemoved),
                TimedOutFilesReset = Volatile.Read(ref _timedOutFilesReset),
                SpaceFreed = Interlocked.Read(ref _spaceFreed)
            });
        }

        /// <inheritdoc/>
        public async Task<DatabaseOptimizationResult> OptimizeDatabasesAsync(CancellationToken ct = default)
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
            var metadataProcessed = 0;
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

                metadataProcessed++;
                if (_databaseOptimizationPauseBetweenBatches > TimeSpan.Zero
                    && metadataProcessed % _databaseOptimizationTenantBatchSize == 0)
                {
                    await Task.Delay(_databaseOptimizationPauseBetweenBatches, ct);
                }
            }

            // Get all tenant IDs from quota databases
            var quotaTenantIds = await _quotaRepository.GetAllTenantIdsAsync(ct);

            // Optimize quota databases (per-tenant, thread-safe)
            var quotaProcessed = 0;
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

                quotaProcessed++;
                if (_databaseOptimizationPauseBetweenBatches > TimeSpan.Zero
                    && quotaProcessed % _databaseOptimizationTenantBatchSize == 0)
                {
                    await Task.Delay(_databaseOptimizationPauseBetweenBatches, ct);
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
        /// Cleans up SQLite corruption backup files left over from database rebuild operations.
        /// These are files named like "metadata.db.corrupted.{timestamp}" or "quotas.db.corrupted.{timestamp}"
        /// that accumulate inside each tenant's subdirectory when a corrupted database is rebuilt.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of backup files removed and space freed in bytes.</returns>
        public Task<(int FilesRemoved, long SpaceFreed)> CleanupInvalidDatabaseFilesAsync(CancellationToken ct = default)
        {
            _logger.LogInformation("Starting cleanup of SQLite corruption backup files...");

            var (metaFiles, metaSpace) = CleanupCorruptedDatabaseBackups(_metadataDirectory, ct);
            var (quotaFiles, quotaSpace) = CleanupCorruptedDatabaseBackups(_quotaDirectory, ct);

            var filesRemoved = metaFiles + quotaFiles;
            var spaceFreed = metaSpace + quotaSpace;

            _logger.LogInformation("Corruption backup cleanup completed. Files removed: {FilesRemoved}, Space freed: {SpaceMB:F2} MB",
                filesRemoved, spaceFreed / 1024.0 / 1024.0);

            return Task.FromResult((filesRemoved, spaceFreed));
        }

        /// <summary>
        /// Cleans up SQLite corruption backup files (*.corrupted.*) inside each tenant subdirectory.
        /// Each tenant directory lives under <paramref name="rootDirectory"/>/{tenantId}/.
        /// </summary>
        private (int FilesRemoved, long SpaceFreed) CleanupCorruptedDatabaseBackups(string rootDirectory, CancellationToken ct = default)
        {
            var filesRemoved = 0;
            long spaceFreed = 0;

            if (!_fileSystem.Directory.Exists(rootDirectory))
                return (0, 0);

            // Each tenant has its own subdirectory; scan all of them.
            var tenantDirs = _fileSystem.Directory.GetDirectories(rootDirectory);
            foreach (var tenantDir in tenantDirs)
            {
                ct.ThrowIfCancellationRequested();

                var allFiles = _fileSystem.Directory.GetFiles(tenantDir);
                foreach (var filePath in allFiles)
                {
                    ct.ThrowIfCancellationRequested();

                    var fileName = _fileSystem.Path.GetFileName(filePath);

                    // Corruption backup files: metadata.db.corrupted.{yyyyMMddHHmmss}
                    if (fileName.IndexOf(".corrupted.", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        try
                        {
                            var fileSize = _fileSystem.FileInfo.New(filePath).Length;
                            _fileSystem.File.Delete(filePath);
                            filesRemoved++;
                            spaceFreed += fileSize;
                            _logger.LogDebug("Deleted corruption backup file: {FileName} ({SizeKB:F2} KB)", fileName, fileSize / 1024.0);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to delete corruption backup file: {FileName}", fileName);
                        }
                    }
                }
            }

            return (filesRemoved, spaceFreed);
        }

        // Maximum directory nesting depth explored by CleanupJunkFilesRecursiveAsync.
        // Prevents stack overflow on pathological directory trees (e.g. symlink cycles or
        // extremely deep sharding hierarchies).  Normal storage depth is 3-5 levels.
        private const int MaxCleanupRecursionDepth = 20;

        /// <summary>
        /// Recursively removes junk files and empty directories, bottom-up.
        /// A directory is considered empty when it contains no files (other than junk files that are
        /// deleted) and no subdirectories survive after recursive processing.
        /// The <paramref name="isProtectedRoot"/> flag prevents the top-level tenant or volume directory
        /// from being deleted even when it is empty -- only its children are cleaned up.
        /// </summary>
        /// <returns>Number of directories deleted.</returns>
        private async Task<int> CleanupJunkFilesRecursiveAsync(
            string directoryPath,
            CancellationToken ct,
            bool isProtectedRoot = false,
            int shardingDepth = 0,
            int depth = 0)
        {
            if (!_fileSystem.Directory.Exists(directoryPath))
                return 0;

            if (depth > MaxCleanupRecursionDepth)
            {
                _logger.LogWarning(
                    "Cleanup recursion depth limit ({Limit}) reached at {DirectoryPath}; skipping deeper subdirectories",
                    MaxCleanupRecursionDepth, directoryPath);
                return 0;
            }

            var removedCount = 0;

            // 1. Process subdirectories first (bottom-up so parents can be emptied).
            var subdirectories = _fileSystem.Directory.GetDirectories(directoryPath);
            foreach (var subdirectory in subdirectories)
            {
                removedCount += await CleanupJunkFilesRecursiveAsync(subdirectory, ct, isProtectedRoot: false, shardingDepth: shardingDepth, depth: depth + 1);
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

            // 3. Delete the directory itself if it is now empty -- but never delete:
            //    - protected roots (volume mount paths, tenant root directories)
            //    - shard directories (depth <= shardingDepth, where depth 0 = tenant root)
            //    Directory structure: {MountPath}/{TenantId(depth=0)}/{Shard0(depth=1)}/{Shard1(depth=2)}/...
            var isShardDirectory = depth <= shardingDepth;
            if (!isProtectedRoot && !isShardDirectory && IsDirectoryEmpty(directoryPath))
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

        private sealed class CleanupTenantContext : ITenantContext
        {
            public CleanupTenantContext(string tenantId)
            {
                TenantId = tenantId ?? throw new ArgumentNullException(nameof(tenantId));
            }

            public string TenantId { get; }

            public TenantStatus Status => TenantStatus.Enabled;
        }
    }
}
