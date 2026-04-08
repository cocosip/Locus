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
        private readonly IQueueProjectionStore _projectionStore;
        private readonly IQueueProjectionCleanupStore _projectionCleanupStore;
        private readonly IMetadataProjectionMaintenanceStore _metadataMaintenanceStore;
        private readonly IQuotaProjectionMaintenanceStore _quotaMaintenanceStore;
        private readonly ITenantQuotaManager _tenantQuotaManager;
        private readonly IDirectoryQuotaManager? _directoryQuotaManager;
        private readonly ITenantManager? _tenantManager;
        private readonly IQueueEventJournal? _queueEventJournal;
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
        private readonly ConcurrentDictionary<string, OrphanDirectoryScanState> _orphanDirectoryScanStates;
        private readonly ConcurrentDictionary<string, DateTime> _orphanScanLastRunUtc;
        private long _statusCleanupIterationCount;
        private long _statusCleanupIterationStopwatchTicks;
        private int _emptyDirectoriesRemoved;
        private int _completedRecordsRemoved;
        private int _permanentlyFailedFilesRemoved;
        private int _orphanedFilesRemoved;
        private int _orphanedFilesRecovered;
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
            ITenantManager? tenantManager = null,
            IQueueEventJournal? queueEventJournal = null,
            IDirectoryQuotaManager? directoryQuotaManager = null,
            IQueueProjectionStore? projectionStore = null,
            IQueueProjectionCleanupStore? projectionCleanupStore = null,
            IMetadataProjectionMaintenanceStore? metadataMaintenanceStore = null,
            IQuotaProjectionMaintenanceStore? quotaMaintenanceStore = null)
        {
            if (metadataRepository == null)
                throw new ArgumentNullException(nameof(metadataRepository));
            if (quotaRepository == null)
                throw new ArgumentNullException(nameof(quotaRepository));

            _projectionStore = projectionStore ?? new MetadataRepositoryQueueProjectionStore(metadataRepository);
            _projectionCleanupStore = projectionCleanupStore ?? new MetadataRepositoryQueueProjectionCleanupStore(metadataRepository);
            _metadataMaintenanceStore = metadataMaintenanceStore ?? new MetadataRepositoryProjectionMaintenanceStore(metadataRepository);
            _quotaMaintenanceStore = quotaMaintenanceStore ?? new DirectoryQuotaRepositoryProjectionMaintenanceStore(quotaRepository);
            _tenantQuotaManager = tenantQuotaManager ?? throw new ArgumentNullException(nameof(tenantQuotaManager));
            _directoryQuotaManager = directoryQuotaManager;
            _tenantManager = tenantManager;
            _queueEventJournal = queueEventJournal;
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _volumes = new ConcurrentDictionary<string, IStorageVolume>();
            _volumeRegistry = volumeRegistry ?? new StorageVolumeRegistry();
            _orphanScanQueues = new ConcurrentDictionary<string, ConcurrentQueue<string>>();
            _orphanScanLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
            _orphanDirectoryScanStates = new ConcurrentDictionary<string, OrphanDirectoryScanState>();
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
                    foreach (var subdirectory in _fileSystem.Directory.EnumerateDirectories(volume.MountPath))
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

            var tenantIds = await GetMetadataTenantIdsSnapshotAsync(ct);
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
        public async Task CleanupCompletedFilesAsync(TimeSpan olderThan, CancellationToken ct = default)
        {
            _logger.LogInformation("Starting cleanup of completed files older than {TimeSpan}", olderThan);

            var cutoffTime = DateTime.UtcNow - olderThan;
            var removedCount = 0;
            long spaceFreed = 0;

            var tenantIds = await GetMetadataTenantIdsSnapshotAsync(ct);
            foreach (var tenantId in tenantIds)
            {
                ct.ThrowIfCancellationRequested();
                var tenantResult = await CleanupCompletedForTenantAsync(tenantId, cutoffTime, ct);
                removedCount += tenantResult.RemovedCount;
                spaceFreed += tenantResult.SpaceFreed;
            }

            Interlocked.Add(ref _completedRecordsRemoved, removedCount);
            Interlocked.Add(ref _spaceFreed, spaceFreed);
            _logger.LogInformation("Cleaned up {Count} completed files, freed {Size} bytes",
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
                var knownPhysicalPaths = await BuildKnownPhysicalPathSetAsync(tenant.TenantId, ct).ConfigureAwait(false);
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
                        {
                            ClearOrphanDirectoryScanState(scanKey, directory);
                            continue;
                        }

                        try
                        {
                            var directoryScanState = GetOrphanDirectoryScanState(scanKey, directory);
                            var resumeAfterPath = directoryScanState.ResumeAfterPath;
                            var directoryFullyScanned = true;

                            var fileEntries = directoryScanState.SortedEntries ?? EnumerateSortedOrphanScanEntries(directory);
                            directoryScanState.SortedEntries = fileEntries;
                            var resumeIndex = FindOrphanScanStartIndex(fileEntries, resumeAfterPath);

                            for (var fileIndex = resumeIndex; fileIndex < fileEntries.Count; fileIndex++)
                            {
                                var fileEntry = fileEntries[fileIndex];

                                ct.ThrowIfCancellationRequested();
                                scannedThisRun++;

                                directoryScanState.ResumeAfterPath = fileEntry.NormalizedPath;

                                var physicalPath = fileEntry.PhysicalPath;
                                var normalizedPhysical = fileEntry.NormalizedPath;
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
                                        metadataExists = knownPhysicalPaths.Contains(normalizedPhysical);

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
                                        directoryFullyScanned = false;
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

                                    var tenantReservationConsumed = false;
                                    var directoryReservationConsumed = false;
                                    try
                                    {
                                        tenantReservationConsumed = await ApplyAcceptedTenantProjectionAsync(metadata.TenantId, default);
                                        directoryReservationConsumed = await ApplyAcceptedDirectoryProjectionAsync(
                                            metadata.TenantId,
                                            metadata.DirectoryPath,
                                            default);
                                        QueueProjectionMetadataState.MarkAcceptedProjectionApplied(metadata);

                                        // Persist rebuilt metadata only after quota compensation succeeds so we never
                                        // re-queue a file whose quota counters still under-report real usage.
                                        await _projectionStore.UpsertProjectedFileAsync(metadata, ct).ConfigureAwait(false);
                                    }
                                    catch
                                    {
                                        await RollbackRebuiltFileQuotaCompensationAsync(
                                            metadata,
                                            tenantReservationConsumed,
                                            directoryReservationConsumed);
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

                                    if (normalizedPhysical != null)
                                        knownPhysicalPaths.Add(normalizedPhysical);

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
                                    directoryFullyScanned = false;
                                    scanQueue.Enqueue(directory);
                                    budgetReached = true;
                                    break;
                                }
                            }

                            if (directoryFullyScanned)
                                ClearOrphanDirectoryScanState(scanKey, directory);
                        }
                        catch (Exception ex)
                        {
                            ClearOrphanDirectoryScanState(scanKey, directory);
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

            Interlocked.Add(ref _orphanedFilesRecovered, rebuiltCount);

            if (rebuiltCount > 0)
            {
                await PruneZeroCountUnlimitedQuotaEntriesAsync(tenant.TenantId, ct);
                _logger.LogInformation(
                    "Orphaned file rebuild complete for tenant {TenantId}: {Count} file(s) re-queued as Pending",
                    tenant.TenantId, rebuiltCount);
            }
            else
            {
                _logger.LogDebug("No orphaned files found for tenant {TenantId}", tenant.TenantId);
            }
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

                try
                {
                    foreach (var tenantDirectory in _fileSystem.Directory.EnumerateDirectories(volume.MountPath))
                    {
                        ct.ThrowIfCancellationRequested();

                        var tenantId = _fileSystem.Path.GetFileName(tenantDirectory);
                        if (!string.IsNullOrWhiteSpace(tenantId))
                            tenantIds.Add(tenantId);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to enumerate tenant directories under volume {VolumeId}", volume.VolumeId);
                    continue;
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

            var tenant = await _tenantManager.TryGetTenantAsync(tenantId, ct);
            if (tenant == null)
            {
                _logger.LogInformation(
                    "Skipping orphaned-file rebuild for tenant {TenantId} because tenant metadata was not found",
                    tenantId);
                return null;
            }

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

        private string GetOrphanScanKey(string tenantId, string volumeId)
        {
            return $"{tenantId}\u001F{volumeId}";
        }

        private string GetOrphanDirectoryScanStateKey(string scanKey, string directoryPath)
        {
            return $"{scanKey}\u001F{NormalizePath(directoryPath) ?? directoryPath}";
        }

        private ConcurrentQueue<string> GetOrphanScanQueue(string scanKey, string tenantPath)
        {
            var queue = _orphanScanQueues.GetOrAdd(scanKey, _ => new ConcurrentQueue<string>());
            if (queue.IsEmpty)
                queue.Enqueue(tenantPath);
            return queue;
        }

        private OrphanDirectoryScanState GetOrphanDirectoryScanState(string scanKey, string directoryPath)
        {
            return _orphanDirectoryScanStates.GetOrAdd(
                GetOrphanDirectoryScanStateKey(scanKey, directoryPath),
                _ => new OrphanDirectoryScanState());
        }

        private void ClearOrphanDirectoryScanState(string scanKey, string directoryPath)
        {
            _orphanDirectoryScanStates.TryRemove(GetOrphanDirectoryScanStateKey(scanKey, directoryPath), out _);
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

        private sealed class OrphanDirectoryScanState
        {
            public string? ResumeAfterPath { get; set; }
            public List<OrphanFileScanEntry>? SortedEntries { get; set; }
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
            var directoryPath = DirectoryPathNormalizer.Normalize(null);

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

        private async Task<bool> ApplyAcceptedTenantProjectionAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
            {
                return await tenantQuotaProjectionManager.ApplyAcceptedProjectionAsync(tenantId, ct);
            }

            if (_tenantQuotaManager is ITenantQuotaCompensationManager tenantQuotaCompensationManager)
            {
                await tenantQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, ct);
                return false;
            }

            await _tenantQuotaManager.IncrementFileCountAsync(tenantId, ct);
            return false;
        }

        private async Task<bool> ApplyAcceptedDirectoryProjectionAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(directoryPath);
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                return await directoryQuotaProjectionManager.ApplyAcceptedProjectionAsync(tenantId, normalizedDirectoryPath, ct);
            }

            await _quotaMaintenanceStore.ForceIncrementAsync(tenantId, normalizedDirectoryPath, ct);
            return false;
        }

        private async Task RollbackAcceptedTenantProjectionAsync(
            string tenantId,
            bool restoreReservation,
            CancellationToken ct = default)
        {
            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
            {
                await tenantQuotaProjectionManager.RollbackAcceptedProjectionAsync(tenantId, restoreReservation, ct);
                return;
            }

            if (restoreReservation)
                await _tenantQuotaManager.DecrementFileCountAsync(tenantId, ct);
        }

        private async Task RollbackAcceptedDirectoryProjectionAsync(
            string tenantId,
            string directoryPath,
            bool restoreReservation,
            CancellationToken ct = default)
        {
            var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(directoryPath);
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                await directoryQuotaProjectionManager.RollbackAcceptedProjectionAsync(
                    tenantId,
                    normalizedDirectoryPath,
                    restoreReservation,
                    ct);
                return;
            }

            if (restoreReservation)
                await _quotaMaintenanceStore.DecrementAsync(tenantId, normalizedDirectoryPath, ct);
        }

        private async Task ApplyDeleteSucceededTenantProjectionAsync(string tenantId, CancellationToken ct = default)
        {
            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
            {
                await tenantQuotaProjectionManager.ApplyDeleteSucceededProjectionAsync(tenantId, ct);
                return;
            }

            await _tenantQuotaManager.DecrementFileCountAsync(tenantId, ct);
        }

        private async Task ApplyDeleteSucceededDirectoryProjectionAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(directoryPath);
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                await directoryQuotaProjectionManager.ApplyDeleteSucceededProjectionAsync(tenantId, normalizedDirectoryPath, ct);
                return;
            }

            await _quotaMaintenanceStore.DecrementAsync(tenantId, normalizedDirectoryPath, ct);
        }

        private async Task RollbackDeleteSucceededTenantProjectionAsync(string tenantId, CancellationToken ct = default)
        {
            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
            {
                await tenantQuotaProjectionManager.RollbackDeleteSucceededProjectionAsync(tenantId, ct);
                return;
            }

            if (_tenantQuotaManager is ITenantQuotaCompensationManager tenantQuotaCompensationManager)
            {
                await tenantQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, ct);
                return;
            }

            await _tenantQuotaManager.IncrementFileCountAsync(tenantId, ct);
        }

        private async Task RollbackDeleteSucceededDirectoryProjectionAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(directoryPath);
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                await directoryQuotaProjectionManager.RollbackDeleteSucceededProjectionAsync(tenantId, normalizedDirectoryPath, ct);
                return;
            }

            await _quotaMaintenanceStore.ForceIncrementAsync(tenantId, normalizedDirectoryPath, ct);
        }

        private async Task RollbackRebuiltFileQuotaCompensationAsync(
            FileMetadata metadata,
            bool tenantRestoreReservation,
            bool directoryRestoreReservation)
        {
            var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(metadata.DirectoryPath);

            if (directoryRestoreReservation)
            {
                try
                {
                    await RollbackAcceptedDirectoryProjectionAsync(
                        metadata.TenantId,
                        normalizedDirectoryPath,
                        directoryRestoreReservation,
                        default);
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

            if (tenantRestoreReservation)
            {
                try
                {
                    await RollbackAcceptedTenantProjectionAsync(metadata.TenantId, tenantRestoreReservation, default);
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
        public async Task ReconcileQuotaCountsAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var metadataEntries = (await _projectionStore.GetProjectedFilesAsync(tenantId, ct).ConfigureAwait(false)).ToArray();
            var expectedTenantCount = metadataEntries.Length;
            var expectedDirectoryCounts = metadataEntries
                .GroupBy(
                    metadata => DirectoryPathNormalizer.Normalize(metadata.DirectoryPath),
                    StringComparer.Ordinal)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

            var existingDirectoryQuotas = (await _quotaMaintenanceStore.GetQuotaRowsAsync(tenantId, ct))
                .Where(quota => !string.IsNullOrWhiteSpace(quota.DirectoryPath)
                    && !string.Equals(quota.DirectoryPath, tenantId, StringComparison.Ordinal))
                .ToDictionary(quota => quota.DirectoryPath, quota => quota, StringComparer.Ordinal);

            var knownDirectoryPaths = new HashSet<string>(existingDirectoryQuotas.Keys, StringComparer.Ordinal);

            foreach (var directoryPath in expectedDirectoryCounts.Keys)
                knownDirectoryPaths.Add(directoryPath);

            foreach (var directoryPath in knownDirectoryPaths)
            {
                var expectedDirectoryCount = expectedDirectoryCounts.TryGetValue(directoryPath, out var count)
                    ? count
                    : 0;

                if (expectedDirectoryCount == 0
                    && existingDirectoryQuotas.TryGetValue(directoryPath, out var existingQuota)
                    && !HasExplicitQuotaLimit(existingQuota))
                {
                    await _quotaMaintenanceStore.RemoveQuotaAsync(tenantId, directoryPath, ct);
                    continue;
                }

                if (_directoryQuotaManager is IDirectoryQuotaReconciliationManager directoryQuotaReconciliationManager)
                {
                    await directoryQuotaReconciliationManager.SetFileCountAsync(tenantId, directoryPath, expectedDirectoryCount, ct);
                }
                else
                {
                    await _quotaMaintenanceStore.SetProjectedCountAsync(tenantId, directoryPath, expectedDirectoryCount, ct);
                }
            }

            if (_tenantQuotaManager is ITenantQuotaReconciliationManager tenantQuotaReconciliationManager)
                await tenantQuotaReconciliationManager.SetFileCountAsync(tenantId, expectedTenantCount, ct);
        }

        /// <inheritdoc/>
        public async Task ReconcileAllQuotaCountsAsync(CancellationToken ct = default)
        {
            var reconcileTenantIds = new HashSet<string>(
                await GetMetadataTenantIdsSnapshotAsync(ct),
                StringComparer.Ordinal);

            if (_tenantManager != null)
            {
                foreach (var quotaTenantId in await GetQuotaTenantIdsSnapshotAsync(ct))
                {
                    ct.ThrowIfCancellationRequested();
                    var tenant = await _tenantManager.TryGetTenantAsync(quotaTenantId, ct);
                    if (tenant != null)
                        reconcileTenantIds.Add(quotaTenantId);
                }
            }

            foreach (var tenantId in reconcileTenantIds)
            {
                ct.ThrowIfCancellationRequested();
                await ReconcileQuotaCountsAsync(tenantId, ct);
            }
        }

        private async Task PruneZeroCountUnlimitedQuotaEntriesAsync(string tenantId, CancellationToken ct = default)
        {
            var quotas = (await _quotaMaintenanceStore.GetQuotaRowsAsync(tenantId, ct)).ToArray();

            foreach (var quota in quotas)
            {
                ct.ThrowIfCancellationRequested();

                if (string.IsNullOrWhiteSpace(quota.DirectoryPath))
                    continue;

                if (quota.CurrentCount != 0)
                    continue;

                if (HasExplicitQuotaLimit(quota))
                    continue;

                    await _quotaMaintenanceStore.RemoveQuotaAsync(tenantId, quota.DirectoryPath, ct);
            }
        }

        private static bool HasExplicitQuotaLimit(DirectoryQuota quota)
        {
            if (quota == null)
                throw new ArgumentNullException(nameof(quota));

            return quota.MaxCount > 0;
        }

        /// <inheritdoc/>
        public async Task CleanupTimedOutProcessingFilesAsync(TimeSpan timeout, CancellationToken ct = default)
        {
            _logger.LogInformation("Starting cleanup of timed-out processing files (timeout: {Timeout})", timeout);

            var cutoffTime = DateTime.UtcNow - timeout;
            var resetCount = 0;
            var tenantIds = await GetMetadataTenantIdsSnapshotAsync(ct);

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

            var tenantIds = await GetMetadataTenantIdsSnapshotAsync(ct);
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
                var batch = await _projectionCleanupStore.GetTimedOutProcessingFilesAsync(
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

                    var reset = await _projectionCleanupStore.TryResetTimedOutFileAsync(
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

        private async Task<(int RemovedCount, long SpaceFreed)> CleanupCompletedForTenantAsync(
            string tenantId,
            DateTime completedCutoffUtc,
            CancellationToken ct = default)
        {
            int removedCount = 0;
            long spaceFreed = 0;
            var physicalPathExistsCache = new Dictionary<string, bool>(_pathComparer);
            var blockedFileKeys = new HashSet<string>(StringComparer.Ordinal);

            while (true)
            {
                var iterationStartedAt = Stopwatch.GetTimestamp();
                var batch = _queueEventJournal != null
                    ? await _projectionCleanupStore.GetDeleteRequestedFilesOlderThanAsync(
                        tenantId,
                        completedCutoffUtc,
                        _statusCleanupBatchSizePerTenant,
                        blockedFileKeys,
                        ct)
                    : await _projectionCleanupStore.GetCompletedFilesOlderThanAsync(
                        tenantId,
                        completedCutoffUtc,
                        _statusCleanupBatchSizePerTenant,
                        blockedFileKeys,
                        ct);
                if (batch.Count == 0)
                {
                    RecordStatusCleanupIteration("completed_cleanup", tenantId, 0, iterationStartedAt);
                    break;
                }

                foreach (var metadata in batch)
                {
                    if (!metadata.CompletedAt.HasValue)
                    {
                        blockedFileKeys.Add(metadata.FileKey);
                        continue;
                    }

                    var physicalFileExists = GetCachedPhysicalFileExists(metadata.PhysicalPath, physicalPathExistsCache);
                    var hasVolume = _volumes.TryGetValue(metadata.VolumeId, out var volume);
                    if (physicalFileExists && !hasVolume)
                    {
                        _logger.LogWarning(
                            "Skipping completed cleanup because volume {VolumeId} is unavailable and file still exists: {PhysicalPath}",
                            metadata.VolumeId,
                            metadata.PhysicalPath);
                        blockedFileKeys.Add(metadata.FileKey);
                        continue;
                    }

                    try
                    {
                        if (physicalFileExists)
                        {
                            try
                            {
                                await volume!.DeleteAsync(metadata.PhysicalPath, ct);
                                physicalPathExistsCache[metadata.PhysicalPath] = false;
                            }
                            catch (Exception ex) when (!RefreshCachedPhysicalFileExists(metadata.PhysicalPath, physicalPathExistsCache))
                            {
                                _logger.LogDebug(
                                    ex,
                                    "DeleteAsync threw after removing completed file {PhysicalPath}",
                                    metadata.PhysicalPath);
                            }

                            if (RefreshCachedPhysicalFileExists(metadata.PhysicalPath, physicalPathExistsCache))
                                throw new IOException($"Failed to delete physical file {metadata.PhysicalPath}");

                            spaceFreed += metadata.FileSize;
                        }

                        if (_queueEventJournal != null)
                        {
                            var deleteSucceededAtUtc = DateTime.UtcNow;
                            await _queueEventJournal.AppendAsync(
                                CreateDeleteSucceededEvent(metadata, deleteSucceededAtUtc),
                                default).ConfigureAwait(false);

                            await _projectionCleanupStore.TryMarkDeleteSucceededAsync(
                                metadata.TenantId,
                                metadata.FileKey,
                                metadata.CompletedAt.Value,
                                deleteSucceededAtUtc,
                                ct).ConfigureAwait(false);
                        }
                        else
                        {
                            await FinalizeCompletedWithoutJournalAsync(metadata, ct).ConfigureAwait(false);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(
                            ex,
                            "Failed to cleanup completed file. Tenant={TenantId}, FileKey={FileKey}",
                            metadata.TenantId,
                            metadata.FileKey);
                        blockedFileKeys.Add(metadata.FileKey);
                        continue;
                    }

                    blockedFileKeys.Add(metadata.FileKey);
                    removedCount++;
                }

                RecordStatusCleanupIteration("completed_cleanup", tenantId, batch.Count, iterationStartedAt);

                if (batch.Count < _statusCleanupBatchSizePerTenant)
                    break;
            }

            return (removedCount, spaceFreed);
        }

        private async Task<(int RemovedCount, long SpaceFreed)> CleanupPermanentlyFailedForTenantAsync(
            string tenantId,
            DateTime failedCutoffUtc,
            CancellationToken ct = default)
        {
            int removedCount = 0;
            long spaceFreed = 0;
            var physicalPathExistsCache = new Dictionary<string, bool>(_pathComparer);
            var blockedFileKeys = new HashSet<string>(StringComparer.Ordinal);

            while (true)
            {
                var iterationStartedAt = Stopwatch.GetTimestamp();
                var batch = await _projectionCleanupStore.GetPermanentlyFailedFilesOlderThanAsync(
                    tenantId,
                    failedCutoffUtc,
                    _statusCleanupBatchSizePerTenant,
                    blockedFileKeys,
                    ct);
                if (batch.Count == 0)
                {
                    RecordStatusCleanupIteration("failed_cleanup", tenantId, 0, iterationStartedAt);
                    break;
                }

                var removedThisIteration = 0;

                foreach (var metadata in batch)
                {
                    if (!metadata.LastFailedAt.HasValue)
                    {
                        blockedFileKeys.Add(metadata.FileKey);
                        continue;
                    }

                    var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(metadata.DirectoryPath);
                    if (string.IsNullOrWhiteSpace(metadata.DirectoryPath))
                    {
                        _logger.LogWarning(
                            "Encountered empty directory path in metadata during cleanup. Falling back to root '/'. Tenant={TenantId}, FileKey={FileKey}",
                            metadata.TenantId,
                            metadata.FileKey);
                    }

                    var physicalFileExists = GetCachedPhysicalFileExists(metadata.PhysicalPath, physicalPathExistsCache);
                    var hasVolume = _volumes.TryGetValue(metadata.VolumeId, out var volume);
                    if (physicalFileExists && !hasVolume)
                    {
                        _logger.LogWarning(
                            "Skipping permanently failed cleanup because volume {VolumeId} is unavailable and file still exists: {PhysicalPath}",
                            metadata.VolumeId,
                            metadata.PhysicalPath);
                        blockedFileKeys.Add(metadata.FileKey);
                        continue;
                    }

                    var directoryQuotaDecremented = false;
                    var tenantQuotaDecremented = false;
                    var metadataRemoved = false;
                    try
                    {
                        await ApplyDeleteSucceededDirectoryProjectionAsync(metadata.TenantId, normalizedDirectoryPath, default);
                        directoryQuotaDecremented = true;

                        await ApplyDeleteSucceededTenantProjectionAsync(metadata.TenantId, default);
                        tenantQuotaDecremented = true;

                        metadataRemoved = await _projectionCleanupStore.TryRemovePermanentlyFailedFileAsync(
                            metadata.TenantId,
                            metadata.FileKey,
                            metadata.LastFailedAt.Value,
                            ct);
                        if (!metadataRemoved)
                        {
                            blockedFileKeys.Add(metadata.FileKey);
                            await RollbackPermanentlyFailedCleanupAsync(
                                metadata,
                                normalizedDirectoryPath,
                                metadataRemoved: false,
                                tenantQuotaDecremented,
                                directoryQuotaDecremented);
                            continue;
                        }

                        if (physicalFileExists)
                        {
                            try
                            {
                                await volume!.DeleteAsync(metadata.PhysicalPath, ct);
                                physicalPathExistsCache[metadata.PhysicalPath] = false;
                            }
                            catch (Exception ex) when (!RefreshCachedPhysicalFileExists(metadata.PhysicalPath, physicalPathExistsCache))
                            {
                                _logger.LogDebug(
                                    ex,
                                    "DeleteAsync threw after removing permanently-failed file {PhysicalPath}",
                                    metadata.PhysicalPath);
                            }

                            if (RefreshCachedPhysicalFileExists(metadata.PhysicalPath, physicalPathExistsCache))
                                throw new IOException($"Failed to delete physical file {metadata.PhysicalPath}");

                            spaceFreed += metadata.FileSize;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex,
                            "Failed to cleanup permanently-failed file. Tenant={TenantId}, Directory={DirectoryPath}, FileKey={FileKey}",
                            metadata.TenantId, normalizedDirectoryPath, metadata.FileKey);
                        blockedFileKeys.Add(metadata.FileKey);
                        await RollbackPermanentlyFailedCleanupAsync(
                            metadata,
                            normalizedDirectoryPath,
                            metadataRemoved,
                            tenantQuotaDecremented,
                            directoryQuotaDecremented);
                        continue;
                    }

                    removedCount++;
                    removedThisIteration++;
                }

                RecordStatusCleanupIteration("failed_cleanup", tenantId, batch.Count, iterationStartedAt);

                if (batch.Count < _statusCleanupBatchSizePerTenant)
                    break;
            }

            return (removedCount, spaceFreed);
        }

        private async Task FinalizeCompletedWithoutJournalAsync(FileMetadata metadata, CancellationToken ct)
        {
            var normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(metadata.DirectoryPath);
            var directoryQuotaDecremented = false;
            var tenantQuotaDecremented = false;
            var metadataRemoved = false;

            try
            {
                await ApplyDeleteSucceededDirectoryProjectionAsync(metadata.TenantId, normalizedDirectoryPath, default);
                directoryQuotaDecremented = true;

                await ApplyDeleteSucceededTenantProjectionAsync(metadata.TenantId, default);
                tenantQuotaDecremented = true;

                metadataRemoved = await _projectionCleanupStore.TryRemoveCompletedFileAsync(
                    metadata.TenantId,
                    metadata.FileKey,
                    metadata.CompletedAt!.Value,
                    ct);
                if (!metadataRemoved)
                    throw new InvalidOperationException($"Failed to remove completed metadata for file {metadata.FileKey}.");
            }
            catch
            {
                await RollbackPermanentlyFailedCleanupAsync(
                    metadata,
                    normalizedDirectoryPath,
                    metadataRemoved,
                    tenantQuotaDecremented,
                    directoryQuotaDecremented);
                throw;
            }
        }

        private static QueueEventRecord CreateDeleteSucceededEvent(FileMetadata metadata, DateTime deleteSucceededAtUtc)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.DeleteSucceeded,
                OccurredAtUtc = deleteSucceededAtUtc,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.DeleteSucceeded,
                ProcessingStartTimeUtc = metadata.ProcessingStartTime,
                RetryCount = metadata.RetryCount,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension
            };
        }

        private bool GetCachedPhysicalFileExists(string physicalPath, IDictionary<string, bool> cache)
        {
            if (cache.TryGetValue(physicalPath, out var exists))
                return exists;

            exists = _fileSystem.File.Exists(physicalPath);
            cache[physicalPath] = exists;
            return exists;
        }

        private bool RefreshCachedPhysicalFileExists(string physicalPath, IDictionary<string, bool> cache)
        {
            var exists = _fileSystem.File.Exists(physicalPath);
            cache[physicalPath] = exists;
            return exists;
        }

        private async Task RollbackPermanentlyFailedCleanupAsync(
            FileMetadata metadata,
            string normalizedDirectoryPath,
            bool metadataRemoved,
            bool tenantQuotaDecremented,
            bool directoryQuotaDecremented)
        {
            if (directoryQuotaDecremented)
            {
                try
                {
                    await RollbackDeleteSucceededDirectoryProjectionAsync(metadata.TenantId, normalizedDirectoryPath, default);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to restore directory quota after permanently-failed cleanup rollback. Tenant={TenantId}, Directory={DirectoryPath}, FileKey={FileKey}",
                        metadata.TenantId,
                        normalizedDirectoryPath,
                        metadata.FileKey);
                }
            }

            if (tenantQuotaDecremented)
            {
                try
                {
                    await RollbackDeleteSucceededTenantProjectionAsync(metadata.TenantId, default);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to restore tenant quota after permanently-failed cleanup rollback. Tenant={TenantId}, FileKey={FileKey}",
                        metadata.TenantId,
                        metadata.FileKey);
                }
            }

            if (metadataRemoved)
            {
                try
                {
                    await _projectionStore.UpsertProjectedFileAsync(metadata, default).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to restore metadata after permanently-failed cleanup rollback. Tenant={TenantId}, FileKey={FileKey}",
                        metadata.TenantId,
                        metadata.FileKey);
                }
            }
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
                OrphanedFilesRecovered = Volatile.Read(ref _orphanedFilesRecovered),
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
            var metadataTenantIds = await GetMetadataTenantIdsSnapshotAsync(ct);

            // Optimize metadata databases (per-tenant, thread-safe)
            var metadataProcessed = 0;
            foreach (var tenantId in metadataTenantIds)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    var (sizeBefore, sizeAfter) = await _metadataMaintenanceStore.OptimizeDatabaseAsync(tenantId, ct);

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
            var quotaTenantIds = await GetQuotaTenantIdsSnapshotAsync(ct);

            // Optimize quota databases (per-tenant, thread-safe)
            var quotaProcessed = 0;
            foreach (var tenantId in quotaTenantIds)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    var (sizeBefore, sizeAfter) = await _quotaMaintenanceStore.OptimizeDatabaseAsync(tenantId, ct);

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
            foreach (var tenantDir in _fileSystem.Directory.EnumerateDirectories(rootDirectory))
            {
                ct.ThrowIfCancellationRequested();

                foreach (var filePath in _fileSystem.Directory.EnumerateFiles(tenantDir))
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
            foreach (var subdirectory in _fileSystem.Directory.EnumerateDirectories(directoryPath))
            {
                removedCount += await CleanupJunkFilesRecursiveAsync(subdirectory, ct, isProtectedRoot: false, shardingDepth: shardingDepth, depth: depth + 1);
            }

            // 2. Delete junk files in the current directory.
            foreach (var file in _fileSystem.Directory.EnumerateFiles(directoryPath))
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
                return !_fileSystem.Directory.EnumerateFileSystemEntries(directoryPath).Any();
            }
            catch
            {
                return false;
            }
        }

        private async Task<string[]> GetMetadataTenantIdsSnapshotAsync(CancellationToken ct)
        {
            var tenantIds = await _projectionStore.GetProjectedTenantIdsAsync(ct).ConfigureAwait(false);
            return tenantIds
                .Where(tenantId => !string.IsNullOrWhiteSpace(tenantId))
                .Distinct(StringComparer.Ordinal)
                .ToArray();
        }

        private async Task<string[]> GetQuotaTenantIdsSnapshotAsync(CancellationToken ct)
        {
            var tenantIds = await _quotaMaintenanceStore.GetTenantIdsAsync(ct);
            return tenantIds
                .Where(tenantId => !string.IsNullOrWhiteSpace(tenantId))
                .Distinct(StringComparer.Ordinal)
                .ToArray();
        }

        private List<OrphanFileScanEntry> EnumerateSortedOrphanScanEntries(string directory)
        {
            var entries = new List<OrphanFileScanEntry>();

            foreach (var physicalPath in _fileSystem.Directory.EnumerateFiles(directory))
            {
                entries.Add(new OrphanFileScanEntry(
                    physicalPath,
                    NormalizePath(physicalPath) ?? physicalPath));
            }

            entries.Sort((left, right) => _pathComparer.Compare(left.NormalizedPath, right.NormalizedPath));

            return entries;
        }

        private int FindOrphanScanStartIndex(IReadOnlyList<OrphanFileScanEntry> entries, string? resumeAfterPath)
        {
            if (string.IsNullOrWhiteSpace(resumeAfterPath) || entries.Count == 0)
                return 0;

            var low = 0;
            var high = entries.Count;

            while (low < high)
            {
                var mid = low + ((high - low) / 2);
                var comparison = _pathComparer.Compare(entries[mid].NormalizedPath, resumeAfterPath);

                if (comparison <= 0)
                {
                    low = mid + 1;
                }
                else
                {
                    high = mid;
                }
            }

            return low;
        }

        private async Task<HashSet<string>> BuildKnownPhysicalPathSetAsync(string tenantId, CancellationToken ct)
        {
            var metadataEntries = await _projectionStore.GetProjectedFilesAsync(tenantId, ct).ConfigureAwait(false);
            var knownPaths = new HashSet<string>(_pathComparer);

            foreach (var metadata in metadataEntries)
            {
                var normalizedPath = NormalizePath(metadata.PhysicalPath);
                if (normalizedPath != null)
                    knownPaths.Add(normalizedPath);
            }

            return knownPaths;
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

        private readonly struct OrphanFileScanEntry
        {
            public OrphanFileScanEntry(string physicalPath, string normalizedPath)
            {
                PhysicalPath = physicalPath;
                NormalizedPath = normalizedPath;
            }

            public string PhysicalPath { get; }

            public string NormalizedPath { get; }
        }
    }
}
