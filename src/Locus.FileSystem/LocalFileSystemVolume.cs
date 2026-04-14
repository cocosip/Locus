using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace Locus.FileSystem
{
    /// <summary>
    /// Local file system implementation of IStorageVolume.
    /// </summary>
    public class LocalFileSystemVolume : IStorageVolume, IStorageVolumeHealthProbe, IStorageVolumeWritePathWarmup
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<LocalFileSystemVolume> _logger;
        private readonly string _mountPath;
        private readonly string _mountPathWithTrailingSeparator;
        private readonly string _volumeId;
        private readonly int _shardingDepth;
        private readonly StringComparison _pathComparison;
        private readonly int _writeBufferSize;
        private readonly int _copyBufferSize;
        private readonly bool _forceFlushAfterWrite;
        private readonly int _knownDirectoryCacheMaxEntries;

        // --- Optimization 1: IsHealthy TTL cache ---
        // Avoid file write+delete on every WriteFileAsync call.
        // Only one thread performs the real check per TTL window (singleflight via CAS).
        private volatile bool _cachedIsHealthy = true;
        private long _lastHealthCheckTicks;          // Stopwatch ticks, Interlocked access
        private readonly long _healthCacheDurationTicks;

        // --- Optimization 1: AvailableSpace TTL cache ---
        // Avoid DriveInfo.GetDrives() on every WriteFileAsync call.
        private long _cachedAvailableSpace;          // Interlocked access
        private long _cachedTotalCapacity;           // Interlocked access
        private long _lastSpaceCheckTicks;           // Stopwatch ticks, Interlocked access
        private readonly long _spaceCacheDurationTicks;

        // --- Optimization 4: Known-directory cache ---
        // Avoid Directory.Exists() stat on every write for already-created shard directories.
        // Cache is bounded to keep long-running memory usage predictable.
        private readonly ConcurrentDictionary<string, byte> _knownDirectories;
        private int _knownDirectoryTrimInProgress;
        // Separate O(1) counter so TrackKnownDirectory can check the size without
        // calling ConcurrentDictionary.Count (which is O(N) due to segment locking).
        private int _knownDirectoryCount;

        private const int DefaultWriteBufferSize = 128 * 1024;
        private const int DefaultCopyBufferSize = 256 * 1024;
        private const int DefaultKnownDirectoryCacheMaxEntries = 200_000;
        private const int DefaultSynchronousSmallWriteThresholdBytes = 1024 * 1024;

        /// <summary>
        /// Initializes a new instance of the <see cref="LocalFileSystemVolume"/> class.
        /// </summary>
        /// <param name="fileSystem">The file system abstraction.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="volumeId">Unique volume identifier.</param>
        /// <param name="mountPath">Root directory of this volume.</param>
        /// <param name="shardingDepth">Number of directory sharding levels (0鈥?). Default 2.</param>
        /// <param name="healthCheckCacheDuration">
        /// How long to cache IsHealthy and AvailableSpace results.
        /// Eliminates the file-write+delete health probe and DriveInfo syscall on every write.
        /// Default: 30 seconds. Pass <see cref="TimeSpan.Zero"/> to disable caching (original behavior).
        /// </param>
        /// <param name="writeBufferSize">Write stream buffer size in bytes. Default 128 KB.</param>
        /// <param name="copyBufferSize">Pooled copy buffer size in bytes. Default 80 KB.</param>
        /// <param name="forceFlushAfterWrite">
        /// Whether to force <see cref="Stream.FlushAsync(CancellationToken)"/> after each write.
        /// Default: true.
        /// </param>
        /// <param name="knownDirectoryCacheMaxEntries">
        /// Maximum number of cached directory entries before trimming. Default 200,000.
        /// </param>
        public LocalFileSystemVolume(
            IFileSystem fileSystem,
            ILogger<LocalFileSystemVolume> logger,
            string volumeId,
            string mountPath,
            int shardingDepth = 2,
            TimeSpan healthCheckCacheDuration = default,
            int writeBufferSize = DefaultWriteBufferSize,
            int copyBufferSize = DefaultCopyBufferSize,
            bool forceFlushAfterWrite = true,
            int knownDirectoryCacheMaxEntries = DefaultKnownDirectoryCacheMaxEntries)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(volumeId))
                throw new ArgumentException("Volume ID cannot be empty", nameof(volumeId));

            if (string.IsNullOrWhiteSpace(mountPath))
                throw new ArgumentException("Mount path cannot be empty", nameof(mountPath));

            if (shardingDepth < 0 || shardingDepth > 3)
                throw new ArgumentException("Sharding depth must be between 0 and 3", nameof(shardingDepth));

            if (writeBufferSize <= 0)
                throw new ArgumentException("Write buffer size must be greater than zero", nameof(writeBufferSize));

            if (copyBufferSize <= 0)
                throw new ArgumentException("Copy buffer size must be greater than zero", nameof(copyBufferSize));

            if (knownDirectoryCacheMaxEntries <= 0)
                throw new ArgumentException("Known directory cache size must be greater than zero", nameof(knownDirectoryCacheMaxEntries));

            _volumeId = volumeId;
            _mountPath = _fileSystem.Path.GetFullPath(mountPath);
            _mountPathWithTrailingSeparator = _mountPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
            _shardingDepth = shardingDepth;
            _pathComparison = Path.DirectorySeparatorChar == '/'
                ? StringComparison.Ordinal
                : StringComparison.OrdinalIgnoreCase;
            _writeBufferSize = writeBufferSize;
            _copyBufferSize = copyBufferSize;
            _forceFlushAfterWrite = forceFlushAfterWrite;
            _knownDirectoryCacheMaxEntries = knownDirectoryCacheMaxEntries;

            // Default TTL: 30 seconds
            var cacheDuration = healthCheckCacheDuration == default
                ? TimeSpan.FromSeconds(30)
                : healthCheckCacheDuration;

            // TimeSpan.Zero means no caching 鈥?set ticks to 0 so every call checks
            _healthCacheDurationTicks = cacheDuration == TimeSpan.Zero
                ? 0
                : (long)(cacheDuration.TotalSeconds * Stopwatch.Frequency);
            _spaceCacheDurationTicks = _healthCacheDurationTicks;

            // Use the same case-sensitivity as _pathComparison so that cache hits on Linux
            // (case-sensitive FS) are never falsely triggered by differently-cased paths.
            _knownDirectories = new ConcurrentDictionary<string, byte>(
                _pathComparison == StringComparison.Ordinal
                    ? StringComparer.Ordinal
                    : StringComparer.OrdinalIgnoreCase);

            // Ensure mount path exists
            if (!_fileSystem.Directory.Exists(_mountPath))
            {
                try
                {
                    _fileSystem.Directory.CreateDirectory(_mountPath);
                    _logger.LogInformation("Created mount path: {MountPath} with sharding depth {ShardingDepth}",
                        _mountPath, _shardingDepth);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to create mount path: {MountPath}", _mountPath);
                    throw;
                }
            }

            // Pre-populate mount path in known-directory cache
            if (_knownDirectories.TryAdd(_mountPath, 0))
                Interlocked.Increment(ref _knownDirectoryCount);
        }

        /// <inheritdoc/>
        public string VolumeId => _volumeId;

        /// <inheritdoc/>
        public string MountPath => _mountPath;

        /// <inheritdoc/>
        public int ShardingDepth => _shardingDepth;

        /// <inheritdoc/>
        public long TotalCapacity
        {
            get
            {
                // TotalCapacity shares the same cache refresh window as AvailableSpace
                EnsureSpaceCacheRefreshed();
                return Interlocked.Read(ref _cachedTotalCapacity);
            }
        }

        /// <inheritdoc/>
        public long AvailableSpace
        {
            get
            {
                EnsureSpaceCacheRefreshed();
                return Interlocked.Read(ref _cachedAvailableSpace);
            }
        }

        /// <inheritdoc/>
        public bool IsHealthy
        {
            get
            {
                if (_healthCacheDurationTicks == 0)
                    return PerformHealthCheckInternal();

                var now = Stopwatch.GetTimestamp();
                var last = Interlocked.Read(ref _lastHealthCheckTicks);

                // First health check should be synchronous so startup callers don't
                // observe the optimistic default value before any real probe runs.
                if (last == 0)
                {
                    var result = PerformHealthCheckInternal();
                    _cachedIsHealthy = result;
                    Interlocked.CompareExchange(ref _lastHealthCheckTicks, now, 0);
                    return result;
                }

                // Within TTL: return cached result immediately (no I/O)
                if (last != 0 && (now - last) < _healthCacheDurationTicks)
                    return _cachedIsHealthy;

                // Singleflight: only the CAS winner triggers the background refresh.
                // Losers return the (slightly stale) cached value 鈥?acceptable.
                if (Interlocked.CompareExchange(ref _lastHealthCheckTicks, now, last) != last)
                    return _cachedIsHealthy;

                // Dispatch the real probe on the thread pool so the caller is never blocked
                // by disk/network I/O (critical when volumes are NFS/SMB mounts).
                // The cached value returned here is at most one TTL window stale.
                //
                // ContinueWith logs any unexpected exception that escapes PerformHealthCheckInternal
                // (it has a catch-all, but defensive programming).
                // The lambda captures `this`; it is released as soon as the task finishes
                // (typically within milliseconds), so there is no memory leak.
                Task.Run(() =>
                {
                    var result = PerformHealthCheckInternal();
                    _cachedIsHealthy = result; // volatile write 鈥?visible to all threads
                }).ContinueWith(
                    t => _logger.LogError(t.Exception, "Unexpected error in background health check for volume {VolumeId}", _volumeId),
                    TaskContinuationOptions.OnlyOnFaulted);
                return _cachedIsHealthy;
            }
        }

        /// <inheritdoc/>
        public bool ProbeHealth()
        {
            var result = PerformHealthCheckInternal();
            _cachedIsHealthy = result;
            Interlocked.Exchange(ref _lastHealthCheckTicks, Stopwatch.GetTimestamp());
            return result;
        }

        /// <inheritdoc/>
        public Task WarmWritePathCacheAsync(CancellationToken ct = default)
        {
            var directoriesScanned = 0;
            var pending = new Stack<string>();
            pending.Push(_mountPath);

            while (pending.Count > 0)
            {
                ct.ThrowIfCancellationRequested();

                var current = pending.Pop();
                TrackKnownDirectoryWithoutTrim(current);
                directoriesScanned++;

                IEnumerable<string> childDirectories;
                try
                {
                    childDirectories = _fileSystem.Directory.EnumerateDirectories(current);
                }
                catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
                {
                    _logger.LogWarning(
                        ex,
                        "Skipping directory cache warmup subtree for volume {VolumeId} because {Directory} could not be enumerated",
                        _volumeId,
                        current);
                    continue;
                }

                foreach (var childDirectory in childDirectories)
                    pending.Push(childDirectory);
            }

            _logger.LogInformation(
                "Warmed write-path directory cache for volume {VolumeId}: scanned {ScannedCount} directories, cached {CachedCount}",
                _volumeId,
                directoriesScanned,
                Volatile.Read(ref _knownDirectoryCount));

            return Task.CompletedTask;
        }

        // ---------------------------------------------------------------------------
        // Private helpers
        // ---------------------------------------------------------------------------

        /// <summary>
        /// Refreshes cached AvailableSpace/TotalCapacity if the TTL has expired.
        /// Uses singleflight (CAS): only one thread calls DriveInfo.GetDrives() per window.
        ///
        /// The CAS winner dispatches the actual drive enumeration onto the thread pool
        /// (same pattern as IsHealthy) so the calling thread 鈥?which may be inside
        /// WriteAsync 鈥?is never blocked by a potentially slow GetDrives() syscall on
        /// network volumes.  All other callers return the (slightly stale) cached values.
        ///
        /// Exception: when _spaceCacheDurationTicks == 0 (caching disabled) the call
        /// is always synchronous for correctness, mirroring the original implementation.
        /// </summary>
        private void EnsureSpaceCacheRefreshed()
        {
            if (_spaceCacheDurationTicks == 0)
            {
                RefreshSpaceFromDrive();
                return;
            }

            var now = Stopwatch.GetTimestamp();
            var last = Interlocked.Read(ref _lastSpaceCheckTicks);

            // First refresh should be synchronous for correctness. Without this,
            // startup health probes can observe zero-space before async refresh finishes.
            if (last == 0)
            {
                if (Interlocked.CompareExchange(ref _lastSpaceCheckTicks, now, 0) == 0)
                    RefreshSpaceFromDrive();
                return;
            }

            if (last != 0 && (now - last) < _spaceCacheDurationTicks)
                return; // Still within TTL 鈥?cached values are fresh enough

            // Singleflight: only the CAS winner triggers the refresh.
            // Losers return immediately with the stale (but acceptable) cached values.
            if (Interlocked.CompareExchange(ref _lastSpaceCheckTicks, now, last) != last)
                return;

            // Dispatch onto the thread pool so the caller is never blocked by
            // DriveInfo.GetDrives() 鈥?a syscall that can take seconds on NFS/SMB mounts.
            Task.Run(RefreshSpaceFromDrive)
                .ContinueWith(
                    t => _logger.LogWarning(t.Exception, "Background space refresh failed for volume {VolumeId}", _volumeId),
                    TaskContinuationOptions.OnlyOnFaulted);
        }

        private void RefreshSpaceFromDrive()
        {
            try
            {
                var driveInfo = GetDriveInfo();
                Interlocked.Exchange(ref _cachedAvailableSpace, driveInfo?.AvailableFreeSpace ?? 0);
                Interlocked.Exchange(ref _cachedTotalCapacity, driveInfo?.TotalSize ?? 0);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get drive info for volume {VolumeId}", _volumeId);
                // Leave cached values unchanged 鈥?use last known values
            }
        }

        /// <summary>
        /// Performs the real health check: directory exists + write/delete probe.
        /// Result is cached by the caller for <see cref="_healthCacheDurationTicks"/>.
        /// </summary>
        private bool PerformHealthCheckInternal()
        {
            try
            {
                if (!_fileSystem.Directory.Exists(_mountPath))
                {
                    _logger.LogWarning("Mount path does not exist: {MountPath}", _mountPath);
                    return false;
                }

                EnsureSpaceCacheRefreshed();
                if (Interlocked.Read(ref _cachedAvailableSpace) <= 0)
                {
                    // Retry synchronously once: async refresh may still be in-flight.
                    RefreshSpaceFromDrive();
                    if (Interlocked.Read(ref _cachedAvailableSpace) <= 0)
                    {
                        _logger.LogWarning("No available space on volume {VolumeId}", _volumeId);
                        return false;
                    }
                }

                // Single probe write 鈥?no retries to avoid blocking the thread pool with Thread.Sleep.
                // A transient failure here simply marks the volume unhealthy for the 30-second TTL
                // window, after which the next IsHealthy access will re-probe.
                var testFilePath = _fileSystem.Path.Combine(_mountPath, $".health-check-{Guid.NewGuid()}.tmp");
                try
                {
                    _fileSystem.File.WriteAllText(testFilePath, "health check");
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Health check probe failed for volume {VolumeId}", _volumeId);
                    return false;
                }
                finally
                {
                    // Always attempt to remove the temp file. Without this, a WriteAllText that
                    // succeeds but whose Delete subsequently throws (e.g. permission change, race
                    // with antivirus) would leave .health-check-*.tmp files accumulating on disk.
                    try { _fileSystem.File.Delete(testFilePath); }
                    catch { /* best-effort; the file will be cleaned up by the next probe cycle */ }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Health check failed for volume {VolumeId}", _volumeId);
                return false;
            }
        }

        // ---------------------------------------------------------------------------
        // IStorageVolume I/O methods
        // ---------------------------------------------------------------------------

        /// <inheritdoc/>
        public Task<Stream> ReadAsync(string path, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be empty", nameof(path));

            ValidatePathWithinMountPath(path);

            if (!_fileSystem.File.Exists(path))
                throw new FileNotFoundException($"File not found: {path}");

            try
            {
                Stream stream = _fileSystem.File.OpenRead(path);
                _logger.LogDebug("Opened file for reading: {Path}", path);
                return Task.FromResult(stream);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read file: {Path}", path);
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task WriteAsync(string path, Stream content, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be empty", nameof(path));

            if (content == null)
                throw new ArgumentNullException(nameof(content));

            ValidatePathWithinMountPath(path);

            try
            {
                var remainingBytes = TryGetRemainingLength(content);

                // Optimization 4: skip Directory.Exists() for directories we already know exist.
                // Shard directories are created once and never deleted during normal operation.
                var directory = _fileSystem.Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(directory) && !_knownDirectories.ContainsKey(directory!))
                    EnsureDirectoryTracked(directory!);

                if (ShouldUseSynchronousSmallWritePath(content, remainingBytes))
                {
                    using (var fileStream = CreateSynchronousWriteStream(path, remainingBytes))
                    {
                        CopySmallSeekableStreamSynchronously(content, fileStream, checked((int)remainingBytes!.Value), ct);
                        if (_forceFlushAfterWrite)
                            fileStream.Flush();
                    }
                }
                else
                {
                    using (var fileStream = CreateWriteStream(path, remainingBytes))
                    {
                        await CopyStreamAsync(content, fileStream, ct).ConfigureAwait(false);
                        if (_forceFlushAfterWrite)
                            await fileStream.FlushAsync(ct).ConfigureAwait(false);
                    }
                }

                // content.Length throws NotSupportedException on non-seekable streams (e.g. NetworkStream).
                _logger.LogDebug("Wrote file: {Path}, Size: {Size} bytes", path,
                    content.CanSeek ? content.Length : -1L);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write file: {Path}", path);
                throw;
            }
        }

        /// <inheritdoc/>
        public Task DeleteAsync(string path, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be empty", nameof(path));

            ValidatePathWithinMountPath(path);

            if (!_fileSystem.File.Exists(path))
            {
                _logger.LogWarning("Attempted to delete non-existent file: {Path}", path);
                return Task.CompletedTask;
            }

            try
            {
                _fileSystem.File.Delete(path);
                _logger.LogDebug("Deleted file: {Path}", path);
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to delete file: {Path}", path);
                throw;
            }
        }

        // ---------------------------------------------------------------------------
        // Drive info helper
        // ---------------------------------------------------------------------------

        /// <summary>
        /// Gets the DriveInfo for the mount path.
        /// Cross-platform compatible: uses RootDirectory.FullName for matching on Linux.
        /// </summary>
        private IDriveInfo? GetDriveInfo()
        {
            try
            {
                var root = _fileSystem.Path.GetPathRoot(_mountPath);
                if (string.IsNullOrEmpty(root))
                    return null;

                var drives = _fileSystem.DriveInfo.GetDrives();

                // Try RootDirectory.FullName first (works on both Windows and Linux).
                // Use _pathComparison so Linux (Ordinal) and Windows (OrdinalIgnoreCase) behave correctly.
                var drive = drives.FirstOrDefault(d =>
                    d.IsReady && d.RootDirectory.FullName.Equals(root, _pathComparison));

                if (drive == null)
                {
                    drive = drives.FirstOrDefault(d =>
                        d.IsReady && d.Name.Equals(root, _pathComparison));
                }

                return drive;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get drive info for volume {VolumeId}", _volumeId);
                return null;
            }
        }

        // ---------------------------------------------------------------------------
        // Path building
        // ---------------------------------------------------------------------------

        /// <summary>
        /// Builds the physical path for a file with automatic directory sharding.
        /// Uses 2-character hex segments from fileKey as subdirectories (similar to FastDFS).
        /// Each sharding level creates a 2-character directory (00-FF), allowing 256 subdirectories per level.
        /// Example with depth=2 and fileKey="a1b2c3d4...": {mountPath}/{tenantId}/a1/b2/a1b2c3d4...
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="fileKey">The file key (hex string).</param>
        /// <param name="fileExtension">The file extension (e.g., ".pdf"). If provided, appended to the file key.</param>
        /// <returns>The full physical path with sharding.</returns>
        public string BuildPhysicalPath(string tenantId, string fileKey, string? fileExtension = null)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // fileKey is always produced by Guid.NewGuid().ToString("N") which is lowercase hex,
            // so ToLowerInvariant() is redundant and avoided here to reduce hot-path allocations.
            // Path.Combine with fixed argument counts avoids the List<string> + ToArray overhead.
            var fileName = string.IsNullOrEmpty(fileExtension) ? fileKey : fileKey + fileExtension;

            switch (_shardingDepth)
            {
                case 0:
                    return _fileSystem.Path.Combine(_mountPath, tenantId, fileName);

                case 1:
                {
                    var shard0 = fileKey.Length >= 2 ? fileKey.Substring(0, 2)
                               : fileKey.Length == 1 ? fileKey[0] + "0"
                               : "00";
                    return _fileSystem.Path.Combine(_mountPath, tenantId, shard0, fileName);
                }

                case 2:
                {
                    var shard0 = fileKey.Length >= 2 ? fileKey.Substring(0, 2)
                               : fileKey.Length == 1 ? fileKey[0] + "0"
                               : "00";
                    var shard1 = fileKey.Length >= 4 ? fileKey.Substring(2, 2)
                               : fileKey.Length == 3 ? fileKey[2] + "0"
                               : "00";
                    return _fileSystem.Path.Combine(_mountPath, tenantId, shard0, shard1, fileName);
                }

                default:
                {
                    // Depth >= 3: fall back to the general loop (rare configuration).
                    var pathParts = new List<string> { _mountPath, tenantId };
                    for (int i = 0; i < _shardingDepth; i++)
                    {
                        int startIndex = i * 2;
                        if (startIndex + 1 < fileKey.Length)
                            pathParts.Add(fileKey.Substring(startIndex, 2));
                        else if (startIndex < fileKey.Length)
                            pathParts.Add(fileKey[startIndex] + "0");
                        else
                            break;
                    }
                    pathParts.Add(fileName);
                    return _fileSystem.Path.Combine(pathParts.ToArray());
                }
            }
        }

        /// <summary>
        /// Returns a string representation of the volume using only cached fields.
        /// Reads directly from the atomic cache variables to avoid triggering disk I/O,
        /// which would be unacceptable in hot logging paths (e.g. structured log formatters).
        /// </summary>
        public override string ToString()
        {
            var availableMB = Interlocked.Read(ref _cachedAvailableSpace) / 1024 / 1024;
            var totalMB = Interlocked.Read(ref _cachedTotalCapacity) / 1024 / 1024;
            return $"LocalFileSystemVolume(Id={_volumeId}, Path={_mountPath}, " +
                   $"ShardingDepth={_shardingDepth}, " +
                   $"Available={availableMB}MB, " +
                   $"Total={totalMB}MB, " +
                   $"Healthy={_cachedIsHealthy})";
        }

        private void ValidatePathWithinMountPath(string path)
        {
            if (IsKnownSafeInternalPath(path))
                return;

            if (!FileSystemPathSanitizer.IsPathWithinBase(_mountPath, path))
                throw new InvalidOperationException($"Path is outside volume mount path: {path}");
        }

        private bool IsKnownSafeInternalPath(string path)
        {
            if (!_fileSystem.Path.IsPathRooted(path))
                return false;

            if (path.IndexOf('\0') >= 0 || path.IndexOf("..", StringComparison.Ordinal) >= 0)
                return false;

            return path.Equals(_mountPath, _pathComparison)
                || path.StartsWith(_mountPathWithTrailingSeparator, _pathComparison);
        }

        private void TrackKnownDirectory(string directory)
        {
            if (_knownDirectories.TryAdd(directory, 0))
                Interlocked.Increment(ref _knownDirectoryCount);

            // Use the O(1) counter instead of ConcurrentDictionary.Count (which is O(N)).
            var observedCount = Volatile.Read(ref _knownDirectoryCount);
            if (observedCount <= _knownDirectoryCacheMaxEntries)
                return;

            if (Interlocked.CompareExchange(ref _knownDirectoryTrimInProgress, 1, 0) != 0)
                return;

            try
            {
                var targetSize = Math.Max(1, _knownDirectoryCacheMaxEntries / 2);
                var removeBudget = Math.Max(0, observedCount - targetSize);
                if (removeBudget == 0)
                    return;

                var removed = 0;
                foreach (var cachedDirectory in _knownDirectories.Keys)
                {
                    if (removed >= removeBudget)
                        break;

                    if (string.Equals(cachedDirectory, _mountPath, _pathComparison))
                        continue;

                    if (_knownDirectories.TryRemove(cachedDirectory, out _))
                    {
                        Interlocked.Decrement(ref _knownDirectoryCount);
                        removed++;
                    }
                }

                if (_knownDirectories.TryAdd(_mountPath, 0))
                    Interlocked.Increment(ref _knownDirectoryCount);
            }
            finally
            {
                Volatile.Write(ref _knownDirectoryTrimInProgress, 0);
            }
        }

        private void EnsureDirectoryTracked(string directory)
        {
            _fileSystem.Directory.CreateDirectory(directory);
            TrackKnownDirectory(directory);
        }

        private void TrackKnownDirectoryWithoutTrim(string directory)
        {
            if (_knownDirectories.TryAdd(directory, 0))
                Interlocked.Increment(ref _knownDirectoryCount);
        }

        private Stream CreateWriteStream(string path, long? expectedBytes)
        {
            // MockFileSystem does not map to OS file handles; keep using abstraction in tests.
            if (_fileSystem.GetType().FullName?.IndexOf("MockFileSystem", StringComparison.OrdinalIgnoreCase) >= 0)
                return _fileSystem.File.Open(path, FileMode.Create, FileAccess.Write, FileShare.None);

            var bufferSize = ResolveWriteBufferSize(expectedBytes);
            return new FileStream(
                path,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
        }

        private Stream CreateSynchronousWriteStream(string path, long? expectedBytes)
        {
            if (_fileSystem.GetType().FullName?.IndexOf("MockFileSystem", StringComparison.OrdinalIgnoreCase) >= 0)
                return _fileSystem.File.Open(path, FileMode.Create, FileAccess.Write, FileShare.None);

            var bufferSize = ResolveWriteBufferSize(expectedBytes);
            return new FileStream(
                path,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize,
                FileOptions.None);
        }

        private async Task CopyStreamAsync(Stream source, Stream destination, CancellationToken ct = default)
        {
            if (TryGetRemainingLength(source) is long remainingBytes
                && remainingBytes >= 0
                && remainingBytes <= _copyBufferSize)
            {
                var exactLength = (int)remainingBytes;
                if (exactLength == 0)
                    return;

                var singleBuffer = ArrayPool<byte>.Shared.Rent(exactLength);
                try
                {
                    var totalRead = 0;
                    while (totalRead < exactLength)
                    {
                        var bytesRead = await source.ReadAsync(singleBuffer, totalRead, exactLength - totalRead, ct).ConfigureAwait(false);
                        if (bytesRead <= 0)
                            break;

                        totalRead += bytesRead;
                    }

                    if (totalRead > 0)
                        await destination.WriteAsync(singleBuffer, 0, totalRead, ct).ConfigureAwait(false);

                    return;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(singleBuffer);
                }
            }

            var rented = ArrayPool<byte>.Shared.Rent(_copyBufferSize);
            try
            {
                while (true)
                {
                    var bytesRead = await source.ReadAsync(rented, 0, _copyBufferSize, ct).ConfigureAwait(false);
                    if (bytesRead <= 0)
                        break;

                    await destination.WriteAsync(rented, 0, bytesRead, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private int ResolveWriteBufferSize(long? expectedBytes)
        {
            if (!expectedBytes.HasValue || expectedBytes.Value <= 0)
                return _writeBufferSize;

            var candidate = expectedBytes.Value > int.MaxValue
                ? _writeBufferSize
                : (int)expectedBytes.Value;

            if (candidate <= 0)
                return _writeBufferSize;

            if (candidate < 4096)
                candidate = 4096;

            return candidate < _writeBufferSize ? candidate : _writeBufferSize;
        }

        private bool ShouldUseSynchronousSmallWritePath(Stream content, long? remainingBytes)
        {
            return content.CanSeek
                && remainingBytes.HasValue
                && remainingBytes.Value > 0
                && remainingBytes.Value <= DefaultSynchronousSmallWriteThresholdBytes;
        }

        private static void CopySmallSeekableStreamSynchronously(
            Stream source,
            Stream destination,
            int exactLength,
            CancellationToken ct)
        {
            if (exactLength <= 0)
                return;

            ct.ThrowIfCancellationRequested();

            var buffer = ArrayPool<byte>.Shared.Rent(exactLength);
            try
            {
                var totalRead = 0;
                while (totalRead < exactLength)
                {
                    ct.ThrowIfCancellationRequested();
                    var bytesRead = source.Read(buffer, totalRead, exactLength - totalRead);
                    if (bytesRead <= 0)
                        break;

                    totalRead += bytesRead;
                }

                if (totalRead > 0)
                    destination.Write(buffer, 0, totalRead);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private static long? TryGetRemainingLength(Stream stream)
        {
            if (!stream.CanSeek)
                return null;

            return stream.Length - stream.Position;
        }
    }
}

