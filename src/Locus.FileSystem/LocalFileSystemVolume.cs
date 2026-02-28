using System;
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
    public class LocalFileSystemVolume : IStorageVolume
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<LocalFileSystemVolume> _logger;
        private readonly string _mountPath;
        private readonly string _volumeId;
        private readonly int _shardingDepth;

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
        // Directories are permanent once created, so this cache never expires.
        private readonly ConcurrentDictionary<string, byte> _knownDirectories;

        /// <summary>
        /// Initializes a new instance of the <see cref="LocalFileSystemVolume"/> class.
        /// </summary>
        /// <param name="fileSystem">The file system abstraction.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="volumeId">Unique volume identifier.</param>
        /// <param name="mountPath">Root directory of this volume.</param>
        /// <param name="shardingDepth">Number of directory sharding levels (0–3). Default 2.</param>
        /// <param name="healthCheckCacheDuration">
        /// How long to cache IsHealthy and AvailableSpace results.
        /// Eliminates the file-write+delete health probe and DriveInfo syscall on every write.
        /// Default: 30 seconds. Pass <see cref="TimeSpan.Zero"/> to disable caching (original behavior).
        /// </param>
        public LocalFileSystemVolume(
            IFileSystem fileSystem,
            ILogger<LocalFileSystemVolume> logger,
            string volumeId,
            string mountPath,
            int shardingDepth = 2,
            TimeSpan healthCheckCacheDuration = default)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(volumeId))
                throw new ArgumentException("Volume ID cannot be empty", nameof(volumeId));

            if (string.IsNullOrWhiteSpace(mountPath))
                throw new ArgumentException("Mount path cannot be empty", nameof(mountPath));

            if (shardingDepth < 0 || shardingDepth > 3)
                throw new ArgumentException("Sharding depth must be between 0 and 3", nameof(shardingDepth));

            _volumeId = volumeId;
            _mountPath = _fileSystem.Path.GetFullPath(mountPath);
            _shardingDepth = shardingDepth;

            // Default TTL: 30 seconds
            var cacheDuration = healthCheckCacheDuration == default
                ? TimeSpan.FromSeconds(30)
                : healthCheckCacheDuration;

            // TimeSpan.Zero means no caching — set ticks to 0 so every call checks
            _healthCacheDurationTicks = cacheDuration == TimeSpan.Zero
                ? 0
                : (long)(cacheDuration.TotalSeconds * Stopwatch.Frequency);
            _spaceCacheDurationTicks = _healthCacheDurationTicks;

            _knownDirectories = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);

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
            _knownDirectories.TryAdd(_mountPath, 0);
        }

        /// <inheritdoc/>
        public string VolumeId => _volumeId;

        /// <inheritdoc/>
        public string MountPath => _mountPath;

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

                // Within TTL: return cached result immediately (no I/O)
                if (last != 0 && (now - last) < _healthCacheDurationTicks)
                    return _cachedIsHealthy;

                // Singleflight: only the CAS winner performs the real check.
                // Losers return the (slightly stale) cached value — acceptable.
                if (Interlocked.CompareExchange(ref _lastHealthCheckTicks, now, last) != last)
                    return _cachedIsHealthy;

                var result = PerformHealthCheckInternal();
                _cachedIsHealthy = result; // volatile write — visible to all threads
                return result;
            }
        }

        // ---------------------------------------------------------------------------
        // Private helpers
        // ---------------------------------------------------------------------------

        /// <summary>
        /// Refreshes cached AvailableSpace/TotalCapacity if the TTL has expired.
        /// Uses singleflight: only one thread calls DriveInfo.GetDrives() per window.
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

            if (last != 0 && (now - last) < _spaceCacheDurationTicks)
                return; // Still within TTL

            if (Interlocked.CompareExchange(ref _lastSpaceCheckTicks, now, last) != last)
                return; // Another thread is refreshing — use stale values for now

            RefreshSpaceFromDrive();
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
                // Leave cached values unchanged — use last known values
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
                    _logger.LogWarning("No available space on volume {VolumeId}", _volumeId);
                    return false;
                }

                // Single probe write — no retries to avoid blocking the thread pool with Thread.Sleep.
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
        public Task<Stream> ReadAsync(string path, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be empty", nameof(path));

            if (!FileSystemPathSanitizer.IsPathWithinBase(_mountPath, path))
                throw new InvalidOperationException($"Path is outside volume mount path: {path}");

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
        public async Task WriteAsync(string path, Stream content, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be empty", nameof(path));

            if (content == null)
                throw new ArgumentNullException(nameof(content));

            if (!FileSystemPathSanitizer.IsPathWithinBase(_mountPath, path))
                throw new InvalidOperationException($"Path is outside volume mount path: {path}");

            try
            {
                // Optimization 4: skip Directory.Exists() for directories we already know exist.
                // Shard directories are created once and never deleted during normal operation.
                var directory = _fileSystem.Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(directory) && !_knownDirectories.ContainsKey(directory!))
                {
                    if (!_fileSystem.Directory.Exists(directory))
                        _fileSystem.Directory.CreateDirectory(directory!);

                    _knownDirectories.TryAdd(directory!, 0);
                }

                using (var fileStream = _fileSystem.File.Create(path))
                {
                    await content.CopyToAsync(fileStream, 81920, ct); // 80 KB buffer
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
        public Task DeleteAsync(string path, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be empty", nameof(path));

            if (!FileSystemPathSanitizer.IsPathWithinBase(_mountPath, path))
                throw new InvalidOperationException($"Path is outside volume mount path: {path}");

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

                // Try RootDirectory.FullName first (works on both Windows and Linux)
                var drive = drives.FirstOrDefault(d =>
                    d.IsReady && d.RootDirectory.FullName.Equals(root, StringComparison.OrdinalIgnoreCase));

                if (drive == null)
                {
                    drive = drives.FirstOrDefault(d =>
                        d.IsReady && d.Name.Equals(root, StringComparison.OrdinalIgnoreCase));
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
        /// Returns a string representation of the volume.
        /// </summary>
        public override string ToString()
        {
            return $"LocalFileSystemVolume(Id={_volumeId}, Path={_mountPath}, " +
                   $"ShardingDepth={_shardingDepth}, " +
                   $"Available={AvailableSpace / 1024 / 1024}MB, " +
                   $"Total={TotalCapacity / 1024 / 1024}MB, " +
                   $"Healthy={IsHealthy})";
        }
    }
}
