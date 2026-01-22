using System;
using System.Collections.Generic;
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

        /// <summary>
        /// Initializes a new instance of the <see cref="LocalFileSystemVolume"/> class.
        /// </summary>
        public LocalFileSystemVolume(
            IFileSystem fileSystem,
            ILogger<LocalFileSystemVolume> logger,
            string volumeId,
            string mountPath,
            int shardingDepth = 2)
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
                try
                {
                    var driveInfo = GetDriveInfo();
                    return driveInfo?.TotalSize ?? 0;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to get total capacity for volume {VolumeId}", _volumeId);
                    return 0;
                }
            }
        }

        /// <inheritdoc/>
        public long AvailableSpace
        {
            get
            {
                try
                {
                    var driveInfo = GetDriveInfo();
                    return driveInfo?.AvailableFreeSpace ?? 0;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to get available space for volume {VolumeId}", _volumeId);
                    return 0;
                }
            }
        }

        /// <inheritdoc/>
        public bool IsHealthy
        {
            get
            {
                try
                {
                    // Check if mount path exists and is accessible
                    if (!_fileSystem.Directory.Exists(_mountPath))
                    {
                        _logger.LogWarning("Mount path does not exist: {MountPath}", _mountPath);
                        return false;
                    }

                    // Check if we have available space
                    var availableSpace = AvailableSpace;
                    if (availableSpace <= 0)
                    {
                        _logger.LogWarning("No available space on volume {VolumeId}", _volumeId);
                        return false;
                    }

                    // Try to create and delete a test file with retry mechanism
                    // This is important for network storage (NFS, Ceph, etc.) where directory
                    // creation might need a moment to fully synchronize
                    var testFilePath = _fileSystem.Path.Combine(_mountPath, $".health-check-{Guid.NewGuid()}.tmp");
                    const int maxRetries = 3;
                    const int retryDelayMs = 100;

                    for (int attempt = 1; attempt <= maxRetries; attempt++)
                    {
                        try
                        {
                            _fileSystem.File.WriteAllText(testFilePath, "health check");
                            _fileSystem.File.Delete(testFilePath);
                            return true; // Success
                        }
                        catch (Exception ex) when (attempt < maxRetries)
                        {
                            _logger.LogDebug(ex, "Health check write/delete test failed for volume {VolumeId} (attempt {Attempt}/{MaxRetries}), retrying...",
                                _volumeId, attempt, maxRetries);
                            System.Threading.Thread.Sleep(retryDelayMs);
                        }
                        catch (Exception ex)
                        {
                            // Final attempt failed
                            _logger.LogWarning(ex, "Health check write/delete test failed for volume {VolumeId} after {MaxRetries} attempts",
                                _volumeId, maxRetries);
                            return false;
                        }
                    }

                    return false;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Health check failed for volume {VolumeId}", _volumeId);
                    return false;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<Stream> ReadAsync(string path, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be empty", nameof(path));

            // Validate path safety
            if (!FileSystemPathSanitizer.IsPathWithinBase(_mountPath, path))
            {
                throw new InvalidOperationException($"Path is outside volume mount path: {path}");
            }

            if (!_fileSystem.File.Exists(path))
            {
                throw new FileNotFoundException($"File not found: {path}");
            }

            try
            {
                // Return a stream that can be read asynchronously
                var stream = _fileSystem.File.OpenRead(path);
                _logger.LogDebug("Opened file for reading: {Path}", path);
                return await Task.FromResult(stream);
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

            // Validate path safety
            if (!FileSystemPathSanitizer.IsPathWithinBase(_mountPath, path))
            {
                throw new InvalidOperationException($"Path is outside volume mount path: {path}");
            }

            try
            {
                // Ensure directory exists
                var directory = _fileSystem.Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(directory) && !_fileSystem.Directory.Exists(directory))
                {
                    _fileSystem.Directory.CreateDirectory(directory!);
                }

                // Write content to file
                using (var fileStream = _fileSystem.File.Create(path))
                {
                    await content.CopyToAsync(fileStream, 81920, ct); // 80KB buffer
                }

                _logger.LogDebug("Wrote file: {Path}, Size: {Size} bytes", path, content.Length);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write file: {Path}", path);
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task DeleteAsync(string path, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException("Path cannot be empty", nameof(path));

            // Validate path safety
            if (!FileSystemPathSanitizer.IsPathWithinBase(_mountPath, path))
            {
                throw new InvalidOperationException($"Path is outside volume mount path: {path}");
            }

            if (!_fileSystem.File.Exists(path))
            {
                _logger.LogWarning("Attempted to delete non-existent file: {Path}", path);
                return; // File doesn't exist, consider it deleted
            }

            try
            {
                _fileSystem.File.Delete(path);
                _logger.LogDebug("Deleted file: {Path}", path);
                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to delete file: {Path}", path);
                throw;
            }
        }

        /// <summary>
        /// Gets the DriveInfo for the mount path.
        /// Cross-platform compatible: uses RootDirectory.FullName for matching on Linux.
        /// </summary>
        private IDriveInfo? GetDriveInfo()
        {
            try
            {
                // Get the root drive for the mount path
                var root = _fileSystem.Path.GetPathRoot(_mountPath);
                if (string.IsNullOrEmpty(root))
                    return null;

                var drives = _fileSystem.DriveInfo.GetDrives();

                // Try to match by RootDirectory.FullName first (works on both Windows and Linux)
                // On Windows: Name="C:\", RootDirectory.FullName="C:\"
                // On Linux: Name="/dev/sda1", RootDirectory.FullName="/"
                var drive = drives.FirstOrDefault(d =>
                    d.IsReady && d.RootDirectory.FullName.Equals(root, StringComparison.OrdinalIgnoreCase));

                // Fallback to Name matching for backwards compatibility
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

        /// <summary>
        /// Builds the physical path for a file with automatic directory sharding.
        /// Uses 2-character hex segments from fileKey as subdirectories (similar to FastDFS).
        /// Each sharding level creates a 2-character directory (00-FF), allowing 256 subdirectories per level.
        /// Example with depth=2 and fileKey="a1b2c3d4...": {mountPath}/{tenantId}/a1/b2/a1b2c3d4...
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="fileKey">The file key (hex string).</param>
        /// <param name="fileExtension">The file extension (e.g., ".pdf", ".docx"). If provided, it will be appended to the file key.</param>
        /// <returns>The full physical path with sharding.</returns>
        public string BuildPhysicalPath(string tenantId, string fileKey, string? fileExtension = null)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var pathParts = new List<string> { _mountPath, tenantId };

            // Add shard directories based on fileKey (2 characters per level)
            // ShardingDepth=0: {mount}/{tenant}/{fileKey}
            // ShardingDepth=1: {mount}/{tenant}/a1/{fileKey}
            // ShardingDepth=2: {mount}/{tenant}/a1/b2/{fileKey}
            // ShardingDepth=3: {mount}/{tenant}/a1/b2/c3/{fileKey}
            for (int i = 0; i < _shardingDepth; i++)
            {
                int startIndex = i * 2;

                // Ensure we have at least 2 characters remaining
                if (startIndex + 1 < fileKey.Length)
                {
                    var shardDir = fileKey.Substring(startIndex, 2).ToLowerInvariant();
                    pathParts.Add(shardDir);
                }
                else if (startIndex < fileKey.Length)
                {
                    // If only 1 character remains, pad with '0'
                    var shardDir = (fileKey[startIndex].ToString() + "0").ToLowerInvariant();
                    pathParts.Add(shardDir);
                }
                else
                {
                    // Not enough characters, stop creating shard directories
                    break;
                }
            }

            // Append file extension to the file key
            var fileNameWithExtension = string.IsNullOrEmpty(fileExtension) ? fileKey : fileKey + fileExtension;
            pathParts.Add(fileNameWithExtension);

            return _fileSystem.Path.Combine(pathParts.ToArray());
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
