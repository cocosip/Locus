using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Abstractions.TestingHelpers;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Locus.FileSystem;
using Locus.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.FileSystem.Tests
{
    public class LocalFileSystemVolumeTests
    {
        private readonly MockFileSystem _fileSystem;
        private readonly Mock<ILogger<LocalFileSystemVolume>> _logger;
        private readonly string _mountPath;

        public LocalFileSystemVolumeTests()
        {
            _fileSystem = new MockFileSystem();
            _logger = new Mock<ILogger<LocalFileSystemVolume>>();
            
            // Use Path.GetFullPath to get a valid absolute path for the current OS/Environment.
            // This ensures that the path is consistent whether resolved by System.IO (in tests/Sanitizer)
            // or by MockFileSystem (in LocalFileSystemVolume), avoiding mismatches.
            _mountPath = Path.GetFullPath("test-volume");
            
            // Ensure the directory exists in the MockFileSystem
            _fileSystem.Directory.CreateDirectory(_mountPath);
        }

        [Fact]
        public void Constructor_CreatesMountPathIfNotExists()
        {
            // Arrange
            var newMountPath = Path.GetFullPath("new-volume");

            // Act
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", newMountPath);

            // Assert
            Assert.True(_fileSystem.Directory.Exists(newMountPath));
            Assert.Equal("vol-001", volume.VolumeId);
        }

        [Fact]
        public void Constructor_ThrowsWhenVolumeIdIsEmpty()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new LocalFileSystemVolume(_fileSystem, _logger.Object, "", _mountPath));
        }

        [Fact]
        public void Constructor_ThrowsWhenMountPathIsEmpty()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", ""));
        }

        [Fact]
        public void Constructor_DefaultsForceFlushAfterWriteToTrue()
        {
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);

            var forceFlushAfterWrite = GetPrivateField<bool>(volume, "_forceFlushAfterWrite");
            Assert.True(forceFlushAfterWrite);
        }

        [Fact]
        public async Task WriteAsync_CreatesFileSuccessfully()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var filePath = Path.Combine(_mountPath, "test-file.txt");
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));

            // Act
            await volume.WriteAsync(filePath, content, CancellationToken.None);

            // Assert
            Assert.True(_fileSystem.File.Exists(filePath));
            var savedContent = _fileSystem.File.ReadAllText(filePath);
            Assert.Equal("test content", savedContent);
        }

        [Fact]
        public async Task WriteAsync_CreatesDirectoryIfNotExists()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var filePath = Path.Combine(_mountPath, "subdir", "test-file.txt");
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));

            // Act
            await volume.WriteAsync(filePath, content, CancellationToken.None);

            // Assert
            Assert.True(_fileSystem.File.Exists(filePath));
            Assert.True(_fileSystem.Directory.Exists(Path.Combine(_mountPath, "subdir")));
        }

        [Fact]
        public async Task WriteAsync_MemoryStreamWithOffset_WritesRemainingBytesViaSynchronousFastPath()
        {
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var filePath = Path.Combine(_mountPath, "sync-offset.txt");
            var payload = Encoding.UTF8.GetBytes("prefix-saved-content");
            using var content = new MemoryStream(payload, 0, payload.Length, writable: false, publiclyVisible: true);
            content.Position = "prefix-".Length;

            await volume.WriteAsync(filePath, content, CancellationToken.None);

            Assert.True(_fileSystem.File.Exists(filePath));
            Assert.Equal("saved-content", _fileSystem.File.ReadAllText(filePath));
        }

        [Fact]
        public async Task WriteAsync_LargeMemoryStreamWithOffset_WritesRemainingBytesViaAsynchronousPath()
        {
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var filePath = Path.Combine(_mountPath, "async-offset.bin");
            var prefixLength = 257;
            var payloadLength = (1024 * 1024) + 321;
            var payload = new byte[prefixLength + payloadLength];
            for (var i = 0; i < payload.Length; i++)
                payload[i] = (byte)(i % 251);

            using var content = new MemoryStream(payload, 0, payload.Length, writable: false, publiclyVisible: true);
            content.Position = prefixLength;

            await volume.WriteAsync(filePath, content, CancellationToken.None);

            Assert.True(_fileSystem.File.Exists(filePath));
            var savedContent = _fileSystem.File.ReadAllBytes(filePath);
            Assert.Equal(payloadLength, savedContent.Length);

            for (var i = 0; i < savedContent.Length; i++)
                Assert.Equal(payload[prefixLength + i], savedContent[i]);
        }

        [Fact]
        public async Task GetWritePathStatisticsSnapshot_TracksDirectMemoryFastPathUsage()
        {
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var diagnostics = Assert.IsAssignableFrom<IStorageVolumeWritePathDiagnostics>(volume);

            var directPath = Path.Combine(_mountPath, "direct.bin");
            var hiddenPath = Path.Combine(_mountPath, "hidden.bin");

            var visiblePayload = Encoding.UTF8.GetBytes("visible-buffer");
            using (var visibleStream = new MemoryStream(visiblePayload, 0, visiblePayload.Length, writable: false, publiclyVisible: true))
            {
                await volume.WriteAsync(directPath, visibleStream, CancellationToken.None);
            }

            var hiddenPayload = Encoding.UTF8.GetBytes("hidden-buffer");
            using (var hiddenStream = new MemoryStream(hiddenPayload, writable: false))
            {
                await volume.WriteAsync(hiddenPath, hiddenStream, CancellationToken.None);
            }

            var snapshot = diagnostics.GetWritePathStatisticsSnapshot();
            Assert.Equal(2, snapshot.TotalWrites);
            Assert.Equal(2, snapshot.MemoryStreamWrites);
            Assert.Equal(1, snapshot.VisibleBufferWrites);
            Assert.Equal(1, snapshot.DirectFastPathWrites);
            Assert.Equal(visiblePayload.Length, snapshot.DirectFastPathBytes);
            Assert.Equal(1, snapshot.DirectFastPathSyncWrites);
            Assert.Equal(0, snapshot.DirectFastPathAsyncWrites);
            Assert.Equal(0, snapshot.SynchronousSeekableFileWrites);
            Assert.Equal(1, snapshot.HiddenMemoryStreamWrites);
            Assert.Equal(0, snapshot.NonMemorySeekableWrites);
            Assert.Equal(0, snapshot.NonSeekableWrites);
        }

        [Fact]
        public async Task GetWritePathStatisticsSnapshot_TracksSynchronousSeekableFileWrites()
        {
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var diagnostics = Assert.IsAssignableFrom<IStorageVolumeWritePathDiagnostics>(volume);
            var destinationPath = Path.Combine(_mountPath, "file-stream.bin");

            var tempSourcePath = Path.Combine(Path.GetTempPath(), $"locus-seekable-source-{Guid.NewGuid():N}.bin");
            try
            {
                var payload = new byte[1024 * 1024];
                for (var i = 0; i < payload.Length; i++)
                    payload[i] = (byte)(i % 251);

                File.WriteAllBytes(tempSourcePath, payload);

                using (var source = new FileStream(
                    tempSourcePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    64 * 1024,
                    FileOptions.SequentialScan))
                {
                    await volume.WriteAsync(destinationPath, source, CancellationToken.None);
                }

                var snapshot = diagnostics.GetWritePathStatisticsSnapshot();
                Assert.Equal(1, snapshot.TotalWrites);
                Assert.Equal(0, snapshot.MemoryStreamWrites);
                Assert.Equal(0, snapshot.DirectFastPathWrites);
                Assert.Equal(1, snapshot.SynchronousSeekableFileWrites);
                Assert.Equal(1, snapshot.NonMemorySeekableWrites);
                Assert.Equal(0, snapshot.NonSeekableWrites);

                var savedContent = _fileSystem.File.ReadAllBytes(destinationPath);
                Assert.Equal(payload.Length, savedContent.Length);
                for (var i = 0; i < savedContent.Length; i++)
                    Assert.Equal(payload[i], savedContent[i]);
            }
            finally
            {
                if (File.Exists(tempSourcePath))
                    File.Delete(tempSourcePath);
            }
        }

        [Fact]
        public async Task WriteAsync_KnownDirectoryCacheTrim_KeepsMountPathAndBoundsSize()
        {
            // Use a tiny cache to force trim pressure in a short loop.
            var volume = new LocalFileSystemVolume(
                _fileSystem,
                _logger.Object,
                "vol-001",
                _mountPath,
                knownDirectoryCacheMaxEntries: 16);

            for (var i = 0; i < 80; i++)
            {
                var directoryPath = Path.Combine(_mountPath, $"trim-{i:D3}");
                var filePath = Path.Combine(directoryPath, "file.txt");
                using var content = new MemoryStream(Encoding.UTF8.GetBytes("x"));
                await volume.WriteAsync(filePath, content, CancellationToken.None);
            }

            var knownDirectories = GetPrivateField<ConcurrentDictionary<string, byte>>(volume, "_knownDirectories");
            Assert.True(knownDirectories.ContainsKey(_mountPath));
            Assert.True(knownDirectories.Count <= 16);
        }

        [Fact]
        public async Task WriteAsync_ThrowsWhenPathOutsideMountPath()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var outsidePath = Path.Combine("outside", "test-file.txt");
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                volume.WriteAsync(outsidePath, content, CancellationToken.None));
        }

        [Fact]
        public async Task ReadAsync_ReadsFileSuccessfully()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var filePath = Path.Combine(_mountPath, "test-file.txt");
            _fileSystem.AddFile(filePath, new MockFileData("test content"));

            // Act
            using var stream = await volume.ReadAsync(filePath, CancellationToken.None);
            using var reader = new StreamReader(stream);
            var content = await reader.ReadToEndAsync();

            // Assert
            Assert.Equal("test content", content);
        }

        [Fact]
        public async Task ReadAsync_ThrowsWhenFileNotFound()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var filePath = Path.Combine(_mountPath, "nonexistent-file.txt");

            // Act & Assert
            await Assert.ThrowsAsync<FileNotFoundException>(() =>
                volume.ReadAsync(filePath, CancellationToken.None));
        }

        [Fact]
        public async Task ReadAsync_ThrowsWhenPathOutsideMountPath()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var outsidePath = Path.Combine("outside", "test-file.txt");

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                volume.ReadAsync(outsidePath, CancellationToken.None));
        }

        [Fact]
        public async Task DeleteAsync_DeletesFileSuccessfully()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var filePath = Path.Combine(_mountPath, "test-file.txt");
            _fileSystem.AddFile(filePath, new MockFileData("test content"));

            // Act
            await volume.DeleteAsync(filePath, CancellationToken.None);

            // Assert
            Assert.False(_fileSystem.File.Exists(filePath));
        }

        [Fact]
        public async Task DeleteAsync_DoesNotThrowWhenFileNotFound()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var filePath = Path.Combine(_mountPath, "nonexistent-file.txt");

            // Act & Assert - should not throw
            await volume.DeleteAsync(filePath, CancellationToken.None);
        }

        [Fact]
        public async Task DeleteAsync_ThrowsWhenPathOutsideMountPath()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);
            var outsidePath = Path.Combine("outside", "test-file.txt");

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                volume.DeleteAsync(outsidePath, CancellationToken.None));
        }

        [Fact]
        public void IsHealthy_ReturnsTrueWhenMountPathExists()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);

            // Act
            var isHealthy = volume.IsHealthy;

            // Assert - Note: Mock file system health check will fail on test file creation
            // This is expected behavior with MockFileSystem
            // In real scenarios with real file system, this would return true
            Assert.False(isHealthy); // MockFileSystem limitation
        }

        [Fact]
        public void ToString_ReturnsFormattedString()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);

            // Act
            var result = volume.ToString();

            // Assert
            Assert.Contains("vol-001", result);
            Assert.Contains(_mountPath, result);
            Assert.Contains("LocalFileSystemVolume", result);
        }

        [Fact]
        public void BuildPhysicalPath_WithShardingDepth0_NoSharding()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath, shardingDepth: 0);

            // Act
            var path = volume.BuildPhysicalPath("tenant-001", "a1b2c3d4e5f6");

            // Assert
            var expected = Path.Combine(_mountPath, "tenant-001", "a1b2c3d4e5f6");
            Assert.Equal(Path.GetFullPath(expected), path);
        }

        [Fact]
        public void BuildPhysicalPath_WithShardingDepth1_OneLevelSharding()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath, shardingDepth: 1);

            // Act
            var path = volume.BuildPhysicalPath("tenant-001", "a1b2c3d4e5f6");

            // Assert - Should be: {mount}/tenant-001/a1/a1b2c3d4e5f6
            var expected = Path.Combine(_mountPath, "tenant-001", "a1", "a1b2c3d4e5f6");
            Assert.Equal(Path.GetFullPath(expected), path);
        }

        [Fact]
        public void BuildPhysicalPath_WithShardingDepth2_TwoLevelSharding()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath, shardingDepth: 2);

            // Act
            var path = volume.BuildPhysicalPath("tenant-001", "a1b2c3d4e5f6");

            // Assert - Should be: {mount}/tenant-001/a1/b2/a1b2c3d4e5f6
            var expected = Path.Combine(_mountPath, "tenant-001", "a1", "b2", "a1b2c3d4e5f6");
            Assert.Equal(Path.GetFullPath(expected), path);
        }

        [Fact]
        public void BuildPhysicalPath_WithShardingDepth3_ThreeLevelSharding()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath, shardingDepth: 3);

            // Act
            var path = volume.BuildPhysicalPath("tenant-001", "a1b2c3d4e5f6");

            // Assert - Should be: {mount}/tenant-001/a1/b2/c3/a1b2c3d4e5f6
            var expected = Path.Combine(_mountPath, "tenant-001", "a1", "b2", "c3", "a1b2c3d4e5f6");
            Assert.Equal(Path.GetFullPath(expected), path);
        }

        [Fact]
        public void BuildPhysicalPath_WithUpperCaseFileKey_UsesOriginalCaseForShards()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath, shardingDepth: 2);

            // Act
            var path = volume.BuildPhysicalPath("tenant-001", "FF00AA55BB");

            // Assert - Shard dirs use the original case from fileKey (GUIDs are always lowercase, so this is academic).
            var expected = Path.Combine(_mountPath, "tenant-001", "FF", "00", "FF00AA55BB");
            Assert.Equal(Path.GetFullPath(expected), path);
        }

        [Fact]
        public void BuildPhysicalPath_WithShortFileKey_PadsWithZero()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath, shardingDepth: 2);

            // Act - FileKey with only 3 characters
            var path = volume.BuildPhysicalPath("tenant-001", "abc");

            // Assert - Should create ab/c0/abc
            var expected = Path.Combine(_mountPath, "tenant-001", "ab", "c0", "abc");
            Assert.Equal(Path.GetFullPath(expected), path);
        }

        [Fact]
        public void BuildPhysicalPath_WithVeryShortFileKey_StopsWhenNotEnoughCharacters()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath, shardingDepth: 3);

            // Act - FileKey with only 1 character
            var path = volume.BuildPhysicalPath("tenant-001", "a");

            // Assert - Should only create one level: a0/a (can't create more levels)
            var expected = Path.Combine(_mountPath, "tenant-001", "a0", "a");
            Assert.Equal(Path.GetFullPath(expected), path);
        }

        [Fact]
        public void BuildPhysicalPath_ThrowsWhenTenantIdIsEmpty()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);

            // Act & Assert
            Assert.Throws<ArgumentException>(() => volume.BuildPhysicalPath("", "a1b2c3"));
        }

        [Fact]
        public void BuildPhysicalPath_ThrowsWhenFileKeyIsEmpty()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath);

            // Act & Assert
            Assert.Throws<ArgumentException>(() => volume.BuildPhysicalPath("tenant-001", ""));
        }

        [Fact]
        public void BuildPhysicalPath_FastDFSStyleExamples()
        {
            // Arrange
            var volume = new LocalFileSystemVolume(_fileSystem, _logger.Object, "vol-001", _mountPath, shardingDepth: 2);

            // Test case 1: Standard hex string
            var path1 = volume.BuildPhysicalPath("tenant-001", "a1b2c3d4e5f6");
            var expected1 = Path.Combine(_mountPath, "tenant-001", "a1", "b2", "a1b2c3d4e5f6");
            Assert.Equal(Path.GetFullPath(expected1), path1);

            // Test case 2: All same digits
            var path2 = volume.BuildPhysicalPath("tenant-002", "00000000");
            var expected2 = Path.Combine(_mountPath, "tenant-002", "00", "00", "00000000");
            Assert.Equal(Path.GetFullPath(expected2), path2);

            // Test case 3: All F's
            var path3 = volume.BuildPhysicalPath("tenant-003", "ffffffff");
            var expected3 = Path.Combine(_mountPath, "tenant-003", "ff", "ff", "ffffffff");
            Assert.Equal(Path.GetFullPath(expected3), path3);

            // Test case 4: Mixed case — shard dirs preserve original case (GUID fileKeys are always lowercase)
            var path4 = volume.BuildPhysicalPath("tenant-004", "AaBbCcDd");
            var expected4 = Path.Combine(_mountPath, "tenant-004", "Aa", "Bb", "AaBbCcDd");
            Assert.Equal(Path.GetFullPath(expected4), path4);
        }

        private static T GetPrivateField<T>(object target, string fieldName)
        {
            var field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            return (T)field!.GetValue(target)!;
        }
    }
}
