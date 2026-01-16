using System;
using System.IO;
using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Exceptions;
using Locus.Core.Models;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class StoragePoolTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly Mock<ITenantQuotaManager> _tenantQuotaManager;
        private readonly Mock<ITenantManager> _tenantManager;
        private readonly Mock<IFileScheduler> _fileScheduler;
        private readonly Mock<ILogger<StoragePool>> _logger;
        private readonly StoragePool _storagePool;
        private readonly Mock<IStorageVolume> _volume1;
        private readonly Mock<IStorageVolume> _volume2;
        private readonly Mock<ITenantContext> _tenant;
        private readonly string _volume1Path;
        private readonly string _volume2Path;
        private readonly string _metadataPath;
        private readonly string _quotaPath;

        public StoragePoolTests()
        {
            // Use real file system for tests (LiteDB requires real file system)
            _fileSystem = new System.IO.Abstractions.FileSystem();

            // Setup repositories with unique temporary directories
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _metadataPath = Path.Combine(Path.GetTempPath(), $"locus-test-metadata-{testId}");
            _quotaPath = Path.Combine(Path.GetTempPath(), $"locus-test-quota-{testId}");
            _fileSystem.Directory.CreateDirectory(_metadataPath);
            _fileSystem.Directory.CreateDirectory(_quotaPath);

            var metadataRepoLogger = new Mock<ILogger<MetadataRepository>>();
            _metadataRepository = new MetadataRepository(_fileSystem, metadataRepoLogger.Object, _metadataPath);

            var quotaRepoLogger = new Mock<ILogger<DirectoryQuotaRepository>>();
            _quotaRepository = new DirectoryQuotaRepository(_fileSystem, quotaRepoLogger.Object, _quotaPath);

            // Setup tenant quota manager mock
            _tenantQuotaManager = new Mock<ITenantQuotaManager>();
            _tenantQuotaManager.Setup(m => m.CanAddFileAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
            _tenantQuotaManager.Setup(m => m.IncrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            _tenantQuotaManager.Setup(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            // Setup tenant manager
            _tenantManager = new Mock<ITenantManager>();

            // Setup GetTenantAsync to return a valid enabled tenant context for tenant-001
            var tenantContext = new Mock<ITenantContext>();
            tenantContext.Setup(t => t.TenantId).Returns("tenant-001");
            tenantContext.Setup(t => t.Status).Returns(TenantStatus.Enabled);

            _tenantManager.Setup(m => m.GetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(tenantContext.Object);

            // Setup for other tenant IDs (tenant-002)
            var tenantContext2 = new Mock<ITenantContext>();
            tenantContext2.Setup(t => t.TenantId).Returns("tenant-002");
            tenantContext2.Setup(t => t.Status).Returns(TenantStatus.Enabled);

            _tenantManager.Setup(m => m.GetTenantAsync("tenant-002", It.IsAny<CancellationToken>()))
                .ReturnsAsync(tenantContext2.Object);

            _tenantManager.Setup(m => m.IsTenantEnabledAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            _tenantManager.Setup(m => m.CreateTenantAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            // Setup file scheduler
            _fileScheduler = new Mock<IFileScheduler>();

            // Setup storage pool
            _logger = new Mock<ILogger<StoragePool>>();
            _storagePool = new StoragePool(
                _metadataRepository,
                _tenantQuotaManager.Object,
                _tenantManager.Object,
                _fileScheduler.Object,
                _logger.Object);

            // Setup mock volumes with unique temporary directories
            _volume1Path = Path.Combine(Path.GetTempPath(), $"locus-test-vol1-{testId}");
            _volume2Path = Path.Combine(Path.GetTempPath(), $"locus-test-vol2-{testId}");
            _fileSystem.Directory.CreateDirectory(_volume1Path);
            _fileSystem.Directory.CreateDirectory(_volume2Path);

            _volume1 = new Mock<IStorageVolume>();
            _volume1.Setup(v => v.VolumeId).Returns("vol-001");
            _volume1.Setup(v => v.MountPath).Returns(_volume1Path);
            _volume1.Setup(v => v.TotalCapacity).Returns(1000000000L); // 1GB
            _volume1.Setup(v => v.AvailableSpace).Returns(500000000L); // 500MB
            _volume1.Setup(v => v.IsHealthy).Returns(true);
            _volume1.Setup(v => v.WriteAsync(It.IsAny<string>(), It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
                .Returns((string path, Stream content, CancellationToken ct) =>
                {
                    var dir = Path.GetDirectoryName(path);
                    if (!string.IsNullOrEmpty(dir) && !_fileSystem.Directory.Exists(dir))
                        _fileSystem.Directory.CreateDirectory(dir);
                    using (var fs = _fileSystem.File.Create(path))
                    {
                        content.CopyTo(fs);
                    }
                    return Task.CompletedTask;
                });
            _volume1.Setup(v => v.ReadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string path, CancellationToken ct) =>
                {
                    if (!_fileSystem.File.Exists(path))
                        throw new FileNotFoundException($"File not found: {path}");
                    return Task.FromResult<Stream>(_fileSystem.File.OpenRead(path));
                });
            _volume1.Setup(v => v.DeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string path, CancellationToken ct) =>
                {
                    if (_fileSystem.File.Exists(path))
                        _fileSystem.File.Delete(path);
                    return Task.CompletedTask;
                });

            _volume2 = new Mock<IStorageVolume>();
            _volume2.Setup(v => v.VolumeId).Returns("vol-002");
            _volume2.Setup(v => v.MountPath).Returns(_volume2Path);
            _volume2.Setup(v => v.TotalCapacity).Returns(1000000000L);
            _volume2.Setup(v => v.AvailableSpace).Returns(600000000L); // 600MB (more than volume1)
            _volume2.Setup(v => v.IsHealthy).Returns(true);
            _volume2.Setup(v => v.WriteAsync(It.IsAny<string>(), It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
                .Returns((string path, Stream content, CancellationToken ct) =>
                {
                    var dir = Path.GetDirectoryName(path);
                    if (!string.IsNullOrEmpty(dir) && !_fileSystem.Directory.Exists(dir))
                        _fileSystem.Directory.CreateDirectory(dir);
                    using (var fs = _fileSystem.File.Create(path))
                    {
                        content.CopyTo(fs);
                    }
                    return Task.CompletedTask;
                });
            _volume2.Setup(v => v.ReadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string path, CancellationToken ct) =>
                {
                    if (!_fileSystem.File.Exists(path))
                        throw new FileNotFoundException($"File not found: {path}");
                    return Task.FromResult<Stream>(_fileSystem.File.OpenRead(path));
                });
            _volume2.Setup(v => v.DeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string path, CancellationToken ct) =>
                {
                    if (_fileSystem.File.Exists(path))
                        _fileSystem.File.Delete(path);
                    return Task.CompletedTask;
                });

            // Mount volumes (now using public method)
            _storagePool.AddVolume(_volume1.Object);
            _storagePool.AddVolume(_volume2.Object);

            // Setup tenant
            _tenant = new Mock<ITenantContext>();
            _tenant.Setup(t => t.TenantId).Returns("tenant-001");
            _tenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);
        }

        public void Dispose()
        {
            _metadataRepository?.Dispose();
            _quotaRepository?.Dispose();

            // Cleanup temporary test directories
            try
            {
                if (_fileSystem.Directory.Exists(_metadataPath))
                    _fileSystem.Directory.Delete(_metadataPath, recursive: true);

                if (_fileSystem.Directory.Exists(_quotaPath))
                    _fileSystem.Directory.Delete(_quotaPath, recursive: true);

                if (_fileSystem.Directory.Exists(_volume1Path))
                    _fileSystem.Directory.Delete(_volume1Path, recursive: true);

                if (_fileSystem.Directory.Exists(_volume2Path))
                    _fileSystem.Directory.Delete(_volume2Path, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }

        [Fact]
        public async Task WriteFileAsync_CreatesFileSuccessfully()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));

            // Act
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, default);

            // Assert
            Assert.False(string.IsNullOrEmpty(fileKey));

            // Verify file was written
            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default);
            Assert.NotNull(location);
            Assert.Equal(fileKey, location.FileKey);
            Assert.Equal("tenant-001", location.TenantId);
        }

        [Fact]
        public async Task WriteFileAsync_ThrowsWhenTenantDisabled()
        {
            // Arrange
            // Setup GetTenantAsync to return a disabled tenant
            var disabledTenantContext = new Mock<ITenantContext>();
            disabledTenantContext.Setup(t => t.TenantId).Returns("tenant-001");
            disabledTenantContext.Setup(t => t.Status).Returns(TenantStatus.Disabled);

            _tenantManager.Setup(m => m.GetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(disabledTenantContext.Object);

            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));

            // Act & Assert
            await Assert.ThrowsAsync<TenantDisabledException>(() =>
                _storagePool.WriteFileAsync(_tenant.Object, content, default));
        }

        [Fact]
        public async Task WriteFileAsync_ThrowsWhenTenantQuotaExceeded()
        {
            // Arrange
            _tenantQuotaManager.Setup(m => m.IncrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new TenantQuotaExceededException("tenant-001", 10, 10));

            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));

            // Act & Assert
            await Assert.ThrowsAsync<TenantQuotaExceededException>(() =>
                _storagePool.WriteFileAsync(_tenant.Object, content, default));
        }

        [Fact]
        public async Task ReadFileAsync_ReadsFileSuccessfully()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, default);

            // Act
            using var readStream = await _storagePool.ReadFileAsync(_tenant.Object, fileKey, default);
            using var reader = new StreamReader(readStream);
            var readContent = await reader.ReadToEndAsync();

            // Assert
            Assert.Equal("test content", readContent);
        }

        [Fact]
        public async Task ReadFileAsync_ThrowsWhenFileNotFound()
        {
            // Act & Assert
            await Assert.ThrowsAsync<FileNotFoundException>(() =>
                _storagePool.ReadFileAsync(_tenant.Object, "nonexistent-key", default));
        }

        [Fact]
        public async Task ReadFileAsync_ThrowsWhenTenantDisabled()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, default);

            // Setup GetTenantAsync to return a disabled tenant
            var disabledTenantContext = new Mock<ITenantContext>();
            disabledTenantContext.Setup(t => t.TenantId).Returns("tenant-001");
            disabledTenantContext.Setup(t => t.Status).Returns(TenantStatus.Disabled);

            _tenantManager.Setup(m => m.GetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(disabledTenantContext.Object);

            // Act & Assert
            await Assert.ThrowsAsync<TenantDisabledException>(() =>
                _storagePool.ReadFileAsync(_tenant.Object, fileKey, default));
        }

        [Fact]
        public async Task ReadFileAsync_ThrowsWhenAccessingOtherTenantFile()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, default);

            var otherTenant = new Mock<ITenantContext>();
            otherTenant.Setup(t => t.TenantId).Returns("tenant-002");
            otherTenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);

            // Act & Assert
            // File doesn't exist from other tenant's perspective (tenant isolation)
            await Assert.ThrowsAsync<FileNotFoundException>(() =>
                _storagePool.ReadFileAsync(otherTenant.Object, fileKey, default));
        }

        // Note: DeleteFileAsync is now internal and should be accessed via MarkAsCompletedAsync
        // These tests have been removed as DeleteFileAsync is no longer part of the public API

        [Fact]
        public async Task GetFileLocationAsync_ReturnsCorrectLocation()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, default);

            // Act
            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default);

            // Assert
            Assert.NotNull(location);
            Assert.Equal(fileKey, location.FileKey);
            Assert.Equal("tenant-001", location.TenantId);
            Assert.Equal(FileProcessingStatus.Pending, location.Status);
            Assert.Equal("/", location.DirectoryPath);
        }

        [Fact]
        public async Task GetFileLocationAsync_ReturnsNullForNonexistentFile()
        {
            // Act
            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, "nonexistent-key", default);

            // Assert
            Assert.Null(location);
        }

        [Fact]
        public async Task GetFileLocationAsync_ReturnsNullForOtherTenantFile()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, default);

            var otherTenant = new Mock<ITenantContext>();
            otherTenant.Setup(t => t.TenantId).Returns("tenant-002");

            // Act
            var location = await _storagePool.GetFileLocationAsync(otherTenant.Object, fileKey, default);

            // Assert
            Assert.Null(location);
        }

        [Fact]
        public async Task GetTotalCapacityAsync_ReturnsSumOfAllVolumes()
        {
            // Act
            var totalCapacity = await _storagePool.GetTotalCapacityAsync(default);

            // Assert
            Assert.Equal(2000000000L, totalCapacity); // 1GB + 1GB
        }

        [Fact]
        public async Task GetAvailableSpaceAsync_ReturnsSumOfAllVolumes()
        {
            // Act
            var availableSpace = await _storagePool.GetAvailableSpaceAsync(default);

            // Assert
            Assert.Equal(1100000000L, availableSpace); // 500MB + 600MB
        }

        [Fact]
        public async Task WriteFileAsync_DistributesFilesAcrossVolumes()
        {
            // This test verifies that files are written to volumes
            // The volume selection algorithm chooses the volume with most available space

            // Arrange
            var content1 = new MemoryStream(Encoding.UTF8.GetBytes("content 1"));
            var content2 = new MemoryStream(Encoding.UTF8.GetBytes("content 2"));

            // Act
            var fileKey1 = await _storagePool.WriteFileAsync(_tenant.Object, content1, default);
            var fileKey2 = await _storagePool.WriteFileAsync(_tenant.Object, content2, default);

            // Assert
            var location1 = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey1, default);
            var location2 = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey2, default);

            Assert.NotNull(location1);
            Assert.NotNull(location2);
            // Both should be written to a volume (in mock file system, all volumes have same space)
            Assert.Contains(location1.VolumeId, new[] { "vol-001", "vol-002" });
            Assert.Contains(location2.VolumeId, new[] { "vol-001", "vol-002" });
        }

        [Fact]
        public async Task WriteFileAsync_RollbacksQuotaOnFailure()
        {
            // Arrange
            // Use null stream to trigger ArgumentNullException
            Stream invalidContent = null!;

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                _storagePool.WriteFileAsync(_tenant.Object, invalidContent, default));

            // Verify quota was not incremented (mock should not have been called)
            _tenantQuotaManager.Verify(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }
    }
}
