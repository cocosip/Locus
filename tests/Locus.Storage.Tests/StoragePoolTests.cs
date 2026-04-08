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
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class StoragePoolTests : IAsyncLifetime
    {
        private readonly IFileSystem _fileSystem;
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly Mock<ITenantQuotaManager> _tenantQuotaManager;
        private readonly Mock<IDirectoryQuotaManager> _directoryQuotaManager;
        private readonly Mock<ITenantManager> _tenantManager;
        private readonly Mock<IFileScheduler> _fileScheduler;
        private readonly Mock<IQueueEventJournal> _queueEventJournal;
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
            // Use real file system for tests (SQLite requires real file system)
            _fileSystem = new System.IO.Abstractions.FileSystem();

            // Setup repositories with unique temporary directories
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _metadataPath = Path.Combine(Path.GetTempPath(), $"locus-test-metadata-{testId}");
            _quotaPath = Path.Combine(Path.GetTempPath(), $"locus-test-quota-{testId}");
            _fileSystem.Directory.CreateDirectory(_metadataPath);
            _fileSystem.Directory.CreateDirectory(_quotaPath);

            var metadataRepoLogger = new Mock<ILogger<MetadataRepository>>();
            _metadataRepository = new MetadataRepository(
                _fileSystem,
                metadataRepoLogger.Object,
                _metadataPath,
                enableBackgroundPersistence: false);

            var quotaRepoLogger = new Mock<ILogger<DirectoryQuotaRepository>>();
            _quotaRepository = new DirectoryQuotaRepository(
                _fileSystem,
                quotaRepoLogger.Object,
                _quotaPath,
                enableBackgroundFlush: false);

            // Setup tenant quota manager mock
            _tenantQuotaManager = new Mock<ITenantQuotaManager>();
            _tenantQuotaManager.Setup(m => m.CanAddFileAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
            _tenantQuotaManager.Setup(m => m.IncrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            _tenantQuotaManager.Setup(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            _directoryQuotaManager = new Mock<IDirectoryQuotaManager>();
            _directoryQuotaManager.Setup(m => m.CanAddFileAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
            _directoryQuotaManager.Setup(m => m.IncrementFileCountAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            _directoryQuotaManager.Setup(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
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
            _queueEventJournal = new Mock<IQueueEventJournal>();
            _queueEventJournal
                .Setup(m => m.AppendAsync(It.IsAny<QueueEventRecord>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            // Setup storage pool
            _logger = new Mock<ILogger<StoragePool>>();
            _storagePool = new StoragePool(
                _metadataRepository,
                _tenantQuotaManager.Object,
                _directoryQuotaManager.Object,
                _tenantManager.Object,
                _fileScheduler.Object,
                _logger.Object,
                queueEventJournal: _queueEventJournal.Object);

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
            _volume1.Setup(v => v.BuildPhysicalPath(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string?>()))
                .Returns((string tenantId, string fileKey, string? ext) =>
                    Path.Combine(_volume1Path, tenantId, string.IsNullOrEmpty(ext) ? fileKey : fileKey + ext));
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
            _volume2.Setup(v => v.BuildPhysicalPath(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string?>()))
                .Returns((string tenantId, string fileKey, string? ext) =>
                    Path.Combine(_volume2Path, tenantId, string.IsNullOrEmpty(ext) ? fileKey : fileKey + ext));
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

            // Volumes are mounted in InitializeAsync to use AddVolumeAsync without blocking.

            // Setup tenant
            _tenant = new Mock<ITenantContext>();
            _tenant.Setup(t => t.TenantId).Returns("tenant-001");
            _tenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);
        }

        // IAsyncLifetime — mount volumes once without blocking the constructor.
        // Pass zero delays so mock volumes are mounted instantly (no Thread.Sleep).
        public async Task InitializeAsync()
        {
            await _storagePool.AddVolumeAsync(_volume1.Object, initialDelayMs: 0, healthCheckDelayMs: 0);
            await _storagePool.AddVolumeAsync(_volume2.Object, initialDelayMs: 0, healthCheckDelayMs: 0);
        }

        [Fact]
        public void Constructor_WithoutJournal_RequiresExplicitLegacyAllowance()
        {
            var ex = Assert.Throws<InvalidOperationException>(() =>
                new StoragePool(
                    _metadataRepository,
                    _tenantQuotaManager.Object,
                    _directoryQuotaManager.Object,
                    _tenantManager.Object,
                    _fileScheduler.Object,
                    _logger.Object));

            Assert.Contains("legacy non-journal mode", ex.Message);
        }

        public async Task DisposeAsync()
        {
            // Dispose repositories to release SQLite connections
            _metadataRepository?.Dispose();
            _quotaRepository?.Dispose();

            // Clear SQLite connection pool to release file handles
            SqliteConnection.ClearAllPools();

            // Cleanup temporary test directories
            // Delete volume paths first (actual data), then metadata and quota paths
            CleanupTestDirectory(_volume1Path);
            CleanupTestDirectory(_volume2Path);
            CleanupTestDirectory(_metadataPath);
            CleanupTestDirectory(_quotaPath);
            await Task.CompletedTask;
        }

        private void CleanupTestDirectory(string path)
        {
            try
            {
                if (_fileSystem.Directory.Exists(path))
                {
                    _fileSystem.Directory.Delete(path, recursive: true);
                }
            }
            catch (Exception ex)
            {
                // Log cleanup failures for debugging, but don't fail the test
                // Cleanup failures are typically due to file handles still being open
                // or concurrent test execution accessing the same directory
                System.Diagnostics.Debug.WriteLine($"Failed to cleanup test directory {path}: {ex.Message}");
            }
        }

        private static FileProcessingLease CreateLease(string tenantId, string fileKey, DateTime processingStartTimeUtc)
        {
            return new FileProcessingLease
            {
                TenantId = tenantId,
                FileKey = fileKey,
                ProcessingStartTimeUtc = processingStartTimeUtc
            };
        }

        [Fact]
        public async Task WriteFileAsync_CreatesFileSuccessfully()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));

            // Act
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);

            // Assert
            Assert.False(string.IsNullOrEmpty(fileKey));

            // Verify file was written
            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default);
            Assert.NotNull(location);
            Assert.Equal(fileKey, location.FileKey);
            Assert.Equal("tenant-001", location.TenantId);
        }

        [Fact]
        public async Task WriteFileAsync_AppendsAcceptedQueueEvent()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));

            // Act
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, "image.dcm", default);

            // Assert
            _queueEventJournal.Verify(
                m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.Accepted
                        && record.TenantId == "tenant-001"
                        && record.FileKey == fileKey
                        && record.Status == FileProcessingStatus.Pending
                        && record.OriginalFileName == "image.dcm"
                        && record.FileExtension == ".dcm"),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task WriteFileAsync_WithLogicalDirectory_PersistsLogicalDirectoryIntoMetadataAndJournal()
        {
            var content = new MemoryStream(Encoding.UTF8.GetBytes("logical-dir"));

            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, "scan.dcm", "/incoming/studies", default);

            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default);

            Assert.NotNull(location);
            Assert.Equal("/incoming/studies", location!.DirectoryPath);

            _queueEventJournal.Verify(
                m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.Accepted
                        && record.FileKey == fileKey
                        && record.DirectoryPath == "/incoming/studies"),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task WriteFileAsync_WithRelativeSegments_NormalizesLogicalDirectoryBeforeQuotaAndJournal()
        {
            var content = new MemoryStream(Encoding.UTF8.GetBytes("logical-dir-normalized"));

            var fileKey = await _storagePool.WriteFileAsync(
                _tenant.Object,
                content,
                "scan.dcm",
                "/incoming/../studies/./ct",
                default);

            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default);

            Assert.NotNull(location);
            Assert.Equal("/studies/ct", location!.DirectoryPath);

            _directoryQuotaManager.Verify(
                manager => manager.IncrementFileCountAsync(
                    "tenant-001",
                    "/studies/ct",
                    It.IsAny<CancellationToken>()),
                Times.Once);

            _queueEventJournal.Verify(
                journal => journal.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.Accepted
                        && record.FileKey == fileKey
                        && record.DirectoryPath == "/studies/ct"),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task WriteFileAsync_WithoutLogicalDirectory_UsesRootDirectory()
        {
            var content = new MemoryStream(Encoding.UTF8.GetBytes("root-dir"));

            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, "scan.dcm", default);

            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default);

            Assert.NotNull(location);
            Assert.Equal("/", location!.DirectoryPath);
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_AppendsProcessingStartedQueueEvent()
        {
            var processingStartTime = DateTime.UtcNow;
            _fileScheduler
                .Setup(s => s.GetNextFileForProcessingAsync(_tenant.Object, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileLocation
                {
                    FileKey = "processing-001",
                    TenantId = "tenant-001",
                    VolumeId = "vol-001",
                    PhysicalPath = Path.Combine(_volume1Path, "tenant-001", "processing-001.dcm"),
                    DirectoryPath = "/incoming",
                    FileSize = 128,
                    CreatedAt = DateTime.UtcNow.AddMinutes(-1),
                    Status = FileProcessingStatus.Processing,
                    RetryCount = 0,
                    ProcessingStartTime = processingStartTime
                });

            _queueEventJournal.Invocations.Clear();

            var location = await _storagePool.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None);

            Assert.NotNull(location);
            Assert.NotNull(location!.Lease);
            Assert.Equal("tenant-001", location.Lease!.TenantId);
            Assert.Equal("processing-001", location.Lease.FileKey);
            Assert.Equal(processingStartTime, location.Lease.ProcessingStartTimeUtc);
            _queueEventJournal.Verify(
                m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.ProcessingStarted
                        && record.TenantId == "tenant-001"
                        && record.FileKey == "processing-001"
                        && record.Status == FileProcessingStatus.Processing
                        && record.ProcessingStartTimeUtc == processingStartTime),
                    It.IsAny<CancellationToken>()),
                Times.Once);
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
                _storagePool.WriteFileAsync(_tenant.Object, content, null, default));
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
                _storagePool.WriteFileAsync(_tenant.Object, content, null, default));
        }

        [Fact]
        public async Task ReadFileAsync_ReadsFileSuccessfully()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);

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
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);

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
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);

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
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);

            // Act
            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default);

            // Assert
            Assert.NotNull(location);
            Assert.Equal(fileKey, location.FileKey);
            Assert.Equal("tenant-001", location.TenantId);
            Assert.Equal(FileProcessingStatus.Pending, location.Status);
            Assert.False(string.IsNullOrEmpty(location.DirectoryPath));
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
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);

            var otherTenant = new Mock<ITenantContext>();
            otherTenant.Setup(t => t.TenantId).Returns("tenant-002");

            // Act
            var location = await _storagePool.GetFileLocationAsync(otherTenant.Object, fileKey, default);

            // Assert
            Assert.Null(location);
        }

        [Fact]
        public async Task GetFileLocationAsync_UsesProjectionStoreWhenProvided()
        {
            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(store => store.GetProjectedFileAsync("tenant-001", "projection-location", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileMetadata
                {
                    FileKey = "projection-location",
                    TenantId = "tenant-001",
                    VolumeId = "vol-001",
                    PhysicalPath = Path.Combine(_volume1Path, "tenant-001", "projection-location.dcm"),
                    DirectoryPath = "/projection",
                    FileSize = 64,
                    CreatedAt = DateTime.UtcNow.AddMinutes(-1),
                    Status = FileProcessingStatus.Pending
                });

            var storagePool = new StoragePool(
                _metadataRepository,
                _tenantQuotaManager.Object,
                _directoryQuotaManager.Object,
                _tenantManager.Object,
                _fileScheduler.Object,
                _logger.Object,
                projectionStore: projectionStore.Object,
                allowLegacyNonJournalMode: true);
            await storagePool.AddVolumeAsync(_volume1.Object, initialDelayMs: 0, healthCheckDelayMs: 0);

            var location = await storagePool.GetFileLocationAsync(_tenant.Object, "projection-location", CancellationToken.None);

            Assert.NotNull(location);
            Assert.Equal("/projection", location!.DirectoryPath);
            projectionStore.VerifyAll();
        }

        [Fact]
        public async Task WriteFileAsync_UsesProjectionWriteStoreWhenProvided()
        {
            var captured = default(FileMetadata);
            var projectionWriteStore = new Mock<IQueueProjectionWriteStore>(MockBehavior.Strict);
            projectionWriteStore
                .Setup(store => store.QueueProjectedFileAsync(It.IsAny<FileMetadata>(), It.IsAny<CancellationToken>()))
                .Callback<FileMetadata, CancellationToken>((metadata, _) => captured = metadata)
                .Returns(Task.CompletedTask);

            var storagePool = new StoragePool(
                _metadataRepository,
                _tenantQuotaManager.Object,
                _directoryQuotaManager.Object,
                _tenantManager.Object,
                _fileScheduler.Object,
                _logger.Object,
                queueEventJournal: _queueEventJournal.Object,
                projectionWriteStore: projectionWriteStore.Object);

            await storagePool.AddVolumeAsync(_volume1.Object, initialDelayMs: 0, healthCheckDelayMs: 0);

            var content = new MemoryStream(Encoding.UTF8.GetBytes("write-store"));

            var fileKey = await storagePool.WriteFileAsync(_tenant.Object, content, "write-store.dcm", CancellationToken.None);

            Assert.NotNull(captured);
            Assert.Equal(fileKey, captured!.FileKey);
            Assert.Equal("tenant-001", captured.TenantId);
            Assert.Equal("/", captured.DirectoryPath);
            Assert.Equal(FileProcessingStatus.Pending, captured.Status);

            projectionWriteStore.Verify(
                store => store.QueueProjectedFileAsync(
                    It.Is<FileMetadata>(metadata =>
                        metadata.FileKey == fileKey
                        && metadata.TenantId == "tenant-001"
                        && metadata.DirectoryPath == "/"),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task GetFileLocationAsync_ThrowsWhenTenantDisabled()
        {
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);

            var disabledTenantContext = new Mock<ITenantContext>();
            disabledTenantContext.Setup(t => t.TenantId).Returns("tenant-001");
            disabledTenantContext.Setup(t => t.Status).Returns(TenantStatus.Disabled);

            _tenantManager.Setup(m => m.GetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(disabledTenantContext.Object);

            await Assert.ThrowsAsync<TenantDisabledException>(() =>
                _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default));
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
        public async Task CapacityQueries_ReuseCachedSnapshotWithinRefreshWindow()
        {
            _volume1.Invocations.Clear();
            _volume2.Invocations.Clear();

            var total1 = await _storagePool.GetTotalCapacityAsync(default);
            var total2 = await _storagePool.GetTotalCapacityAsync(default);
            var available1 = await _storagePool.GetAvailableSpaceAsync(default);
            var available2 = await _storagePool.GetAvailableSpaceAsync(default);

            Assert.Equal(2000000000L, total1);
            Assert.Equal(total1, total2);
            Assert.Equal(1100000000L, available1);
            Assert.Equal(available1, available2);

            _volume1.VerifyGet(v => v.IsHealthy, Times.AtMost(1));
            _volume2.VerifyGet(v => v.IsHealthy, Times.AtMost(1));
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
            var fileKey1 = await _storagePool.WriteFileAsync(_tenant.Object, content1, null, default);
            var fileKey2 = await _storagePool.WriteFileAsync(_tenant.Object, content2, null, default);

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
                _storagePool.WriteFileAsync(_tenant.Object, invalidContent, null, default));

            // Verify quota was not incremented (mock should not have been called)
            _tenantQuotaManager.Verify(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task WriteFileAsync_RollsBackTenantQuota_WhenDirectoryQuotaRejected()
        {
            _directoryQuotaManager
                .Setup(m => m.IncrementFileCountAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new DirectoryQuotaExceededException("/a", 1, 1));

            var content = new MemoryStream(Encoding.UTF8.GetBytes("quota-fail"));

            await Assert.ThrowsAsync<DirectoryQuotaExceededException>(() =>
                _storagePool.WriteFileAsync(_tenant.Object, content, null, default));

            _tenantQuotaManager.Verify(
                m => m.DecrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Once);
            _directoryQuotaManager.Verify(
                m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task WriteFileAsync_RollsBackBothQuotas_WhenPhysicalWriteFails()
        {
            _volume2
                .Setup(v => v.WriteAsync(It.IsAny<string>(), It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new IOException("disk full"));

            var content = new MemoryStream(Encoding.UTF8.GetBytes("write-fail"));

            await Assert.ThrowsAsync<IOException>(() =>
                _storagePool.WriteFileAsync(_tenant.Object, content, null, default));

            _tenantQuotaManager.Verify(
                m => m.DecrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Once);
            _directoryQuotaManager.Verify(
                m => m.DecrementFileCountAsync("tenant-001", It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task WriteFileAsync_PreservesFileExtension()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("PDF content"));

            // Act
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, "invoice.pdf", default);

            // Assert
            Assert.False(string.IsNullOrEmpty(fileKey));

            // Verify file metadata contains original file name and extension
            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default);
            Assert.NotNull(location);

            // Physical path should contain the extension
            Assert.EndsWith(".pdf", location.PhysicalPath);

            // Verify file exists on disk with extension
            Assert.True(_fileSystem.File.Exists(location.PhysicalPath));
        }

        [Fact]
        public async Task WriteFileAsync_WithoutFileName_NoExtension()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("content without extension"));

            // Act
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);

            // Assert
            Assert.False(string.IsNullOrEmpty(fileKey));

            // Verify physical path does not have an extension
            var location = await _storagePool.GetFileLocationAsync(_tenant.Object, fileKey, default);
            Assert.NotNull(location);
            Assert.DoesNotContain(".", Path.GetFileName(location.PhysicalPath));
        }

        [Fact]
        public async Task WriteFileAsync_DifferentExtensions_PreservesCorrectly()
        {
            // Arrange & Act
            var pdfContent = new MemoryStream(Encoding.UTF8.GetBytes("PDF"));
            var docxContent = new MemoryStream(Encoding.UTF8.GetBytes("DOCX"));
            var jpgContent = new MemoryStream(Encoding.UTF8.GetBytes("JPG"));

            var pdfKey = await _storagePool.WriteFileAsync(_tenant.Object, pdfContent, "document.pdf", default);
            var docxKey = await _storagePool.WriteFileAsync(_tenant.Object, docxContent, "report.docx", default);
            var jpgKey = await _storagePool.WriteFileAsync(_tenant.Object, jpgContent, "photo.jpg", default);

            // Assert
            var pdfLocation = await _storagePool.GetFileLocationAsync(_tenant.Object, pdfKey, default);
            var docxLocation = await _storagePool.GetFileLocationAsync(_tenant.Object, docxKey, default);
            var jpgLocation = await _storagePool.GetFileLocationAsync(_tenant.Object, jpgKey, default);

            Assert.NotNull(pdfLocation);
            Assert.NotNull(docxLocation);
            Assert.NotNull(jpgLocation);

            Assert.EndsWith(".pdf", pdfLocation.PhysicalPath);
            Assert.EndsWith(".docx", docxLocation.PhysicalPath);
            Assert.EndsWith(".jpg", jpgLocation.PhysicalPath);
        }

        [Fact]
        public async Task WriteFileAsync_NonSeekableStream_PersistsActualFileSize()
        {
            using var nonSeekable = new NonSeekableReadStream(Encoding.UTF8.GetBytes("non-seekable-content"));

            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, nonSeekable, "payload.bin", default);
            var info = await _storagePool.GetFileInfoAsync(_tenant.Object, fileKey, default);

            Assert.NotNull(info);
            Assert.Equal("non-seekable-content".Length, info!.FileSize);
        }

        [Fact]
        public async Task GetFileInfoAsync_ThrowsWhenTenantDisabled()
        {
            var content = new MemoryStream(Encoding.UTF8.GetBytes("test content"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);

            var disabledTenantContext = new Mock<ITenantContext>();
            disabledTenantContext.Setup(t => t.TenantId).Returns("tenant-001");
            disabledTenantContext.Setup(t => t.Status).Returns(TenantStatus.Disabled);

            _tenantManager.Setup(m => m.GetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(disabledTenantContext.Object);

            await Assert.ThrowsAsync<TenantDisabledException>(() =>
                _storagePool.GetFileInfoAsync(_tenant.Object, fileKey, default));
        }

        [Fact]
        public async Task MarkAsCompletedAsync_ConcurrentDuplicateCalls_LeavesCompletedStateOnce()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("to complete"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);
            var processingStart = DateTime.UtcNow;

            var metadata = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            Assert.NotNull(metadata);
            metadata!.Status = FileProcessingStatus.Processing;
            metadata.ProcessingStartTime = processingStart;
            await _metadataRepository.AddOrUpdateAsync(metadata, CancellationToken.None);

            _fileScheduler
                .Setup(s => s.MarkAsCompletedAsync(It.IsAny<FileProcessingLease>(), It.IsAny<CancellationToken>()))
                .Returns(async (FileProcessingLease lease, CancellationToken token) =>
                {
                    var current = await _metadataRepository.GetAsync(lease.TenantId, lease.FileKey, token);
                    if (current != null)
                    {
                        current.Status = FileProcessingStatus.Completed;
                        current.ProcessingStartTime = null;
                        current.CompletedAt = DateTime.UtcNow;
                        await _metadataRepository.AddOrUpdateAsync(current, token);
                    }
                });

            var tasks = Enumerable.Range(0, 20)
                .Select(_ => _storagePool.MarkAsCompletedAsync(CreateLease("tenant-001", fileKey, processingStart), CancellationToken.None))
                .ToArray();

            // Act
            await Task.WhenAll(tasks);

            // Assert
            _tenantQuotaManager.Verify(
                m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
            _directoryQuotaManager.Verify(
                m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);

            var metadataAfterCompletion = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            Assert.NotNull(metadataAfterCompletion);
            Assert.Equal(FileProcessingStatus.Completed, metadataAfterCompletion!.Status);
            Assert.Null(metadataAfterCompletion.ProcessingStartTime);
            Assert.NotNull(metadataAfterCompletion.CompletedAt);
        }

        [Fact]
        public async Task MarkAsCompletedAsync_WithStaleLease_DoesNotDecrementQuotaOrInvokeScheduler()
        {
            // Arrange
            var content = new MemoryStream(Encoding.UTF8.GetBytes("stale lease"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, null, default);
            var activeProcessingStart = DateTime.UtcNow;
            var staleProcessingStart = activeProcessingStart.AddMinutes(-1);

            var metadata = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            Assert.NotNull(metadata);
            metadata!.Status = FileProcessingStatus.Processing;
            metadata.ProcessingStartTime = activeProcessingStart;
            await _metadataRepository.AddOrUpdateAsync(metadata, CancellationToken.None);

            // Act
            var exception = await Assert.ThrowsAsync<FileProcessingLeaseMismatchException>(() =>
                _storagePool.MarkAsCompletedAsync(CreateLease("tenant-001", fileKey, staleProcessingStart), CancellationToken.None));

            // Assert
            Assert.Equal(fileKey, exception.FileKey);
            Assert.Equal(staleProcessingStart, exception.ExpectedProcessingStartTimeUtc);
            Assert.Equal(activeProcessingStart, exception.ActualProcessingStartTimeUtc);

            _directoryQuotaManager.Verify(
                m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
            _tenantQuotaManager.Verify(
                m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
            _fileScheduler.Verify(
                m => m.MarkAsCompletedAsync(It.IsAny<FileProcessingLease>(), It.IsAny<CancellationToken>()),
                Times.Never);

            var metadataAfterFailure = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            Assert.NotNull(metadataAfterFailure);
            Assert.Equal(FileProcessingStatus.Processing, metadataAfterFailure!.Status);
            Assert.Equal(activeProcessingStart, metadataAfterFailure.ProcessingStartTime);
        }

        [Fact]
        public async Task MarkAsCompletedAsync_WhenCompletionFails_KeepsQuotaAndMetadata()
        {
            var tenantQuotaManager = new TenantQuotaManager(
                _quotaRepository,
                new Mock<ILogger<TenantQuotaManager>>().Object);
            var directoryQuotaManager = new DirectoryQuotaManager(
                _quotaRepository,
                new Mock<ILogger<DirectoryQuotaManager>>().Object);
            var failingScheduler = new Mock<IFileScheduler>();

            var storagePool = new StoragePool(
                _metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                _tenantManager.Object,
                failingScheduler.Object,
                _logger.Object,
                allowLegacyNonJournalMode: true);

            const string tenantId = "tenant-001";
            const string directoryPath = "/compensate";
            const string targetFileKey = "compensate-1";
            var processingStart = DateTime.UtcNow;

            await tenantQuotaManager.SetTenantLimitAsync(tenantId, 2, CancellationToken.None);
            await directoryQuotaManager.SetLimitAsync(tenantId, directoryPath, 2, CancellationToken.None);

            await tenantQuotaManager.IncrementFileCountAsync(tenantId, CancellationToken.None);
            await tenantQuotaManager.IncrementFileCountAsync(tenantId, CancellationToken.None);
            await directoryQuotaManager.IncrementFileCountAsync(tenantId, directoryPath, CancellationToken.None);
            await directoryQuotaManager.IncrementFileCountAsync(tenantId, directoryPath, CancellationToken.None);

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = targetFileKey,
                TenantId = tenantId,
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volume1Path, "compensate-1.dat"),
                DirectoryPath = directoryPath,
                FileSize = 1,
                Status = FileProcessingStatus.Processing,
                CreatedAt = DateTime.UtcNow,
                ProcessingStartTime = processingStart
            }, CancellationToken.None);

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "compensate-2",
                TenantId = tenantId,
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volume1Path, "compensate-2.dat"),
                DirectoryPath = directoryPath,
                FileSize = 1,
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            }, CancellationToken.None);

            failingScheduler
                .Setup(s => s.MarkAsCompletedAsync(
                    It.Is<FileProcessingLease>(lease =>
                        lease.TenantId == tenantId
                        && lease.FileKey == targetFileKey
                        && lease.ProcessingStartTimeUtc == processingStart),
                    It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    await tenantQuotaManager.SetTenantLimitAsync(tenantId, 1, CancellationToken.None);
                    await directoryQuotaManager.SetLimitAsync(tenantId, directoryPath, 1, CancellationToken.None);
                    throw new IOException("forced completion failure");
                });

            await Assert.ThrowsAsync<IOException>(() =>
                storagePool.MarkAsCompletedAsync(CreateLease(tenantId, targetFileKey, processingStart), CancellationToken.None));

            var tenantCount = await tenantQuotaManager.GetFileCountAsync(tenantId, CancellationToken.None);
            var directoryCount = await directoryQuotaManager.GetFileCountAsync(tenantId, directoryPath, CancellationToken.None);
            var metadata = await _metadataRepository.GetByFileKeyAsync(targetFileKey, CancellationToken.None);

            Assert.Equal(2, tenantCount);
            Assert.Equal(2, directoryCount);
            Assert.NotNull(metadata);
        }

        [Fact]
        public async Task MarkAsCompletedAsync_AppendsProcessingCompletedAndDeleteRequestedQueueEvents()
        {
            var content = new MemoryStream(Encoding.UTF8.GetBytes("complete-event"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, "done.dcm", CancellationToken.None);
            var processingStart = DateTime.UtcNow;

            var metadata = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            Assert.NotNull(metadata);
            metadata!.Status = FileProcessingStatus.Processing;
            metadata.ProcessingStartTime = processingStart;
            metadata.OriginalFileName = "done.dcm";
            metadata.FileExtension = ".dcm";
            await _metadataRepository.AddOrUpdateAsync(metadata, CancellationToken.None);

            _fileScheduler
                .Setup(s => s.MarkAsCompletedAsync(
                    It.Is<FileProcessingLease>(lease =>
                        lease.TenantId == "tenant-001"
                        && lease.FileKey == fileKey
                        && lease.ProcessingStartTimeUtc == processingStart),
                    It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    var current = await _metadataRepository.GetAsync("tenant-001", fileKey, CancellationToken.None);
                    Assert.NotNull(current);
                    current!.Status = FileProcessingStatus.Completed;
                    current.ProcessingStartTime = null;
                    current.CompletedAt = DateTime.UtcNow;
                    await _metadataRepository.AddOrUpdateAsync(current, CancellationToken.None);
                });

            _queueEventJournal.Invocations.Clear();

            await _storagePool.MarkAsCompletedAsync(CreateLease("tenant-001", fileKey, processingStart), CancellationToken.None);

            _queueEventJournal.Verify(
                m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.ProcessingCompleted
                        && record.TenantId == "tenant-001"
                        && record.FileKey == fileKey
                        && record.Status == FileProcessingStatus.Completed
                        && record.ProcessingStartTimeUtc == processingStart
                        && record.FileExtension == ".dcm"),
                    It.IsAny<CancellationToken>()),
                Times.Once);

            _queueEventJournal.Verify(
                m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.DeleteRequested
                        && record.TenantId == "tenant-001"
                        && record.FileKey == fileKey
                        && record.Status == FileProcessingStatus.DeleteRequested),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task MarkAsCompletedAsync_UsesProjectionStoreForLeaseValidationAndEventPayload()
        {
            var processingStart = DateTime.UtcNow.AddMinutes(-2);
            var completedAt = DateTime.UtcNow.AddMinutes(-1);
            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .SetupSequence(store => store.GetProjectedFileAsync("tenant-001", "projection-complete", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileMetadata
                {
                    FileKey = "projection-complete",
                    TenantId = "tenant-001",
                    VolumeId = "vol-001",
                    PhysicalPath = Path.Combine(_volume1Path, "tenant-001", "projection-complete.dcm"),
                    DirectoryPath = "/projection",
                    FileSize = 8,
                    CreatedAt = DateTime.UtcNow.AddMinutes(-3),
                    Status = FileProcessingStatus.Processing,
                    ProcessingStartTime = processingStart
                })
                .ReturnsAsync(new FileMetadata
                {
                    FileKey = "projection-complete",
                    TenantId = "tenant-001",
                    VolumeId = "vol-001",
                    PhysicalPath = Path.Combine(_volume1Path, "tenant-001", "projection-complete.dcm"),
                    DirectoryPath = "/projection",
                    FileSize = 8,
                    CreatedAt = DateTime.UtcNow.AddMinutes(-3),
                    Status = FileProcessingStatus.Completed,
                    CompletedAt = completedAt
                });

            var storagePool = new StoragePool(
                _metadataRepository,
                _tenantQuotaManager.Object,
                _directoryQuotaManager.Object,
                _tenantManager.Object,
                _fileScheduler.Object,
                _logger.Object,
                queueEventJournal: _queueEventJournal.Object,
                projectionStore: projectionStore.Object);

            _fileScheduler
                .Setup(s => s.MarkAsCompletedAsync(
                    It.Is<FileProcessingLease>(lease =>
                        lease.TenantId == "tenant-001"
                        && lease.FileKey == "projection-complete"
                        && lease.ProcessingStartTimeUtc == processingStart),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            _queueEventJournal.Invocations.Clear();

            await storagePool.MarkAsCompletedAsync(
                CreateLease("tenant-001", "projection-complete", processingStart),
                CancellationToken.None);

            _queueEventJournal.Verify(
                m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.ProcessingCompleted
                        && record.FileKey == "projection-complete"
                        && record.DirectoryPath == "/projection"),
                    It.IsAny<CancellationToken>()),
                Times.Once);
            _queueEventJournal.Verify(
                m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.DeleteRequested
                        && record.FileKey == "projection-complete"
                        && record.DirectoryPath == "/projection"),
                    It.IsAny<CancellationToken>()),
                Times.Once);
            projectionStore.VerifyAll();
        }

        [Fact]
        public async Task MarkAsFailedAsync_AppendsProcessingFailedQueueEvent()
        {
            var content = new MemoryStream(Encoding.UTF8.GetBytes("failed-event"));
            var fileKey = await _storagePool.WriteFileAsync(_tenant.Object, content, "failed.dcm", CancellationToken.None);
            var processingStart = DateTime.UtcNow;
            var retryAt = DateTime.UtcNow.AddMinutes(1);

            var metadata = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            Assert.NotNull(metadata);
            metadata!.Status = FileProcessingStatus.Processing;
            metadata.ProcessingStartTime = processingStart;
            metadata.OriginalFileName = "failed.dcm";
            metadata.FileExtension = ".dcm";
            await _metadataRepository.AddOrUpdateAsync(metadata, CancellationToken.None);

            _fileScheduler
                .Setup(s => s.MarkAsFailedAsync(
                    It.Is<FileProcessingLease>(lease =>
                        lease.TenantId == "tenant-001"
                        && lease.FileKey == fileKey
                        && lease.ProcessingStartTimeUtc == processingStart),
                    "decode failed",
                    It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    var current = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
                    Assert.NotNull(current);
                    current!.Status = FileProcessingStatus.Pending;
                    current.ProcessingStartTime = null;
                    current.RetryCount = 1;
                    current.LastError = "decode failed";
                    current.LastFailedAt = DateTime.UtcNow;
                    current.AvailableForProcessingAt = retryAt;
                    await _metadataRepository.AddOrUpdateAsync(current, CancellationToken.None);
                });

            _queueEventJournal.Invocations.Clear();

            await _storagePool.MarkAsFailedAsync(CreateLease("tenant-001", fileKey, processingStart), "decode failed", CancellationToken.None);

            _queueEventJournal.Verify(
                m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.ProcessingFailed
                        && record.TenantId == "tenant-001"
                        && record.FileKey == fileKey
                        && record.Status == FileProcessingStatus.Pending
                        && record.RetryCount == 1
                        && record.ErrorMessage == "decode failed"
                        && record.AvailableForProcessingAtUtc == retryAt),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task MarkAsFailedAsync_UsesProjectionStoreForQueueEventPayload()
        {
            var processingStart = DateTime.UtcNow.AddMinutes(-2);
            var retryAt = DateTime.UtcNow.AddMinutes(1);
            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(store => store.GetProjectedFileAsync("tenant-001", "projection-failed", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileMetadata
                {
                    FileKey = "projection-failed",
                    TenantId = "tenant-001",
                    VolumeId = "vol-001",
                    PhysicalPath = Path.Combine(_volume1Path, "tenant-001", "projection-failed.dcm"),
                    DirectoryPath = "/projection",
                    FileSize = 8,
                    CreatedAt = DateTime.UtcNow.AddMinutes(-3),
                    Status = FileProcessingStatus.Pending,
                    RetryCount = 1,
                    LastError = "decode failed",
                    LastFailedAt = DateTime.UtcNow,
                    AvailableForProcessingAt = retryAt
                });

            var storagePool = new StoragePool(
                _metadataRepository,
                _tenantQuotaManager.Object,
                _directoryQuotaManager.Object,
                _tenantManager.Object,
                _fileScheduler.Object,
                _logger.Object,
                queueEventJournal: _queueEventJournal.Object,
                projectionStore: projectionStore.Object);

            _fileScheduler
                .Setup(s => s.MarkAsFailedAsync(
                    It.Is<FileProcessingLease>(lease =>
                        lease.TenantId == "tenant-001"
                        && lease.FileKey == "projection-failed"
                        && lease.ProcessingStartTimeUtc == processingStart),
                    "decode failed",
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            _queueEventJournal.Invocations.Clear();

            await storagePool.MarkAsFailedAsync(
                CreateLease("tenant-001", "projection-failed", processingStart),
                "decode failed",
                CancellationToken.None);

            _queueEventJournal.Verify(
                m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.ProcessingFailed
                        && record.FileKey == "projection-failed"
                        && record.DirectoryPath == "/projection"
                        && record.AvailableForProcessingAtUtc == retryAt),
                    It.IsAny<CancellationToken>()),
                Times.Once);
            projectionStore.VerifyAll();
        }

        [Fact]
        public void Constructor_WithNonPositiveCompletionGuardStripeCount_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new StoragePool(
                _metadataRepository,
                _tenantQuotaManager.Object,
                _directoryQuotaManager.Object,
                _tenantManager.Object,
                _fileScheduler.Object,
                _logger.Object,
                completionGuardStripeCount: 0));
        }

        private sealed class NonSeekableReadStream : Stream
        {
            private readonly MemoryStream _inner;

            public NonSeekableReadStream(byte[] bytes)
            {
                _inner = new MemoryStream(bytes);
            }

            public override bool CanRead => true;

            public override bool CanSeek => false;

            public override bool CanWrite => false;

            public override long Length => throw new NotSupportedException();

            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return _inner.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                    _inner.Dispose();

                base.Dispose(disposing);
            }
        }
    }
}
