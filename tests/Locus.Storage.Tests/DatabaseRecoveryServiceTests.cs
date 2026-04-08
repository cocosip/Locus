using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class DatabaseRecoveryServiceTests : IDisposable
    {
        private sealed class TestDatabaseRebuildLockHandle : IDatabaseRebuildLockHandle
        {
            public string? BackupPath { get; set; }

            public void Dispose()
            {
            }
        }

        private readonly IFileSystem _fileSystem;
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly Mock<ILogger<DatabaseRecoveryService>> _logger;
        private readonly DatabaseRecoveryService _recoveryService;
        private readonly string _volumePath;
        private readonly string _metadataDir;
        private readonly string _quotaDir;
        private readonly string _testId;

        public DatabaseRecoveryServiceTests()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _testId = Guid.NewGuid().ToString("N").Substring(0, 8);

            _metadataDir = Path.Combine(Path.GetTempPath(), $"locus-test-recovery-meta-{_testId}");
            _quotaDir = Path.Combine(Path.GetTempPath(), $"locus-test-recovery-quota-{_testId}");
            _volumePath = Path.Combine(Path.GetTempPath(), $"locus-test-recovery-vol-{_testId}");

            _fileSystem.Directory.CreateDirectory(_metadataDir);
            _fileSystem.Directory.CreateDirectory(_quotaDir);
            _fileSystem.Directory.CreateDirectory(_volumePath);

            var metadataRepoLogger = new Mock<ILogger<MetadataRepository>>();
            _metadataRepository = new MetadataRepository(
                _fileSystem,
                metadataRepoLogger.Object,
                _metadataDir,
                enableBackgroundPersistence: false);

            var quotaRepoLogger = new Mock<ILogger<DirectoryQuotaRepository>>();
            _quotaRepository = new DirectoryQuotaRepository(
                _fileSystem,
                quotaRepoLogger.Object,
                _quotaDir,
                enableBackgroundFlush: false);

            _logger = new Mock<ILogger<DatabaseRecoveryService>>();
            _recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir);
        }

        public void Dispose()
        {
            _metadataRepository?.Dispose();
            _quotaRepository?.Dispose();
            SqliteConnection.ClearAllPools();

            try
            {
                if (_fileSystem.Directory.Exists(_metadataDir))
                    _fileSystem.Directory.Delete(_metadataDir, recursive: true);
                if (_fileSystem.Directory.Exists(_quotaDir))
                    _fileSystem.Directory.Delete(_quotaDir, recursive: true);
                if (_fileSystem.Directory.Exists(_volumePath))
                    _fileSystem.Directory.Delete(_volumePath, recursive: true);
            }
            catch { }
        }

        [Fact]
        public void IsDatabaseCorrupted_ReturnsFalseForNonExistentDatabase()
        {
            // Arrange
            var dbPath = Path.Combine(_metadataDir, "nonexistent.db");

            // Act
            var result = _recoveryService.IsDatabaseCorrupted(dbPath);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task IsDatabaseCorrupted_ReturnsFalseForHealthyDatabase()
        {
            // Arrange - use unique tenant ID and temporary repository
            var tenantId = $"tenant-healthy-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var dbPath = Path.Combine(_metadataDir, tenantId, "metadata.db");

            // Create a temporary repository to create the database, then dispose it
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir, enableBackgroundPersistence: false))
            {
                var metadata = new FileMetadata
                {
                    FileKey = "file-001",
                    TenantId = tenantId,
                    VolumeId = "vol-001",
                    PhysicalPath = "/path/to/file",
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                };
                await tempRepo.AddOrUpdateAsync(metadata, default);
            } // Dispose releases database connection

            // Wait a moment for file system to release locks
            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            // Act
            var result = _recoveryService.IsDatabaseCorrupted(dbPath);

            // Assert
            Assert.False(result);
        }

        [Fact]
        public async Task IsDatabaseCorrupted_ReturnsTrueForCorruptedDatabase()
        {
            // Arrange
            var tenantId = $"tenant-corrupt-test-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var dbPath = Path.Combine(_metadataDir, tenantId, "metadata.db");

            // First create a valid SQLite database
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir, enableBackgroundPersistence: false))
            {
                await tempRepo.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = "test",
                    TenantId = tenantId,
                    VolumeId = "vol",
                    PhysicalPath = "/test",
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                }, default);
            }

            // Wait for database to be fully written and closed
            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            // Now corrupt it by overwriting with random data
            var garbage = new byte[512]; // Smaller size to corrupt header
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            // Act
            var result = _recoveryService.IsDatabaseCorrupted(dbPath);

            // Assert
            Assert.True(result);
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_RebuildsFromPhysicalFiles()
        {
            // Arrange
            var tenantId = $"tenant-rebuild-meta-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenantPath = Path.Combine(_volumePath, tenantId);
            _fileSystem.Directory.CreateDirectory(tenantPath);

            // Create physical files
            var file1 = Path.Combine(tenantPath, "file1.txt");
            var file2 = Path.Combine(tenantPath, "subdir", "file2.txt");
            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(file2)!);
            _fileSystem.File.WriteAllText(file1, "content1");
            _fileSystem.File.WriteAllText(file2, "content2");

            // Create a valid database first, then corrupt it
            var dbPath = Path.Combine(_metadataDir, tenantId, "metadata.db");
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir, enableBackgroundPersistence: false))
            {
                await tempRepo.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = "temp",
                    TenantId = tenantId,
                    VolumeId = "vol",
                    PhysicalPath = "/temp",
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                }, default);
            }

            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            // Corrupt the database
            var garbage = new byte[512];
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            // Act
            var result = await _recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(3, result.RecordsRebuilt);
            Assert.NotNull(result.BackupPath);
            Assert.True(_fileSystem.File.Exists(result.BackupPath));

            // Verify new database file exists
            Assert.True(_fileSystem.File.Exists(dbPath));
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_PrefersQueueProjectionRecoveryWhenAvailable()
        {
            var tenantId = $"tenant-rebuild-journal-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenantPath = Path.Combine(_volumePath, tenantId);
            _fileSystem.Directory.CreateDirectory(tenantPath);
            var physicalFile = Path.Combine(tenantPath, "journal-file.dcm");
            _fileSystem.File.WriteAllText(physicalFile, "content");

            var dbPath = Path.Combine(_metadataDir, tenantId, "metadata.db");
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir, enableBackgroundPersistence: false))
            {
                await tempRepo.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = "temp",
                    TenantId = tenantId,
                    VolumeId = "vol",
                    PhysicalPath = "/temp",
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                }, default);
            }

            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            var garbage = new byte[512];
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            var projectionMaintenance = new Mock<IQueueProjectionMaintenanceService>(MockBehavior.Strict);
            projectionMaintenance
                .Setup(x => x.RebuildTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>(async (_, ct) =>
                {
                    await _metadataRepository.AddOrUpdateAsync(new FileMetadata
                    {
                        FileKey = "journal-file",
                        TenantId = tenantId,
                        VolumeId = "vol-001",
                        PhysicalPath = physicalFile,
                        DirectoryPath = "/",
                        FileSize = 7,
                        Status = FileProcessingStatus.Pending,
                        CreatedAt = DateTime.UtcNow
                    }, ct);

                    return new QueueProjectionTenantState
                    {
                        TenantId = tenantId,
                        CursorOffset = 128,
                        JournalSizeBytes = 128,
                        LagBytes = 0,
                        HasSnapshot = true,
                        SnapshotCursorOffset = 128,
                        SnapshotFileCount = 1
                    };
                });

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                queueProjectionMaintenanceService: projectionMaintenance.Object);

            var result = await recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            Assert.True(result.Success);
            Assert.Equal(1, result.RecordsRebuilt);
            projectionMaintenance.Verify(x => x.RebuildTenantAsync(tenantId, It.IsAny<CancellationToken>()), Times.Once);
            Assert.Single(await _metadataRepository.GetByTenantAsync(tenantId, default));
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_UsesProjectionStoreForQueueRecoveryCountsWhenProvided()
        {
            var tenantId = $"tenant-rebuild-journal-projection-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var physicalFile = Path.Combine(_volumePath, tenantId, "projection-file.dcm");
            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalFile)!);
            _fileSystem.File.WriteAllText(physicalFile, "content");

            var projectionMaintenance = new Mock<IQueueProjectionMaintenanceService>(MockBehavior.Strict);
            projectionMaintenance
                .Setup(x => x.RebuildTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new QueueProjectionTenantState
                {
                    TenantId = tenantId,
                    CursorOffset = 256,
                    JournalSizeBytes = 256,
                    LagBytes = 0,
                    HasSnapshot = true,
                    SnapshotCursorOffset = 256,
                    SnapshotFileCount = 1
                });

            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(x => x.GetProjectedFilesAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[]
                {
                    new FileMetadata
                    {
                        FileKey = "projection-file",
                        TenantId = tenantId,
                        VolumeId = "vol-001",
                        PhysicalPath = physicalFile,
                        DirectoryPath = "/logical/recovered",
                        FileSize = 7,
                        Status = FileProcessingStatus.Pending,
                        CreatedAt = DateTime.UtcNow
                    }
                });

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                queueProjectionMaintenanceService: projectionMaintenance.Object,
                projectionStore: projectionStore.Object);

            var result = await recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            Assert.True(result.Success);
            Assert.Equal(1, result.RecordsRebuilt);
            projectionMaintenance.Verify(x => x.RebuildTenantAsync(tenantId, It.IsAny<CancellationToken>()), Times.Once);
            projectionStore.Verify(x => x.GetProjectedFilesAsync(tenantId, It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_UsesStableFileKeyAndConfiguredVolumeId()
        {
            // Arrange
            var tenantId = $"tenant-stable-key-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenantPath = Path.Combine(_volumePath, tenantId);
            _fileSystem.Directory.CreateDirectory(tenantPath);

            var physicalFile = Path.Combine(tenantPath, "invoice-001.pdf");
            _fileSystem.File.WriteAllText(physicalFile, "content");

            var dbPath = Path.Combine(_metadataDir, tenantId, "metadata.db");
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir, enableBackgroundPersistence: false))
            {
                await tempRepo.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = "temp",
                    TenantId = tenantId,
                    VolumeId = "vol",
                    PhysicalPath = "/temp",
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                }, default);
            }

            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            var garbage = new byte[512];
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            var recoveryWithVolumeMap = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    [_volumePath] = "vol-configured"
                });

            // Act
            var result = await recoveryWithVolumeMap.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            // Assert
            Assert.True(result.Success);
            var metadata = (await _metadataRepository.GetByTenantAsync(tenantId, default)).Single();
            Assert.Equal("invoice-001", metadata.FileKey);
            Assert.Equal("vol-configured", metadata.VolumeId);
            Assert.Equal(".pdf", metadata.FileExtension);
            Assert.Equal("invoice-001.pdf", metadata.OriginalFileName);
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_NormalizesRootDirectoryPathToSlash()
        {
            var tenantId = $"tenant-root-dir-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenantPath = Path.Combine(_volumePath, tenantId);
            _fileSystem.Directory.CreateDirectory(tenantPath);

            var rootFile = Path.Combine(tenantPath, "root-file.txt");
            _fileSystem.File.WriteAllText(rootFile, "content");

            var dbPath = Path.Combine(_metadataDir, tenantId, "metadata.db");
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir, enableBackgroundPersistence: false))
            {
                await tempRepo.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = "temp",
                    TenantId = tenantId,
                    VolumeId = "vol",
                    PhysicalPath = "/temp",
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                }, default);
            }

            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            var garbage = new byte[512];
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            var result = await _recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            Assert.True(result.Success);

            var metadata = (await _metadataRepository.GetByTenantAsync(tenantId, default)).Single();
            Assert.Equal("/", metadata.DirectoryPath);
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_PhysicalScanPreservesMeaningfulNestedLogicalDirectory()
        {
            var tenantId = $"tenant-nested-dir-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var nestedPath = Path.Combine(_volumePath, tenantId, "incoming", "studies");
            _fileSystem.Directory.CreateDirectory(nestedPath);
            _fileSystem.File.WriteAllText(Path.Combine(nestedPath, "study-001.dcm"), "content");

            var result = await _recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            Assert.True(result.Success);

            var metadata = (await _metadataRepository.GetByTenantAsync(tenantId, default)).Single();
            Assert.Equal("/incoming/studies", metadata.DirectoryPath);
            Assert.NotNull(metadata.Metadata);
            Assert.True(metadata.Metadata!.TryGetValue("queue.accepted_projection_applied", out var acceptedApplied));
            Assert.Equal(bool.TrueString, acceptedApplied);
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_PhysicalScanStripsDetectedShardDirectories()
        {
            var tenantId = $"tenant-shard-dir-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            const string fileKey = "a1b2c3d4e5f60718293a4b5c6d7e8f90";
            var shardedPath = Path.Combine(_volumePath, tenantId, "a1", "b2");
            _fileSystem.Directory.CreateDirectory(shardedPath);
            _fileSystem.File.WriteAllText(Path.Combine(shardedPath, fileKey + ".dcm"), "content");

            var result = await _recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            Assert.True(result.Success);

            var metadata = (await _metadataRepository.GetByTenantAsync(tenantId, default)).Single();
            Assert.Equal("/", metadata.DirectoryPath);
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_DoesNotReleaseUnheldLock_WhenCancelledBeforeAcquire()
        {
            var tenantId = $"tenant-cancel-lock-{Guid.NewGuid().ToString("N").Substring(0, 8)}";

            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir, enableBackgroundPersistence: false))
            {
                await tempRepo.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = "temp",
                    TenantId = tenantId,
                    VolumeId = "vol",
                    PhysicalPath = "/temp",
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                }, default);
            }

            await Task.Delay(100);

            using (var cts = new CancellationTokenSource())
            {
                cts.Cancel();

                var result = await _recoveryService.RebuildMetadataDatabaseAsync(
                    tenantId,
                    new[] { _volumePath },
                    cts.Token);

                Assert.False(result.Success);
                Assert.DoesNotContain(
                    result.Errors,
                    message => message.Contains("SemaphoreFullException", StringComparison.OrdinalIgnoreCase));
            }
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_ReturnsSuccessWhenNoDatabaseExists()
        {
            // Arrange
            var tenantId = "tenant-no-db";

            // Act
            var result = await _recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(0, result.RecordsRebuilt);
            Assert.Null(result.BackupPath);
            Assert.Empty(result.Errors);
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_CreatesDatabaseWhenMissingButPhysicalFilesExist()
        {
            var tenantId = $"tenant-no-db-files-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenantPath = Path.Combine(_volumePath, tenantId, "incoming");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            _fileSystem.File.WriteAllText(Path.Combine(tenantPath, "study-001.dcm"), "dicom");

            var result = await _recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            Assert.True(result.Success);
            Assert.Equal(1, result.RecordsRebuilt);
            Assert.Null(result.BackupPath);
            Assert.Single(await _metadataRepository.GetByTenantAsync(tenantId, default));
        }

        [Fact]
        public async Task RebuildQuotaDatabaseAsync_RebuildsFromDirectories()
        {
            // Arrange
            var tenantId = $"tenant-rebuild-quota-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenantPath = Path.Combine(_volumePath, tenantId);
            _fileSystem.Directory.CreateDirectory(tenantPath);

            // Create directory structure with files
            var dir1 = Path.Combine(tenantPath, "documents");
            var dir2 = Path.Combine(tenantPath, "images");
            _fileSystem.Directory.CreateDirectory(dir1);
            _fileSystem.Directory.CreateDirectory(dir2);
            _fileSystem.File.WriteAllText(Path.Combine(dir1, "file1.txt"), "content");
            _fileSystem.File.WriteAllText(Path.Combine(dir1, "file2.txt"), "content");
            _fileSystem.File.WriteAllText(Path.Combine(dir2, "image.png"), "content");

            // Create a valid database first, then corrupt it
            var dbPath = Path.Combine(_quotaDir, tenantId, "quotas.db");
            using (var tempRepo = new DirectoryQuotaRepository(_fileSystem, new Mock<ILogger<DirectoryQuotaRepository>>().Object, _quotaDir, enableBackgroundFlush: false))
            {
                await tempRepo.GetOrCreateAsync(tenantId, "/", default);
            }

            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            // Corrupt the database
            var garbage = new byte[512];
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            // Act
            var result = await _recoveryService.RebuildQuotaDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(2, result.RecordsRebuilt);
            Assert.NotNull(result.BackupPath);

            // Verify new database file exists
            Assert.True(_fileSystem.File.Exists(dbPath));

            var quotas = (await _quotaRepository.GetAllAsync(tenantId, default)).ToArray();
            Assert.Contains(quotas, q => q.DirectoryPath == "/documents" && q.CurrentCount == 2);
            Assert.Contains(quotas, q => q.DirectoryPath == "/images" && q.CurrentCount == 1);
            Assert.Contains(quotas, q => q.DirectoryPath == tenantId && q.CurrentCount == 3);
            Assert.DoesNotContain(quotas, q => q.DirectoryPath == "/" && q.CurrentCount == 3);
        }

        [Fact]
        public async Task RebuildQuotaDatabaseAsync_PrefersMetadataReconciliationWhenAvailable()
        {
            var tenantId = $"tenant-rebuild-quota-metadata-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var dbPath = Path.Combine(_quotaDir, tenantId, "quotas.db");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "quota-file-001",
                TenantId = tenantId,
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumePath, tenantId, "docs", "quota-file-001.dcm"),
                DirectoryPath = "/docs",
                FileSize = 11,
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            }, default);

            using (var tempRepo = new DirectoryQuotaRepository(_fileSystem, new Mock<ILogger<DirectoryQuotaRepository>>().Object, _quotaDir, enableBackgroundFlush: false))
            {
                await tempRepo.GetOrCreateAsync(tenantId, "/", default);
            }

            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            var garbage = new byte[512];
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            var cleanupService = new Mock<IStorageCleanupService>(MockBehavior.Strict);
            cleanupService
                .Setup(x => x.ReconcileQuotaCountsAsync(tenantId, It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>(async (_, ct) =>
                {
                    await _quotaRepository.SetCurrentCountAsync(tenantId, "/docs", 1, ct);
                });

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                storageCleanupService: cleanupService.Object);

            var result = await recoveryService.RebuildQuotaDatabaseAsync(
                tenantId,
                new[] { _volumePath },
                default);

            Assert.True(result.Success);
            Assert.True(result.RecordsRebuilt >= 1);
            cleanupService.Verify(x => x.ReconcileQuotaCountsAsync(tenantId, It.IsAny<CancellationToken>()), Times.Once);
            Assert.Contains((await _quotaRepository.GetAllAsync(tenantId, default)).ToArray(), q => q.DirectoryPath == "/docs" && q.CurrentCount == 1);
        }

        [Fact]
        public async Task RebuildQuotaDatabaseAsync_UsesProjectionStoreMetadataWhenProvided()
        {
            var tenantId = $"tenant-rebuild-quota-projection-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(x => x.GetProjectedFilesAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[]
                {
                    new FileMetadata
                    {
                        FileKey = "quota-projection-file",
                        TenantId = tenantId,
                        VolumeId = "vol-001",
                        PhysicalPath = Path.Combine(_volumePath, tenantId, "docs", "quota-projection-file.dcm"),
                        DirectoryPath = "/logical/inbox",
                        FileSize = 11,
                        Status = FileProcessingStatus.Pending,
                        CreatedAt = DateTime.UtcNow
                    }
                });

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                projectionStore: projectionStore.Object);

            var result = await recoveryService.RebuildQuotaDatabaseAsync(
                tenantId,
                Array.Empty<string>(),
                default);

            Assert.True(result.Success);
            Assert.True(result.RecordsRebuilt >= 2);
            projectionStore.Verify(x => x.GetProjectedFilesAsync(tenantId, It.IsAny<CancellationToken>()), Times.Once);

            var quotas = (await _quotaRepository.GetAllAsync(tenantId, default)).ToArray();
            Assert.Contains(quotas, q => q.DirectoryPath == "/logical/inbox" && q.CurrentCount == 1);
            Assert.Contains(quotas, q => q.DirectoryPath == tenantId && q.CurrentCount == 1);
        }

        [Fact]
        public async Task RebuildQuotaDatabaseAsync_UsesQuotaMaintenanceStoreWhenProvided()
        {
            var tenantId = $"tenant-rebuild-quota-maint-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(x => x.GetProjectedFilesAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[]
                {
                    new FileMetadata
                    {
                        FileKey = "quota-maint-file",
                        TenantId = tenantId,
                        VolumeId = "vol-001",
                        PhysicalPath = Path.Combine(_volumePath, tenantId, "docs", "quota-maint-file.dcm"),
                        DirectoryPath = "/logical/inbox",
                        FileSize = 11,
                        Status = FileProcessingStatus.Pending,
                        CreatedAt = DateTime.UtcNow
                    }
                });

            var quotaMaintenanceStore = new Mock<IQuotaProjectionMaintenanceStore>(MockBehavior.Strict);
            quotaMaintenanceStore
                .Setup(store => store.BeginDatabaseRebuildAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new TestDatabaseRebuildLockHandle());
            quotaMaintenanceStore
                .SetupSequence(store => store.GetQuotaRowsAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(Array.Empty<DirectoryQuota>())
                .ReturnsAsync(new[]
                {
                    new DirectoryQuota { DirectoryPath = "/logical/inbox", CurrentCount = 1, MaxCount = 0, Enabled = false },
                    new DirectoryQuota { DirectoryPath = tenantId, CurrentCount = 1, MaxCount = 0, Enabled = false }
                });
            quotaMaintenanceStore
                .Setup(store => store.SetProjectedCountForRebuildAsync(tenantId, "/logical/inbox", 1, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            quotaMaintenanceStore
                .Setup(store => store.SetProjectedCountForRebuildAsync(tenantId, tenantId, 1, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                projectionStore: projectionStore.Object,
                quotaMaintenanceStore: quotaMaintenanceStore.Object);

            var result = await recoveryService.RebuildQuotaDatabaseAsync(
                tenantId,
                Array.Empty<string>(),
                default);

            Assert.True(result.Success);
            Assert.Equal(2, result.RecordsRebuilt);
            quotaMaintenanceStore.Verify(
                store => store.BeginDatabaseRebuildAsync(tenantId, It.IsAny<CancellationToken>()),
                Times.Once);
            quotaMaintenanceStore.Verify(
                store => store.SetProjectedCountForRebuildAsync(tenantId, "/logical/inbox", 1, It.IsAny<CancellationToken>()),
                Times.Once);
            quotaMaintenanceStore.Verify(
                store => store.SetProjectedCountForRebuildAsync(tenantId, tenantId, 1, It.IsAny<CancellationToken>()),
                Times.Once);
            quotaMaintenanceStore.VerifyAll();
        }

        [Fact]
        public async Task RebuildQuotaDatabaseAsync_SynchronizesAtomicCounterState()
        {
            // Arrange
            var tenantId = $"tenant-rebuild-counter-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenantPath = Path.Combine(_volumePath, tenantId);
            var docsPath = Path.Combine(tenantPath, "documents");
            _fileSystem.Directory.CreateDirectory(docsPath);
            _fileSystem.File.WriteAllText(Path.Combine(docsPath, "a.txt"), "a");
            _fileSystem.File.WriteAllText(Path.Combine(docsPath, "b.txt"), "b");

            var dbPath = Path.Combine(_quotaDir, tenantId, "quotas.db");
            using (var tempRepo = new DirectoryQuotaRepository(_fileSystem, new Mock<ILogger<DirectoryQuotaRepository>>().Object, _quotaDir, enableBackgroundFlush: false))
            {
                await tempRepo.GetOrCreateAsync(tenantId, "/", default);
            }

            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            var garbage = new byte[512];
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            await _recoveryService.RebuildQuotaDatabaseAsync(tenantId, new[] { _volumePath }, default);

            // Set limit equal to rebuilt logical-root count; a further increment must be rejected.
            var quota = await _quotaRepository.GetOrCreateAsync(tenantId, "/", default);
            quota.MaxCount = 2;
            quota.Enabled = true;
            await _quotaRepository.UpdateAsync(tenantId, quota, default);

            // Act
            var incremented = await _quotaRepository.TryIncrementAsync(tenantId, "/", default);

            // Assert
            Assert.False(incremented);
        }

        [Fact]
        public async Task CheckAllDatabasesAsync_ReturnsHealthyForAllHealthyDatabases()
        {
            // Arrange - use unique tenant IDs and temporary repositories
            var tenant1 = $"tenant-check-1-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenant2 = $"tenant-check-2-{Guid.NewGuid().ToString("N").Substring(0, 8)}";

            // Create databases using temporary repositories, then dispose them
            using (var tempMetaRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir, enableBackgroundPersistence: false))
            {
                await tempMetaRepo.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = "file-001",
                    TenantId = tenant1,
                    VolumeId = "vol-001",
                    PhysicalPath = "/path",
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                }, default);
            }

            using (var tempQuotaRepo = new DirectoryQuotaRepository(_fileSystem, new Mock<ILogger<DirectoryQuotaRepository>>().Object, _quotaDir, enableBackgroundFlush: false))
            {
                await tempQuotaRepo.GetOrCreateAsync(tenant2, "/", default);
            }

            // Wait for file system to release locks
            await Task.Delay(100);

            // Act
            var report = await _recoveryService.CheckAllDatabasesAsync(default);

            // Assert
            Assert.True(report.AllHealthy);
            Assert.Equal(2, report.HealthyDatabases);
            Assert.Empty(report.CorruptedDatabases);
        }

        [Fact]
        public async Task CheckAllDatabasesAsync_DetectsCorruptedDatabases()
        {
            // Arrange
            var tenant1 = $"tenant-corrupt-1-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenant2 = $"tenant-corrupt-2-{Guid.NewGuid().ToString("N").Substring(0, 8)}";

            // Create valid databases first, then corrupt them
            using (var tempMetaRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir, enableBackgroundPersistence: false))
            {
                await tempMetaRepo.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = "test1",
                    TenantId = tenant1,
                    VolumeId = "vol",
                    PhysicalPath = "/test",
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                }, default);
            }

            using (var tempQuotaRepo = new DirectoryQuotaRepository(_fileSystem, new Mock<ILogger<DirectoryQuotaRepository>>().Object, _quotaDir, enableBackgroundFlush: false))
            {
                await tempQuotaRepo.GetOrCreateAsync(tenant2, "/", default);
            }

            // Wait for databases to be fully written
            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            // Now corrupt both databases
            var metaDbPath = Path.Combine(_metadataDir, tenant1, "metadata.db");
            var quotaDbPath = Path.Combine(_quotaDir, tenant2, "quotas.db");

            var garbage = new byte[512];
            new Random().NextBytes(garbage);

            using (var stream = _fileSystem.File.Open(metaDbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            using (var stream = _fileSystem.File.Open(quotaDbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            // Act
            var report = await _recoveryService.CheckAllDatabasesAsync(default);

            // Assert
            Assert.False(report.AllHealthy);
            Assert.Equal(0, report.HealthyDatabases);
            Assert.Equal(2, report.CorruptedDatabases.Count);

            var metaCorrupted = report.CorruptedDatabases.FirstOrDefault(d => d.DatabaseType == "Metadata");
            Assert.NotNull(metaCorrupted);
            Assert.Equal(tenant1, metaCorrupted.TenantId);
            Assert.True(metaCorrupted.IsCorrupted);

            var quotaCorrupted = report.CorruptedDatabases.FirstOrDefault(d => d.DatabaseType == "Quota");
            Assert.NotNull(quotaCorrupted);
            Assert.Equal(tenant2, quotaCorrupted.TenantId);
            Assert.True(quotaCorrupted.IsCorrupted);
        }

        [Fact]
        public async Task CheckAllDatabasesAsync_HandlesNonExistentDirectories()
        {
            // Arrange
            var tempMetaDir = Path.Combine(Path.GetTempPath(), $"locus-test-nodir-{_testId}");
            var tempQuotaDir = Path.Combine(Path.GetTempPath(), $"locus-test-nodir-quota-{_testId}");

            var recovery = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _logger.Object,
                tempMetaDir,
                tempQuotaDir
            );

            // Act
            var report = await recovery.CheckAllDatabasesAsync(default);

            // Assert
            Assert.True(report.AllHealthy);
            Assert.Equal(0, report.HealthyDatabases);
            Assert.Empty(report.CorruptedDatabases);
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_ThrowsForInvalidTenantId()
        {
            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await _recoveryService.RebuildMetadataDatabaseAsync("", new[] { _volumePath }, default));

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await _recoveryService.RebuildMetadataDatabaseAsync(null!, new[] { _volumePath }, default));
        }

        [Fact]
        public async Task RebuildQuotaDatabaseAsync_ThrowsForInvalidTenantId()
        {
            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await _recoveryService.RebuildQuotaDatabaseAsync("", new[] { _volumePath }, default));

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await _recoveryService.RebuildQuotaDatabaseAsync(null!, new[] { _volumePath }, default));
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_HandlesEmptyVolumePaths()
        {
            // Arrange
            var tenantId = "tenant-no-volumes";
            var tenantSubDir = Path.Combine(_metadataDir, tenantId);
            _fileSystem.Directory.CreateDirectory(tenantSubDir);
            var dbPath = Path.Combine(tenantSubDir, "metadata.db");
            _fileSystem.File.WriteAllText(dbPath, "corrupted");

            // Act
            var result = await _recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                Array.Empty<string>(),
                default);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(0, result.RecordsRebuilt);
        }

        [Fact]
        public async Task RebuildMetadataDatabaseAsync_SkipsNonExistentVolumes()
        {
            // Arrange
            var tenantId = "tenant-missing-volumes";
            var tenantSubDir = Path.Combine(_metadataDir, tenantId);
            _fileSystem.Directory.CreateDirectory(tenantSubDir);
            var dbPath = Path.Combine(tenantSubDir, "metadata.db");
            _fileSystem.File.WriteAllText(dbPath, "corrupted");

            var nonExistentVolume = Path.Combine(Path.GetTempPath(), $"non-existent-{_testId}");

            // Act
            var result = await _recoveryService.RebuildMetadataDatabaseAsync(
                tenantId,
                new[] { nonExistentVolume },
                default);

            // Assert
            Assert.True(result.Success);
            Assert.Equal(0, result.RecordsRebuilt);
        }
    }
}
