using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Exceptions;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class FileSchedulerTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly MetadataRepository _repository;
        private readonly Mock<ILogger<FileScheduler>> _logger;
        private readonly FileScheduler _scheduler;
        private readonly string _metadataDir;
        private readonly Mock<ITenantContext> _tenant;

        public FileSchedulerTests()
        {
            // Use real file system for tests (SQLite requires real file system)
            _fileSystem = new System.IO.Abstractions.FileSystem();

            // Setup repositories with unique temporary directories
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _metadataDir = Path.Combine(Path.GetTempPath(), $"locus-test-scheduler-{testId}");
            _fileSystem.Directory.CreateDirectory(_metadataDir);

            var repoLogger = new Mock<ILogger<MetadataRepository>>();
            _repository = new MetadataRepository(
                _fileSystem,
                repoLogger.Object,
                _metadataDir,
                enableBackgroundPersistence: false);

            _logger = new Mock<ILogger<FileScheduler>>();
            _scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object, allowLegacyNonJournalMode: true);

            _tenant = new Mock<ITenantContext>();
            _tenant.Setup(t => t.TenantId).Returns("tenant-001");
            _tenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);
        }

        public void Dispose()
        {
            _repository?.Dispose();
            SqliteConnection.ClearAllPools();

            // Cleanup temporary test directory
            try
            {
                if (_fileSystem.Directory.Exists(_metadataDir))
                    _fileSystem.Directory.Delete(_metadataDir, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors in tests
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
        public void Constructor_WithoutJournal_RequiresExplicitLegacyAllowance()
        {
            var ex = Assert.Throws<InvalidOperationException>(() =>
                new FileScheduler(_repository, _fileSystem, _logger.Object));

            Assert.Contains("legacy non-journal mode", ex.Message);
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_ReturnsOldestPendingFile()
        {
            // Arrange
            var file1 = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10)
            };

            var file2 = new FileMetadata
            {
                FileKey = "file-002",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-5)
            };

            await _repository.AddOrUpdateAsync(file1, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file2, CancellationToken.None);

            // Act
            var location = await _scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None);

            // Assert
            Assert.NotNull(location);
            Assert.Equal("file-001", location.FileKey);
            Assert.Equal(FileProcessingStatus.Processing, location.Status);
            Assert.NotNull(location.Lease);
            Assert.Equal("tenant-001", location.Lease!.TenantId);
            Assert.Equal("file-001", location.Lease.FileKey);
            Assert.Equal(location.ProcessingStartTime, location.Lease.ProcessingStartTimeUtc);

            // Verify file is marked as processing in repository
            var metadata = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Processing, metadata.Status);
            Assert.NotNull(metadata.ProcessingStartTime);
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_ReturnsNullWhenNoFilesAvailable()
        {
            // Act
            var result = await _scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_UsesProjectionStoreWhenProvided()
        {
            var processingStart = DateTime.UtcNow;
            var projected = new FileMetadata
            {
                FileKey = "projected-file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                CreatedAt = processingStart.AddMinutes(-1),
                ProcessingStartTime = processingStart,
                PhysicalPath = "/projected/file-001.dat",
                DirectoryPath = "/logical/inbox"
            };

            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(m => m.LeaseNextPendingFileAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(projected);

            var scheduler = new FileScheduler(
                _repository,
                _fileSystem,
                _logger.Object,
                projectionStore: projectionStore.Object,
                allowLegacyNonJournalMode: true);

            var location = await scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None);

            Assert.NotNull(location);
            Assert.Equal("projected-file-001", location!.FileKey);
            Assert.Equal("/logical/inbox", location.DirectoryPath);
            Assert.NotNull(location.Lease);
            Assert.Equal(processingStart, location.Lease!.ProcessingStartTimeUtc);
            projectionStore.Verify(
                m => m.LeaseNextPendingFileAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task CleanupOrphanedMetadataAsync_UsesProjectionStoreWhenProvided()
        {
            var missingPath = Path.Combine(_metadataDir, "projection-missing-file.dat");
            var projected = new FileMetadata
            {
                FileKey = "projection-orphan",
                TenantId = "tenant-001",
                PhysicalPath = missingPath,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            };

            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(m => m.GetProjectedTenantIdsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[] { "tenant-001" });
            projectionStore
                .Setup(m => m.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[] { projected });
            projectionStore
                .Setup(m => m.RemoveProjectedFileAsync("tenant-001", "projection-orphan", It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var scheduler = new FileScheduler(
                projectionStore.Object,
                _fileSystem,
                _logger.Object,
                allowLegacyNonJournalMode: true);

            var removed = await scheduler.CleanupOrphanedMetadataAsync(CancellationToken.None);

            Assert.Equal(1, removed);
            projectionStore.Verify(
                m => m.GetProjectedTenantIdsAsync(It.IsAny<CancellationToken>()),
                Times.Once);
            projectionStore.Verify(
                m => m.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Once);
            projectionStore.Verify(
                m => m.RemoveProjectedFileAsync("tenant-001", "projection-orphan", It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_WhenJournalAppendFails_RollsBackToPending()
        {
            var metadata = new FileMetadata
            {
                FileKey = "file-journal-start",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-1),
            };
            await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);

            var journal = new Mock<IQueueEventJournal>();
            journal
                .Setup(m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record => record.EventType == QueueEventType.ProcessingStarted),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("journal append failed"));

            var scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object, queueEventJournal: journal.Object);

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None));

            var current = await _repository.GetAsync("tenant-001", "file-journal-start", CancellationToken.None);
            Assert.NotNull(current);
            Assert.Equal(FileProcessingStatus.Pending, current!.Status);
            Assert.Null(current.ProcessingStartTime);
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_SkipsProcessingFiles()
        {
            // Arrange
            var file1 = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10)
            };

            var file2 = new FileMetadata
            {
                FileKey = "file-002",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-5)
            };

            await _repository.AddOrUpdateAsync(file1, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file2, CancellationToken.None);

            // Act
            var location = await _scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None);

            // Assert
            Assert.NotNull(location);
            Assert.Equal("file-002", location.FileKey);
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_RespectsRetryDelay()
        {
            // Arrange
            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Pending,
                AvailableForProcessingAt = DateTime.UtcNow.AddMinutes(10), // Not available yet
                CreatedAt = DateTime.UtcNow.AddMinutes(-10)
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            var result = await _scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public async Task GetNextBatchForProcessingAsync_ReturnsBatch()
        {
            // Arrange — add files oldest-first so the FIFO queue returns them in CreatedAt order.
            for (int i = 4; i >= 0; i--)
            {
                var file = new FileMetadata
                {
                    FileKey = $"file-{i:D3}",
                    TenantId = "tenant-001",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow.AddMinutes(-i)
                };
                await _repository.AddOrUpdateAsync(file, CancellationToken.None);
            }

            // Act
            var locations = await _scheduler.GetNextBatchForProcessingAsync(_tenant.Object, 3, CancellationToken.None);

            // Assert
            var list = locations.ToList();
            Assert.Equal(3, list.Count);

            // Queue is FIFO — first inserted (file-004, oldest) is returned first.
            Assert.Equal("file-004", list[0].FileKey);
            Assert.Equal("file-003", list[1].FileKey);
            Assert.Equal("file-002", list[2].FileKey);

            // All should be marked as processing
            foreach (var location in list)
            {
                Assert.Equal(FileProcessingStatus.Processing, location.Status);
                Assert.NotNull(location.Lease);
            }
        }

        [Fact]
        public async Task CleanupOrphanedMetadataAsync_IgnoresCancellationAfterQuotaDecrementAndRemovesMetadata()
        {
            var tenantQuotaManager = new Mock<ITenantQuotaManager>();
            var directoryQuotaManager = new Mock<IDirectoryQuotaManager>();
            var fileKey = "orphan-cleanup";
            var physicalPath = Path.Combine(_metadataDir, "missing-file.txt");

            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = fileKey,
                TenantId = "tenant-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            }, CancellationToken.None);

            using var cts = new CancellationTokenSource();
            tenantQuotaManager
                .Setup(m => m.DecrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()))
                .Returns((string _, CancellationToken __) =>
                {
                    cts.Cancel();
                    return Task.CompletedTask;
                });
            directoryQuotaManager
                .Setup(m => m.DecrementFileCountAsync("tenant-001", "/", It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var scheduler = new FileScheduler(
                _repository,
                _fileSystem,
                _logger.Object,
                tenantQuotaManager: tenantQuotaManager.Object,
                directoryQuotaManager: directoryQuotaManager.Object,
                allowLegacyNonJournalMode: true);

            var removed = await scheduler.CleanupOrphanedMetadataAsync(cts.Token);

            Assert.Equal(1, removed);
            Assert.Null(await _repository.GetAsync("tenant-001", fileKey, CancellationToken.None));
        }

        [Fact]
        public async Task CleanupOrphanedMetadataAsync_NormalizesMissingDirectoryPathToRootQuota()
        {
            var tenantQuotaManager = new Mock<ITenantQuotaManager>();
            var directoryQuotaManager = new Mock<IDirectoryQuotaManager>();
            var fileKey = "orphan-root-fallback";
            var physicalPath = Path.Combine(_metadataDir, "missing-root-file.txt");

            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = fileKey,
                TenantId = "tenant-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            }, CancellationToken.None);

            tenantQuotaManager
                .Setup(m => m.DecrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            directoryQuotaManager
                .Setup(m => m.DecrementFileCountAsync("tenant-001", "/", It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var scheduler = new FileScheduler(
                _repository,
                _fileSystem,
                _logger.Object,
                tenantQuotaManager: tenantQuotaManager.Object,
                directoryQuotaManager: directoryQuotaManager.Object,
                allowLegacyNonJournalMode: true);

            var removed = await scheduler.CleanupOrphanedMetadataAsync(CancellationToken.None);

            Assert.Equal(1, removed);
            tenantQuotaManager.Verify(
                m => m.DecrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Once);
            directoryQuotaManager.Verify(
                m => m.DecrementFileCountAsync("tenant-001", "/", It.IsAny<CancellationToken>()),
                Times.Once);
            Assert.Null(await _repository.GetAsync("tenant-001", fileKey, CancellationToken.None));
        }

        [Fact]
        public async Task CleanupOrphanedMetadataAsync_SkipsRowsWhenVolumeIsUnavailableAndPhysicalPathStillExists()
        {
            var tenantQuotaManager = new Mock<ITenantQuotaManager>(MockBehavior.Strict);
            var directoryQuotaManager = new Mock<IDirectoryQuotaManager>(MockBehavior.Strict);
            var volumeRegistry = new StorageVolumeRegistry();
            var fileKey = "orphan-volume-unavailable";
            var physicalPath = Path.Combine(_metadataDir, "still-there.dat");
            _fileSystem.File.WriteAllText(physicalPath, "content");

            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = fileKey,
                TenantId = "tenant-001",
                VolumeId = "vol-missing",
                PhysicalPath = physicalPath,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            }, CancellationToken.None);

            var scheduler = new FileScheduler(
                _repository,
                _fileSystem,
                _logger.Object,
                volumeRegistry: volumeRegistry,
                tenantQuotaManager: tenantQuotaManager.Object,
                directoryQuotaManager: directoryQuotaManager.Object,
                allowLegacyNonJournalMode: true);

            var removed = await scheduler.CleanupOrphanedMetadataAsync(CancellationToken.None);

            Assert.Equal(0, removed);
            Assert.NotNull(await _repository.GetAsync("tenant-001", fileKey, CancellationToken.None));
            Assert.True(_fileSystem.File.Exists(physicalPath));
        }

        [Fact]
        public async Task CleanupOrphanedMetadataAsync_SkipsRowsWhenVolumeIsUnhealthy()
        {
            var tenantQuotaManager = new Mock<ITenantQuotaManager>(MockBehavior.Strict);
            var directoryQuotaManager = new Mock<IDirectoryQuotaManager>(MockBehavior.Strict);
            var volumeRegistry = new StorageVolumeRegistry();
            var volume = new Mock<IStorageVolume>(MockBehavior.Strict);
            var fileKey = "orphan-volume-unhealthy";
            var physicalPath = Path.Combine(_metadataDir, "missing-unhealthy.dat");

            volume.SetupGet(v => v.VolumeId).Returns("vol-unhealthy");
            volume.SetupGet(v => v.IsHealthy).Returns(false);
            volumeRegistry.Register(volume.Object);

            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = fileKey,
                TenantId = "tenant-001",
                VolumeId = "vol-unhealthy",
                PhysicalPath = physicalPath,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            }, CancellationToken.None);

            var scheduler = new FileScheduler(
                _repository,
                _fileSystem,
                _logger.Object,
                volumeRegistry: volumeRegistry,
                tenantQuotaManager: tenantQuotaManager.Object,
                directoryQuotaManager: directoryQuotaManager.Object,
                allowLegacyNonJournalMode: true);

            var removed = await scheduler.CleanupOrphanedMetadataAsync(CancellationToken.None);

            Assert.Equal(0, removed);
            Assert.NotNull(await _repository.GetAsync("tenant-001", fileKey, CancellationToken.None));
        }

        [Fact]
        public async Task MarkAsCompletedAsync_UpdatesMetadataToCompleted()
        {
            // Arrange
            var processingStart = DateTime.UtcNow;
            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                PhysicalPath = "/test/file-001.txt",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = processingStart
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            await _scheduler.MarkAsCompletedAsync(CreateLease("tenant-001", "file-001", processingStart), CancellationToken.None);

            // Assert
            var metadata = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Completed, metadata!.Status);
            Assert.Null(metadata.ProcessingStartTime);
            Assert.NotNull(metadata.CompletedAt);
        }

        [Fact]
        public async Task MarkAsCompletedAsync_DoesNotDeletePhysicalFile()
        {
            // Arrange
            var physicalPath = Path.Combine(_metadataDir, "file-001.txt");
            var processingStart = DateTime.UtcNow;

            // Create physical file
            var directory = Path.GetDirectoryName(physicalPath);
            if (!string.IsNullOrEmpty(directory))
                _fileSystem.Directory.CreateDirectory(directory);
            _fileSystem.File.WriteAllText(physicalPath, "test content");

            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                PhysicalPath = physicalPath,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = processingStart
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            await _scheduler.MarkAsCompletedAsync(CreateLease("tenant-001", "file-001", processingStart), CancellationToken.None);

            // Assert
            Assert.True(_fileSystem.File.Exists(physicalPath));
        }

        [Fact]
        public async Task MarkAsCompletedAsync_DoesNotUseRegisteredVolumeDelete()
        {
            var physicalPath = Path.Combine(_metadataDir, "volume-delete.txt");
            _fileSystem.File.WriteAllText(physicalPath, "test content");
            var processingStart = DateTime.UtcNow;

            var file = new FileMetadata
            {
                FileKey = "file-volume-delete",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = processingStart
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            var volume = new Mock<IStorageVolume>();
            volume.SetupGet(v => v.VolumeId).Returns("vol-001");
            var registry = new StorageVolumeRegistry();
            registry.Register(volume.Object);
            var scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object, volumeRegistry: registry, allowLegacyNonJournalMode: true);

            await scheduler.MarkAsCompletedAsync(CreateLease("tenant-001", "file-volume-delete", processingStart), CancellationToken.None);

            volume.Verify(v => v.DeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
            Assert.True(_fileSystem.File.Exists(physicalPath));
        }

        [Fact]
        public async Task MarkAsCompletedAsync_WhenDirectlyRepeatedAfterCompletion_IsIdempotent()
        {
            var file = new FileMetadata
            {
                FileKey = "file-repeat-complete",
                TenantId = "tenant-001",
                PhysicalPath = "/test/file-repeat-complete.txt",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = DateTime.UtcNow
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            await _scheduler.MarkAsCompletedAsync(
                CreateLease("tenant-001", "file-repeat-complete", file.ProcessingStartTime!.Value),
                CancellationToken.None);
            await _scheduler.MarkAsCompletedAsync(
                CreateLease("tenant-001", "file-repeat-complete", file.ProcessingStartTime!.Value),
                CancellationToken.None);

            var updated = await _repository.GetAsync("tenant-001", "file-repeat-complete", CancellationToken.None);
            Assert.NotNull(updated);
            Assert.Equal(FileProcessingStatus.Completed, updated!.Status);
            Assert.Null(updated.ProcessingStartTime);
            Assert.NotNull(updated.CompletedAt);
        }

        [Fact]
        public async Task MarkAsCompletedAsync_WhenJournalBatchAppendFails_LeavesFileProcessing()
        {
            var processingStart = DateTime.UtcNow;
            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "file-journal-complete",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = processingStart,
                CreatedAt = DateTime.UtcNow.AddMinutes(-2),
                PhysicalPath = Path.Combine(_metadataDir, "file-journal-complete.dcm"),
                DirectoryPath = "/incoming",
                VolumeId = "vol-001",
            }, CancellationToken.None);

            var journal = new Mock<IQueueEventJournal>();
            journal
                .Setup(m => m.AppendBatchAsync(
                    It.Is<IReadOnlyList<QueueEventRecord>>(records =>
                        records.Count == 2
                        && records[0].EventType == QueueEventType.ProcessingCompleted
                        && records[1].EventType == QueueEventType.DeleteRequested),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("journal batch append failed"));

            var scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object, queueEventJournal: journal.Object);

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                scheduler.MarkAsCompletedAsync(
                    CreateLease("tenant-001", "file-journal-complete", processingStart),
                    CancellationToken.None));

            var current = await _repository.GetAsync("tenant-001", "file-journal-complete", CancellationToken.None);
            Assert.NotNull(current);
            Assert.Equal(FileProcessingStatus.Processing, current!.Status);
            Assert.Equal(processingStart, current.ProcessingStartTime);
            Assert.Null(current.CompletedAt);
        }

        [Fact]
        public async Task MarkAsCompletedAsync_FileAlreadyCompleted_KeepsCompletedState()
        {
            var processingStart = DateTime.UtcNow;
            var file = new FileMetadata
            {
                FileKey = "file-already-complete",
                TenantId = "tenant-001",
                PhysicalPath = "/test/file-already-complete.txt",
                Status = FileProcessingStatus.Completed,
                CompletedAt = processingStart.AddSeconds(5)
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            await _scheduler.MarkAsCompletedAsync(
                CreateLease("tenant-001", "file-already-complete", processingStart),
                CancellationToken.None);

            var metadata = await _repository.GetAsync("tenant-001", "file-already-complete", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Completed, metadata!.Status);
            Assert.Equal(file.CompletedAt, metadata.CompletedAt);
        }

        [Fact]
        public async Task MarkAsCompletedAsync_ThrowsWhenProcessingLeaseChanged()
        {
            var originalLease = DateTime.UtcNow.AddMinutes(-2);
            var replacementLease = DateTime.UtcNow.AddMinutes(-1);
            var physicalPath = Path.Combine(_metadataDir, "lease-complete.txt");
            _fileSystem.File.WriteAllText(physicalPath, "test content");

            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "file-lease-complete",
                TenantId = "tenant-001",
                PhysicalPath = physicalPath,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = replacementLease
            }, CancellationToken.None);

            var ex = await Assert.ThrowsAsync<FileProcessingLeaseMismatchException>(() =>
                _scheduler.MarkAsCompletedAsync(CreateLease("tenant-001", "file-lease-complete", originalLease), CancellationToken.None));

            Assert.Equal("file-lease-complete", ex.FileKey);
            Assert.Equal(replacementLease, ex.ActualProcessingStartTimeUtc);
            Assert.True(_fileSystem.File.Exists(physicalPath));

            var metadata = await _repository.GetAsync("tenant-001", "file-lease-complete", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Processing, metadata!.Status);
            Assert.Equal(replacementLease, metadata.ProcessingStartTime);
        }

        [Fact]
        public async Task MarkAsFailedAsync_ReturnsFileToPending()
        {
            // Arrange
            var processingStart = DateTime.UtcNow;
            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                RetryCount = 0,
                ProcessingStartTime = processingStart
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            await _scheduler.MarkAsFailedAsync(CreateLease("tenant-001", "file-001", processingStart), "Test error", CancellationToken.None);

            // Assert
            var metadata = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Pending, metadata.Status);
            Assert.Equal(1, metadata.RetryCount);
            Assert.Equal("Test error", metadata.LastError);
            Assert.NotNull(metadata.LastFailedAt);
            Assert.NotNull(metadata.AvailableForProcessingAt);
            Assert.Null(metadata.ProcessingStartTime);
        }

        [Fact]
        public async Task MarkAsFailedAsync_PermanentlyFailedAfterMaxRetries()
        {
            // Arrange
            var retryPolicy = new FileRetryPolicy { MaxRetryCount = 3 };
            var scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object, retryPolicy, allowLegacyNonJournalMode: true);
            var processingStart = DateTime.UtcNow;

            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                RetryCount = 2, // Already failed twice
                ProcessingStartTime = processingStart
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act - third failure should mark as permanently failed
            await scheduler.MarkAsFailedAsync(CreateLease("tenant-001", "file-001", processingStart), "Final error", CancellationToken.None);

            // Assert
            var metadata = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.PermanentlyFailed, metadata.Status);
            Assert.Equal(3, metadata.RetryCount);
            Assert.Null(metadata.AvailableForProcessingAt);
        }

        [Fact]
        public async Task MarkAsFailedAsync_WhenJournalAppendFails_LeavesFileProcessing()
        {
            var processingStart = DateTime.UtcNow;
            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "file-journal-fail",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = processingStart,
                RetryCount = 0,
                CreatedAt = DateTime.UtcNow.AddMinutes(-2),
                PhysicalPath = Path.Combine(_metadataDir, "file-journal-fail.dcm"),
                DirectoryPath = "/incoming",
                VolumeId = "vol-001",
            }, CancellationToken.None);

            var journal = new Mock<IQueueEventJournal>();
            journal
                .Setup(m => m.AppendAsync(
                    It.Is<QueueEventRecord>(record => record.EventType == QueueEventType.ProcessingFailed),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidOperationException("journal append failed"));

            var scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object, queueEventJournal: journal.Object);

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                scheduler.MarkAsFailedAsync(
                    CreateLease("tenant-001", "file-journal-fail", processingStart),
                    "decode failed",
                    CancellationToken.None));

            var current = await _repository.GetAsync("tenant-001", "file-journal-fail", CancellationToken.None);
            Assert.NotNull(current);
            Assert.Equal(FileProcessingStatus.Processing, current!.Status);
            Assert.Equal(processingStart, current.ProcessingStartTime);
            Assert.Equal(0, current.RetryCount);
            Assert.Null(current.LastFailedAt);
        }

        [Fact]
        public async Task MarkAsFailedAsync_UsesExponentialBackoff()
        {
            // Arrange
            var retryPolicy = new FileRetryPolicy
            {
                InitialRetryDelay = TimeSpan.FromSeconds(5),
                UseExponentialBackoff = true
            };

            var scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object, retryPolicy, allowLegacyNonJournalMode: true);
            var firstProcessingStart = DateTime.UtcNow;

            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                RetryCount = 0,
                ProcessingStartTime = firstProcessingStart
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act - first failure
            await scheduler.MarkAsFailedAsync(CreateLease("tenant-001", "file-001", firstProcessingStart), "Error 1", CancellationToken.None);

            // Assert - delay should be 5 seconds (5 * 2^0)
            var metadata1 = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata1);
            var delay1 = metadata1.AvailableForProcessingAt!.Value - DateTime.UtcNow;
            Assert.InRange(delay1.TotalSeconds, 4, 6);

            // Act - second failure
            metadata1.Status = FileProcessingStatus.Processing;
            var secondProcessingStart = DateTime.UtcNow.AddTicks(1);
            metadata1.ProcessingStartTime = secondProcessingStart;
            await _repository.AddOrUpdateAsync(metadata1, CancellationToken.None);
            await scheduler.MarkAsFailedAsync(CreateLease("tenant-001", "file-001", secondProcessingStart), "Error 2", CancellationToken.None);

            // Assert - delay should be 10 seconds (5 * 2^1)
            var metadata2 = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata2);
            var delay2 = metadata2.AvailableForProcessingAt!.Value - DateTime.UtcNow;
            Assert.InRange(delay2.TotalSeconds, 9, 11);
        }

        [Fact]
        public async Task MarkAsFailedAsync_ThrowsWhenProcessingLeaseChanged()
        {
            var originalLease = DateTime.UtcNow.AddMinutes(-2);
            var replacementLease = DateTime.UtcNow.AddMinutes(-1);

            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "file-lease-fail",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                RetryCount = 0,
                ProcessingStartTime = replacementLease
            }, CancellationToken.None);

            var ex = await Assert.ThrowsAsync<FileProcessingLeaseMismatchException>(() =>
                _scheduler.MarkAsFailedAsync(CreateLease("tenant-001", "file-lease-fail", originalLease), "stale worker", CancellationToken.None));

            Assert.Equal("file-lease-fail", ex.FileKey);
            Assert.Equal(replacementLease, ex.ActualProcessingStartTimeUtc);

            var metadata = await _repository.GetAsync("tenant-001", "file-lease-fail", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Processing, metadata!.Status);
            Assert.Equal(0, metadata.RetryCount);
            Assert.Equal(replacementLease, metadata.ProcessingStartTime);
        }

        [Fact]
        public async Task ResetProcessingStatusAsync_ResetsFileState()
        {
            // Arrange
            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.PermanentlyFailed,
                RetryCount = 5,
                LastError = "Previous error",
                LastFailedAt = DateTime.UtcNow.AddMinutes(-10),
                AvailableForProcessingAt = DateTime.UtcNow.AddMinutes(10)
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            await _scheduler.ResetProcessingStatusAsync("tenant-001", "file-001", CancellationToken.None);

            // Assert
            var metadata = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Pending, metadata.Status);
            Assert.Equal(0, metadata.RetryCount);
            Assert.Null(metadata.LastError);
            Assert.Null(metadata.LastFailedAt);
            Assert.Null(metadata.AvailableForProcessingAt);
            Assert.Null(metadata.ProcessingStartTime);
        }

        [Fact]
        public async Task ResetProcessingStatusAsync_ThrowsWhenFileIsStillProcessing()
        {
            var leaseStart = DateTime.UtcNow.AddMinutes(-1);
            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "file-processing",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                RetryCount = 2,
                LastError = "still running",
                LastFailedAt = DateTime.UtcNow.AddMinutes(-2),
                ProcessingStartTime = leaseStart
            }, CancellationToken.None);

            await Assert.ThrowsAsync<FileAlreadyProcessingException>(() =>
                _scheduler.ResetProcessingStatusAsync("tenant-001", "file-processing", CancellationToken.None));

            var metadata = await _repository.GetAsync("tenant-001", "file-processing", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Processing, metadata!.Status);
            Assert.Equal(2, metadata.RetryCount);
            Assert.Equal("still running", metadata.LastError);
            Assert.Equal(leaseStart, metadata.ProcessingStartTime);
        }

        [Fact]
        public async Task GetFileStatusAsync_ReturnsCorrectStatus()
        {
            // Arrange
            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            var status = await _scheduler.GetFileStatusAsync("tenant-001", "file-001", CancellationToken.None);

            // Assert
            Assert.Equal(FileProcessingStatus.Processing, status);
        }

        [Fact]
        public async Task ConcurrentGetNextFile_NoDuplicates()
        {
            // Arrange
            for (int i = 0; i < 10; i++)
            {
                var file = new FileMetadata
                {
                    FileKey = $"file-{i:D3}",
                    TenantId = "tenant-001",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow.AddMinutes(-i)
                };
                await _repository.AddOrUpdateAsync(file, CancellationToken.None);
            }

            // Act - simulate 5 threads concurrently getting files
            var tasks = new List<Task<FileLocation?>>();
            for (int i = 0; i < 5; i++)
            {
                tasks.Add(Task.Run(async () =>
                    await _scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None)));
            }

            var locations = await Task.WhenAll(tasks);

            // Assert - all file keys should be unique
            var nonNullLocations = locations.Where(l => l != null).ToList();
            var fileKeys = nonNullLocations.Select(l => l!.FileKey).ToList();
            Assert.Equal(5, fileKeys.Distinct().Count());

            // All should be marked as processing
            foreach (var location in nonNullLocations)
            {
                Assert.NotNull(location);
                Assert.Equal(FileProcessingStatus.Processing, location.Status);
            }
        }

        [Fact]
        public async Task MarkAsProcessingAsync_ThrowsWhenAlreadyProcessing()
        {
            // Arrange
            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act & Assert
            await Assert.ThrowsAsync<FileAlreadyProcessingException>(() =>
                _scheduler.MarkAsProcessingAsync("tenant-001", "file-001", CancellationToken.None));
        }

        [Fact]
        public async Task MarkAsProcessingAsync_ThrowsWhenStatusIsNotPending()
        {
            var file = new FileMetadata
            {
                FileKey = "file-failed",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Failed,
                CreatedAt = DateTime.UtcNow.AddMinutes(-1)
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                _scheduler.MarkAsProcessingAsync("tenant-001", "file-failed", CancellationToken.None));

            var metadata = await _repository.GetAsync("tenant-001", "file-failed", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Failed, metadata!.Status);
            Assert.Null(metadata.ProcessingStartTime);
        }

        [Fact]
        public async Task MarkAsProcessingAsync_ConcurrentCalls_OnlyOneSucceeds()
        {
            var file = new FileMetadata
            {
                FileKey = "file-race",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-1)
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            var tasks = Enumerable.Range(0, 16)
                .Select(async _ =>
                {
                    try
                    {
                        await _scheduler.MarkAsProcessingAsync("tenant-001", "file-race", CancellationToken.None);
                        return (Exception?)null;
                    }
                    catch (Exception ex)
                    {
                        return ex;
                    }
                })
                .ToArray();

            var results = await Task.WhenAll(tasks);

            Assert.Single(results, result => result == null);
            foreach (var ex in results.Where(result => result != null))
                Assert.IsType<FileAlreadyProcessingException>(ex);

            var metadata = await _repository.GetAsync("tenant-001", "file-race", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Processing, metadata!.Status);
            Assert.NotNull(metadata.ProcessingStartTime);
        }

        [Fact]
        public async Task MarkAsFailedAsync_RequiresTenantScopedLease()
        {
            var processingStart = DateTime.UtcNow;

            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "cross-tenant-file",
                TenantId = "tenant-002",
                Status = FileProcessingStatus.Processing,
                RetryCount = 0,
                ProcessingStartTime = processingStart
            }, CancellationToken.None);

            await _scheduler.MarkAsFailedAsync(
                CreateLease("tenant-002", "cross-tenant-file", processingStart),
                "cross-tenant failure",
                CancellationToken.None);

            var metadata = await _repository.GetAsync("tenant-002", "cross-tenant-file", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Pending, metadata!.Status);
            Assert.Equal(1, metadata.RetryCount);
            Assert.Equal("cross-tenant failure", metadata.LastError);
        }

        [Fact]
        public async Task GetFileStatusAsync_ThrowsWhenFileNotFound()
        {
            // Act & Assert
            // Fix 2: GetFileStatusAsync now throws FileNotFoundException (not FileAlreadyProcessingException)
            // when the file key does not exist in any tenant's metadata.
            await Assert.ThrowsAsync<FileNotFoundException>(() =>
                _scheduler.GetFileStatusAsync("tenant-001", "nonexistent", CancellationToken.None));
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_ReturnsPendingFileWithoutPhysicalExistenceCheck()
        {
            // Arrange
            // File.Exists is intentionally removed from the hot allocation path.
            // Physical-file existence is now validated by CleanupOrphanedMetadataAsync
            // (maintenance path), not on every GetNext call.
            var file1 = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                PhysicalPath = "/test/missing-file.txt", // Physical file absent — scheduler no longer checks
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10)
            };

            var file2 = new FileMetadata
            {
                FileKey = "file-002",
                TenantId = "tenant-001",
                PhysicalPath = Path.Combine(_metadataDir, "existing-file.txt"),
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-5)
            };

            await _repository.AddOrUpdateAsync(file1, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file2, CancellationToken.None);

            // Act — scheduler returns the oldest Pending file (FIFO), regardless of physical existence
            var location = await _scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None);

            // Assert — file-001 is returned first (oldest CreatedAt); no file is skipped or removed
            Assert.NotNull(location);
            Assert.Equal("file-001", location.FileKey);

            // file-001 metadata still present (not removed by the scheduler)
            var metadata1 = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata1);
            Assert.Equal(FileProcessingStatus.Processing, metadata1.Status);
        }

        [Fact]
        public async Task GetNextBatchForProcessingAsync_ReturnsAllPendingFilesWithoutPhysicalExistenceCheck()
        {
            // Arrange
            // File.Exists is intentionally removed from the hot allocation path.
            // Physical-file existence is now validated by CleanupOrphanedMetadataAsync.
            var file1 = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                PhysicalPath = "/test/missing-file1.txt", // Physical file absent — scheduler no longer checks
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10)
            };

            var file2 = new FileMetadata
            {
                FileKey = "file-002",
                TenantId = "tenant-001",
                PhysicalPath = Path.Combine(_metadataDir, "existing-file.txt"),
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-5)
            };

            var file3 = new FileMetadata
            {
                FileKey = "file-003",
                TenantId = "tenant-001",
                PhysicalPath = "/test/missing-file3.txt", // Physical file absent — scheduler no longer checks
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-3)
            };

            await _repository.AddOrUpdateAsync(file1, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file2, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file3, CancellationToken.None);

            // Act — all 3 files are returned; no existence check is performed
            var locations = await _scheduler.GetNextBatchForProcessingAsync(_tenant.Object, 3, CancellationToken.None);

            // Assert — all 3 files allocated as Processing
            var list = locations.ToList();
            Assert.Equal(3, list.Count);
            Assert.Contains(list, l => l.FileKey == "file-001");
            Assert.Contains(list, l => l.FileKey == "file-002");
            Assert.Contains(list, l => l.FileKey == "file-003");

            // All metadata records still present (none removed by the scheduler)
            Assert.NotNull(await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None));
            Assert.NotNull(await _repository.GetAsync("tenant-001", "file-003", CancellationToken.None));
        }

        [Fact]
        public async Task CleanupOrphanedMetadataAsync_RemovesMetadataForMissingFiles()
        {
            // Arrange
            var file1 = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                PhysicalPath = "/test/missing-file1.txt", // File doesn't exist
                Status = FileProcessingStatus.Pending
            };

            var physicalPath2 = Path.Combine(_metadataDir, "existing-file2.txt");
            var file2 = new FileMetadata
            {
                FileKey = "file-002",
                TenantId = "tenant-001",
                PhysicalPath = physicalPath2,
                Status = FileProcessingStatus.Pending
            };

            var file3 = new FileMetadata
            {
                FileKey = "file-003",
                TenantId = "tenant-001",
                PhysicalPath = "/test/missing-file3.txt", // File doesn't exist
                Status = FileProcessingStatus.Processing
            };

            // Only create physical file for file2
            _fileSystem.File.WriteAllText(physicalPath2, "test content");

            await _repository.AddOrUpdateAsync(file1, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file2, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file3, CancellationToken.None);

            // Act
            var removedCount = await _scheduler.CleanupOrphanedMetadataAsync(CancellationToken.None);

            // Assert
            Assert.Equal(2, removedCount);

            // Verify orphaned metadata removed
            Assert.Null(await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None));
            Assert.NotNull(await _repository.GetAsync("tenant-001", "file-002", CancellationToken.None)); // Should exist
            Assert.Null(await _repository.GetAsync("tenant-001", "file-003", CancellationToken.None));
        }

        [Fact]
        public async Task CleanupOrphanedMetadataAsync_ReturnsZeroWhenNoOrphans()
        {
            // Arrange
            var physicalPath = Path.Combine(_metadataDir, "existing-file3.txt");
            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                PhysicalPath = physicalPath,
                Status = FileProcessingStatus.Pending
            };

            _fileSystem.File.WriteAllText(physicalPath, "test content");
            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            var removedCount = await _scheduler.CleanupOrphanedMetadataAsync(CancellationToken.None);

            // Assert
            Assert.Equal(0, removedCount);
            Assert.NotNull(await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None));
        }

        [Fact]
        public async Task CleanupOrphanedMetadataAsync_RemovesMissingRowsForColdTenantsLoadedFromDisk()
        {
            var coldTenantId = $"tenant-cold-{Guid.NewGuid():N}";

            using (var seedRepository = new MetadataRepository(
                _fileSystem,
                new Mock<ILogger<MetadataRepository>>().Object,
                _metadataDir,
                enableBackgroundPersistence: false))
            {
                await seedRepository.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = "cold-file",
                    TenantId = coldTenantId,
                    VolumeId = "vol-001",
                    PhysicalPath = Path.Combine(_metadataDir, "missing-cold-file.dat"),
                    DirectoryPath = "/",
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = DateTime.UtcNow
                }, CancellationToken.None);
            }

            await Task.Delay(100);
            SqliteConnection.ClearAllPools();

            var tenantQuotaManager = new Mock<ITenantQuotaManager>(MockBehavior.Strict);
            tenantQuotaManager
                .Setup(m => m.DecrementFileCountAsync(coldTenantId, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var directoryQuotaManager = new Mock<IDirectoryQuotaManager>(MockBehavior.Strict);
            directoryQuotaManager
                .Setup(m => m.DecrementFileCountAsync(coldTenantId, "/", It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            using (var reopenedRepository = new MetadataRepository(
                _fileSystem,
                new Mock<ILogger<MetadataRepository>>().Object,
                _metadataDir,
                enableBackgroundPersistence: false))
            {
                var scheduler = new FileScheduler(
                    reopenedRepository,
                    _fileSystem,
                    _logger.Object,
                    tenantQuotaManager: tenantQuotaManager.Object,
                    directoryQuotaManager: directoryQuotaManager.Object,
                    allowLegacyNonJournalMode: true);

                var removedCount = await scheduler.CleanupOrphanedMetadataAsync(CancellationToken.None);

                Assert.Equal(1, removedCount);
                Assert.Null(await reopenedRepository.GetAsync(coldTenantId, "cold-file", CancellationToken.None));
            }

            tenantQuotaManager.Verify(
                m => m.DecrementFileCountAsync(coldTenantId, It.IsAny<CancellationToken>()),
                Times.Once);
            directoryQuotaManager.Verify(
                m => m.DecrementFileCountAsync(coldTenantId, "/", It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task CleanupOrphanedMetadataAsync_RemovesAllRowsSharingMissingPhysicalPath()
        {
            var tenantQuotaManager = new Mock<ITenantQuotaManager>(MockBehavior.Strict);
            var directoryQuotaManager = new Mock<IDirectoryQuotaManager>(MockBehavior.Strict);
            var sharedMissingPath = Path.Combine(_metadataDir, "shared-missing-file.dat");

            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "dup-file-001",
                TenantId = "tenant-001",
                PhysicalPath = sharedMissingPath,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            }, CancellationToken.None);

            await _repository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "dup-file-002",
                TenantId = "tenant-001",
                PhysicalPath = sharedMissingPath,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Failed,
                CreatedAt = DateTime.UtcNow
            }, CancellationToken.None);

            tenantQuotaManager
                .Setup(m => m.DecrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            directoryQuotaManager
                .Setup(m => m.DecrementFileCountAsync("tenant-001", "/", It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var scheduler = new FileScheduler(
                _repository,
                _fileSystem,
                _logger.Object,
                tenantQuotaManager: tenantQuotaManager.Object,
                directoryQuotaManager: directoryQuotaManager.Object,
                allowLegacyNonJournalMode: true);

            var removedCount = await scheduler.CleanupOrphanedMetadataAsync(CancellationToken.None);

            Assert.Equal(2, removedCount);
            Assert.Null(await _repository.GetAsync("tenant-001", "dup-file-001", CancellationToken.None));
            Assert.Null(await _repository.GetAsync("tenant-001", "dup-file-002", CancellationToken.None));
            tenantQuotaManager.Verify(
                m => m.DecrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Exactly(2));
            directoryQuotaManager.Verify(
                m => m.DecrementFileCountAsync("tenant-001", "/", It.IsAny<CancellationToken>()),
                Times.Exactly(2));
        }
    }
}
