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
            // Use real file system for tests (LiteDB requires real file system)
            _fileSystem = new System.IO.Abstractions.FileSystem();

            // Setup repositories with unique temporary directories
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _metadataDir = Path.Combine(Path.GetTempPath(), $"locus-test-scheduler-{testId}");
            _fileSystem.Directory.CreateDirectory(_metadataDir);

            var repoLogger = new Mock<ILogger<MetadataRepository>>();
            _repository = new MetadataRepository(_fileSystem, repoLogger.Object, _metadataDir);

            _logger = new Mock<ILogger<FileScheduler>>();
            _scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object);

            _tenant = new Mock<ITenantContext>();
            _tenant.Setup(t => t.TenantId).Returns("tenant-001");
            _tenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);
        }

        public void Dispose()
        {
            _repository?.Dispose();

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

            // Verify file is marked as processing in repository
            var metadata = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Processing, metadata.Status);
            Assert.NotNull(metadata.ProcessingStartTime);
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_ThrowsWhenNoFilesAvailable()
        {
            // Act & Assert
            await Assert.ThrowsAsync<NoFilesAvailableException>(() =>
                _scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None));
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

            // Act & Assert
            await Assert.ThrowsAsync<NoFilesAvailableException>(() =>
                _scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None));
        }

        [Fact]
        public async Task GetNextBatchForProcessingAsync_ReturnsBatch()
        {
            // Arrange
            for (int i = 0; i < 5; i++)
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

            // Should return oldest files first
            Assert.Equal("file-004", list[0].FileKey);
            Assert.Equal("file-003", list[1].FileKey);
            Assert.Equal("file-002", list[2].FileKey);

            // All should be marked as processing
            foreach (var location in list)
            {
                Assert.Equal(FileProcessingStatus.Processing, location.Status);
            }
        }

        [Fact]
        public async Task MarkAsCompletedAndDeleteAsync_RemovesMetadata()
        {
            // Arrange
            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                PhysicalPath = "/test/file-001.txt",
                Status = FileProcessingStatus.Processing
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            await _scheduler.MarkAsCompletedAsync("file-001", CancellationToken.None);

            // Assert
            var metadata = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.Null(metadata);
        }

        [Fact]
        public async Task MarkAsCompletedAndDeleteAsync_DeletesPhysicalFile()
        {
            // Arrange
            var physicalPath = Path.Combine(_metadataDir, "file-001.txt");

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
                Status = FileProcessingStatus.Processing
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            await _scheduler.MarkAsCompletedAsync("file-001", CancellationToken.None);

            // Assert
            Assert.False(_fileSystem.File.Exists(physicalPath));
        }

        [Fact]
        public async Task MarkAsFailedAsync_ReturnsFileToPending()
        {
            // Arrange
            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                RetryCount = 0
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act
            await _scheduler.MarkAsFailedAsync("file-001", "Test error", CancellationToken.None);

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
            var scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object, retryPolicy);

            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                RetryCount = 2 // Already failed twice
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act - third failure should mark as permanently failed
            await scheduler.MarkAsFailedAsync("file-001", "Final error", CancellationToken.None);

            // Assert
            var metadata = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.PermanentlyFailed, metadata.Status);
            Assert.Equal(3, metadata.RetryCount);
            Assert.Null(metadata.AvailableForProcessingAt);
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

            var scheduler = new FileScheduler(_repository, _fileSystem, _logger.Object, retryPolicy);

            var file = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                Status = FileProcessingStatus.Processing,
                RetryCount = 0
            };

            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            // Act - first failure
            await scheduler.MarkAsFailedAsync("file-001", "Error 1", CancellationToken.None);

            // Assert - delay should be 5 seconds (5 * 2^0)
            var metadata1 = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata1);
            var delay1 = metadata1.AvailableForProcessingAt!.Value - DateTime.UtcNow;
            Assert.InRange(delay1.TotalSeconds, 4, 6);

            // Act - second failure
            metadata1.Status = FileProcessingStatus.Processing;
            await _repository.AddOrUpdateAsync(metadata1, CancellationToken.None);
            await scheduler.MarkAsFailedAsync("file-001", "Error 2", CancellationToken.None);

            // Assert - delay should be 10 seconds (5 * 2^1)
            var metadata2 = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.NotNull(metadata2);
            var delay2 = metadata2.AvailableForProcessingAt!.Value - DateTime.UtcNow;
            Assert.InRange(delay2.TotalSeconds, 9, 11);
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
            await _scheduler.ResetProcessingStatusAsync("file-001", CancellationToken.None);

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
            var status = await _scheduler.GetFileStatusAsync("file-001", CancellationToken.None);

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
                _scheduler.MarkAsProcessingAsync("file-001", CancellationToken.None));
        }

        [Fact]
        public async Task GetFileStatusAsync_ThrowsWhenFileNotFound()
        {
            // Act & Assert
            await Assert.ThrowsAsync<FileAlreadyProcessingException>(() =>
                _scheduler.GetFileStatusAsync("nonexistent", CancellationToken.None));
        }

        [Fact]
        public async Task GetNextFileForProcessingAsync_SkipsAndRemovesMissingPhysicalFiles()
        {
            // Arrange
            var file1 = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                PhysicalPath = "/test/missing-file.txt", // File doesn't exist
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10)
            };

            var physicalPath2 = Path.Combine(_metadataDir, "existing-file.txt");
            var file2 = new FileMetadata
            {
                FileKey = "file-002",
                TenantId = "tenant-001",
                PhysicalPath = physicalPath2,
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-5)
            };

            // Create physical file for file2
            _fileSystem.File.WriteAllText(physicalPath2, "test content");

            await _repository.AddOrUpdateAsync(file1, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file2, CancellationToken.None);

            // Act
            var location = await _scheduler.GetNextFileForProcessingAsync(_tenant.Object, CancellationToken.None);

            // Assert - should get file2, and file1 metadata should be removed
            Assert.NotNull(location);
            Assert.Equal("file-002", location.FileKey);

            var orphanedMetadata = await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None);
            Assert.Null(orphanedMetadata); // Should be removed
        }

        [Fact]
        public async Task GetNextBatchForProcessingAsync_SkipsAndRemovesMissingPhysicalFiles()
        {
            // Arrange
            var file1 = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                PhysicalPath = "/test/missing-file1.txt", // File doesn't exist
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10)
            };

            var physicalPath2 = Path.Combine(_metadataDir, "existing-file.txt");
            var file2 = new FileMetadata
            {
                FileKey = "file-002",
                TenantId = "tenant-001",
                PhysicalPath = physicalPath2,
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-5)
            };

            var file3 = new FileMetadata
            {
                FileKey = "file-003",
                TenantId = "tenant-001",
                PhysicalPath = "/test/missing-file3.txt", // File doesn't exist
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow.AddMinutes(-3)
            };

            // Only create physical file for file2
            _fileSystem.File.WriteAllText(physicalPath2, "test content");

            await _repository.AddOrUpdateAsync(file1, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file2, CancellationToken.None);
            await _repository.AddOrUpdateAsync(file3, CancellationToken.None);

            // Act
            var locations = await _scheduler.GetNextBatchForProcessingAsync(_tenant.Object, 3, CancellationToken.None);

            // Assert - should only get file2
            var list = locations.ToList();
            Assert.Single(list);
            Assert.Equal("file-002", list[0].FileKey);

            // Orphaned metadata should be removed
            Assert.Null(await _repository.GetAsync("tenant-001", "file-001", CancellationToken.None));
            Assert.Null(await _repository.GetAsync("tenant-001", "file-003", CancellationToken.None));
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
    }
}
