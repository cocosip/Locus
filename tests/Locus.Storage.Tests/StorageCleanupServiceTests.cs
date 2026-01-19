using System;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class StorageCleanupServiceTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly Mock<ILogger<StorageCleanupService>> _logger;
        private readonly StorageCleanupService _cleanupService;
        private readonly Mock<IStorageVolume> _volume;
        private readonly Mock<ITenantContext> _tenant;
        private readonly string _volumePath;
        private readonly string _metadataDir;
        private readonly string _quotaDir;

        public StorageCleanupServiceTests()
        {
            // Use real file system for tests (LiteDB requires real file system)
            _fileSystem = new System.IO.Abstractions.FileSystem();

            // Setup repositories with unique temporary directories
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _metadataDir = Path.Combine(Path.GetTempPath(), $"locus-test-cleanup-meta-{testId}");
            _quotaDir = Path.Combine(Path.GetTempPath(), $"locus-test-cleanup-quota-{testId}");
            _fileSystem.Directory.CreateDirectory(_metadataDir);
            _fileSystem.Directory.CreateDirectory(_quotaDir);

            var metadataRepoLogger = new Mock<ILogger<MetadataRepository>>();
            _metadataRepository = new MetadataRepository(_fileSystem, metadataRepoLogger.Object, _metadataDir);

            var quotaRepoLogger = new Mock<ILogger<DirectoryQuotaRepository>>();
            _quotaRepository = new DirectoryQuotaRepository(_fileSystem, quotaRepoLogger.Object, _quotaDir);

            // Setup cleanup service
            _logger = new Mock<ILogger<StorageCleanupService>>();
            _cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir);

            // Setup mock volume
            _volumePath = Path.Combine(Path.GetTempPath(), $"locus-test-cleanup-vol-{testId}");
            _fileSystem.Directory.CreateDirectory(_volumePath);

            _volume = new Mock<IStorageVolume>();
            _volume.Setup(v => v.VolumeId).Returns("vol-001");
            _volume.Setup(v => v.MountPath).Returns(_volumePath);
            _volume.Setup(v => v.DeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string path, CancellationToken ct) =>
                {
                    if (_fileSystem.File.Exists(path))
                        _fileSystem.File.Delete(path);
                    return Task.CompletedTask;
                });

            // Register volume with cleanup service (now using public method)
            _cleanupService.RegisterVolume(_volume.Object);

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
                if (_fileSystem.Directory.Exists(_metadataDir))
                    _fileSystem.Directory.Delete(_metadataDir, recursive: true);

                if (_fileSystem.Directory.Exists(_quotaDir))
                    _fileSystem.Directory.Delete(_quotaDir, recursive: true);

                if (_fileSystem.Directory.Exists(_volumePath))
                    _fileSystem.Directory.Delete(_volumePath, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }

        [Fact]
        public async Task CleanupEmptyDirectoriesAsync_RemovesEmptyDirectories()
        {
            // Arrange
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            var emptyDir1 = Path.Combine(tenantPath, "empty1");
            var emptyDir2 = Path.Combine(tenantPath, "empty2");
            var nonEmptyDir = Path.Combine(tenantPath, "nonempty");

            _fileSystem.Directory.CreateDirectory(emptyDir1);
            _fileSystem.Directory.CreateDirectory(emptyDir2);
            _fileSystem.Directory.CreateDirectory(nonEmptyDir);
            _fileSystem.File.WriteAllText(Path.Combine(nonEmptyDir, "file.txt"), "content");

            // Act
            await _cleanupService.CleanupEmptyDirectoriesAsync("tenant-001", default);

            // Assert
            Assert.False(_fileSystem.Directory.Exists(emptyDir1));
            Assert.False(_fileSystem.Directory.Exists(emptyDir2));
            Assert.True(_fileSystem.Directory.Exists(nonEmptyDir));

            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            Assert.True(stats.EmptyDirectoriesRemoved >= 2);
        }

        [Fact]
        public async Task CleanupEmptyDirectoriesAsync_WithTenantContext_RemovesEmptyDirectories()
        {
            // Arrange
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            var emptyDir = Path.Combine(tenantPath, "empty");
            _fileSystem.Directory.CreateDirectory(emptyDir);

            // Act
            await _cleanupService.CleanupEmptyDirectoriesAsync(_tenant.Object, default);

            // Assert
            Assert.False(_fileSystem.Directory.Exists(emptyDir));
        }

        [Fact]
        public async Task CleanupAllEmptyDirectoriesAsync_RemovesAllEmptyDirectories()
        {
            // Arrange
            var tenant1Path = Path.Combine(_volumePath, "tenant-001");
            var tenant2Path = Path.Combine(_volumePath, "tenant-002");
            var emptyDir1 = Path.Combine(tenant1Path, "empty");
            var emptyDir2 = Path.Combine(tenant2Path, "empty");

            _fileSystem.Directory.CreateDirectory(emptyDir1);
            _fileSystem.Directory.CreateDirectory(emptyDir2);

            // Act
            await _cleanupService.CleanupAllEmptyDirectoriesAsync(default);

            // Assert
            Assert.False(_fileSystem.Directory.Exists(emptyDir1));
            Assert.False(_fileSystem.Directory.Exists(emptyDir2));
        }

        [Fact]
        public async Task CleanupTimedOutProcessingFilesAsync_ResetsTimedOutFiles()
        {
            // Arrange
            var timedOutMetadata = new FileMetadata
            {
                FileKey = "file-001",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumePath, "file-001.dat"),
                DirectoryPath = "/",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = DateTime.UtcNow.AddMinutes(-60), // 60 minutes ago
                CreatedAt = DateTime.UtcNow.AddHours(-1)
            };

            await _metadataRepository.AddOrUpdateAsync(timedOutMetadata, default);

            // Act
            await _cleanupService.CleanupTimedOutProcessingFilesAsync(TimeSpan.FromMinutes(30), default);

            // Assert
            var updatedMetadata = await _metadataRepository.GetAsync("tenant-001", "file-001", default);
            Assert.NotNull(updatedMetadata);
            Assert.Equal(FileProcessingStatus.Pending, updatedMetadata.Status);
            Assert.Null(updatedMetadata.ProcessingStartTime);

            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(1, stats.TimedOutFilesReset);
        }

        [Fact]
        public async Task CleanupTimedOutProcessingFilesAsync_DoesNotResetRecentFiles()
        {
            // Arrange
            var recentMetadata = new FileMetadata
            {
                FileKey = "file-002",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumePath, "file-002.dat"),
                DirectoryPath = "/",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = DateTime.UtcNow.AddMinutes(-10), // 10 minutes ago
                CreatedAt = DateTime.UtcNow.AddMinutes(-10)
            };

            await _metadataRepository.AddOrUpdateAsync(recentMetadata, default);

            // Act
            await _cleanupService.CleanupTimedOutProcessingFilesAsync(TimeSpan.FromMinutes(30), default);

            // Assert
            var updatedMetadata = await _metadataRepository.GetAsync("tenant-001", "file-002", default);
            Assert.NotNull(updatedMetadata);
            Assert.Equal(FileProcessingStatus.Processing, updatedMetadata.Status); // Still processing
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_DeletesOldFailedFiles()
        {
            // Arrange
            var physicalPath = Path.Combine(_volumePath, "failed-file.dat");
            _fileSystem.File.WriteAllText(physicalPath, "failed content");

            var failedMetadata = new FileMetadata
            {
                FileKey = "file-003",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/failed",
                FileSize = 14,
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-10), // 10 days ago
                CreatedAt = DateTime.UtcNow.AddDays(-10)
            };

            await _metadataRepository.AddOrUpdateAsync(failedMetadata, default);
            await _quotaRepository.GetOrCreateAsync("tenant-001", "/failed", default);
            await _quotaRepository.TryIncrementAsync("tenant-001", "/failed", default);

            // Act
            await _cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), default);

            // Assert
            var metadata = await _metadataRepository.GetAsync("tenant-001", "file-003", default);
            Assert.Null(metadata); // Metadata removed

            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(1, stats.PermanentlyFailedFilesRemoved);
            Assert.True(stats.SpaceFreed > 0);
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_DoesNotDeleteRecentFailedFiles()
        {
            // Arrange
            var failedMetadata = new FileMetadata
            {
                FileKey = "file-004",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumePath, "recent-failed.dat"),
                DirectoryPath = "/",
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-2), // 2 days ago
                CreatedAt = DateTime.UtcNow.AddDays(-2)
            };

            await _metadataRepository.AddOrUpdateAsync(failedMetadata, default);

            // Act
            await _cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), default);

            // Assert
            var metadata = await _metadataRepository.GetAsync("tenant-001", "file-004", default);
            Assert.NotNull(metadata); // Still exists
        }

        [Fact]
        public async Task CleanupCompletedFileRecordsAsync_RemovesOldCompletedRecords()
        {
            // Arrange
            var completedMetadata = new FileMetadata
            {
                FileKey = "file-005",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumePath, "completed.dat"),
                DirectoryPath = "/",
                Status = FileProcessingStatus.Completed,
                CreatedAt = DateTime.UtcNow.AddDays(-40) // 40 days ago
            };

            await _metadataRepository.AddOrUpdateAsync(completedMetadata, default);

            // Act
            await _cleanupService.CleanupCompletedFileRecordsAsync(TimeSpan.FromDays(30), default);

            // Assert
            var metadata = await _metadataRepository.GetAsync("tenant-001", "file-005", default);
            Assert.Null(metadata); // Record removed

            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(1, stats.CompletedRecordsRemoved);
        }

        [Fact]
        public async Task CleanupOrphanedFilesAsync_DeletesOrphanedFiles()
        {
            // Arrange
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            var orphanedFile = Path.Combine(tenantPath, "orphaned.dat");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            _fileSystem.File.WriteAllText(orphanedFile, "orphaned content");

            // No metadata for this file - it's orphaned

            // Act
            await _cleanupService.CleanupOrphanedFilesAsync(_tenant.Object, default);

            // Assert
            Assert.False(_fileSystem.File.Exists(orphanedFile));

            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(1, stats.OrphanedFilesRemoved);
            Assert.True(stats.SpaceFreed > 0);
        }

        [Fact]
        public async Task CleanupOrphanedFilesAsync_DoesNotDeleteFilesWithMetadata()
        {
            // Arrange
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            var validFile = Path.Combine(tenantPath, "valid.dat");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            _fileSystem.File.WriteAllText(validFile, "valid content");

            // Add metadata for this file
            var metadata = new FileMetadata
            {
                FileKey = "file-006",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = validFile,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            };
            await _metadataRepository.AddOrUpdateAsync(metadata, default);

            // Act
            await _cleanupService.CleanupOrphanedFilesAsync(_tenant.Object, default);

            // Assert
            Assert.True(_fileSystem.File.Exists(validFile)); // File still exists
        }

        [Fact]
        public async Task GetCleanupStatisticsAsync_ReturnsAccumulatedStats()
        {
            // Arrange
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            var emptyDir = Path.Combine(tenantPath, "empty");
            _fileSystem.Directory.CreateDirectory(emptyDir);

            // Act
            await _cleanupService.CleanupEmptyDirectoriesAsync("tenant-001", default);
            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);

            // Assert
            Assert.True(stats.EmptyDirectoriesRemoved > 0);
        }
    }
}
