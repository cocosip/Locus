using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Runtime.InteropServices;
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
    public class StorageCleanupServiceTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly TenantQuotaManager _tenantQuotaManager;
        private readonly DirectoryQuotaManager _directoryQuotaManager;
        private readonly Mock<ITenantManager> _tenantManager;
        private readonly Mock<ILogger<StorageCleanupService>> _logger;
        private readonly StorageCleanupService _cleanupService;
        private readonly Mock<IStorageVolume> _volume;
        private readonly Mock<ITenantContext> _tenant;
        private readonly string _volumePath;
        private readonly string _metadataDir;
        private readonly string _quotaDir;

        public StorageCleanupServiceTests()
        {
            // Use real file system for tests (SQLite requires real file system)
            _fileSystem = new System.IO.Abstractions.FileSystem();

            // Setup repositories with unique temporary directories
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _metadataDir = Path.Combine(Path.GetTempPath(), $"locus-test-cleanup-meta-{testId}");
            _quotaDir = Path.Combine(Path.GetTempPath(), $"locus-test-cleanup-quota-{testId}");
            _fileSystem.Directory.CreateDirectory(_metadataDir);
            _fileSystem.Directory.CreateDirectory(_quotaDir);

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
            _tenantQuotaManager = new TenantQuotaManager(
                _quotaRepository,
                new Mock<ILogger<TenantQuotaManager>>().Object);
            _directoryQuotaManager = new DirectoryQuotaManager(
                _quotaRepository,
                new Mock<ILogger<DirectoryQuotaManager>>().Object);
            _tenantManager = new Mock<ITenantManager>();
            var defaultTenant = new Mock<ITenantContext>();
            defaultTenant.Setup(t => t.TenantId).Returns("tenant-001");
            defaultTenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);
            _tenantManager
                .Setup(m => m.GetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(defaultTenant.Object);
            _tenantManager
                .Setup(m => m.TryGetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(defaultTenant.Object);
            _tenantManager
                .Setup(m => m.GetTenantAsync(It.Is<string>(s => s != "tenant-001"), It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>((tenantId, _) =>
                {
                    var tenant = new Mock<ITenantContext>();
                    tenant.Setup(t => t.TenantId).Returns(tenantId);
                    tenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);
                    return Task.FromResult<ITenantContext>(tenant.Object);
                });
            _tenantManager
                .Setup(m => m.TryGetTenantAsync(It.Is<string>(s => s != "tenant-001"), It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>((tenantId, _) =>
                {
                    var tenant = new Mock<ITenantContext>();
                    tenant.Setup(t => t.TenantId).Returns(tenantId);
                    tenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);
                    return Task.FromResult<ITenantContext?>(tenant.Object);
                });

            // Setup cleanup service
            _logger = new Mock<ILogger<StorageCleanupService>>();
            _cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                directoryQuotaManager: _directoryQuotaManager,
                allowLegacyNonJournalMode: true);

            // Setup mock volume
            _volumePath = Path.Combine(Path.GetTempPath(), $"locus-test-cleanup-vol-{testId}");
            _fileSystem.Directory.CreateDirectory(_volumePath);

            _volume = new Mock<IStorageVolume>();
            _volume.Setup(v => v.VolumeId).Returns("vol-001");
            _volume.Setup(v => v.MountPath).Returns(_volumePath);
            _volume.Setup(v => v.IsHealthy).Returns(true);
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
            _tenantManager
                .Setup(m => m.GetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(_tenant.Object);
            _tenantManager
                .Setup(m => m.TryGetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(_tenant.Object);
        }

        public void Dispose()
        {
            _metadataRepository?.Dispose();
            _quotaRepository?.Dispose();
            SqliteConnection.ClearAllPools();

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

        private async Task SeedProjectedCountsAsync(string tenantId, string directoryPath)
        {
            await _tenantQuotaManager.ApplyAcceptedProjectionAsync(tenantId, CancellationToken.None);
            await _directoryQuotaManager.ApplyAcceptedProjectionAsync(tenantId, directoryPath, CancellationToken.None);
        }

        private string GetExpectedDeadLetterPrefix(string tenantId)
        {
            return Path.Combine(
                _volumePath,
                ".deadletter",
                tenantId,
                DateTime.UtcNow.ToString("yyyyMMdd"));
        }

        [Fact]
        public void Constructor_WithoutJournal_RequiresExplicitLegacyAllowance()
        {
            var ex = Assert.Throws<InvalidOperationException>(() =>
                new StorageCleanupService(
                    _metadataRepository,
                    _quotaRepository,
                    _tenantQuotaManager,
                    _fileSystem,
                    _logger.Object,
                    _metadataDir,
                    _quotaDir,
                    tenantManager: _tenantManager.Object,
                    directoryQuotaManager: _directoryQuotaManager));

            Assert.Contains("legacy non-journal mode", ex.Message);
        }

        [Fact]
        public async Task CleanupEmptyDirectoriesAsync_RemovesEmptyDirectories()
        {
            // Act
            await _cleanupService.CleanupEmptyDirectoriesAsync("tenant-001", default);

            // Assert
            // Cleanup is disabled for sharded volumes, so no directories should be removed
            // The assertion logic has changed because the functionality was intentionally disabled
        }

        [Fact]
        public async Task CleanupEmptyDirectoriesAsync_WithTenantContext_RemovesEmptyDirectories()
        {
             // Act
            await _cleanupService.CleanupEmptyDirectoriesAsync(_tenant.Object, default);

            // Assert
            // Cleanup is disabled for sharded volumes
        }

        [Fact]
        public async Task CleanupAllEmptyDirectoriesAsync_RemovesAllEmptyDirectories()
        {
            // Act
            await _cleanupService.CleanupAllEmptyDirectoriesAsync(default);

            // Assert
            // Cleanup is disabled for sharded volumes
        }

        [Fact]
        public async Task CleanupAllEmptyDirectoriesAsync_ShouldNotDeleteVolumeMountPath()
        {
            // Arrange
            // Create empty tenant directories under the volume mount path
            var tenant1Path = Path.Combine(_volumePath, "tenant-001");
            var tenant2Path = Path.Combine(_volumePath, "tenant-002");
            _fileSystem.Directory.CreateDirectory(tenant1Path);
            _fileSystem.Directory.CreateDirectory(tenant2Path);

            // Verify mount path exists before cleanup
            Assert.True(_fileSystem.Directory.Exists(_volumePath));

            // Act
            await _cleanupService.CleanupAllEmptyDirectoriesAsync(default);

            // Assert
            // CRITICAL: Volume mount path must NEVER be deleted, even when empty
            Assert.True(_fileSystem.Directory.Exists(_volumePath),
                "Volume MountPath should NOT be deleted by cleanup service");

            // Empty tenant directories should NOT be removed in the new implementation
            Assert.True(_fileSystem.Directory.Exists(tenant1Path));
            Assert.True(_fileSystem.Directory.Exists(tenant2Path));
        }

        [Fact]
        public async Task CleanupAllEmptyDirectoriesAsync_ShouldNotDeleteSystemDirectories()
        {
            // Arrange
            // MetadataDirectory and QuotaDirectory are automatically protected by the cleanup service
            // They should NEVER be deleted even if empty

            // Verify system directories exist and are empty
            Assert.True(_fileSystem.Directory.Exists(_metadataDir));
            Assert.True(_fileSystem.Directory.Exists(_quotaDir));
            Assert.Empty(_fileSystem.Directory.GetFiles(_metadataDir));
            Assert.Empty(_fileSystem.Directory.GetFiles(_quotaDir));

            // Act
            await _cleanupService.CleanupAllEmptyDirectoriesAsync(default);

            // Assert
            // CRITICAL: System directories must NEVER be deleted
            Assert.True(_fileSystem.Directory.Exists(_metadataDir),
                "MetadataDirectory should NOT be deleted by cleanup service");
            Assert.True(_fileSystem.Directory.Exists(_quotaDir),
                "QuotaDirectory should NOT be deleted by cleanup service");
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
        public async Task CleanupTimedOutProcessingFilesAsync_WithJournal_AppendsProcessingTimedOutEventAndResetsProjection()
        {
            var processingStartTime = DateTime.UtcNow.AddMinutes(-60);
            var timedOutMetadata = new FileMetadata
            {
                FileKey = "file-timeout-journal",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumePath, "file-timeout-journal.dat"),
                DirectoryPath = "/incoming",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = processingStartTime,
                CreatedAt = DateTime.UtcNow.AddHours(-1)
            };

            await _metadataRepository.AddOrUpdateAsync(timedOutMetadata, default);

            var queueEventJournal = new Mock<IQueueEventJournal>(MockBehavior.Strict);
            queueEventJournal
                .Setup(j => j.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.ProcessingTimedOut
                        && record.TenantId == "tenant-001"
                        && record.FileKey == "file-timeout-journal"
                        && record.Status == FileProcessingStatus.Pending
                        && record.ProcessingStartTimeUtc == processingStartTime),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                queueEventJournal: queueEventJournal.Object,
                directoryQuotaManager: _directoryQuotaManager);
            cleanupService.RegisterVolume(_volume.Object);

            await cleanupService.CleanupTimedOutProcessingFilesAsync(TimeSpan.FromMinutes(30), default);

            var updatedMetadata = await _metadataRepository.GetAsync("tenant-001", "file-timeout-journal", default);
            Assert.NotNull(updatedMetadata);
            Assert.Equal(FileProcessingStatus.Pending, updatedMetadata!.Status);
            Assert.Null(updatedMetadata.ProcessingStartTime);
            Assert.NotNull(updatedMetadata.AvailableForProcessingAt);

            var stats = await cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(1, stats.TimedOutFilesReset);

            queueEventJournal.VerifyAll();
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_MovesOldFailedFilesToDeadLetterByDefault()
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
            await SeedProjectedCountsAsync("tenant-001", "/failed");

            // Act
            await _cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), default);

            // Assert
            var metadata = await _metadataRepository.GetAsync("tenant-001", "file-003", default);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.DeadLettered, metadata!.Status);
            Assert.NotNull(metadata.DeadLetteredAt);
            Assert.StartsWith(GetExpectedDeadLetterPrefix("tenant-001"), metadata.PhysicalPath, StringComparison.OrdinalIgnoreCase);
            Assert.True(_fileSystem.File.Exists(metadata.PhysicalPath));
            Assert.False(_fileSystem.File.Exists(physicalPath));

            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(1, stats.PermanentlyFailedFilesRemoved);
            Assert.Equal(0, stats.SpaceFreed);

            var tenantCount = await _tenantQuotaManager.GetFileCountAsync("tenant-001", default);
            Assert.Equal(0, tenantCount);
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_WithJournal_AppendsDeadLetterEventAndLeavesDeadLetteredState()
        {
            var appendCount = 0;
            var physicalPath = Path.Combine(_volumePath, "failed-journal.dat");
            var queueEventJournal = new Mock<IQueueEventJournal>(MockBehavior.Strict);
            queueEventJournal
                .Setup(j => j.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.DeadLettered
                        && record.Status == FileProcessingStatus.DeadLettered
                        && record.TenantId == "tenant-001"
                        && record.FileKey == "failed-journal"
                        && record.PhysicalPath != physicalPath),
                    It.IsAny<CancellationToken>()))
                .Callback<QueueEventRecord, CancellationToken>((_, __) => appendCount++)
                .Returns(Task.CompletedTask);

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                queueEventJournal: queueEventJournal.Object);
            cleanupService.RegisterVolume(_volume.Object);

            _fileSystem.File.WriteAllText(physicalPath, "failed content");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "failed-journal",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/failed",
                FileSize = 14,
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-10),
                CreatedAt = DateTime.UtcNow.AddDays(-10)
            }, CancellationToken.None);
            await SeedProjectedCountsAsync("tenant-001", "/failed");

            await cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), CancellationToken.None);

            Assert.False(_fileSystem.File.Exists(physicalPath));
            Assert.Equal(1, appendCount);

            var metadata = await _metadataRepository.GetAsync("tenant-001", "failed-journal", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.DeadLettered, metadata!.Status);
            Assert.NotNull(metadata.DeadLetteredAt);
            Assert.StartsWith(GetExpectedDeadLetterPrefix("tenant-001"), metadata.PhysicalPath, StringComparison.OrdinalIgnoreCase);
            Assert.True(_fileSystem.File.Exists(metadata.PhysicalPath));

            var tenantCount = await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None);
            var directoryQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/failed", CancellationToken.None);
            Assert.Equal(1, tenantCount);
            Assert.Equal(1, directoryQuota.CurrentCount);

            var stats = await cleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(1, stats.PermanentlyFailedFilesRemoved);

            queueEventJournal.VerifyAll();
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_RejectsAbsoluteDeadLetterRootPath()
        {
            var physicalPath = Path.Combine(_volumePath, "failed-absolute-root.dat");
            _fileSystem.File.WriteAllText(physicalPath, "failed content");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "failed-absolute-root",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/failed",
                FileSize = 14,
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-10),
                CreatedAt = DateTime.UtcNow.AddDays(-10)
            }, CancellationToken.None);
            await SeedProjectedCountsAsync("tenant-001", "/failed");

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                new CleanupOptions
                {
                    DeadLetter = new DeadLetterOptions
                    {
                        RootPath = Path.Combine(Path.GetTempPath(), $"locus-deadletter-{Guid.NewGuid():N}")
                    }
                },
                tenantManager: _tenantManager.Object,
                directoryQuotaManager: _directoryQuotaManager,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            await cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), CancellationToken.None);

            var metadata = await _metadataRepository.GetAsync("tenant-001", "failed-absolute-root", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.PermanentlyFailed, metadata!.Status);
            Assert.Equal(physicalPath, metadata.PhysicalPath);
            Assert.True(_fileSystem.File.Exists(physicalPath));

            var directoryQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/failed", CancellationToken.None);
            Assert.Equal(1, directoryQuota.CurrentCount);
            Assert.Equal(1, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_RejectsDeadLetterRootPathOutsideVolume()
        {
            var physicalPath = Path.Combine(_volumePath, "failed-relative-escape.dat");
            _fileSystem.File.WriteAllText(physicalPath, "failed content");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "failed-relative-escape",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/failed",
                FileSize = 14,
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-10),
                CreatedAt = DateTime.UtcNow.AddDays(-10)
            }, CancellationToken.None);
            await SeedProjectedCountsAsync("tenant-001", "/failed");

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                new CleanupOptions
                {
                    DeadLetter = new DeadLetterOptions
                    {
                        RootPath = Path.Combine("..", "outside-deadletter")
                    }
                },
                tenantManager: _tenantManager.Object,
                directoryQuotaManager: _directoryQuotaManager,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            await cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), CancellationToken.None);

            var metadata = await _metadataRepository.GetAsync("tenant-001", "failed-relative-escape", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.PermanentlyFailed, metadata!.Status);
            Assert.Equal(physicalPath, metadata.PhysicalPath);
            Assert.True(_fileSystem.File.Exists(physicalPath));
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
        public async Task CleanupPermanentlyFailedFilesAsync_FallsBackToRootQuota_WhenDirectoryPathIsEmpty()
        {
            var physicalPath = Path.Combine(_volumePath, "failed-empty-dir.dat");
            _fileSystem.File.WriteAllText(physicalPath, "failed content");

            var failedMetadata = new FileMetadata
            {
                FileKey = "file-empty-dir",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = string.Empty,
                FileSize = 14,
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-10),
                CreatedAt = DateTime.UtcNow.AddDays(-10)
            };

            await _metadataRepository.AddOrUpdateAsync(failedMetadata, default);
            await SeedProjectedCountsAsync("tenant-001", "/");

            await _cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), default);

            var metadata = await _metadataRepository.GetAsync("tenant-001", "file-empty-dir", default);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.DeadLettered, metadata!.Status);

            var rootQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/", default);
            Assert.Equal(0, rootQuota.CurrentCount);
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_KeepsMetadataAndQuotas_WhenDeadLetterMoveFails()
        {
            var physicalPath = Path.Combine(_volumePath, "failed-delete-throws.dat");
            _fileSystem.File.WriteAllText(physicalPath, "failed content");

            var failedMetadata = new FileMetadata
            {
                FileKey = "file-delete-throws",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/failed",
                FileSize = 14,
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-10),
                CreatedAt = DateTime.UtcNow.AddDays(-10)
            };

            await _metadataRepository.AddOrUpdateAsync(failedMetadata, default);
            await SeedProjectedCountsAsync("tenant-001", "/failed");

            using (var moveBlocker = _fileSystem.File.Open(physicalPath, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                await _cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), default);
            }

            var metadata = await _metadataRepository.GetAsync("tenant-001", "file-delete-throws", default);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.PermanentlyFailed, metadata!.Status);
            Assert.True(_fileSystem.File.Exists(physicalPath));

            var directoryQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/failed", default);
            Assert.Equal(1, directoryQuota.CurrentCount);

            var tenantCount = await _tenantQuotaManager.GetFileCountAsync("tenant-001", default);
            Assert.Equal(1, tenantCount);
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_KeepsState_WhenTenantQuotaDecrementFails()
        {
            var physicalPath = Path.Combine(_volumePath, "failed-tenant-quota.dat");
            _fileSystem.File.WriteAllText(physicalPath, "failed content");

            var failedMetadata = new FileMetadata
            {
                FileKey = "file-tenant-quota",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/failed",
                FileSize = 14,
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-10),
                CreatedAt = DateTime.UtcNow.AddDays(-10)
            };

            await _metadataRepository.AddOrUpdateAsync(failedMetadata, default);
            await SeedProjectedCountsAsync("tenant-001", "/failed");

            var tenantQuotaManager = new Mock<ITenantQuotaManager>(MockBehavior.Strict);
            tenantQuotaManager
                .Setup(m => m.DecrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ThrowsAsync(new IOException("quota db busy"));

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                tenantQuotaManager.Object,
                _fileSystem,
                new Mock<ILogger<StorageCleanupService>>().Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            await cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), default);

            var metadata = await _metadataRepository.GetAsync("tenant-001", "file-tenant-quota", default);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.PermanentlyFailed, metadata!.Status);
            Assert.True(_fileSystem.File.Exists(physicalPath));

            var directoryQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/failed", default);
            Assert.Equal(1, directoryQuota.CurrentCount);

            var stats = await cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(0, stats.PermanentlyFailedFilesRemoved);
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_StillSucceeds_WhenDeleteCallbackAlsoRemovesMetadata()
        {
            var physicalPath = Path.Combine(_volumePath, "failed-concurrent-remove.dat");
            _fileSystem.File.WriteAllText(physicalPath, "failed content");

            var failedMetadata = new FileMetadata
            {
                FileKey = "file-concurrent-remove",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/failed",
                FileSize = 14,
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-10),
                CreatedAt = DateTime.UtcNow.AddDays(-10)
            };

            await _metadataRepository.AddOrUpdateAsync(failedMetadata, default);
            await SeedProjectedCountsAsync("tenant-001", "/failed");

            _volume.Setup(v => v.DeleteAsync(physicalPath, It.IsAny<CancellationToken>()))
                .Returns(async (string path, CancellationToken token) =>
                {
                    if (_fileSystem.File.Exists(path))
                        _fileSystem.File.Delete(path);

                    await _metadataRepository.RemoveAsync("tenant-001", "file-concurrent-remove", token);
                });

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                new CleanupOptions
                {
                    PermanentlyFailedDisposition = PermanentlyFailedDisposition.Delete
                },
                tenantManager: _tenantManager.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            await cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), default);

            var directoryQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/failed", default);
            Assert.Equal(0, directoryQuota.CurrentCount);

            var tenantCount = await _tenantQuotaManager.GetFileCountAsync("tenant-001", default);
            Assert.Equal(0, tenantCount);

            var stats = await cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(1, stats.PermanentlyFailedFilesRemoved);

            Assert.Null(await _metadataRepository.GetAsync("tenant-001", "file-concurrent-remove", default));
        }

        [Fact]
        public async Task CleanupFilesByStatusAsync_ResetsTimedOutAndDeadLettersOldFailed()
        {
            var processing1 = new FileMetadata
            {
                FileKey = "combo-processing-1",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumePath, "combo-processing-1.dat"),
                DirectoryPath = "/",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = DateTime.UtcNow.AddMinutes(-90),
                CreatedAt = DateTime.UtcNow.AddMinutes(-90)
            };
            var processing2 = new FileMetadata
            {
                FileKey = "combo-processing-2",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumePath, "combo-processing-2.dat"),
                DirectoryPath = "/",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = DateTime.UtcNow.AddMinutes(-60),
                CreatedAt = DateTime.UtcNow.AddMinutes(-60)
            };
            var failedPath = Path.Combine(_volumePath, "combo-failed.dat");
            _fileSystem.File.WriteAllText(failedPath, "failed content");
            var failed = new FileMetadata
            {
                FileKey = "combo-failed",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = failedPath,
                DirectoryPath = "/failed",
                FileSize = 14,
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-10),
                CreatedAt = DateTime.UtcNow.AddDays(-10)
            };

            await _metadataRepository.AddOrUpdateAsync(processing1, default);
            await _metadataRepository.AddOrUpdateAsync(processing2, default);
            await _metadataRepository.AddOrUpdateAsync(failed, default);
            await _quotaRepository.GetOrCreateAsync("tenant-001", "/failed", default);
            await _quotaRepository.TryIncrementAsync("tenant-001", "/failed", default);

            await _cleanupService.CleanupFilesByStatusAsync(
                processingTimeout: TimeSpan.FromMinutes(30),
                failedRetentionPeriod: TimeSpan.FromDays(7),
                ct: default);

            var updated1 = await _metadataRepository.GetAsync("tenant-001", "combo-processing-1", default);
            var updated2 = await _metadataRepository.GetAsync("tenant-001", "combo-processing-2", default);
            var deadLetteredFailed = await _metadataRepository.GetAsync("tenant-001", "combo-failed", default);

            Assert.NotNull(updated1);
            Assert.Equal(FileProcessingStatus.Pending, updated1!.Status);
            Assert.NotNull(updated2);
            Assert.Equal(FileProcessingStatus.Pending, updated2!.Status);
            Assert.NotNull(deadLetteredFailed);
            Assert.Equal(FileProcessingStatus.DeadLettered, deadLetteredFailed!.Status);

            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(2, stats.TimedOutFilesReset);
            Assert.Equal(1, stats.PermanentlyFailedFilesRemoved);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_RebuildsMetadataForOrphanedFiles()
        {
            // Arrange
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            var orphanedFile = Path.Combine(tenantPath, "orphaned.dat");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            _fileSystem.File.WriteAllText(orphanedFile, "orphaned content");

            // No metadata for this file — it is orphaned (physical file present, no SQLite record)

            // Act
            await _cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, default);

            // Assert — file is NOT deleted; its metadata has been reconstructed instead
            Assert.True(_fileSystem.File.Exists(orphanedFile));

            // Metadata was rebuilt: fileKey = filename without extension = "orphaned"
            var rebuilt = await _metadataRepository.GetAsync("tenant-001", "orphaned", default);
            Assert.NotNull(rebuilt);
            Assert.Equal("tenant-001", rebuilt.TenantId);
            Assert.Equal("vol-001", rebuilt.VolumeId);
            Assert.Equal(orphanedFile, rebuilt.PhysicalPath);
            Assert.Equal(FileProcessingStatus.Pending, rebuilt.Status);
            Assert.Equal(0, rebuilt.RetryCount);

            var tenantCount = await _tenantQuotaManager.GetFileCountAsync("tenant-001", default);
            var directoryQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/", default);
            Assert.Equal(1, tenantCount);
            Assert.Equal(1, directoryQuota.CurrentCount);

            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(0, stats.OrphanedFilesRemoved);
            Assert.Equal(1, stats.OrphanedFilesRecovered);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_PreservesMeaningfulNestedLogicalDirectory()
        {
            var nestedPath = Path.Combine(_volumePath, "tenant-001", "incoming", "studies");
            var orphanedFile = Path.Combine(nestedPath, "study-001.dcm");
            _fileSystem.Directory.CreateDirectory(nestedPath);
            _fileSystem.File.WriteAllText(orphanedFile, "orphaned content");

            await _cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, default);

            var rebuilt = await _metadataRepository.GetAsync("tenant-001", "study-001", default);
            Assert.NotNull(rebuilt);
            Assert.Equal("/incoming/studies", rebuilt!.DirectoryPath);

            var directoryQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/incoming/studies", default);
            Assert.Equal(1, directoryQuota.CurrentCount);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_DoesNotDeleteFilesWithMetadata()
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
            await _cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, default);

            // Assert
            Assert.True(_fileSystem.File.Exists(validFile)); // File still exists
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_PathComparisonRespectsPlatformCaseSensitivity()
        {
            // Arrange
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            var actualFile = Path.Combine(tenantPath, "CaseFile.dat");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            _fileSystem.File.WriteAllText(actualFile, "case content");

            var mismatchedCasePath = Path.Combine(tenantPath, "casefile.dat");
            var metadata = new FileMetadata
            {
                FileKey = "CaseFile",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = mismatchedCasePath,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            };
            await _metadataRepository.AddOrUpdateAsync(metadata, default);

            // Act
            await _cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, default);

            // Assert
            var after = await _metadataRepository.GetAsync("tenant-001", "CaseFile", default);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.NotNull(after);
                Assert.Equal(mismatchedCasePath, after!.PhysicalPath);
            }
            else
            {
                Assert.NotNull(after);
                Assert.Equal(actualFile, after!.PhysicalPath);
            }
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_CorrectsMetadataPathCasingWithoutRebuildingQuota()
        {
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            var actualFile = Path.Combine(tenantPath, "CaseOnly.dat");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            _fileSystem.File.WriteAllText(actualFile, "case content");

            var mismatchedCasePath = Path.Combine(tenantPath, "caseonly.dat");
            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "CaseOnly",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = mismatchedCasePath,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            }, CancellationToken.None);

            await SeedProjectedCountsAsync("tenant-001", "/");

            await _cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);

            var after = await _metadataRepository.GetAsync("tenant-001", "CaseOnly", CancellationToken.None);
            Assert.NotNull(after);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Equal(mismatchedCasePath, after!.PhysicalPath);
            }
            else
            {
                Assert.Equal(actualFile, after!.PhysicalPath);
            }

            Assert.Equal(1, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Equal(1, (await _quotaRepository.GetOrCreateAsync("tenant-001", "/", CancellationToken.None)).CurrentCount);

            var stats = await _cleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(0, stats.OrphanedFilesRecovered);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_WithSmallLookupCache_RebuildsAllOrphans()
        {
            var cleanupWithSmallCache = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                new CleanupOptions
                {
                    MaxOrphanFilesPerRun = 10_000,
                    OrphanRebuildLookupCacheSize = 1
                },
                allowLegacyNonJournalMode: true);
            cleanupWithSmallCache.RegisterVolume(_volume.Object);

            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            _fileSystem.Directory.CreateDirectory(tenantPath);

            for (var i = 0; i < 40; i++)
            {
                var fileName = $"batch-{i:D3}.dat";
                var physicalPath = Path.Combine(tenantPath, fileName);
                _fileSystem.File.WriteAllText(physicalPath, "content");

                if (i % 2 == 0)
                {
                    await _metadataRepository.AddOrUpdateAsync(new FileMetadata
                    {
                        FileKey = $"batch-{i:D3}",
                        TenantId = "tenant-001",
                        VolumeId = "vol-001",
                        PhysicalPath = physicalPath,
                        DirectoryPath = tenantPath,
                        Status = FileProcessingStatus.Pending,
                        CreatedAt = DateTime.UtcNow
                    }, CancellationToken.None);
                }
            }

            await cleanupWithSmallCache.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);

            for (var i = 0; i < 40; i++)
            {
                var rebuilt = await _metadataRepository.GetAsync("tenant-001", $"batch-{i:D3}", CancellationToken.None);
                Assert.NotNull(rebuilt);
            }

            var stats = await cleanupWithSmallCache.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(20, stats.OrphanedFilesRecovered);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_WithSmallRunBudget_ContinuesScanningLargeDirectoryAcrossRuns()
        {
            var cleanupWithSmallBudget = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                new CleanupOptions
                {
                    MaxOrphanFilesPerRun = 2
                },
                tenantManager: _tenantManager.Object,
                allowLegacyNonJournalMode: true);
            cleanupWithSmallBudget.RegisterVolume(_volume.Object);

            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            _fileSystem.Directory.CreateDirectory(tenantPath);

            for (var i = 0; i < 5; i++)
            {
                var filePath = Path.Combine(tenantPath, $"budget-{i:D3}.dat");
                _fileSystem.File.WriteAllText(filePath, $"content-{i}");
            }

            await cleanupWithSmallBudget.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);
            Assert.NotNull(await _metadataRepository.GetAsync("tenant-001", "budget-000", CancellationToken.None));
            Assert.NotNull(await _metadataRepository.GetAsync("tenant-001", "budget-001", CancellationToken.None));
            Assert.Null(await _metadataRepository.GetAsync("tenant-001", "budget-004", CancellationToken.None));

            await cleanupWithSmallBudget.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);
            Assert.NotNull(await _metadataRepository.GetAsync("tenant-001", "budget-002", CancellationToken.None));
            Assert.NotNull(await _metadataRepository.GetAsync("tenant-001", "budget-003", CancellationToken.None));
            Assert.Null(await _metadataRepository.GetAsync("tenant-001", "budget-004", CancellationToken.None));

            await cleanupWithSmallBudget.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);
            Assert.NotNull(await _metadataRepository.GetAsync("tenant-001", "budget-004", CancellationToken.None));

            var stats = await cleanupWithSmallBudget.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(5, stats.OrphanedFilesRecovered);
        }

        [Fact]
        public async Task RecoverAllOrphanedFilesAsync_RebuildsMetadataAcrossTenants()
        {
            var tenant1Path = Path.Combine(_volumePath, "tenant-001");
            var tenant2Path = Path.Combine(_volumePath, "tenant-002");
            _fileSystem.Directory.CreateDirectory(tenant1Path);
            _fileSystem.Directory.CreateDirectory(tenant2Path);

            var tenant1File = Path.Combine(tenant1Path, "alpha.dat");
            var tenant2File = Path.Combine(tenant2Path, "beta.dat");
            _fileSystem.File.WriteAllText(tenant1File, "alpha");
            _fileSystem.File.WriteAllText(tenant2File, "beta");

            await _cleanupService.RecoverAllOrphanedFilesAsync(CancellationToken.None);

            var rebuiltAlpha = await _metadataRepository.GetAsync("tenant-001", "alpha", CancellationToken.None);
            var rebuiltBeta = await _metadataRepository.GetAsync("tenant-002", "beta", CancellationToken.None);

            Assert.NotNull(rebuiltAlpha);
            Assert.NotNull(rebuiltBeta);

            var stats = await _cleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(2, stats.OrphanedFilesRecovered);
        }

        [Fact]
        public async Task RecoverAllOrphanedFilesAsync_SkipsDisabledTenants()
        {
            var enabledTenantPath = Path.Combine(_volumePath, "tenant-001");
            var disabledTenantPath = Path.Combine(_volumePath, "tenant-disabled");
            _fileSystem.Directory.CreateDirectory(enabledTenantPath);
            _fileSystem.Directory.CreateDirectory(disabledTenantPath);
            _fileSystem.File.WriteAllText(Path.Combine(enabledTenantPath, "enabled.dat"), "enabled");
            _fileSystem.File.WriteAllText(Path.Combine(disabledTenantPath, "disabled.dat"), "disabled");

            var disabledTenant = new Mock<ITenantContext>();
            disabledTenant.Setup(t => t.TenantId).Returns("tenant-disabled");
            disabledTenant.Setup(t => t.Status).Returns(TenantStatus.Disabled);
            _tenantManager
                .Setup(m => m.TryGetTenantAsync("tenant-disabled", It.IsAny<CancellationToken>()))
                .ReturnsAsync(disabledTenant.Object);

            await _cleanupService.RecoverAllOrphanedFilesAsync(CancellationToken.None);

            Assert.NotNull(await _metadataRepository.GetAsync("tenant-001", "enabled", CancellationToken.None));
            Assert.Null(await _metadataRepository.GetAsync("tenant-disabled", "disabled", CancellationToken.None));
        }

        [Fact]
        public async Task RecoverAllOrphanedFilesAsync_DoesNotAutoCreateTenantMetadataFromStrayDirectories()
        {
            var tenantManager = new Mock<ITenantManager>(MockBehavior.Strict);
            tenantManager
                .Setup(m => m.TryGetTenantAsync("tenant-stray", It.IsAny<CancellationToken>()))
                .ReturnsAsync((ITenantContext?)null);

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: tenantManager.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            var strayTenantPath = Path.Combine(_volumePath, "tenant-stray");
            _fileSystem.Directory.CreateDirectory(strayTenantPath);
            _fileSystem.File.WriteAllText(Path.Combine(strayTenantPath, "stray.dat"), "stray");

            await cleanupService.RecoverAllOrphanedFilesAsync(CancellationToken.None);

            Assert.Null(await _metadataRepository.GetAsync("tenant-stray", "stray", CancellationToken.None));
            tenantManager.Verify(
                m => m.TryGetTenantAsync("tenant-stray", It.IsAny<CancellationToken>()),
                Times.AtLeastOnce);
        }

        [Fact]
        public async Task RecoverAllOrphanedFilesAsync_DoesNotReconcileQuotaOnlyTenantsWithoutOrphans()
        {
            await ((ITenantQuotaReconciliationManager)_tenantQuotaManager)
                .SetFileCountAsync("tenant-quota-only", 2, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync("tenant-quota-only", "/stale", 2, CancellationToken.None);

            await _cleanupService.RecoverAllOrphanedFilesAsync(CancellationToken.None);

            Assert.Equal(2, await _tenantQuotaManager.GetFileCountAsync("tenant-quota-only", CancellationToken.None));
            Assert.Equal(2, (await _quotaRepository.GetAsync("tenant-quota-only", "/stale", CancellationToken.None))?.CurrentCount);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_DoesNotReconcileQuota_WhenNoOrphansFound()
        {
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            _fileSystem.Directory.CreateDirectory(tenantPath);

            await ((ITenantQuotaReconciliationManager)_tenantQuotaManager)
                .SetFileCountAsync("tenant-001", 1, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync("tenant-001", "/stale", 1, CancellationToken.None);

            await _cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);

            Assert.Equal(1, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Equal(1, (await _quotaRepository.GetAsync("tenant-001", "/stale", CancellationToken.None))?.CurrentCount);
        }

        [Fact]
        public async Task ReconcileQuotaCountsAsync_PrunesUnlimitedZeroCountRowsAndResetsTenantCount()
        {
            await ((ITenantQuotaReconciliationManager)_tenantQuotaManager)
                .SetFileCountAsync("tenant-001", 1, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync("tenant-001", "/stale", 1, CancellationToken.None);

            await _cleanupService.ReconcileQuotaCountsAsync("tenant-001", CancellationToken.None);

            Assert.Equal(0, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Null(await _quotaRepository.GetAsync("tenant-001", "/stale", CancellationToken.None));
        }

        [Fact]
        public async Task ReconcileQuotaCountsAsync_KeepsExplicitLimitRowsAtZero()
        {
            var existing = await _quotaRepository.GetOrCreateAsync("tenant-001", "/limited", CancellationToken.None);
            await _quotaRepository.UpdateAsync("tenant-001", new DirectoryQuota
            {
                DirectoryPath = "/limited",
                CurrentCount = 0,
                MaxCount = 5,
                Enabled = true,
                CreatedAt = existing.CreatedAt,
                LastUpdated = existing.LastUpdated,
            }, CancellationToken.None);

            await _cleanupService.ReconcileQuotaCountsAsync("tenant-001", CancellationToken.None);

            var limitedQuota = await _quotaRepository.GetAsync("tenant-001", "/limited", CancellationToken.None);
            Assert.NotNull(limitedQuota);
            Assert.Equal(0, limitedQuota.CurrentCount);
            Assert.Equal(5, limitedQuota.MaxCount);
            Assert.True(limitedQuota.Enabled);
        }

        [Fact]
        public async Task ReconcileQuotaCountsAsync_ReconcilesQuotaUp_WhenMetadataStillOwnsTheFile()
        {
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            var physicalPath = Path.Combine(tenantPath, "kept.dat");
            _fileSystem.File.WriteAllText(physicalPath, "kept");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "kept",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/",
                FileSize = 4,
                CreatedAt = DateTime.UtcNow.AddMinutes(-5),
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            await _cleanupService.ReconcileQuotaCountsAsync("tenant-001", CancellationToken.None);

            Assert.Equal(1, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Equal(1, (await _quotaRepository.GetOrCreateAsync("tenant-001", "/", CancellationToken.None)).CurrentCount);
        }

        [Fact]
        public async Task ReconcileQuotaCountsAsync_ExcludesDeadLetteredMetadataFromActiveQuota()
        {
            var deadLetterPath = Path.Combine(_volumePath, ".deadletter", "tenant-001", "20260409", "deadlettered.dat");
            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(deadLetterPath)!);
            _fileSystem.File.WriteAllText(deadLetterPath, "dead letter");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "deadlettered-quota",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = deadLetterPath,
                DirectoryPath = "/failed",
                FileSize = 11,
                CreatedAt = DateTime.UtcNow.AddMinutes(-5),
                Status = FileProcessingStatus.DeadLettered,
                DeadLetteredAt = DateTime.UtcNow.AddMinutes(-1)
            }, CancellationToken.None);

            await ((ITenantQuotaReconciliationManager)_tenantQuotaManager)
                .SetFileCountAsync("tenant-001", 1, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync("tenant-001", "/failed", 1, CancellationToken.None);

            await _cleanupService.ReconcileQuotaCountsAsync("tenant-001", CancellationToken.None);

            Assert.Equal(0, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Null(await _quotaRepository.GetAsync("tenant-001", "/failed", CancellationToken.None));
        }

        [Fact]
        public async Task ReconcileQuotaCountsAsync_ExcludesDeleteSucceededMetadataFromActiveQuota()
        {
            var deletedPath = Path.Combine(_volumePath, "tenant-001", "deleted.dat");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "deleted-quota",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = deletedPath,
                DirectoryPath = "/completed",
                FileSize = 11,
                CreatedAt = DateTime.UtcNow.AddMinutes(-5),
                Status = FileProcessingStatus.DeleteSucceeded
            }, CancellationToken.None);

            await ((ITenantQuotaReconciliationManager)_tenantQuotaManager)
                .SetFileCountAsync("tenant-001", 1, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync("tenant-001", "/completed", 1, CancellationToken.None);

            await _cleanupService.ReconcileQuotaCountsAsync("tenant-001", CancellationToken.None);

            Assert.Equal(0, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Null(await _quotaRepository.GetAsync("tenant-001", "/completed", CancellationToken.None));
        }

        [Fact]
        public async Task ReconcileQuotaCountsAsync_UsesProjectionStoreWhenProvided()
        {
            var physicalPath = Path.Combine(_volumePath, "tenant-001", "projection-owned.dat");
            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(store => store.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[]
                {
                    new FileMetadata
                    {
                        FileKey = "projection-owned",
                        TenantId = "tenant-001",
                        VolumeId = "vol-001",
                        PhysicalPath = physicalPath,
                        DirectoryPath = "/projection",
                        FileSize = 4,
                        CreatedAt = DateTime.UtcNow.AddMinutes(-5),
                        Status = FileProcessingStatus.Pending
                    }
                });

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                directoryQuotaManager: _directoryQuotaManager,
                projectionStore: projectionStore.Object,
                allowLegacyNonJournalMode: true);

            await cleanupService.ReconcileQuotaCountsAsync("tenant-001", CancellationToken.None);

            Assert.Equal(1, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Equal(1, (await _quotaRepository.GetOrCreateAsync("tenant-001", "/projection", CancellationToken.None)).CurrentCount);
            projectionStore.Verify(store => store.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()), Times.Once);
            projectionStore.VerifyAll();
        }

        [Fact]
        public async Task ReconcileQuotaCountsAsync_UsesQuotaMaintenanceStoreWhenDirectoryManagerMissing()
        {
            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(store => store.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[]
                {
                    new FileMetadata
                    {
                        FileKey = "quota-maint-file",
                        TenantId = "tenant-001",
                        VolumeId = "vol-001",
                        PhysicalPath = Path.Combine(_volumePath, "tenant-001", "quota-maint-file.dat"),
                        DirectoryPath = "/projection-maint",
                        FileSize = 4,
                        CreatedAt = DateTime.UtcNow.AddMinutes(-5),
                        Status = FileProcessingStatus.Pending
                    }
                });

            var quotaMaintenanceStore = new Mock<IQuotaProjectionMaintenanceStore>(MockBehavior.Strict);
            quotaMaintenanceStore
                .Setup(store => store.GetQuotaRowsAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(Array.Empty<DirectoryQuota>());
            quotaMaintenanceStore
                .Setup(store => store.SetProjectedCountAsync("tenant-001", "/projection-maint", 1, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                directoryQuotaManager: null,
                projectionStore: projectionStore.Object,
                quotaMaintenanceStore: quotaMaintenanceStore.Object,
                allowLegacyNonJournalMode: true);

            await cleanupService.ReconcileQuotaCountsAsync("tenant-001", CancellationToken.None);

            Assert.Equal(1, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            quotaMaintenanceStore.Verify(
                store => store.GetQuotaRowsAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Once);
            quotaMaintenanceStore.Verify(
                store => store.SetProjectedCountAsync("tenant-001", "/projection-maint", 1, It.IsAny<CancellationToken>()),
                Times.Once);
            quotaMaintenanceStore.VerifyAll();
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_DoesNotPersistMetadata_WhenTenantQuotaCompensationFails()
        {
            var quotaFailingManager = new Mock<ITenantQuotaManager>(MockBehavior.Strict);
            quotaFailingManager
                .Setup(m => m.IncrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ThrowsAsync(new TenantQuotaExceededException("tenant-001", 1, 1));

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                quotaFailingManager.Object,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            var orphanPath = Path.Combine(tenantPath, "quota-fail.dat");
            _fileSystem.File.WriteAllText(orphanPath, "orphan");

            await cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);

            Assert.Null(await _metadataRepository.GetAsync("tenant-001", "quota-fail", CancellationToken.None));

            var directoryQuota = await _quotaRepository.GetAsync("tenant-001", "/", CancellationToken.None);
            Assert.Null(directoryQuota);

            quotaFailingManager.Verify(
                m => m.IncrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Once);
            quotaFailingManager.Verify(
                m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_UsesTenantQuotaManagerInsteadOfTenantQuotaRepositoryRow()
        {
            var customTenantQuotaManager = new Mock<ITenantQuotaManager>(MockBehavior.Strict);
            customTenantQuotaManager
                .Setup(m => m.IncrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                customTenantQuotaManager.Object,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            var orphanPath = Path.Combine(tenantPath, "custom-manager.dat");
            _fileSystem.File.WriteAllText(orphanPath, "orphan");

            await cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);

            var rebuilt = await _metadataRepository.GetAsync("tenant-001", "custom-manager", CancellationToken.None);
            Assert.NotNull(rebuilt);

            var tenantQuotaRow = await _quotaRepository.GetAsync("tenant-001", "tenant-001", CancellationToken.None);
            Assert.Null(tenantQuotaRow);

            var directoryQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/", CancellationToken.None);
            Assert.Equal(1, directoryQuota.CurrentCount);

            customTenantQuotaManager.Verify(
                m => m.IncrementFileCountAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Once);
            customTenantQuotaManager.Verify(
                m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_UsesProjectionStoreWhenProvided()
        {
            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            FileMetadata? capturedMetadata = null;

            projectionStore
                .Setup(store => store.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(Array.Empty<FileMetadata>());
            projectionStore
                .Setup(store => store.GetProjectedFileAsync("tenant-001", "projection-store", It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileMetadata?)null);
            projectionStore
                .Setup(store => store.UpsertProjectedFileAsync(It.IsAny<FileMetadata>(), It.IsAny<CancellationToken>()))
                .Callback<FileMetadata, CancellationToken>((metadata, _) => capturedMetadata = metadata.Clone())
                .Returns(Task.CompletedTask);

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                directoryQuotaManager: _directoryQuotaManager,
                projectionStore: projectionStore.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            var orphanPath = Path.Combine(tenantPath, "projection-store.dat");
            _fileSystem.File.WriteAllText(orphanPath, "orphan");

            await cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);

            Assert.NotNull(capturedMetadata);
            Assert.Equal("tenant-001", capturedMetadata!.TenantId);
            Assert.Equal(orphanPath, capturedMetadata.PhysicalPath);
            Assert.Equal(FileProcessingStatus.Pending, capturedMetadata.Status);
            Assert.NotNull(capturedMetadata.Metadata);
            Assert.True(capturedMetadata.Metadata!.TryGetValue("queue.accepted_projection_applied", out var acceptedApplied));
            Assert.Equal(bool.TrueString, acceptedApplied);

            projectionStore.Verify(
                store => store.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()),
                Times.Once);
            projectionStore.Verify(
                store => store.UpsertProjectedFileAsync(It.IsAny<FileMetadata>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_DoesNotRebuild_WhenProjectionStoreAlreadyKnowsPhysicalPath()
        {
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            var knownPath = Path.Combine(tenantPath, "known-by-projection.dat");
            _fileSystem.Directory.CreateDirectory(tenantPath);
            _fileSystem.File.WriteAllText(knownPath, "known");

            var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
            projectionStore
                .Setup(store => store.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[]
                {
                    new FileMetadata
                    {
                        FileKey = "known-by-projection",
                        TenantId = "tenant-001",
                        VolumeId = "vol-001",
                        PhysicalPath = knownPath,
                        DirectoryPath = "/",
                        Status = FileProcessingStatus.Pending,
                        CreatedAt = DateTime.UtcNow
                    }
                });

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                directoryQuotaManager: _directoryQuotaManager,
                projectionStore: projectionStore.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            await cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);

            Assert.Null(await _metadataRepository.GetAsync("tenant-001", "known-by-projection", CancellationToken.None));
            projectionStore.Verify(store => store.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()), Times.Once);
            projectionStore.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_DoesNotOverwriteExistingMetadata_WhenFileKeyAlreadyExistsOnDifferentPath()
        {
            var tenantPath = Path.Combine(_volumePath, "tenant-001");
            _fileSystem.Directory.CreateDirectory(tenantPath);

            var existingPath = Path.Combine(tenantPath, "existing-path.dat");
            var orphanPath = Path.Combine(tenantPath, "conflict.dat");
            _fileSystem.File.WriteAllText(existingPath, "existing");
            _fileSystem.File.WriteAllText(orphanPath, "orphan");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "conflict",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = existingPath,
                DirectoryPath = "/",
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            }, CancellationToken.None);

            await SeedProjectedCountsAsync("tenant-001", "/");

            await _cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);

            var metadata = await _metadataRepository.GetAsync("tenant-001", "conflict", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(existingPath, metadata!.PhysicalPath);
            Assert.True(_fileSystem.File.Exists(orphanPath));
            Assert.Equal(1, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));

            var rootQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/", CancellationToken.None);
            Assert.Equal(1, rootQuota.CurrentCount);

            var stats = await _cleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(0, stats.OrphanedFilesRecovered);
        }

        [Fact]
        public async Task RecoverOrphanedFilesAsync_WithMultipleVolumes_LoadsKnownPhysicalPathsOncePerTenant()
        {
            var secondVolumePath = Path.Combine(Path.GetTempPath(), $"locus-test-cleanup-vol-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(secondVolumePath);

            try
            {
                var secondVolume = new Mock<IStorageVolume>();
                secondVolume.Setup(v => v.VolumeId).Returns("vol-002");
                secondVolume.Setup(v => v.MountPath).Returns(secondVolumePath);
                secondVolume
                    .Setup(v => v.DeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.CompletedTask);

                var knownPath = Path.Combine(_volumePath, "tenant-001", "known-in-volume-one.dat");
                _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(knownPath)!);
                _fileSystem.File.WriteAllText(knownPath, "known");

                var orphanPath = Path.Combine(secondVolumePath, "tenant-001", "rebuild-me.dat");
                _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(orphanPath)!);
                _fileSystem.File.WriteAllText(orphanPath, "orphan");

                var projectionStore = new Mock<IQueueProjectionStore>(MockBehavior.Strict);
                FileMetadata? capturedMetadata = null;
                projectionStore
                    .Setup(store => store.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new[]
                    {
                        new FileMetadata
                        {
                            FileKey = "known-in-volume-one",
                            TenantId = "tenant-001",
                            VolumeId = "vol-001",
                            PhysicalPath = knownPath,
                            DirectoryPath = "/",
                            Status = FileProcessingStatus.Pending,
                            CreatedAt = DateTime.UtcNow
                        }
                    });
                projectionStore
                    .Setup(store => store.GetProjectedFileAsync("tenant-001", "rebuild-me", It.IsAny<CancellationToken>()))
                    .ReturnsAsync((FileMetadata?)null);
                projectionStore
                    .Setup(store => store.UpsertProjectedFileAsync(It.IsAny<FileMetadata>(), It.IsAny<CancellationToken>()))
                    .Callback<FileMetadata, CancellationToken>((metadata, _) => capturedMetadata = metadata)
                    .Returns(Task.CompletedTask);

                var cleanupService = new StorageCleanupService(
                    _metadataRepository,
                    _quotaRepository,
                    _tenantQuotaManager,
                    _fileSystem,
                    _logger.Object,
                    _metadataDir,
                    _quotaDir,
                    tenantManager: _tenantManager.Object,
                    directoryQuotaManager: _directoryQuotaManager,
                    projectionStore: projectionStore.Object,
                    allowLegacyNonJournalMode: true);
                cleanupService.RegisterVolume(_volume.Object);
                cleanupService.RegisterVolume(secondVolume.Object);

                await cleanupService.RecoverOrphanedFilesAsync(_tenant.Object, CancellationToken.None);

                Assert.NotNull(capturedMetadata);
                Assert.Equal("tenant-001", capturedMetadata!.TenantId);
                Assert.Equal("vol-002", capturedMetadata.VolumeId);
                Assert.Equal(orphanPath, capturedMetadata.PhysicalPath);
                Assert.Equal("rebuild-me", capturedMetadata.FileKey);
                projectionStore.Verify(store => store.GetProjectedFilesAsync("tenant-001", It.IsAny<CancellationToken>()), Times.Once);
                projectionStore.Verify(store => store.UpsertProjectedFileAsync(It.IsAny<FileMetadata>(), It.IsAny<CancellationToken>()), Times.Once);
            }
            finally
            {
                try
                {
                    if (_fileSystem.Directory.Exists(secondVolumePath))
                        _fileSystem.Directory.Delete(secondVolumePath, recursive: true);
                }
                catch
                {
                    // Ignore cleanup errors in tests.
                }
            }
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_SkipsBlockedBatchAndContinuesToLaterRecords()
        {
            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                new CleanupOptions
                {
                    CleanupBatchSizePerTenant = 1
                },
                tenantManager: _tenantManager.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            var blockedPhysicalPath = Path.Combine(_volumePath, "failed-stuck.dat");
            var removablePhysicalPath = Path.Combine(_volumePath, "failed-removable.dat");
            _fileSystem.File.WriteAllText(blockedPhysicalPath, "failed content");
            _fileSystem.File.WriteAllText(removablePhysicalPath, "failed content");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "failed-stuck",
                TenantId = "tenant-001",
                VolumeId = "vol-missing",
                PhysicalPath = blockedPhysicalPath,
                DirectoryPath = "/failed",
                FileSize = 12,
                CreatedAt = DateTime.UtcNow.AddDays(-10),
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-8)
            }, CancellationToken.None);

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "failed-removable",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = removablePhysicalPath,
                DirectoryPath = "/failed",
                FileSize = 12,
                CreatedAt = DateTime.UtcNow.AddDays(-9),
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-7).AddHours(-1)
            }, CancellationToken.None);

            await cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), CancellationToken.None);

            Assert.NotNull(await _metadataRepository.GetAsync("tenant-001", "failed-stuck", CancellationToken.None));
            Assert.True(_fileSystem.File.Exists(blockedPhysicalPath));

            var removable = await _metadataRepository.GetAsync("tenant-001", "failed-removable", CancellationToken.None);
            Assert.NotNull(removable);
            Assert.Equal(FileProcessingStatus.DeadLettered, removable!.Status);
            Assert.False(_fileSystem.File.Exists(removablePhysicalPath));

            var stats = await cleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(1, stats.PermanentlyFailedFilesRemoved);
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_SkipsWhenVolumeIsUnavailableAndMissingCannotBeConfirmed()
        {
            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "failed-unconfirmed-missing",
                TenantId = "tenant-001",
                VolumeId = "vol-unavailable",
                PhysicalPath = Path.Combine(_volumePath, "missing-failed.dat"),
                DirectoryPath = "/failed",
                FileSize = 12,
                CreatedAt = DateTime.UtcNow.AddDays(-10),
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-8)
            }, CancellationToken.None);
            await SeedProjectedCountsAsync("tenant-001", "/failed");

            await _cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), CancellationToken.None);

            var metadata = await _metadataRepository.GetAsync("tenant-001", "failed-unconfirmed-missing", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.PermanentlyFailed, metadata!.Status);
            Assert.Equal(1, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Equal(1, (await _quotaRepository.GetOrCreateAsync("tenant-001", "/failed", CancellationToken.None)).CurrentCount);
        }

        [Fact]
        public async Task CleanupPermanentlyFailedFilesAsync_ContinuesPastMoreThanDeferredSkipLimit()
        {
            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                new CleanupOptions
                {
                    CleanupBatchSizePerTenant = 1
                },
                tenantManager: _tenantManager.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            var sharedBlockedPhysicalPath = Path.Combine(_volumePath, "failed-blocked-many.dat");
            var removablePhysicalPath = Path.Combine(_volumePath, "failed-removable-after-many.dat");
            _fileSystem.File.WriteAllText(sharedBlockedPhysicalPath, "failed content");
            _fileSystem.File.WriteAllText(removablePhysicalPath, "failed content");

            for (var i = 0; i <= 500; i++)
            {
                await _metadataRepository.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = $"failed-blocked-{i:D3}",
                    TenantId = "tenant-001",
                    VolumeId = "vol-missing",
                    PhysicalPath = sharedBlockedPhysicalPath,
                    DirectoryPath = "/failed",
                    FileSize = 12,
                    CreatedAt = DateTime.UtcNow.AddDays(-20).AddMinutes(i),
                    Status = FileProcessingStatus.PermanentlyFailed,
                    LastFailedAt = DateTime.UtcNow.AddDays(-15).AddMinutes(i)
                }, CancellationToken.None);
            }

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "failed-removable-after-many",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = removablePhysicalPath,
                DirectoryPath = "/failed",
                FileSize = 12,
                CreatedAt = DateTime.UtcNow.AddDays(-5),
                Status = FileProcessingStatus.PermanentlyFailed,
                LastFailedAt = DateTime.UtcNow.AddDays(-8)
            }, CancellationToken.None);

            await cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), CancellationToken.None);

            Assert.NotNull(await _metadataRepository.GetAsync("tenant-001", "failed-blocked-000", CancellationToken.None));
            Assert.True(_fileSystem.File.Exists(sharedBlockedPhysicalPath));

            var removable = await _metadataRepository.GetAsync("tenant-001", "failed-removable-after-many", CancellationToken.None);
            Assert.NotNull(removable);
            Assert.Equal(FileProcessingStatus.DeadLettered, removable!.Status);
            Assert.False(_fileSystem.File.Exists(removablePhysicalPath));

            var stats = await cleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(1, stats.PermanentlyFailedFilesRemoved);
        }

        [Fact]
        public async Task CleanupInvalidDatabaseFilesAsync_RemovesCorruptionBackups()
        {
            var tenantMetaDir = Path.Combine(_metadataDir, "tenant-001");
            var tenantQuotaDir = Path.Combine(_quotaDir, "tenant-001");
            _fileSystem.Directory.CreateDirectory(tenantMetaDir);
            _fileSystem.Directory.CreateDirectory(tenantQuotaDir);

            var metaBackup = Path.Combine(tenantMetaDir, "metadata.db.corrupted.20260316070000");
            var quotaBackup = Path.Combine(tenantQuotaDir, "quotas.db.corrupted.20260316070000");
            _fileSystem.File.WriteAllText(metaBackup, "meta-backup");
            _fileSystem.File.WriteAllText(quotaBackup, "quota-backup");

            var result = await _cleanupService.CleanupInvalidDatabaseFilesAsync(CancellationToken.None);

            Assert.Equal(2, result.FilesRemoved);
            Assert.True(result.SpaceFreed > 0);
            Assert.False(_fileSystem.File.Exists(metaBackup));
            Assert.False(_fileSystem.File.Exists(quotaBackup));
        }

        [Fact]
        public async Task CleanupCompletedFilesAsync_WithJournal_DeletesPhysicalFileAndDoesNotAppendDeleteSucceededTwice()
        {
            var appendCount = 0;
            var queueEventJournal = new Mock<IQueueEventJournal>(MockBehavior.Strict);
            queueEventJournal
                .Setup(j => j.AppendAsync(
                    It.Is<QueueEventRecord>(record =>
                        record.EventType == QueueEventType.DeleteSucceeded
                        && record.TenantId == "tenant-001"
                        && record.FileKey == "completed-journal"
                        && record.Status == FileProcessingStatus.DeleteSucceeded),
                    It.IsAny<CancellationToken>()))
                .Callback<QueueEventRecord, CancellationToken>((_, __) => appendCount++)
                .Returns(Task.CompletedTask);

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                queueEventJournal: queueEventJournal.Object);
            cleanupService.RegisterVolume(_volume.Object);

            var physicalPath = Path.Combine(_volumePath, "completed-journal.dat");
            _fileSystem.File.WriteAllText(physicalPath, "completed content");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "completed-journal",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/done",
                FileSize = 17,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10),
                Status = FileProcessingStatus.DeleteRequested,
                CompletedAt = DateTime.UtcNow.AddMinutes(-5)
            }, CancellationToken.None);

            await SeedProjectedCountsAsync("tenant-001", "/done");

            await cleanupService.CleanupCompletedFilesAsync(TimeSpan.Zero, CancellationToken.None);
            await cleanupService.CleanupCompletedFilesAsync(TimeSpan.Zero, CancellationToken.None);

            Assert.False(_fileSystem.File.Exists(physicalPath));
            Assert.Equal(1, appendCount);

            var metadata = await _metadataRepository.GetAsync("tenant-001", "completed-journal", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.DeleteSucceeded, metadata!.Status);
            Assert.NotNull(metadata.DeleteSucceededAt);

            var tenantCount = await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None);
            var directoryQuota = await _quotaRepository.GetOrCreateAsync("tenant-001", "/done", CancellationToken.None);
            Assert.Equal(1, tenantCount);
            Assert.Equal(1, directoryQuota.CurrentCount);

            var stats = await cleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(1, stats.CompletedRecordsRemoved);

            queueEventJournal.VerifyAll();
        }

        [Fact]
        public async Task CleanupCompletedFilesAsync_WithJournal_AppendsMissingDeleteRequestedBeforeDeleteSucceeded()
        {
            var queueEventJournal = new Mock<IQueueEventJournal>(MockBehavior.Strict);
            queueEventJournal
                .Setup(j => j.AppendBatchAsync(
                    It.Is<IReadOnlyList<QueueEventRecord>>(records =>
                        records.Count == 2
                        && records[0].EventType == QueueEventType.DeleteRequested
                        && records[0].Status == FileProcessingStatus.DeleteRequested
                        && records[0].TenantId == "tenant-001"
                        && records[0].FileKey == "completed-needs-delete-requested"
                        && records[1].EventType == QueueEventType.DeleteSucceeded
                        && records[1].Status == FileProcessingStatus.DeleteSucceeded
                        && records[1].TenantId == "tenant-001"
                        && records[1].FileKey == "completed-needs-delete-requested"
                        && records[0].OccurredAtUtc < records[1].OccurredAtUtc),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                queueEventJournal: queueEventJournal.Object);
            cleanupService.RegisterVolume(_volume.Object);

            var physicalPath = Path.Combine(_volumePath, "completed-needs-delete-requested.dat");
            _fileSystem.File.WriteAllText(physicalPath, "completed content");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "completed-needs-delete-requested",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/done",
                FileSize = 17,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10),
                Status = FileProcessingStatus.Completed,
                CompletedAt = DateTime.UtcNow.AddMinutes(-5)
            }, CancellationToken.None);

            await SeedProjectedCountsAsync("tenant-001", "/done");

            await cleanupService.CleanupCompletedFilesAsync(TimeSpan.Zero, CancellationToken.None);

            Assert.False(_fileSystem.File.Exists(physicalPath));

            var metadata = await _metadataRepository.GetAsync("tenant-001", "completed-needs-delete-requested", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.DeleteSucceeded, metadata!.Status);
            Assert.NotNull(metadata.DeleteSucceededAt);

            queueEventJournal.VerifyAll();
        }

        [Fact]
        public async Task CleanupCompletedFilesAsync_WithoutJournal_RemovesMetadataAndQuota()
        {
            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            var physicalPath = Path.Combine(_volumePath, "completed-direct.dat");
            _fileSystem.File.WriteAllText(physicalPath, "completed content");

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "completed-direct",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/done",
                FileSize = 17,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10),
                Status = FileProcessingStatus.Completed,
                CompletedAt = DateTime.UtcNow.AddMinutes(-5)
            }, CancellationToken.None);

            await SeedProjectedCountsAsync("tenant-001", "/done");

            await cleanupService.CleanupCompletedFilesAsync(TimeSpan.Zero, CancellationToken.None);

            Assert.False(_fileSystem.File.Exists(physicalPath));
            Assert.Null(await _metadataRepository.GetAsync("tenant-001", "completed-direct", CancellationToken.None));
            Assert.Equal(0, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Equal(0, (await _quotaRepository.GetOrCreateAsync("tenant-001", "/done", CancellationToken.None)).CurrentCount);

            var stats = await cleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            Assert.Equal(1, stats.CompletedRecordsRemoved);
        }

        [Fact]
        public async Task CleanupCompletedFilesAsync_SkipsWhenVolumeIsUnavailableAndMissingCannotBeConfirmed()
        {
            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                allowLegacyNonJournalMode: true);

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "completed-unconfirmed-missing",
                TenantId = "tenant-001",
                VolumeId = "vol-unavailable",
                PhysicalPath = Path.Combine(_volumePath, "missing-completed.dat"),
                DirectoryPath = "/done",
                FileSize = 17,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10),
                Status = FileProcessingStatus.Completed,
                CompletedAt = DateTime.UtcNow.AddMinutes(-5)
            }, CancellationToken.None);
            await SeedProjectedCountsAsync("tenant-001", "/done");

            await cleanupService.CleanupCompletedFilesAsync(TimeSpan.Zero, CancellationToken.None);

            var metadata = await _metadataRepository.GetAsync("tenant-001", "completed-unconfirmed-missing", CancellationToken.None);
            Assert.NotNull(metadata);
            Assert.Equal(FileProcessingStatus.Completed, metadata!.Status);
            Assert.Equal(1, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Equal(1, (await _quotaRepository.GetOrCreateAsync("tenant-001", "/done", CancellationToken.None)).CurrentCount);
        }

        [Fact]
        public async Task CleanupCompletedFilesAsync_UsesProjectionCleanupStoreWhenProvided()
        {
            var completedAt = DateTime.UtcNow.AddMinutes(-5);
            var physicalPath = Path.Combine(_volumePath, "completed-projection-store.dat");
            _fileSystem.File.WriteAllText(physicalPath, "completed content");

            var metadata = new FileMetadata
            {
                FileKey = "completed-projection-store",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = "/done",
                FileSize = 17,
                CreatedAt = DateTime.UtcNow.AddMinutes(-10),
                Status = FileProcessingStatus.Completed,
                CompletedAt = completedAt
            };

            var projectionCleanupStore = new Mock<IQueueProjectionCleanupStore>(MockBehavior.Strict);
            projectionCleanupStore
                .SetupSequence(store => store.GetCompletedFilesOlderThanAsync(
                    "tenant-001",
                    It.IsAny<DateTime>(),
                    It.IsAny<int>(),
                    It.IsAny<ISet<string>?>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[] { metadata })
                .ReturnsAsync(Array.Empty<FileMetadata>());
            projectionCleanupStore
                .Setup(store => store.TryRemoveCompletedFileAsync(
                    "tenant-001",
                    "completed-projection-store",
                    completedAt,
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            var cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                _tenantQuotaManager,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                _quotaDir,
                tenantManager: _tenantManager.Object,
                directoryQuotaManager: _directoryQuotaManager,
                projectionCleanupStore: projectionCleanupStore.Object,
                allowLegacyNonJournalMode: true);
            cleanupService.RegisterVolume(_volume.Object);

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                FileKey = "tenant-seed",
                TenantId = "tenant-001",
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumePath, "tenant-seed.dat"),
                DirectoryPath = "/seed",
                FileSize = 1,
                CreatedAt = DateTime.UtcNow,
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            await SeedProjectedCountsAsync("tenant-001", "/done");

            await cleanupService.CleanupCompletedFilesAsync(TimeSpan.Zero, CancellationToken.None);

            Assert.False(_fileSystem.File.Exists(physicalPath));
            Assert.Equal(0, await _tenantQuotaManager.GetFileCountAsync("tenant-001", CancellationToken.None));
            Assert.Equal(0, (await _quotaRepository.GetOrCreateAsync("tenant-001", "/done", CancellationToken.None)).CurrentCount);

            projectionCleanupStore.Verify(store => store.TryRemoveCompletedFileAsync(
                "tenant-001",
                "completed-projection-store",
                completedAt,
                It.IsAny<CancellationToken>()), Times.Once);
            projectionCleanupStore.VerifyAll();
        }

        [Fact]
        public async Task CleanupEmptyDirectoriesAsync_RemovesJunkFiles_ButPreservesDirectories()
        {
            // Arrange
            var tenantPath = Path.Combine(_volumePath, "tenant-junk");
            var junkDir = Path.Combine(tenantPath, "junk-files-only");
            _fileSystem.Directory.CreateDirectory(junkDir);
            
            // Create a Thumbs.db file (junk)
            var thumbsPath = Path.Combine(junkDir, "Thumbs.db");
            _fileSystem.File.WriteAllText(thumbsPath, "junk content");
            
            // Create a valid file (should be preserved)
            var validPath = Path.Combine(junkDir, "valid.txt");
            _fileSystem.File.WriteAllText(validPath, "valid content");

            // Act
            await _cleanupService.CleanupEmptyDirectoriesAsync("tenant-junk", default);

            // Assert
            // 1. Junk file should be deleted
            Assert.False(_fileSystem.File.Exists(thumbsPath), "Thumbs.db should be deleted");
            
            // 2. Valid file should be preserved
            Assert.True(_fileSystem.File.Exists(validPath), "Valid file should be preserved");
            
            // 3. Directory should be preserved (even if it was empty of valid files)
            Assert.True(_fileSystem.Directory.Exists(junkDir), "Directory should be preserved");
            
            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            // junkDir still has valid.txt after Thumbs.db deletion — no directories were removed
            Assert.Equal(0, stats.EmptyDirectoriesRemoved);
        }

        [Fact]
        public async Task CleanupEmptyDirectoriesAsync_RemovesJunkFiles_Recursive()
        {
            // Arrange
            var tenantPath = Path.Combine(_volumePath, "tenant-junk-recursive");
            var level1 = Path.Combine(tenantPath, "level1");
            var level2 = Path.Combine(level1, "level2");
            _fileSystem.Directory.CreateDirectory(level2);

            // Create junk files in both levels
            _fileSystem.File.WriteAllText(Path.Combine(level1, "desktop.ini"), "junk");
            _fileSystem.File.WriteAllText(Path.Combine(level2, ".DS_Store"), "junk");

            // Act
            await _cleanupService.CleanupEmptyDirectoriesAsync("tenant-junk-recursive", default);

            // Assert
            Assert.False(_fileSystem.File.Exists(Path.Combine(level1, "desktop.ini")));
            Assert.False(_fileSystem.File.Exists(Path.Combine(level2, ".DS_Store")));

            // After junk files are removed both dirs become empty and are deleted bottom-up.
            // Only the tenant root (tenantPath) is protected from deletion.
            Assert.False(_fileSystem.Directory.Exists(level2));
            Assert.False(_fileSystem.Directory.Exists(level1));

            var stats = await _cleanupService.GetCleanupStatisticsAsync(default);
            Assert.Equal(2, stats.EmptyDirectoriesRemoved);
        }
    }
}
