using System;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class DatabaseRecoveryServiceTests : IDisposable
    {
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
            _metadataRepository = new MetadataRepository(_fileSystem, metadataRepoLogger.Object, _metadataDir);

            var quotaRepoLogger = new Mock<ILogger<DirectoryQuotaRepository>>();
            _quotaRepository = new DirectoryQuotaRepository(_fileSystem, quotaRepoLogger.Object, _quotaDir);

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
            var dbPath = Path.Combine(_metadataDir, $"{tenantId}.db");

            // Create a temporary repository to create the database, then dispose it
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir))
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
            var dbPath = Path.Combine(_metadataDir, $"{tenantId}.db");

            // First create a valid LiteDB database
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir))
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
            var dbPath = Path.Combine(_metadataDir, $"{tenantId}.db");
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir))
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
            Assert.Equal(2, result.RecordsRebuilt);
            Assert.NotNull(result.BackupPath);
            Assert.True(_fileSystem.File.Exists(result.BackupPath));

            // Verify new database file exists
            Assert.True(_fileSystem.File.Exists(dbPath));
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
            Assert.Single(result.Errors);
            Assert.Contains("No database file found", result.Errors[0]);
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
            var dbPath = Path.Combine(_quotaDir, $"{tenantId}-quotas.db");
            using (var tempRepo = new DirectoryQuotaRepository(_fileSystem, new Mock<ILogger<DirectoryQuotaRepository>>().Object, _quotaDir))
            {
                await tempRepo.GetOrCreateAsync(tenantId, "/", default);
            }

            await Task.Delay(100);

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
            Assert.True(result.RecordsRebuilt >= 2);
            Assert.NotNull(result.BackupPath);

            // Verify new database file exists
            Assert.True(_fileSystem.File.Exists(dbPath));
        }

        [Fact]
        public async Task CheckAllDatabasesAsync_ReturnsHealthyForAllHealthyDatabases()
        {
            // Arrange - use unique tenant IDs and temporary repositories
            var tenant1 = $"tenant-check-1-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var tenant2 = $"tenant-check-2-{Guid.NewGuid().ToString("N").Substring(0, 8)}";

            // Create databases using temporary repositories, then dispose them
            using (var tempMetaRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir))
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

            using (var tempQuotaRepo = new DirectoryQuotaRepository(_fileSystem, new Mock<ILogger<DirectoryQuotaRepository>>().Object, _quotaDir))
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
            using (var tempMetaRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir))
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

            using (var tempQuotaRepo = new DirectoryQuotaRepository(_fileSystem, new Mock<ILogger<DirectoryQuotaRepository>>().Object, _quotaDir))
            {
                await tempQuotaRepo.GetOrCreateAsync(tenant2, "/", default);
            }

            // Wait for databases to be fully written
            await Task.Delay(100);

            // Now corrupt both databases
            var metaDbPath = Path.Combine(_metadataDir, $"{tenant1}.db");
            var quotaDbPath = Path.Combine(_quotaDir, $"{tenant2}-quotas.db");

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
            var dbPath = Path.Combine(_metadataDir, $"{tenantId}.db");
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
            var dbPath = Path.Combine(_metadataDir, $"{tenantId}.db");
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
