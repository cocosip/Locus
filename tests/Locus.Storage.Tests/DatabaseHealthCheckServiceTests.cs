using System;
using System.Collections.Generic;
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
    public class DatabaseHealthCheckServiceTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;
        private readonly Mock<ILogger<DatabaseHealthCheckService>> _logger;
        private readonly Mock<ILogger<DatabaseRecoveryService>> _recoveryLogger;
        private readonly string _volumePath;
        private readonly string _metadataDir;
        private readonly string _quotaDir;
        private readonly string _testId;

        public DatabaseHealthCheckServiceTests()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _testId = Guid.NewGuid().ToString("N").Substring(0, 8);

            _metadataDir = Path.Combine(Path.GetTempPath(), $"locus-test-health-meta-{_testId}");
            _quotaDir = Path.Combine(Path.GetTempPath(), $"locus-test-health-quota-{_testId}");
            _volumePath = Path.Combine(Path.GetTempPath(), $"locus-test-health-vol-{_testId}");

            _fileSystem.Directory.CreateDirectory(_metadataDir);
            _fileSystem.Directory.CreateDirectory(_quotaDir);
            _fileSystem.Directory.CreateDirectory(_volumePath);

            var metadataRepoLogger = new Mock<ILogger<MetadataRepository>>();
            _metadataRepository = new MetadataRepository(_fileSystem, metadataRepoLogger.Object, _metadataDir);

            var quotaRepoLogger = new Mock<ILogger<DirectoryQuotaRepository>>();
            _quotaRepository = new DirectoryQuotaRepository(_fileSystem, quotaRepoLogger.Object, _quotaDir);

            _logger = new Mock<ILogger<DatabaseHealthCheckService>>();
            _recoveryLogger = new Mock<ILogger<DatabaseRecoveryService>>();
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
        public async Task StartAsync_LogsHealthyWhenAllDatabasesAreHealthy()
        {
            // Arrange - use unique tenant and temporary repository
            var tenant1 = $"tenant-healthy-{Guid.NewGuid().ToString("N").Substring(0, 8)}";

            // Create database using temporary repository, then dispose it
            using (var tempRepo = new MetadataRepository(_fileSystem, new Mock<ILogger<MetadataRepository>>().Object, _metadataDir))
            {
                await tempRepo.AddOrUpdateAsync(new FileMetadata
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

            // Wait for file system to release locks
            await Task.Delay(100);

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _recoveryLogger.Object,
                _metadataDir,
                _quotaDir);

            var healthCheckService = new DatabaseHealthCheckService(
                recoveryService,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                new[] { _volumePath });

            // Act
            await healthCheckService.StartAsync(default);

            // Assert - Verify healthy message was logged
            _logger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("healthy")),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.AtLeastOnce);
        }

        [Fact]
        public async Task StartAsync_LogsWarningWhenDatabasesAreCorrupted()
        {
            // Arrange
            var tenantId = $"tenant-corrupted-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var dbPath = Path.Combine(_metadataDir, $"{tenantId}.db");

            // Create a valid database first
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

            // Wait for database to be fully written
            await Task.Delay(100);

            // Now corrupt it
            var garbage = new byte[512];
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _recoveryLogger.Object,
                _metadataDir,
                _quotaDir);

            var healthCheckService = new DatabaseHealthCheckService(
                recoveryService,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                new[] { _volumePath });

            // Act
            await healthCheckService.StartAsync(default);

            // Assert - Verify error was logged for corrupted database
            _logger.Verify(
                x => x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("CORRUPTED DATABASE DETECTED")),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.AtLeastOnce);
        }

        [Fact]
        public async Task StartAsync_DetectsOrphanedFiles()
        {
            // Arrange - Create physical files but no database
            var tenantId = "tenant-orphaned";
            var tenantPath = Path.Combine(_volumePath, tenantId);
            _fileSystem.Directory.CreateDirectory(tenantPath);
            _fileSystem.File.WriteAllText(Path.Combine(tenantPath, "orphaned.txt"), "content");

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _recoveryLogger.Object,
                _metadataDir,
                _quotaDir);

            var healthCheckService = new DatabaseHealthCheckService(
                recoveryService,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                new[] { _volumePath });

            // Act
            await healthCheckService.StartAsync(default);

            // Assert - Verify orphaned files warning was logged
            _logger.Verify(
                x => x.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("METADATA LOSS DETECTED")),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_LogsNormalForFirstStartup()
        {
            // Arrange - No databases, no files
            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _recoveryLogger.Object,
                _metadataDir,
                _quotaDir);

            var healthCheckService = new DatabaseHealthCheckService(
                recoveryService,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                new[] { _volumePath });

            // Act
            await healthCheckService.StartAsync(default);

            // Assert - Verify first startup message was logged
            _logger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("first startup")),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_HandlesNoVolumesConfigured()
        {
            // Arrange - Empty volume paths
            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _recoveryLogger.Object,
                _metadataDir,
                _quotaDir);

            var healthCheckService = new DatabaseHealthCheckService(
                recoveryService,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                Array.Empty<string>());

            // Act
            await healthCheckService.StartAsync(default);

            // Assert - Verify no volumes message was logged
            _logger.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("No storage volumes")),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_HandlesExceptionGracefully()
        {
            // Arrange - Create a scenario that will throw
            var mockRecoveryService = new Mock<IDatabaseRecoveryService>();
            mockRecoveryService
                .Setup(x => x.CheckAllDatabasesAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Test exception"));

            var healthCheckService = new DatabaseHealthCheckService(
                mockRecoveryService.Object,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                new[] { _volumePath });

            // Act
            await healthCheckService.StartAsync(default);

            // Assert - Verify error was logged
            _logger.Verify(
                x => x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Error during database health check")),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task StopAsync_CompletesSuccessfully()
        {
            // Arrange
            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _recoveryLogger.Object,
                _metadataDir,
                _quotaDir);

            var healthCheckService = new DatabaseHealthCheckService(
                recoveryService,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                new[] { _volumePath });

            // Act & Assert - Should not throw
            await healthCheckService.StopAsync(default);
        }

        [Fact]
        public async Task StartAsync_DetectsMultipleOrphanedTenants()
        {
            // Arrange - Create multiple tenants with files but no databases
            var tenant1 = "tenant-orphaned-1";
            var tenant2 = "tenant-orphaned-2";

            var tenant1Path = Path.Combine(_volumePath, tenant1);
            var tenant2Path = Path.Combine(_volumePath, tenant2);

            _fileSystem.Directory.CreateDirectory(tenant1Path);
            _fileSystem.Directory.CreateDirectory(tenant2Path);

            _fileSystem.File.WriteAllText(Path.Combine(tenant1Path, "file1.txt"), "content");
            _fileSystem.File.WriteAllText(Path.Combine(tenant2Path, "file2.txt"), "content");

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _recoveryLogger.Object,
                _metadataDir,
                _quotaDir);

            var healthCheckService = new DatabaseHealthCheckService(
                recoveryService,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                new[] { _volumePath });

            // Act
            await healthCheckService.StartAsync(default);

            // Assert - Verify multiple orphaned tenants were detected
            _logger.Verify(
                x => x.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) =>
                        v.ToString()!.Contains("METADATA LOSS DETECTED") &&
                        v.ToString()!.Contains("2")),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task StartAsync_AutoRecoversCorruptedQuotaDatabase_WhenEnabled()
        {
            // Arrange
            var tenantId = $"tenant-auto-recover-{Guid.NewGuid().ToString("N").Substring(0, 8)}";
            var dbPath = Path.Combine(_quotaDir, $"{tenantId}-quotas.db");

            // Create a valid quota database first
            using (var tempRepo = new DirectoryQuotaRepository(_fileSystem, new Mock<ILogger<DirectoryQuotaRepository>>().Object, _quotaDir))
            {
                await tempRepo.GetOrCreateAsync(tenantId, "/", default);
            }

            await Task.Delay(100);

            // Corrupt it
            var garbage = new byte[512];
            new Random().NextBytes(garbage);
            using (var stream = _fileSystem.File.Open(dbPath, FileMode.Open, FileAccess.Write))
            {
                stream.Write(garbage, 0, garbage.Length);
            }

            var recoveryService = new DatabaseRecoveryService(
                _metadataRepository,
                _quotaRepository,
                _fileSystem,
                _recoveryLogger.Object,
                _metadataDir,
                _quotaDir);

            var healthCheckService = new DatabaseHealthCheckService(
                recoveryService,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                new[] { _volumePath },
                autoRecoverCorruptedDatabases: true,
                failFastOnRecoveryFailure: false);

            // Act
            await healthCheckService.StartAsync(default);

            // Assert
            var report = await recoveryService.CheckAllDatabasesAsync(default);
            Assert.Empty(report.CorruptedDatabases);
        }

        [Fact]
        public async Task StartAsync_ThrowsWhenFailFastEnabledAndRecoveryFails()
        {
            // Arrange
            var corruptedReport = new DatabaseHealthReport
            {
                HealthyDatabases = 0,
                CorruptedDatabases = new List<DatabaseHealthInfo>
                {
                    new DatabaseHealthInfo
                    {
                        DatabaseType = "Quota",
                        TenantId = "tenant-fail-fast",
                        DatabasePath = "/tmp/tenant-fail-fast-quotas.db",
                        IsCorrupted = true
                    }
                }
            };

            var mockRecoveryService = new Mock<IDatabaseRecoveryService>();
            mockRecoveryService
                .Setup(x => x.CheckAllDatabasesAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(corruptedReport);
            mockRecoveryService
                .Setup(x => x.RebuildQuotaDatabaseAsync(
                    It.IsAny<string>(),
                    It.IsAny<IEnumerable<string>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DatabaseRebuildResult
                {
                    DatabaseType = "Quota",
                    TenantId = "tenant-fail-fast",
                    Success = false,
                    Errors = new List<string> { "Rebuild failed" }
                });

            var healthCheckService = new DatabaseHealthCheckService(
                mockRecoveryService.Object,
                _fileSystem,
                _logger.Object,
                _metadataDir,
                new[] { _volumePath },
                autoRecoverCorruptedDatabases: true,
                failFastOnRecoveryFailure: true);

            // Act & Assert
            await Assert.ThrowsAnyAsync<Exception>(() => healthCheckService.StartAsync(default));
        }
    }
}
