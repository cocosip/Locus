using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions.TestingHelpers;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class FileWatcherTests
    {
        private readonly MockFileSystem _fileSystem;
        private readonly Mock<IStoragePool> _storagePool;
        private readonly Mock<ITenantManager> _tenantManager;
        private readonly Mock<ILogger<FileWatcher>> _logger;
        private readonly FileWatcher _fileWatcher;
        private readonly string _configRoot;

        public FileWatcherTests()
        {
            _fileSystem = new MockFileSystem();
            _storagePool = new Mock<IStoragePool>();
            _tenantManager = new Mock<ITenantManager>();
            _logger = new Mock<ILogger<FileWatcher>>();
            _configRoot = Path.Combine("test-config", "watchers");

            _fileWatcher = new FileWatcher(
                _fileSystem,
                _storagePool.Object,
                _tenantManager.Object,
                _logger.Object,
                _configRoot);
        }

        [Fact]
        public async Task RegisterWatcherAsync_CreatesConfiguration()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            mockTenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = false
            };

            // Act
            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Assert
            Assert.NotEmpty(config.WatcherId);
            var configPath = Path.Combine(_configRoot, $"{config.WatcherId}.json");
            Assert.True(_fileSystem.File.Exists(configPath));
        }

        [Fact]
        public async Task RegisterWatcherAsync_ThrowsWhenTenantNotFound()
        {
            // Arrange
            var tenantId = "nonexistent-tenant";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => null!);

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                MultiTenantMode = false
            };

            // Act & Assert
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None));
        }

        [Fact]
        public async Task RegisterWatcherAsync_ThrowsWhenWatchPathNotExists()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\nonexistent";

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                MultiTenantMode = false
            };

            // Act & Assert
            await Assert.ThrowsAsync<DirectoryNotFoundException>(() =>
                _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None));
        }

        [Fact]
        public async Task ScanNowAsync_SingleTenantMode_ImportsFiles()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            // Create test files
            var file1 = Path.Combine(watchPath, "file1.txt");
            var file2 = Path.Combine(watchPath, "file2.txt");
            _fileSystem.File.WriteAllText(file1, "content1");
            _fileSystem.File.WriteAllText(file2, "content2");

            // Set file write time to past to meet MinFileAge requirement
            _fileSystem.File.SetLastWriteTimeUtc(file1, DateTime.UtcNow.AddMinutes(-1));
            _fileSystem.File.SetLastWriteTimeUtc(file2, DateTime.UtcNow.AddMinutes(-1));

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            mockTenant.Setup(t => t.Status).Returns(TenantStatus.Enabled);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            _storagePool.Setup(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync("generated-key");

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = false,
                PostImportAction = PostImportAction.Keep
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            var result = await _fileWatcher.ScanNowAsync(config.WatcherId, CancellationToken.None);

            // Assert
            Assert.Equal(2, result.FilesDiscovered);
            Assert.Equal(2, result.FilesImported);
            Assert.Equal(0, result.FilesFailed);
            _storagePool.Verify(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()), Times.Exactly(2));
        }

        [Fact]
        public async Task ScanNowAsync_MultiTenantMode_ImportsFilesPerTenant()
        {
            // Arrange
            var watchPath = @"C:\watch";
            var tenant1Path = Path.Combine(watchPath, "tenant-001");
            var tenant2Path = Path.Combine(watchPath, "tenant-002");

            _fileSystem.Directory.CreateDirectory(tenant1Path);
            _fileSystem.Directory.CreateDirectory(tenant2Path);

            // Create files for tenant-001
            var file1 = Path.Combine(tenant1Path, "file1.txt");
            _fileSystem.File.WriteAllText(file1, "content1");
            _fileSystem.File.SetLastWriteTimeUtc(file1, DateTime.UtcNow.AddMinutes(-1));

            // Create files for tenant-002
            var file2 = Path.Combine(tenant2Path, "file2.txt");
            _fileSystem.File.WriteAllText(file2, "content2");
            _fileSystem.File.SetLastWriteTimeUtc(file2, DateTime.UtcNow.AddMinutes(-1));

            // Setup tenant mocks
            var mockTenant1 = new Mock<ITenantContext>();
            mockTenant1.Setup(t => t.TenantId).Returns("tenant-001");
            mockTenant1.Setup(t => t.Status).Returns(TenantStatus.Enabled);

            var mockTenant2 = new Mock<ITenantContext>();
            mockTenant2.Setup(t => t.TenantId).Returns("tenant-002");
            mockTenant2.Setup(t => t.Status).Returns(TenantStatus.Enabled);

            _tenantManager.Setup(m => m.GetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant1.Object);
            _tenantManager.Setup(m => m.GetTenantAsync("tenant-002", It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant2.Object);
            _tenantManager.Setup(m => m.IsTenantEnabledAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
            _tenantManager.Setup(m => m.IsTenantEnabledAsync("tenant-002", It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            _storagePool.Setup(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync("generated-key");

            var config = new FileWatcherConfiguration
            {
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = true,
                PostImportAction = PostImportAction.Keep
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            var result = await _fileWatcher.ScanNowAsync(config.WatcherId, CancellationToken.None);

            // Assert
            Assert.Equal(2, result.FilesDiscovered);
            Assert.Equal(2, result.FilesImported);
            Assert.Equal(0, result.FilesFailed);

            // Verify each tenant's files were imported
            _storagePool.Verify(s => s.WriteFileAsync(
                It.Is<ITenantContext>(t => t.TenantId == "tenant-001"),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()), Times.Once);
            _storagePool.Verify(s => s.WriteFileAsync(
                It.Is<ITenantContext>(t => t.TenantId == "tenant-002"),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ScanNowAsync_SkipsDisabledTenants()
        {
            // Arrange
            var watchPath = @"C:\watch";
            var tenant1Path = Path.Combine(watchPath, "tenant-001");
            var tenant2Path = Path.Combine(watchPath, "tenant-002");

            _fileSystem.Directory.CreateDirectory(tenant1Path);
            _fileSystem.Directory.CreateDirectory(tenant2Path);

            var file1 = Path.Combine(tenant1Path, "file1.txt");
            var file2 = Path.Combine(tenant2Path, "file2.txt");
            _fileSystem.File.WriteAllText(file1, "content1");
            _fileSystem.File.WriteAllText(file2, "content2");
            _fileSystem.File.SetLastWriteTimeUtc(file1, DateTime.UtcNow.AddMinutes(-1));
            _fileSystem.File.SetLastWriteTimeUtc(file2, DateTime.UtcNow.AddMinutes(-1));

            var mockTenant1 = new Mock<ITenantContext>();
            mockTenant1.Setup(t => t.TenantId).Returns("tenant-001");

            var mockTenant2 = new Mock<ITenantContext>();
            mockTenant2.Setup(t => t.TenantId).Returns("tenant-002");

            _tenantManager.Setup(m => m.GetTenantAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant1.Object);
            _tenantManager.Setup(m => m.GetTenantAsync("tenant-002", It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant2.Object);
            _tenantManager.Setup(m => m.IsTenantEnabledAsync("tenant-001", It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
            _tenantManager.Setup(m => m.IsTenantEnabledAsync("tenant-002", It.IsAny<CancellationToken>()))
                .ReturnsAsync(false); // Disabled

            _storagePool.Setup(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync("generated-key");

            var config = new FileWatcherConfiguration
            {
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = true,
                PostImportAction = PostImportAction.Keep
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            var result = await _fileWatcher.ScanNowAsync(config.WatcherId, CancellationToken.None);

            // Assert
            Assert.Equal(1, result.FilesDiscovered); // Only tenant-001's file
            Assert.Equal(1, result.FilesImported);
            _storagePool.Verify(s => s.WriteFileAsync(
                It.Is<ITenantContext>(t => t.TenantId == "tenant-001"),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()), Times.Once);
            _storagePool.Verify(s => s.WriteFileAsync(
                It.Is<ITenantContext>(t => t.TenantId == "tenant-002"),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task ScanNowAsync_SkipsYoungFiles()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            var oldFile = Path.Combine(watchPath, "old.txt");
            var newFile = Path.Combine(watchPath, "new.txt");
            _fileSystem.File.WriteAllText(oldFile, "old content");
            _fileSystem.File.WriteAllText(newFile, "new content");

            // Old file meets MinFileAge requirement
            _fileSystem.File.SetLastWriteTimeUtc(oldFile, DateTime.UtcNow.AddMinutes(-1));
            // New file is too recent
            _fileSystem.File.SetLastWriteTimeUtc(newFile, DateTime.UtcNow.AddSeconds(-1));

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            _storagePool.Setup(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync("generated-key");

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = false,
                MinFileAge = TimeSpan.FromSeconds(5),
                PostImportAction = PostImportAction.Keep
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            var result = await _fileWatcher.ScanNowAsync(config.WatcherId, CancellationToken.None);

            // Assert
            Assert.Equal(2, result.FilesDiscovered);
            Assert.Equal(1, result.FilesImported); // Only old file
            Assert.Equal(1, result.FilesSkipped); // New file skipped
            _storagePool.Verify(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ScanNowAsync_PostImportAction_Delete()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            var filePath = Path.Combine(watchPath, "file.txt");
            _fileSystem.File.WriteAllText(filePath, "content");
            _fileSystem.File.SetLastWriteTimeUtc(filePath, DateTime.UtcNow.AddMinutes(-1));

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            _storagePool.Setup(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync("generated-key");

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = false,
                PostImportAction = PostImportAction.Delete
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            var result = await _fileWatcher.ScanNowAsync(config.WatcherId, CancellationToken.None);

            // Assert
            Assert.Equal(1, result.FilesImported);
            Assert.False(_fileSystem.File.Exists(filePath), "File should be deleted after import");
        }

        [Fact]
        public async Task ScanNowAsync_PostImportAction_Move()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            var moveToPath = @"C:\processed";
            _fileSystem.Directory.CreateDirectory(watchPath);

            var originalPath = Path.Combine(watchPath, "file.txt");
            _fileSystem.File.WriteAllText(originalPath, "content");
            _fileSystem.File.SetLastWriteTimeUtc(originalPath, DateTime.UtcNow.AddMinutes(-1));

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            _storagePool.Setup(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync("generated-key");

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = false,
                PostImportAction = PostImportAction.Move,
                MoveToDirectory = moveToPath
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            var result = await _fileWatcher.ScanNowAsync(config.WatcherId, CancellationToken.None);

            // Assert
            Assert.Equal(1, result.FilesImported);
            Assert.False(_fileSystem.File.Exists(originalPath), "File should be moved from original location");
            var movedPath = Path.Combine(moveToPath, "file.txt");
            Assert.True(_fileSystem.File.Exists(movedPath), "File should exist in move-to location");
        }

        [Fact]
        public async Task GetAllWatchersAsync_ReturnsAllConfigurations()
        {
            // Arrange
            var tenant1 = "tenant-001";
            var tenant2 = "tenant-002";
            var watchPath1 = @"C:\watch1";
            var watchPath2 = @"C:\watch2";

            _fileSystem.Directory.CreateDirectory(watchPath1);
            _fileSystem.Directory.CreateDirectory(watchPath2);

            var mockTenant1 = new Mock<ITenantContext>();
            mockTenant1.Setup(t => t.TenantId).Returns(tenant1);
            var mockTenant2 = new Mock<ITenantContext>();
            mockTenant2.Setup(t => t.TenantId).Returns(tenant2);

            _tenantManager.Setup(m => m.GetTenantAsync(tenant1, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant1.Object);
            _tenantManager.Setup(m => m.GetTenantAsync(tenant2, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant2.Object);

            var config1 = new FileWatcherConfiguration
            {
                TenantId = tenant1,
                WatchPath = watchPath1,
                MultiTenantMode = false
            };
            var config2 = new FileWatcherConfiguration
            {
                TenantId = tenant2,
                WatchPath = watchPath2,
                MultiTenantMode = false
            };

            await _fileWatcher.RegisterWatcherAsync(config1, CancellationToken.None);
            await _fileWatcher.RegisterWatcherAsync(config2, CancellationToken.None);

            // Act
            var watchers = await _fileWatcher.GetAllWatchersAsync(CancellationToken.None);

            // Assert
            Assert.Equal(2, watchers.Count());
            Assert.Contains(watchers, w => w.TenantId == tenant1);
            Assert.Contains(watchers, w => w.TenantId == tenant2);
        }

        [Fact]
        public async Task EnableWatcherAsync_UpdatesWatcherStatus()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                Enabled = false,
                MultiTenantMode = false
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            await _fileWatcher.EnableWatcherAsync(config.WatcherId, CancellationToken.None);

            // Assert
            var watcher = await _fileWatcher.GetWatcherAsync(config.WatcherId, CancellationToken.None);
            Assert.NotNull(watcher);
            Assert.True(watcher.Enabled);
        }

        [Fact]
        public async Task DisableWatcherAsync_UpdatesWatcherStatus()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = false
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            await _fileWatcher.DisableWatcherAsync(config.WatcherId, CancellationToken.None);

            // Assert
            var watcher = await _fileWatcher.GetWatcherAsync(config.WatcherId, CancellationToken.None);
            Assert.NotNull(watcher);
            Assert.False(watcher.Enabled);
        }

        [Fact]
        public async Task ScanNowAsync_SkipsEmptyFiles()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            // Create an empty file (0 bytes)
            var emptyFile = Path.Combine(watchPath, "empty.txt");
            _fileSystem.File.WriteAllText(emptyFile, "");
            _fileSystem.File.SetLastWriteTimeUtc(emptyFile, DateTime.UtcNow.AddMinutes(-1));

            // Create a normal file
            var normalFile = Path.Combine(watchPath, "normal.txt");
            _fileSystem.File.WriteAllText(normalFile, "content");
            _fileSystem.File.SetLastWriteTimeUtc(normalFile, DateTime.UtcNow.AddMinutes(-1));

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            _storagePool.Setup(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync("generated-key");

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = false,
                PostImportAction = PostImportAction.Keep
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            var result = await _fileWatcher.ScanNowAsync(config.WatcherId, CancellationToken.None);

            // Assert
            Assert.Equal(2, result.FilesDiscovered);
            Assert.Equal(1, result.FilesImported); // Only normal file
            Assert.Equal(1, result.FilesSkipped);   // Empty file skipped

            // Verify only normal file was imported
            _storagePool.Verify(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ScanNowAsync_SkipsFilesExceedingSizeLimit()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            // Create a large file
            var largeFile = Path.Combine(watchPath, "large.txt");
            _fileSystem.File.WriteAllText(largeFile, new string('x', 1000)); // 1000 bytes
            _fileSystem.File.SetLastWriteTimeUtc(largeFile, DateTime.UtcNow.AddMinutes(-1));

            // Create a small file
            var smallFile = Path.Combine(watchPath, "small.txt");
            _fileSystem.File.WriteAllText(smallFile, "small");
            _fileSystem.File.SetLastWriteTimeUtc(smallFile, DateTime.UtcNow.AddMinutes(-1));

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            _storagePool.Setup(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()))
                .ReturnsAsync("generated-key");

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                Enabled = true,
                MultiTenantMode = false,
                MaxFileSizeBytes = 500,  // Only allow files up to 500 bytes
                PostImportAction = PostImportAction.Keep
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            var result = await _fileWatcher.ScanNowAsync(config.WatcherId, CancellationToken.None);

            // Assert
            Assert.Equal(2, result.FilesDiscovered);
            Assert.Equal(1, result.FilesImported); // Only small file
            Assert.Equal(1, result.FilesSkipped);   // Large file skipped

            // Verify only small file was imported
            _storagePool.Verify(s => s.WriteFileAsync(
                It.IsAny<ITenantContext>(),
                It.IsAny<Stream>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task RemoveWatcherAsync_DeletesConfiguration()
        {
            // Arrange
            var tenantId = "tenant-001";
            var watchPath = @"C:\watch";
            _fileSystem.Directory.CreateDirectory(watchPath);

            var mockTenant = new Mock<ITenantContext>();
            mockTenant.Setup(t => t.TenantId).Returns(tenantId);
            _tenantManager.Setup(m => m.GetTenantAsync(tenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            var config = new FileWatcherConfiguration
            {
                TenantId = tenantId,
                WatchPath = watchPath,
                MultiTenantMode = false
            };

            await _fileWatcher.RegisterWatcherAsync(config, CancellationToken.None);

            // Act
            await _fileWatcher.RemoveWatcherAsync(config.WatcherId, CancellationToken.None);

            // Assert
            var watcher = await _fileWatcher.GetWatcherAsync(config.WatcherId, CancellationToken.None);
            Assert.Null(watcher);
        }
    }
}
