using System;
using System.IO;
using System.IO.Abstractions.TestingHelpers;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class FileWatcherAutoManagerTests
    {
        private readonly MockFileSystem _fileSystem;
        private readonly Mock<IFileWatcher> _fileWatcher;
        private readonly Mock<ITenantManager> _tenantManager;
        private readonly Mock<ILogger<FileWatcherAutoManager>> _logger;
        private readonly FileWatcherAutoManager _autoManager;
        private readonly string _configRoot;

        public FileWatcherAutoManagerTests()
        {
            _fileSystem = new MockFileSystem();
            _fileWatcher = new Mock<IFileWatcher>();
            _tenantManager = new Mock<ITenantManager>();
            _logger = new Mock<ILogger<FileWatcherAutoManager>>();
            _configRoot = Path.Combine("test-auto-manager");

            _autoManager = new FileWatcherAutoManager(
                _fileSystem,
                _fileWatcher.Object,
                _tenantManager.Object,
                _logger.Object,
                _configRoot);
        }

        [Fact]
        public async Task ApplyRootConfigurationAsync_MultiTenantMode_CreatesSingleWatcher()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = true,
                Enabled = true,
                MaxConcurrentImports = 4
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            // Act
            var watchersCreated = await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Assert
            Assert.Equal(1, watchersCreated);
            _fileWatcher.Verify(x => x.RegisterWatcherAsync(
                It.Is<FileWatcherConfiguration>(c =>
                    c.MultiTenantMode == true &&
                    c.WatchPath == rootPath &&
                    c.MaxConcurrentImports == 4),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ApplyRootConfigurationAsync_SingleTenantMode_CreatesWatcherWithDefaultTenant()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = false,
                Enabled = true
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            var mockTenant = new Mock<ITenantContext>();
            _tenantManager.Setup(x => x.GetTenantAsync("default", It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            // Act
            var watchersCreated = await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Assert
            Assert.Equal(1, watchersCreated);
            _fileWatcher.Verify(x => x.RegisterWatcherAsync(
                It.Is<FileWatcherConfiguration>(c =>
                    c.MultiTenantMode == false &&
                    c.TenantId == "default" &&
                    c.WatchPath == rootPath),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ApplyRootConfigurationAsync_SingleTenantMode_CreatesDefaultTenantIfNotExists()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = false
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            _tenantManager.Setup(x => x.GetTenantAsync("default", It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Tenant not found"));

            var mockTenant = new Mock<ITenantContext>();
            _tenantManager.Setup(x => x.CreateTenantAsync("default", It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            // Act
            var watchersCreated = await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Assert
            _tenantManager.Verify(x => x.CreateTenantAsync("default", It.IsAny<CancellationToken>()), Times.Once);
            Assert.Equal(1, watchersCreated);
        }

        [Fact]
        public async Task ApplyRootConfigurationAsync_ThrowsWhenRootPathIsNull()
        {
            // Arrange
            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = null!
            };

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(() =>
                _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None));
        }

        [Fact]
        public async Task ApplyRootConfigurationAsync_ThrowsWhenRootPathDoesNotExist()
        {
            // Arrange
            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = "/nonexistent-path"
            };

            // Act & Assert
            await Assert.ThrowsAsync<DirectoryNotFoundException>(() =>
                _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None));
        }

        [Fact]
        public async Task ApplyRootConfigurationAsync_UpdatesExistingWatcher()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = true,
                MaxConcurrentImports = 8
            };

            var existingConfig = new FileWatcherConfiguration
            {
                WatcherId = "auto-multi-tenant-12345678",
                MaxConcurrentImports = 4
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(existingConfig);

            // Act
            var watchersCreated = await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Assert
            Assert.Equal(1, watchersCreated);
            _fileWatcher.Verify(x => x.UpdateWatcherAsync(
                It.Is<FileWatcherConfiguration>(c => c.MaxConcurrentImports == 8),
                It.IsAny<CancellationToken>()), Times.Once);
            _fileWatcher.Verify(x => x.RegisterWatcherAsync(
                It.IsAny<FileWatcherConfiguration>(),
                It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task ApplyRootConfigurationAsync_SavesConfiguration()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = true
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            // Act
            await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Assert
            var configPath = Path.Combine(_configRoot, "root-config.json");
            Assert.True(_fileSystem.File.Exists(configPath));
        }

        [Fact]
        public async Task GetRootConfigurationAsync_ReturnsNullWhenNoConfigurationExists()
        {
            // Act
            var config = await _autoManager.GetRootConfigurationAsync(CancellationToken.None);

            // Assert
            Assert.Null(config);
        }

        [Fact]
        public async Task GetRootConfigurationAsync_ReturnsCurrentConfiguration()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = true,
                MaxConcurrentImports = 6
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Act
            var loadedConfig = await _autoManager.GetRootConfigurationAsync(CancellationToken.None);

            // Assert
            Assert.NotNull(loadedConfig);
            Assert.Equal(rootPath, loadedConfig.RootPath);
            Assert.True(loadedConfig.MultiTenantMode);
            Assert.Equal(6, loadedConfig.MaxConcurrentImports);
        }

        [Fact]
        public async Task RemoveAllWatchersAsync_RemovesAllManagedWatchers()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = true
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Act
            await _autoManager.RemoveAllWatchersAsync(CancellationToken.None);

            // Assert
            _fileWatcher.Verify(x => x.RemoveWatcherAsync(
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task RemoveAllWatchersAsync_DeletesConfigurationFile()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = true
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            var configPath = Path.Combine(_configRoot, "root-config.json");
            Assert.True(_fileSystem.File.Exists(configPath));

            // Act
            await _autoManager.RemoveAllWatchersAsync(CancellationToken.None);

            // Assert
            Assert.False(_fileSystem.File.Exists(configPath));
        }

        [Fact]
        public async Task RemoveAllWatchersAsync_ContinuesOnFailure()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = true
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            _fileWatcher.Setup(x => x.RemoveWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("Failed to remove watcher"));

            await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Act - should not throw
            await _autoManager.RemoveAllWatchersAsync(CancellationToken.None);

            // Assert - config file should still be deleted
            var configPath = Path.Combine(_configRoot, "root-config.json");
            Assert.False(_fileSystem.File.Exists(configPath));
        }

        [Fact]
        public async Task DiscoverAndCreateWatchersAsync_ReturnsZeroWhenNoConfigurationSet()
        {
            // Act
            var watchersCreated = await _autoManager.DiscoverAndCreateWatchersAsync(CancellationToken.None);

            // Assert
            Assert.Equal(0, watchersCreated);
        }

        [Fact]
        public async Task DiscoverAndCreateWatchersAsync_ReturnsZeroWhenNotMultiTenantMode()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = false
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            var mockTenant = new Mock<ITenantContext>();
            _tenantManager.Setup(x => x.GetTenantAsync("default", It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockTenant.Object);

            await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Act
            var watchersCreated = await _autoManager.DiscoverAndCreateWatchersAsync(CancellationToken.None);

            // Assert
            Assert.Equal(0, watchersCreated);
        }

        [Fact]
        public async Task DiscoverAndCreateWatchersAsync_ReturnsZeroWhenAutoDiscoveryDisabled()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = true,
                AutoDiscoverTenants = false
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Act
            var watchersCreated = await _autoManager.DiscoverAndCreateWatchersAsync(CancellationToken.None);

            // Assert
            Assert.Equal(0, watchersCreated);
        }

        [Fact]
        public async Task DiscoverAndCreateWatchersAsync_CreatesWatcherInMultiTenantMode()
        {
            // Arrange
            var rootPath = "/watch-root";
            _fileSystem.Directory.CreateDirectory(rootPath);

            var rootConfig = new FileWatcherRootConfiguration
            {
                RootPath = rootPath,
                MultiTenantMode = true,
                AutoDiscoverTenants = true
            };

            _fileWatcher.Setup(x => x.GetWatcherAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((FileWatcherConfiguration)null!);

            await _autoManager.ApplyRootConfigurationAsync(rootConfig, CancellationToken.None);

            // Act
            var watchersCreated = await _autoManager.DiscoverAndCreateWatchersAsync(CancellationToken.None);

            // Assert
            Assert.Equal(1, watchersCreated);
        }
    }
}
