using System;
using System.IO;
using System.IO.Abstractions.TestingHelpers;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class FileWatcherOptionsManagerTests
    {
        private readonly MockFileSystem _fileSystem;
        private readonly Mock<ILogger<FileWatcherOptionsManager>> _logger;
        private readonly FileWatcherOptionsManager _optionsManager;
        private readonly string _configRoot;

        public FileWatcherOptionsManagerTests()
        {
            _fileSystem = new MockFileSystem();
            _logger = new Mock<ILogger<FileWatcherOptionsManager>>();
            _configRoot = Path.Combine("test-config");

            _optionsManager = new FileWatcherOptionsManager(
                _fileSystem,
                _logger.Object,
                _configRoot);
        }

        [Fact]
        public async Task GetOptionsAsync_CreatesDefaultConfiguration()
        {
            // Act
            var options = await _optionsManager.GetOptionsAsync(CancellationToken.None);

            // Assert
            Assert.NotNull(options);
            Assert.True(options.Enabled);
            Assert.Equal(TimeSpan.FromSeconds(30), options.DefaultPollingInterval);
            Assert.Equal(TimeSpan.FromSeconds(5), options.MinimumPollingInterval);
            Assert.Equal(TimeSpan.FromHours(1), options.MaximumPollingInterval);
            Assert.Equal(TimeSpan.FromMinutes(1), options.DisabledCheckInterval);

            // Verify config file was created
            var configPath = Path.Combine(_configRoot, "file-watcher-options.json");
            Assert.True(_fileSystem.File.Exists(configPath));
        }

        [Fact]
        public async Task UpdateOptionsAsync_SavesConfiguration()
        {
            // Arrange
            var newOptions = new FileWatcherOptions
            {
                Enabled = false,
                DefaultPollingInterval = TimeSpan.FromSeconds(10),
                MinimumPollingInterval = TimeSpan.FromSeconds(5),
                MaximumPollingInterval = TimeSpan.FromMinutes(10),
                DisabledCheckInterval = TimeSpan.FromMinutes(2)
            };

            // Act
            await _optionsManager.UpdateOptionsAsync(newOptions, CancellationToken.None);

            // Assert
            var loadedOptions = await _optionsManager.GetOptionsAsync(CancellationToken.None);
            Assert.False(loadedOptions.Enabled);
            Assert.Equal(TimeSpan.FromSeconds(10), loadedOptions.DefaultPollingInterval);
            Assert.Equal(TimeSpan.FromMinutes(2), loadedOptions.DisabledCheckInterval);
        }

        [Fact]
        public async Task UpdateOptionsAsync_ThrowsWhenDefaultIntervalTooLow()
        {
            // Arrange
            var invalidOptions = new FileWatcherOptions
            {
                DefaultPollingInterval = TimeSpan.FromSeconds(1),  // Less than minimum
                MinimumPollingInterval = TimeSpan.FromSeconds(5)
            };

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(() =>
                _optionsManager.UpdateOptionsAsync(invalidOptions, CancellationToken.None));
        }

        [Fact]
        public async Task UpdateOptionsAsync_ThrowsWhenDefaultIntervalTooHigh()
        {
            // Arrange
            var invalidOptions = new FileWatcherOptions
            {
                DefaultPollingInterval = TimeSpan.FromHours(2),  // Greater than maximum
                MaximumPollingInterval = TimeSpan.FromHours(1)
            };

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(() =>
                _optionsManager.UpdateOptionsAsync(invalidOptions, CancellationToken.None));
        }

        [Fact]
        public async Task EnableServiceAsync_EnablesService()
        {
            // Arrange - disable first
            await _optionsManager.DisableServiceAsync(CancellationToken.None);
            Assert.False(await _optionsManager.IsServiceEnabledAsync(CancellationToken.None));

            // Act
            await _optionsManager.EnableServiceAsync(CancellationToken.None);

            // Assert
            Assert.True(await _optionsManager.IsServiceEnabledAsync(CancellationToken.None));
        }

        [Fact]
        public async Task DisableServiceAsync_DisablesService()
        {
            // Arrange - ensure enabled first
            await _optionsManager.EnableServiceAsync(CancellationToken.None);
            Assert.True(await _optionsManager.IsServiceEnabledAsync(CancellationToken.None));

            // Act
            await _optionsManager.DisableServiceAsync(CancellationToken.None);

            // Assert
            Assert.False(await _optionsManager.IsServiceEnabledAsync(CancellationToken.None));
        }

        [Fact]
        public async Task IsServiceEnabledAsync_ReturnsTrueByDefault()
        {
            // Act
            var isEnabled = await _optionsManager.IsServiceEnabledAsync(CancellationToken.None);

            // Assert
            Assert.True(isEnabled);
        }

        [Fact]
        public async Task GetOptionsAsync_UsesCachedOptions()
        {
            // Arrange
            var options1 = await _optionsManager.GetOptionsAsync(CancellationToken.None);

            // Modify file system directly (simulate external change)
            var configPath = Path.Combine(_configRoot, "file-watcher-options.json");
            _fileSystem.File.WriteAllText(configPath, "{\"enabled\":false}");

            // Act - should still return cached options
            var options2 = await _optionsManager.GetOptionsAsync(CancellationToken.None);

            // Assert - cached options still have Enabled = true
            Assert.True(options2.Enabled);
            Assert.Same(options1, options2);  // Same instance
        }

        [Fact]
        public async Task UpdateOptionsAsync_InvalidatesCacheAndReloads()
        {
            // Arrange
            var options1 = await _optionsManager.GetOptionsAsync(CancellationToken.None);
            Assert.True(options1.Enabled);

            // Act - update through API
            var newOptions = new FileWatcherOptions
            {
                Enabled = false,
                DefaultPollingInterval = TimeSpan.FromSeconds(15),
                MinimumPollingInterval = TimeSpan.FromSeconds(5),
                MaximumPollingInterval = TimeSpan.FromHours(1)
            };
            await _optionsManager.UpdateOptionsAsync(newOptions, CancellationToken.None);

            // Assert - cache should be updated
            var options2 = await _optionsManager.GetOptionsAsync(CancellationToken.None);
            Assert.False(options2.Enabled);
            Assert.Equal(TimeSpan.FromSeconds(15), options2.DefaultPollingInterval);
        }

        [Fact]
        public async Task EnableServiceAsync_IdempotentWhenAlreadyEnabled()
        {
            // Arrange
            await _optionsManager.EnableServiceAsync(CancellationToken.None);

            // Act - enable again
            await _optionsManager.EnableServiceAsync(CancellationToken.None);

            // Assert - should not throw
            Assert.True(await _optionsManager.IsServiceEnabledAsync(CancellationToken.None));
        }

        [Fact]
        public async Task DisableServiceAsync_IdempotentWhenAlreadyDisabled()
        {
            // Arrange
            await _optionsManager.DisableServiceAsync(CancellationToken.None);

            // Act - disable again
            await _optionsManager.DisableServiceAsync(CancellationToken.None);

            // Assert - should not throw
            Assert.False(await _optionsManager.IsServiceEnabledAsync(CancellationToken.None));
        }

        [Fact]
        public async Task GetOptionsAsync_LoadsExistingConfiguration()
        {
            // Arrange - create config file manually
            var configPath = Path.Combine(_configRoot, "file-watcher-options.json");
            _fileSystem.Directory.CreateDirectory(_configRoot);
            _fileSystem.File.WriteAllText(configPath, @"{
                ""enabled"": false,
                ""defaultPollingInterval"": ""00:00:20"",
                ""minimumPollingInterval"": ""00:00:03"",
                ""maximumPollingInterval"": ""00:30:00"",
                ""disabledCheckInterval"": ""00:05:00""
            }");

            // Act
            var manager = new FileWatcherOptionsManager(_fileSystem, _logger.Object, _configRoot);
            var options = await manager.GetOptionsAsync(CancellationToken.None);

            // Assert
            Assert.False(options.Enabled);
            Assert.Equal(TimeSpan.FromSeconds(20), options.DefaultPollingInterval);
            Assert.Equal(TimeSpan.FromSeconds(3), options.MinimumPollingInterval);
            Assert.Equal(TimeSpan.FromMinutes(30), options.MaximumPollingInterval);
            Assert.Equal(TimeSpan.FromMinutes(5), options.DisabledCheckInterval);
        }
    }
}
