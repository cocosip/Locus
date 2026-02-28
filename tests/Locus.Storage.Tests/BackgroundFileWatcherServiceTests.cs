using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class BackgroundFileWatcherServiceTests
    {
        [Fact]
        public async Task ExecuteAsync_FastWatcherIsNotSlowedBySlowWatcher()
        {
            var options = new FileWatcherOptions
            {
                Enabled = true,
                DefaultPollingInterval = TimeSpan.FromMilliseconds(100),
                MinimumPollingInterval = TimeSpan.FromMilliseconds(10),
                MaximumPollingInterval = TimeSpan.FromSeconds(5),
                DisabledCheckInterval = TimeSpan.FromMilliseconds(100)
            };

            var fastWatcher = new FileWatcherConfiguration
            {
                WatcherId = "fast",
                Enabled = true,
                PollingInterval = TimeSpan.FromMilliseconds(30),
                WatchPath = "/watch/fast"
            };
            var slowWatcher = new FileWatcherConfiguration
            {
                WatcherId = "slow",
                Enabled = true,
                PollingInterval = TimeSpan.FromMilliseconds(500),
                WatchPath = "/watch/slow"
            };

            var service = CreateService(options, new[] { fastWatcher, slowWatcher }, out var scanCounts);

            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(
                    () => GetCount(scanCounts, "fast") >= 1 && GetCount(scanCounts, "slow") >= 1,
                    TimeSpan.FromSeconds(2),
                    "Timed out waiting for initial watcher scan.");

                await Task.Delay(TimeSpan.FromMilliseconds(260));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            var fastCount = GetCount(scanCounts, "fast");
            var slowCount = GetCount(scanCounts, "slow");

            Assert.True(fastCount >= 4, $"Fast watcher should scan repeatedly; actual count={fastCount}.");
            Assert.Equal(1, slowCount);
        }

        [Fact]
        public async Task ExecuteAsync_SlowWatcherIsNotOverScannedByFastWatcher()
        {
            var options = new FileWatcherOptions
            {
                Enabled = true,
                DefaultPollingInterval = TimeSpan.FromMilliseconds(100),
                MinimumPollingInterval = TimeSpan.FromMilliseconds(10),
                MaximumPollingInterval = TimeSpan.FromSeconds(5),
                DisabledCheckInterval = TimeSpan.FromMilliseconds(100)
            };

            var fastWatcher = new FileWatcherConfiguration
            {
                WatcherId = "fast",
                Enabled = true,
                PollingInterval = TimeSpan.FromMilliseconds(40),
                WatchPath = "/watch/fast"
            };
            var slowWatcher = new FileWatcherConfiguration
            {
                WatcherId = "slow",
                Enabled = true,
                PollingInterval = TimeSpan.FromMilliseconds(250),
                WatchPath = "/watch/slow"
            };

            var service = CreateService(options, new[] { fastWatcher, slowWatcher }, out var scanCounts);

            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(
                    () => GetCount(scanCounts, "fast") >= 1 && GetCount(scanCounts, "slow") >= 1,
                    TimeSpan.FromSeconds(2),
                    "Timed out waiting for initial watcher scan.");

                await Task.Delay(TimeSpan.FromMilliseconds(620));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            var fastCount = GetCount(scanCounts, "fast");
            var slowCount = GetCount(scanCounts, "slow");

            Assert.True(fastCount >= slowCount * 2, $"Fast watcher should run much more often. Fast={fastCount}, Slow={slowCount}.");
            Assert.InRange(slowCount, 2, 4);
        }

        private static BackgroundFileWatcherService CreateService(
            FileWatcherOptions options,
            IReadOnlyCollection<FileWatcherConfiguration> watchers,
            out ConcurrentDictionary<string, int> scanCounts)
        {
            var fileWatcher = new Mock<IFileWatcher>();
            var optionsManager = new Mock<IFileWatcherOptionsManager>();

            var localCounts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
            scanCounts = localCounts;

            optionsManager
                .Setup(m => m.GetOptionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(options);
            optionsManager
                .Setup(m => m.IsServiceEnabledAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(options.Enabled);

            fileWatcher
                .Setup(m => m.GetAllWatchersAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(watchers);

            fileWatcher
                .Setup(m => m.ScanNowAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>((watcherId, _) =>
                {
                    localCounts.AddOrUpdate(watcherId, 1, (_, current) => current + 1);
                    return Task.FromResult(new FileWatcherScanResult());
                });

            return new BackgroundFileWatcherService(
                fileWatcher.Object,
                optionsManager.Object,
                NullLogger<BackgroundFileWatcherService>.Instance);
        }

        private static int GetCount(ConcurrentDictionary<string, int> counts, string watcherId)
        {
            return counts.TryGetValue(watcherId, out var value) ? value : 0;
        }

        private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, string timeoutMessage)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                    return;

                await Task.Delay(10);
            }

            Assert.True(condition(), timeoutMessage);
        }
    }
}
