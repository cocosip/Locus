using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
                DisabledCheckInterval = TimeSpan.FromMilliseconds(100),
                MaxParallelWatcherScans = 4
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
                DisabledCheckInterval = TimeSpan.FromMilliseconds(100),
                MaxParallelWatcherScans = 4
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

        [Fact]
        public async Task ExecuteAsync_RespectsMaxParallelWatcherScans()
        {
            var options = new FileWatcherOptions
            {
                Enabled = true,
                DefaultPollingInterval = TimeSpan.FromMilliseconds(100),
                MinimumPollingInterval = TimeSpan.FromMilliseconds(10),
                MaximumPollingInterval = TimeSpan.FromSeconds(5),
                DisabledCheckInterval = TimeSpan.FromMilliseconds(100),
                MaxParallelWatcherScans = 2
            };

            var watchers = new[]
            {
                new FileWatcherConfiguration { WatcherId = "w1", Enabled = true, PollingInterval = TimeSpan.FromSeconds(5), WatchPath = "/watch/w1" },
                new FileWatcherConfiguration { WatcherId = "w2", Enabled = true, PollingInterval = TimeSpan.FromSeconds(5), WatchPath = "/watch/w2" },
                new FileWatcherConfiguration { WatcherId = "w3", Enabled = true, PollingInterval = TimeSpan.FromSeconds(5), WatchPath = "/watch/w3" },
                new FileWatcherConfiguration { WatcherId = "w4", Enabled = true, PollingInterval = TimeSpan.FromSeconds(5), WatchPath = "/watch/w4" }
            };

            var fileWatcher = new Mock<IFileWatcher>();
            var optionsManager = new Mock<IFileWatcherOptionsManager>();
            var scanCounts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
            var inFlight = 0;
            var maxInFlight = 0;

            optionsManager
                .Setup(m => m.GetOptionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(options);
            optionsManager
                .Setup(m => m.IsServiceEnabledAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            fileWatcher
                .Setup(m => m.GetAllWatchersAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(watchers);

            fileWatcher
                .Setup(m => m.ScanNowAsync(It.IsAny<FileWatcherConfiguration>(), It.IsAny<CancellationToken>()))
                .Returns<FileWatcherConfiguration, CancellationToken>(async (watcher, token) =>
                {
                    scanCounts.AddOrUpdate(watcher.WatcherId, 1, (_, current) => current + 1);

                    var currentInFlight = Interlocked.Increment(ref inFlight);
                    UpdateMax(ref maxInFlight, currentInFlight);

                    try
                    {
                        await Task.Delay(120, token);
                        return new FileWatcherScanResult();
                    }
                    finally
                    {
                        Interlocked.Decrement(ref inFlight);
                    }
                });

            var service = new BackgroundFileWatcherService(
                fileWatcher.Object,
                optionsManager.Object,
                NullLogger<BackgroundFileWatcherService>.Instance);

            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(
                    () => watchers.All(w => GetCount(scanCounts, w.WatcherId) >= 1),
                    TimeSpan.FromSeconds(3),
                    "Timed out waiting for all watchers to be scanned at least once.");
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            Assert.InRange(maxInFlight, 2, 2);
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
                .Setup(m => m.ScanNowAsync(It.IsAny<FileWatcherConfiguration>(), It.IsAny<CancellationToken>()))
                .Returns<FileWatcherConfiguration, CancellationToken>((watcher, _) =>
                {
                    localCounts.AddOrUpdate(watcher.WatcherId, 1, (_, current) => current + 1);
                    return Task.FromResult(new FileWatcherScanResult());
                });

            return new BackgroundFileWatcherService(
                fileWatcher.Object,
                optionsManager.Object,
                NullLogger<BackgroundFileWatcherService>.Instance);
        }

        [Fact]
        public async Task ExecuteAsync_UsesConfigurationScanOverload()
        {
            var options = new FileWatcherOptions
            {
                Enabled = true,
                DefaultPollingInterval = TimeSpan.FromMilliseconds(100),
                MinimumPollingInterval = TimeSpan.FromMilliseconds(10),
                MaximumPollingInterval = TimeSpan.FromSeconds(5),
                DisabledCheckInterval = TimeSpan.FromMilliseconds(100),
                MaxParallelWatcherScans = 1
            };

            var watcher = new FileWatcherConfiguration
            {
                WatcherId = "single",
                Enabled = true,
                PollingInterval = TimeSpan.FromSeconds(5),
                WatchPath = "/watch/single"
            };

            var fileWatcher = new Mock<IFileWatcher>(MockBehavior.Strict);
            var optionsManager = new Mock<IFileWatcherOptionsManager>(MockBehavior.Strict);
            var scanned = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            optionsManager
                .Setup(m => m.GetOptionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(options);
            optionsManager
                .Setup(m => m.IsServiceEnabledAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);

            fileWatcher
                .Setup(m => m.GetAllWatchersAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new[] { watcher });
            fileWatcher
                .Setup(m => m.ScanNowAsync(It.IsAny<FileWatcherConfiguration>(), It.IsAny<CancellationToken>()))
                .Returns<FileWatcherConfiguration, CancellationToken>((cfg, _) =>
                {
                    scanned.TrySetResult(cfg == watcher);
                    return Task.FromResult(new FileWatcherScanResult());
                });
            fileWatcher
                .Setup(m => m.ScanNowAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new InvalidOperationException("String-based overload should not be used by background service."));

            var service = new BackgroundFileWatcherService(
                fileWatcher.Object,
                optionsManager.Object,
                NullLogger<BackgroundFileWatcherService>.Instance);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await service.StartAsync(CancellationToken.None);
            try
            {
                await Task.WhenAny(scanned.Task, Task.Delay(Timeout.InfiniteTimeSpan, cts.Token));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            Assert.True(scanned.Task.IsCompletedSuccessfully);
            Assert.True(await scanned.Task);
            fileWatcher.Verify(m => m.ScanNowAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        private static int GetCount(ConcurrentDictionary<string, int> counts, string watcherId)
        {
            return counts.TryGetValue(watcherId, out var value) ? value : 0;
        }

        private static void UpdateMax(ref int target, int candidate)
        {
            while (true)
            {
                var current = Volatile.Read(ref target);
                if (candidate <= current)
                    return;

                if (Interlocked.CompareExchange(ref target, candidate, current) == current)
                    return;
            }
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
