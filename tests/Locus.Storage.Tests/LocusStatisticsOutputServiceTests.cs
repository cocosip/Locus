using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Statistics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class LocusStatisticsOutputServiceTests
    {
        [Fact]
        public void AddLocus_WhenStatisticsOutputEnabled_RegistersHostedOutputService()
        {
            var services = new ServiceCollection();
            services.AddLogging();

            services.AddLocus(options =>
            {
                AddVolume(options);
                options.Statistics.Enabled = true;
                options.Statistics.Output.Enabled = true;
                options.Statistics.Output.Interval = TimeSpan.FromSeconds(30);
                options.Statistics.Output.QueryWindow = TimeSpan.FromMinutes(1);
            });

            using var provider = services.BuildServiceProvider();

            Assert.Contains(
                provider.GetServices<IHostedService>(),
                service => service.GetType().Name == "LocusStatisticsOutputService");
        }

        [Fact]
        public async Task RunOnceAsync_WhenSnapshotHasValues_LogsSummary()
        {
            var statistics = new InMemoryLocusStatisticsRecorder(new LocusStatisticsOptions
            {
                Enabled = true,
                WindowSize = TimeSpan.FromMinutes(1),
                Retention = TimeSpan.FromMinutes(5)
            });
            statistics.Record("storage.write.success.count", 2, DateTimeOffset.UtcNow);
            statistics.Record("storage.write.bytes", 2 * 1024 * 1024, DateTimeOffset.UtcNow);
            statistics.Record("storage.file.dequeued.count", 1, DateTimeOffset.UtcNow);
            var logger = new Mock<ILogger<LocusStatisticsOutputService>>();
            var service = new TestLocusStatisticsOutputService(
                statistics,
                new LocusStatisticsOptions
                {
                    Enabled = true,
                    Output = new LocusStatisticsOutputOptions
                    {
                        Enabled = true,
                        QueryWindow = TimeSpan.FromMinutes(1),
                        Interval = TimeSpan.FromMinutes(1)
                    }
                },
                logger.Object);

            await service.RunOnceAsync(CancellationToken.None);

            Assert.Contains(
                logger.Invocations,
                invocation => invocation.Method.Name == nameof(ILogger.Log)
                    && invocation.Arguments.Count >= 3
                    && invocation.Arguments[2]?.ToString()?.Contains("Locus statistics summary") == true);
        }

        private static void AddVolume(LocusOptions options)
        {
            options.Volumes.Add(new VolumeConfiguration
            {
                VolumeId = "vol-001",
                MountPath = Path.Combine(Path.GetTempPath(), "locus-statistics-output"),
                VolumeType = "LocalFileSystem",
                InitialDelayMs = 0,
                HealthCheckDelayMs = 0
            });
        }

        private sealed class TestLocusStatisticsOutputService : LocusStatisticsOutputService
        {
            public TestLocusStatisticsOutputService(
                ILocusStatisticsReader reader,
                LocusStatisticsOptions options,
                ILogger<LocusStatisticsOutputService> logger)
                : base(reader, options, logger)
            {
            }

            public Task RunOnceAsync(CancellationToken ct)
            {
                return WriteSnapshotAsync(ct);
            }
        }
    }
}
