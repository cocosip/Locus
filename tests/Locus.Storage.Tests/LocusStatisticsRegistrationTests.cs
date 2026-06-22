using System;
using System.IO;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Statistics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Locus.Storage.Tests
{
    public class LocusStatisticsRegistrationTests
    {
        [Theory]
        [InlineData("EnableMetricsBridge")]
        [InlineData("EnablePersistentHistory")]
        [InlineData("PersistentFlushInterval")]
        public void LocusStatisticsOptions_DoesNotExposeReservedMetricsOrPersistenceSwitches(string propertyName)
        {
            Assert.Null(typeof(LocusStatisticsOptions).GetProperty(propertyName));
        }

        [Fact]
        public void LocusStatisticsOptions_DefaultsUseOperationalFileProcessingWindows()
        {
            var options = new LocusStatisticsOptions();

            Assert.Equal(TimeSpan.FromMinutes(5), options.WindowSize);
            Assert.Equal(TimeSpan.FromHours(1), options.Retention);
            Assert.Equal(LocusStatisticsOptions.DefaultMaxSeries, options.MaxSeries);
            Assert.Equal(TimeSpan.FromMinutes(5), options.Output.Interval);
            Assert.Equal(TimeSpan.FromMinutes(15), options.Output.QueryWindow);
        }

        [Fact]
        public void AddLocus_WhenStatisticsDisabled_RegistersNoopRecorderAndReader()
        {
            var services = new ServiceCollection();

            services.AddLocus(options =>
            {
                AddVolume(options);
                options.Statistics.Enabled = false;
            });

            using var provider = services.BuildServiceProvider();

            Assert.Same(
                NoopLocusStatisticsRecorder.Instance,
                provider.GetRequiredService<ILocusStatisticsRecorder>());
            Assert.Same(
                NoopLocusStatisticsRecorder.Instance,
                provider.GetRequiredService<ILocusStatisticsReader>());
        }

        [Fact]
        public void AddLocus_WhenStatisticsEnabled_RegistersInMemoryRecorderAndReader()
        {
            var services = new ServiceCollection();

            services.AddLocus(options =>
            {
                AddVolume(options);
                options.Statistics.Enabled = true;
                options.Statistics.WindowSize = TimeSpan.FromSeconds(10);
                options.Statistics.Retention = TimeSpan.FromMinutes(5);
            });

            using var provider = services.BuildServiceProvider();

            var recorder = provider.GetRequiredService<ILocusStatisticsRecorder>();
            var reader = provider.GetRequiredService<ILocusStatisticsReader>();

            Assert.IsType<InMemoryLocusStatisticsRecorder>(recorder);
            Assert.Same(recorder, reader);
        }

        [Fact]
        public void AddLocus_WhenStatisticsRetentionIsSmallerThanWindow_Throws()
        {
            var services = new ServiceCollection();

            var ex = Assert.Throws<InvalidOperationException>(() => services.AddLocus(options =>
            {
                AddVolume(options);
                options.Statistics.Enabled = true;
                options.Statistics.WindowSize = TimeSpan.FromMinutes(10);
                options.Statistics.Retention = TimeSpan.FromMinutes(1);
            }));

            Assert.Contains("Statistics Retention", ex.Message, StringComparison.Ordinal);
        }

        [Theory]
        [InlineData(1_023)]
        [InlineData(262_145)]
        public void AddLocus_WhenStatisticsMaxSeriesIsOutsideAllowedRange_Throws(int maxSeries)
        {
            var services = new ServiceCollection();

            var ex = Assert.Throws<InvalidOperationException>(() => services.AddLocus(options =>
            {
                AddVolume(options);
                options.Statistics.Enabled = true;
                options.Statistics.MaxSeries = maxSeries;
            }));

            Assert.Contains("Statistics MaxSeries must be between 1024 and 262144", ex.Message, StringComparison.Ordinal);
        }

        private static void AddVolume(LocusOptions options)
        {
            options.Volumes.Add(new VolumeConfiguration
            {
                VolumeId = "vol-001",
                MountPath = Path.Combine(Path.GetTempPath(), "locus-statistics-registration"),
                VolumeType = "LocalFileSystem",
                InitialDelayMs = 0,
                HealthCheckDelayMs = 0
            });
        }
    }
}
