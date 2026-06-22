using System;
using System.Collections.Generic;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Statistics;
using Xunit;

namespace Locus.Storage.Tests
{
    public class LocusStatisticsRecorderTests
    {
        [Fact]
        public void NoopRecorder_WhenQueried_ReturnsEmptySnapshot()
        {
            ILocusStatisticsRecorder recorder = NoopLocusStatisticsRecorder.Instance;
            recorder.Record("storage.write.success.count", 1, DateTimeOffset.UtcNow);

            var snapshot = ((ILocusStatisticsReader)recorder).GetSnapshot(new LocusStatisticsQuery
            {
                From = DateTimeOffset.UtcNow.AddMinutes(-1),
                To = DateTimeOffset.UtcNow.AddMinutes(1)
            });

            Assert.Empty(snapshot.Measurements);
            Assert.Equal(0, snapshot.WriteFileCount);
            Assert.Equal(0, snapshot.WriteBytes);
            Assert.Equal(0, snapshot.WriteMegabytesPerSecond);
        }

        [Fact]
        public void InMemoryRecorder_WhenEventsFallInsideQueryWindow_AggregatesCountsAndRates()
        {
            var options = new LocusStatisticsOptions
            {
                Enabled = true,
                WindowSize = TimeSpan.FromSeconds(10),
                Retention = TimeSpan.FromMinutes(5)
            };
            var recorder = new InMemoryLocusStatisticsRecorder(options);
            var at = new DateTimeOffset(2026, 6, 22, 10, 0, 5, TimeSpan.Zero);

            recorder.Record("storage.write.success.count", 2, at);
            recorder.Record("storage.write.bytes", 4 * 1024 * 1024, at);
            recorder.Record("storage.file.dequeued.count", 1, at);
            recorder.Record("watcher.files.imported", 3, at);
            recorder.Record("watcher.bytes.imported", 2 * 1024 * 1024, at);

            var snapshot = recorder.GetSnapshot(new LocusStatisticsQuery
            {
                From = at.AddSeconds(-5),
                To = at.AddSeconds(5)
            });

            Assert.Equal(2, snapshot.WriteFileCount);
            Assert.Equal(4 * 1024 * 1024, snapshot.WriteBytes);
            Assert.Equal(1, snapshot.DequeuedFileCount);
            Assert.Equal(3, snapshot.WatcherImportedFileCount);
            Assert.Equal(2 * 1024 * 1024, snapshot.WatcherImportedBytes);
            Assert.Equal(0.4, snapshot.WriteMegabytesPerSecond, precision: 3);
            Assert.Contains(snapshot.Measurements, m => m.Name == "storage.write.bytes" && m.Value == 4 * 1024 * 1024);
            Assert.Contains(snapshot.Measurements, m => m.Name == "storage.write.megabytes_per_second" && Math.Abs(m.Value - 0.4) < 0.0001);
        }

        [Fact]
        public void InMemoryRecorder_WhenTenantDimensionDisabled_DoesNotSplitByTenantId()
        {
            var options = new LocusStatisticsOptions
            {
                Enabled = true,
                WindowSize = TimeSpan.FromMinutes(1),
                Retention = TimeSpan.FromMinutes(5),
                Dimensions = new LocusStatisticsDimensionOptions
                {
                    TenantId = false,
                    VolumeId = true
                }
            };
            var recorder = new InMemoryLocusStatisticsRecorder(options);
            var at = new DateTimeOffset(2026, 6, 22, 10, 0, 0, TimeSpan.Zero);

            recorder.Record(
                "storage.write.success.count",
                1,
                at,
                new Dictionary<string, string?>
                {
                    ["tenant_id"] = "tenant-a",
                    ["volume_id"] = "vol-001"
                });
            recorder.Record(
                "storage.write.success.count",
                1,
                at,
                new Dictionary<string, string?>
                {
                    ["tenant_id"] = "tenant-b",
                    ["volume_id"] = "vol-001"
                });

            var snapshot = recorder.GetSnapshot(new LocusStatisticsQuery
            {
                From = at.AddMinutes(-1),
                To = at.AddMinutes(1),
                VolumeId = "vol-001"
            });

            Assert.Single(snapshot.Measurements);
            Assert.Equal(2, snapshot.WriteFileCount);
            Assert.Equal("vol-001", snapshot.Measurements[0].Dimensions["volume_id"]);
            Assert.False(snapshot.Measurements[0].Dimensions.ContainsKey("tenant_id"));
        }
    }
}
