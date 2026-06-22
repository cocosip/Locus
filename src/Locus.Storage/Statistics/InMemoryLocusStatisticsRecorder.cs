using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Locus.Core.Abstractions;
using Locus.Core.Models;

namespace Locus.Storage.Statistics
{
    /// <summary>
    /// In-memory statistics recorder backed by fixed-size time buckets.
    /// </summary>
    public sealed class InMemoryLocusStatisticsRecorder : ILocusStatisticsRecorder, ILocusStatisticsReader
    {
        private const long PruneEveryRecords = 1024;

        private readonly LocusStatisticsOptions _options;
        private readonly ConcurrentDictionary<BucketKey, CounterCell> _counters;
        private long _recordCount;

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryLocusStatisticsRecorder"/> class.
        /// </summary>
        public InMemoryLocusStatisticsRecorder(LocusStatisticsOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _options.Validate();
            _counters = new ConcurrentDictionary<BucketKey, CounterCell>();
        }

        /// <inheritdoc/>
        public void Record(
            string name,
            long value,
            DateTimeOffset timestamp,
            IReadOnlyDictionary<string, string?>? dimensions = null)
        {
            if (string.IsNullOrWhiteSpace(name) || value == 0)
                return;

            var key = CreateKey(name, timestamp, dimensions);
            var cell = _counters.GetOrAdd(key, _ => new CounterCell());
            cell.Add(value);

            if (Interlocked.Increment(ref _recordCount) % PruneEveryRecords == 0)
                Prune(timestamp);
        }

        /// <inheritdoc/>
        public LocusStatisticsSnapshot GetSnapshot(LocusStatisticsQuery query)
        {
            var snapshot = new LocusStatisticsSnapshot();
            if (query.To <= query.From)
                return snapshot;

            Prune(DateTimeOffset.UtcNow);

            var totals = new Dictionary<AggregateKey, long>();
            foreach (var kvp in _counters)
            {
                var key = kvp.Key;
                var bucketEndTicks = key.BucketStartTicks + _options.WindowSize.Ticks;
                if (bucketEndTicks <= query.From.UtcTicks || key.BucketStartTicks >= query.To.UtcTicks)
                    continue;

                if (!Matches(query.TenantId, key.TenantId)
                    || !Matches(query.VolumeId, key.VolumeId)
                    || !Matches(query.WatcherId, key.WatcherId)
                    || !Matches(query.Operation, key.Operation))
                {
                    continue;
                }

                var aggregateKey = new AggregateKey(
                    key.Name,
                    key.TenantId,
                    key.VolumeId,
                    key.WatcherId,
                    key.Operation);
                var value = kvp.Value.Read();
                totals.TryGetValue(aggregateKey, out var current);
                totals[aggregateKey] = current + value;
            }

            var seconds = (query.To - query.From).TotalSeconds;
            foreach (var kvp in totals)
            {
                var key = kvp.Key;
                var value = kvp.Value;
                var dimensions = key.ToDimensions();
                AddKnownTotal(snapshot, key.Name, value);
                snapshot.Measurements.Add(new LocusStatisticsMeasurement(
                    key.Name,
                    value,
                    dimensions));

                if (seconds > 0 && string.Equals(key.Name, "storage.write.bytes", StringComparison.Ordinal))
                {
                    snapshot.Measurements.Add(new LocusStatisticsMeasurement(
                        "storage.write.megabytes_per_second",
                        value / 1024d / 1024d / seconds,
                        dimensions));
                }
            }

            if (seconds > 0)
                snapshot.WriteMegabytesPerSecond = snapshot.WriteBytes / 1024d / 1024d / seconds;

            return snapshot;
        }

        private BucketKey CreateKey(
            string name,
            DateTimeOffset timestamp,
            IReadOnlyDictionary<string, string?>? dimensions)
        {
            var bucketStartTicks = timestamp.UtcTicks - (timestamp.UtcTicks % _options.WindowSize.Ticks);
            return new BucketKey(
                bucketStartTicks,
                name,
                GetDimension(dimensions, "tenant_id", _options.Dimensions.TenantId),
                GetDimension(dimensions, "volume_id", _options.Dimensions.VolumeId),
                GetDimension(dimensions, "watcher_id", _options.Dimensions.WatcherId),
                GetDimension(dimensions, "operation", _options.Dimensions.Operation));
        }

        private static string? GetDimension(
            IReadOnlyDictionary<string, string?>? dimensions,
            string name,
            bool enabled)
        {
            if (!enabled || dimensions == null)
                return null;

            return dimensions.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value)
                ? value
                : null;
        }

        private void Prune(DateTimeOffset now)
        {
            var cutoffTicks = now.UtcTicks - _options.Retention.Ticks - _options.WindowSize.Ticks;
            foreach (var key in _counters.Keys)
            {
                if (key.BucketStartTicks < cutoffTicks)
                    _counters.TryRemove(key, out _);
            }
        }

        private static bool Matches(string? requested, string? actual)
        {
            return string.IsNullOrEmpty(requested) || string.Equals(requested, actual, StringComparison.Ordinal);
        }

        private static void AddKnownTotal(LocusStatisticsSnapshot snapshot, string name, long value)
        {
            switch (name)
            {
                case "storage.write.success.count":
                    snapshot.WriteFileCount += value;
                    break;
                case "storage.write.bytes":
                    snapshot.WriteBytes += value;
                    break;
                case "storage.file.dequeued.count":
                    snapshot.DequeuedFileCount += value;
                    break;
                case "storage.file.read.count":
                    snapshot.ReadFileCount += value;
                    break;
                case "storage.file.completed.count":
                    snapshot.CompletedFileCount += value;
                    break;
                case "metadata.sqlite.persisted.operation.count":
                    snapshot.SqlitePersistedOperationCount += value;
                    break;
                case "watcher.files.imported":
                    snapshot.WatcherImportedFileCount += value;
                    break;
                case "watcher.bytes.imported":
                    snapshot.WatcherImportedBytes += value;
                    break;
            }
        }

        private sealed class CounterCell
        {
            private long _value;

            public void Add(long value)
            {
                Interlocked.Add(ref _value, value);
            }

            public long Read()
            {
                return Interlocked.Read(ref _value);
            }
        }

        private readonly struct BucketKey : IEquatable<BucketKey>
        {
            public readonly long BucketStartTicks;
            public readonly string Name;
            public readonly string? TenantId;
            public readonly string? VolumeId;
            public readonly string? WatcherId;
            public readonly string? Operation;

            public BucketKey(long bucketStartTicks, string name, string? tenantId, string? volumeId, string? watcherId, string? operation)
            {
                BucketStartTicks = bucketStartTicks;
                Name = name;
                TenantId = tenantId;
                VolumeId = volumeId;
                WatcherId = watcherId;
                Operation = operation;
            }

            public bool Equals(BucketKey other)
            {
                return BucketStartTicks == other.BucketStartTicks
                    && string.Equals(Name, other.Name, StringComparison.Ordinal)
                    && string.Equals(TenantId, other.TenantId, StringComparison.Ordinal)
                    && string.Equals(VolumeId, other.VolumeId, StringComparison.Ordinal)
                    && string.Equals(WatcherId, other.WatcherId, StringComparison.Ordinal)
                    && string.Equals(Operation, other.Operation, StringComparison.Ordinal);
            }

            public override bool Equals(object? obj)
            {
                return obj is BucketKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hash = BucketStartTicks.GetHashCode();
                    hash = (hash * 397) ^ StringComparer.Ordinal.GetHashCode(Name);
                    hash = (hash * 397) ^ (TenantId == null ? 0 : StringComparer.Ordinal.GetHashCode(TenantId));
                    hash = (hash * 397) ^ (VolumeId == null ? 0 : StringComparer.Ordinal.GetHashCode(VolumeId));
                    hash = (hash * 397) ^ (WatcherId == null ? 0 : StringComparer.Ordinal.GetHashCode(WatcherId));
                    hash = (hash * 397) ^ (Operation == null ? 0 : StringComparer.Ordinal.GetHashCode(Operation));
                    return hash;
                }
            }
        }

        private readonly struct AggregateKey : IEquatable<AggregateKey>
        {
            public readonly string Name;
            public readonly string? TenantId;
            public readonly string? VolumeId;
            public readonly string? WatcherId;
            public readonly string? Operation;

            public AggregateKey(string name, string? tenantId, string? volumeId, string? watcherId, string? operation)
            {
                Name = name;
                TenantId = tenantId;
                VolumeId = volumeId;
                WatcherId = watcherId;
                Operation = operation;
            }

            public IReadOnlyDictionary<string, string> ToDimensions()
            {
                var dimensions = new Dictionary<string, string>(StringComparer.Ordinal);
                if (TenantId != null)
                    dimensions["tenant_id"] = TenantId;
                if (VolumeId != null)
                    dimensions["volume_id"] = VolumeId;
                if (WatcherId != null)
                    dimensions["watcher_id"] = WatcherId;
                if (Operation != null)
                    dimensions["operation"] = Operation;
                return dimensions;
            }

            public bool Equals(AggregateKey other)
            {
                return string.Equals(Name, other.Name, StringComparison.Ordinal)
                    && string.Equals(TenantId, other.TenantId, StringComparison.Ordinal)
                    && string.Equals(VolumeId, other.VolumeId, StringComparison.Ordinal)
                    && string.Equals(WatcherId, other.WatcherId, StringComparison.Ordinal)
                    && string.Equals(Operation, other.Operation, StringComparison.Ordinal);
            }

            public override bool Equals(object? obj)
            {
                return obj is AggregateKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hash = StringComparer.Ordinal.GetHashCode(Name);
                    hash = (hash * 397) ^ (TenantId == null ? 0 : StringComparer.Ordinal.GetHashCode(TenantId));
                    hash = (hash * 397) ^ (VolumeId == null ? 0 : StringComparer.Ordinal.GetHashCode(VolumeId));
                    hash = (hash * 397) ^ (WatcherId == null ? 0 : StringComparer.Ordinal.GetHashCode(WatcherId));
                    hash = (hash * 397) ^ (Operation == null ? 0 : StringComparer.Ordinal.GetHashCode(Operation));
                    return hash;
                }
            }
        }
    }
}
