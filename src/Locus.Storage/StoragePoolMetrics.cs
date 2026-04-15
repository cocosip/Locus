using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Locus.Storage
{
    internal static class StoragePoolMetrics
    {
        private static readonly Meter Meter = new Meter("Locus.Storage.StoragePool", "1.0.0");
        private static readonly Counter<long> WriteRequestCounter = Meter.CreateCounter<long>(
            "locus.storage_pool.write.request.count",
            unit: "{request}",
            description: "Number of write requests received by StoragePool.");
        private static readonly Counter<long> WriteSuccessCounter = Meter.CreateCounter<long>(
            "locus.storage_pool.write.success.count",
            unit: "{request}",
            description: "Number of StoragePool write requests completed successfully.");
        private static readonly Counter<long> WriteFailureCounter = Meter.CreateCounter<long>(
            "locus.storage_pool.write.failure.count",
            unit: "{request}",
            description: "Number of StoragePool write requests that failed.");
        private static readonly Counter<long> WriteRetryCounter = Meter.CreateCounter<long>(
            "locus.storage_pool.write.retry.count",
            unit: "{retry}",
            description: "Number of StoragePool write retries after retryable volume-write failures.");
        private static readonly Counter<long> CleanupFailureCounter = Meter.CreateCounter<long>(
            "locus.storage_pool.write.cleanup_failure.count",
            unit: "{failure}",
            description: "Number of cleanup failures after StoragePool write errors.");
        private static readonly Counter<long> QuotaRollbackFailureCounter = Meter.CreateCounter<long>(
            "locus.storage_pool.write.quota_rollback_failure.count",
            unit: "{failure}",
            description: "Number of quota rollback failures after StoragePool write errors.");
        private static readonly Histogram<long> WriteBytesHistogram = Meter.CreateHistogram<long>(
            "locus.storage_pool.write.bytes",
            unit: "By",
            description: "Bytes requested per StoragePool write.");
        private static readonly Histogram<double> WriteDurationHistogram = Meter.CreateHistogram<double>(
            "locus.storage_pool.write.duration",
            unit: "ms",
            description: "End-to-end latency of StoragePool write requests.");
        private static readonly Histogram<double> TenantQuotaDurationHistogram = Meter.CreateHistogram<double>(
            "locus.storage_pool.write.tenant_quota.duration",
            unit: "ms",
            description: "Latency spent reserving tenant quota during StoragePool writes.");
        private static readonly Histogram<double> DirectoryQuotaDurationHistogram = Meter.CreateHistogram<double>(
            "locus.storage_pool.write.directory_quota.duration",
            unit: "ms",
            description: "Latency spent reserving directory quota during StoragePool writes.");
        private static readonly Histogram<double> VolumeWriteDurationHistogram = Meter.CreateHistogram<double>(
            "locus.storage_pool.write.volume_write.duration",
            unit: "ms",
            description: "Latency spent writing physical bytes to a storage volume.");
        private static readonly Histogram<double> AcceptanceDurationHistogram = Meter.CreateHistogram<double>(
            "locus.storage_pool.write.acceptance.duration",
            unit: "ms",
            description: "Latency spent completing the accepted-write path before projection enqueue.");
        private static readonly Histogram<double> ProjectionEnqueueDurationHistogram = Meter.CreateHistogram<double>(
            "locus.storage_pool.write.projection_enqueue.duration",
            unit: "ms",
            description: "Latency spent enqueueing accepted metadata projection writes.");
        private static readonly Histogram<long> RetryCountHistogram = Meter.CreateHistogram<long>(
            "locus.storage_pool.write.retry_attempts",
            unit: "{retry}",
            description: "Retry attempts used by successful or failed StoragePool writes.");

        public static void RecordWriteStarted(long? bytes)
        {
            WriteRequestCounter.Add(1);
            if (bytes.HasValue && bytes.Value >= 0)
                WriteBytesHistogram.Record(bytes.Value);
        }

        public static void RecordWriteRetry()
        {
            WriteRetryCounter.Add(1);
        }

        public static void RecordWriteSucceeded(
            int retryCount,
            long totalDurationTicks,
            long tenantQuotaTicks,
            long directoryQuotaTicks,
            long volumeWriteTicks,
            long acceptanceTicks,
            long projectionEnqueueTicks)
        {
            WriteSuccessCounter.Add(1);
            RecordDurations(
                totalDurationTicks,
                tenantQuotaTicks,
                directoryQuotaTicks,
                volumeWriteTicks,
                acceptanceTicks,
                projectionEnqueueTicks);

            RetryCountHistogram.Record(retryCount);
        }

        public static void RecordWriteFailed(
            string failureStage,
            int retryCount,
            long totalDurationTicks,
            long tenantQuotaTicks,
            long directoryQuotaTicks,
            long volumeWriteTicks,
            long acceptanceTicks,
            long projectionEnqueueTicks)
        {
            WriteFailureCounter.Add(
                1,
                new[]
                {
                    new KeyValuePair<string, object?>("failure_stage", failureStage),
                });
            RecordDurations(
                totalDurationTicks,
                tenantQuotaTicks,
                directoryQuotaTicks,
                volumeWriteTicks,
                acceptanceTicks,
                projectionEnqueueTicks);

            RetryCountHistogram.Record(retryCount);
        }

        public static void RecordCleanupFailure(string stage)
        {
            CleanupFailureCounter.Add(
                1,
                new[]
                {
                    new KeyValuePair<string, object?>("stage", stage),
                });
        }

        public static void RecordQuotaRollbackFailure(string scope)
        {
            QuotaRollbackFailureCounter.Add(
                1,
                new[]
                {
                    new KeyValuePair<string, object?>("scope", scope),
                });
        }

        private static void RecordDurations(
            long totalDurationTicks,
            long tenantQuotaTicks,
            long directoryQuotaTicks,
            long volumeWriteTicks,
            long acceptanceTicks,
            long projectionEnqueueTicks)
        {
            WriteDurationHistogram.Record(ToMilliseconds(totalDurationTicks));

            if (tenantQuotaTicks > 0)
                TenantQuotaDurationHistogram.Record(ToMilliseconds(tenantQuotaTicks));

            if (directoryQuotaTicks > 0)
                DirectoryQuotaDurationHistogram.Record(ToMilliseconds(directoryQuotaTicks));

            if (volumeWriteTicks > 0)
                VolumeWriteDurationHistogram.Record(ToMilliseconds(volumeWriteTicks));

            if (acceptanceTicks > 0)
                AcceptanceDurationHistogram.Record(ToMilliseconds(acceptanceTicks));

            if (projectionEnqueueTicks > 0)
                ProjectionEnqueueDurationHistogram.Record(ToMilliseconds(projectionEnqueueTicks));
        }

        private static double ToMilliseconds(long ticks)
        {
            return ticks * 1000.0 / Stopwatch.Frequency;
        }
    }
}
