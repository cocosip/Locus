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

        public static void RecordWriteStarted(string sourceKind, string acceptancePath, long? bytes)
        {
            var tags = CreateCommonTags(
                volumeId: null,
                sourceKind,
                acceptancePath,
                retried: false);

            WriteRequestCounter.Add(1, tags);
            if (bytes.HasValue && bytes.Value >= 0)
                WriteBytesHistogram.Record(bytes.Value, tags);
        }

        public static void RecordWriteRetry(string? volumeId, string sourceKind, string acceptancePath)
        {
            WriteRetryCounter.Add(
                1,
                CreateCommonTags(
                    volumeId,
                    sourceKind,
                    acceptancePath,
                    retried: true));
        }

        public static void RecordWriteSucceeded(
            string? volumeId,
            string sourceKind,
            string acceptancePath,
            long? bytes,
            int retryCount,
            long totalDurationTicks,
            long tenantQuotaTicks,
            long directoryQuotaTicks,
            long volumeWriteTicks,
            long acceptanceTicks,
            long projectionEnqueueTicks)
        {
            var tags = CreateCommonTags(volumeId, sourceKind, acceptancePath, retryCount > 0);

            WriteSuccessCounter.Add(1, tags);
            RecordDurations(
                tags,
                totalDurationTicks,
                tenantQuotaTicks,
                directoryQuotaTicks,
                volumeWriteTicks,
                acceptanceTicks,
                projectionEnqueueTicks);

            RetryCountHistogram.Record(retryCount, tags);
        }

        public static void RecordWriteFailed(
            string? volumeId,
            string sourceKind,
            string acceptancePath,
            string failureStage,
            long? bytes,
            int retryCount,
            long totalDurationTicks,
            long tenantQuotaTicks,
            long directoryQuotaTicks,
            long volumeWriteTicks,
            long acceptanceTicks,
            long projectionEnqueueTicks,
            bool fileWritten,
            bool acceptanceCompleted)
        {
            var tags = CreateFailureTags(
                volumeId,
                sourceKind,
                acceptancePath,
                retryCount > 0,
                failureStage,
                fileWritten,
                acceptanceCompleted);

            WriteFailureCounter.Add(1, tags);
            RecordDurations(
                tags,
                totalDurationTicks,
                tenantQuotaTicks,
                directoryQuotaTicks,
                volumeWriteTicks,
                acceptanceTicks,
                projectionEnqueueTicks);

            RetryCountHistogram.Record(retryCount, tags);
        }

        public static void RecordCleanupFailure(string stage, string? volumeId)
        {
            CleanupFailureCounter.Add(
                1,
                new KeyValuePair<string, object?>("stage", stage),
                new KeyValuePair<string, object?>("volume_id", volumeId ?? "unknown"));
        }

        public static void RecordQuotaRollbackFailure(string scope)
        {
            QuotaRollbackFailureCounter.Add(
                1,
                new KeyValuePair<string, object?>("scope", scope));
        }

        private static void RecordDurations(
            KeyValuePair<string, object?>[] tags,
            long totalDurationTicks,
            long tenantQuotaTicks,
            long directoryQuotaTicks,
            long volumeWriteTicks,
            long acceptanceTicks,
            long projectionEnqueueTicks)
        {
            WriteDurationHistogram.Record(ToMilliseconds(totalDurationTicks), tags);

            if (tenantQuotaTicks > 0)
                TenantQuotaDurationHistogram.Record(ToMilliseconds(tenantQuotaTicks), tags);

            if (directoryQuotaTicks > 0)
                DirectoryQuotaDurationHistogram.Record(ToMilliseconds(directoryQuotaTicks), tags);

            if (volumeWriteTicks > 0)
                VolumeWriteDurationHistogram.Record(ToMilliseconds(volumeWriteTicks), tags);

            if (acceptanceTicks > 0)
                AcceptanceDurationHistogram.Record(ToMilliseconds(acceptanceTicks), tags);

            if (projectionEnqueueTicks > 0)
                ProjectionEnqueueDurationHistogram.Record(ToMilliseconds(projectionEnqueueTicks), tags);
        }

        private static KeyValuePair<string, object?>[] CreateCommonTags(
            string? volumeId,
            string sourceKind,
            string acceptancePath,
            bool retried)
        {
            return new[]
            {
                new KeyValuePair<string, object?>("volume_id", volumeId ?? "unknown"),
                new KeyValuePair<string, object?>("source_kind", sourceKind),
                new KeyValuePair<string, object?>("acceptance_path", acceptancePath),
                new KeyValuePair<string, object?>("retried", retried),
            };
        }

        private static KeyValuePair<string, object?>[] CreateFailureTags(
            string? volumeId,
            string sourceKind,
            string acceptancePath,
            bool retried,
            string failureStage,
            bool fileWritten,
            bool acceptanceCompleted)
        {
            return new[]
            {
                new KeyValuePair<string, object?>("volume_id", volumeId ?? "unknown"),
                new KeyValuePair<string, object?>("source_kind", sourceKind),
                new KeyValuePair<string, object?>("acceptance_path", acceptancePath),
                new KeyValuePair<string, object?>("retried", retried),
                new KeyValuePair<string, object?>("failure_stage", failureStage),
                new KeyValuePair<string, object?>("file_written", fileWritten),
                new KeyValuePair<string, object?>("acceptance_completed", acceptanceCompleted),
            };
        }

        private static double ToMilliseconds(long ticks)
        {
            return ticks * 1000.0 / Stopwatch.Frequency;
        }
    }
}
