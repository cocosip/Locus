using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Locus.Storage
{
    internal static class QueueJournalOperationMetrics
    {
        private static readonly Meter Meter = new Meter("Locus.Storage.QueueJournal", "1.0.0");
        private static readonly Counter<long> AppendBatchCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.append.batch.count",
            unit: "{batch}",
            description: "Number of queue journal append batches written.");
        private static readonly Counter<long> FlushCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.flush.count",
            unit: "{flush}",
            description: "Number of queue journal append-stream flushes.");
        private static readonly Histogram<long> AppendBytesHistogram = Meter.CreateHistogram<long>(
            "locus.queue_journal.append.bytes",
            unit: "By",
            description: "Bytes written per queue journal append batch.");
        private static readonly Histogram<double> AppendDurationHistogram = Meter.CreateHistogram<double>(
            "locus.queue_journal.append.duration",
            unit: "ms",
            description: "Latency of queue journal append batches.");
        private static readonly Histogram<double> FlushDurationHistogram = Meter.CreateHistogram<double>(
            "locus.queue_journal.flush.duration",
            unit: "ms",
            description: "Latency of queue journal append-stream flushes.");

        public static void RecordAppendBatch(
            int totalBytes,
            long durationTicks)
        {
            AppendBatchCounter.Add(1);
            AppendBytesHistogram.Record(totalBytes);
            AppendDurationHistogram.Record(ToMilliseconds(durationTicks));
        }

        public static void RecordFlush(long durationTicks)
        {
            FlushCounter.Add(1);
            FlushDurationHistogram.Record(ToMilliseconds(durationTicks));
        }

        private static double ToMilliseconds(long ticks)
        {
            return ticks * 1000.0 / Stopwatch.Frequency;
        }
    }
}
