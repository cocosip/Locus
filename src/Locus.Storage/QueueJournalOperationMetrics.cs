using System.Collections.Generic;
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
        private static readonly Counter<long> AppendRecordCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.append.record.count",
            unit: "{record}",
            description: "Number of queue journal records appended.");
        private static readonly Counter<long> DeferredAckCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.append.deferred_ack.count",
            unit: "{batch}",
            description: "Number of queue journal append batches acknowledged before a flush.");
        private static readonly Counter<long> FlushCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.flush.count",
            unit: "{flush}",
            description: "Number of queue journal append-stream flushes.");
        private static readonly Histogram<long> AppendBytesHistogram = Meter.CreateHistogram<long>(
            "locus.queue_journal.append.bytes",
            unit: "By",
            description: "Bytes written per queue journal append batch.");
        private static readonly Histogram<long> AppendBatchSizeHistogram = Meter.CreateHistogram<long>(
            "locus.queue_journal.append.batch_size",
            unit: "{record}",
            description: "Record count per queue journal append batch.");
        private static readonly Histogram<long> AppendRequestBatchSizeHistogram = Meter.CreateHistogram<long>(
            "locus.queue_journal.append.request_batch_size",
            unit: "{request}",
            description: "Request count coalesced into each queue journal append batch.");
        private static readonly Histogram<double> AppendDurationHistogram = Meter.CreateHistogram<double>(
            "locus.queue_journal.append.duration",
            unit: "ms",
            description: "Latency of queue journal append batches.");
        private static readonly Histogram<double> FlushDurationHistogram = Meter.CreateHistogram<double>(
            "locus.queue_journal.flush.duration",
            unit: "ms",
            description: "Latency of queue journal append-stream flushes.");

        public static void RecordAppendBatch(
            JournalFormat format,
            QueueEventJournalAckMode ackMode,
            int requestCount,
            int recordCount,
            int totalBytes,
            bool flushedBeforeAck,
            string writeMode,
            long durationTicks)
        {
            var tags = CreateTags(format, ackMode, writeMode);

            AppendBatchCounter.Add(1, tags);
            AppendRecordCounter.Add(recordCount, tags);
            AppendBatchSizeHistogram.Record(recordCount, tags);
            AppendRequestBatchSizeHistogram.Record(requestCount, tags);
            AppendBytesHistogram.Record(totalBytes, tags);
            AppendDurationHistogram.Record(ToMilliseconds(durationTicks), tags);

            if (!flushedBeforeAck)
                DeferredAckCounter.Add(1, tags);
        }

        public static void RecordFlush(
            JournalFormat format,
            QueueEventJournalAckMode ackMode,
            string reason,
            long durationTicks)
        {
            var tags = new[]
            {
                new KeyValuePair<string, object?>("format", format.ToString()),
                new KeyValuePair<string, object?>("ack_mode", ackMode.ToString()),
                new KeyValuePair<string, object?>("reason", reason),
            };

            FlushCounter.Add(1, tags);
            FlushDurationHistogram.Record(ToMilliseconds(durationTicks), tags);
        }

        private static KeyValuePair<string, object?>[] CreateTags(
            JournalFormat format,
            QueueEventJournalAckMode ackMode,
            string writeMode)
        {
            return new[]
            {
                new KeyValuePair<string, object?>("format", format.ToString()),
                new KeyValuePair<string, object?>("ack_mode", ackMode.ToString()),
                new KeyValuePair<string, object?>("write_mode", writeMode),
            };
        }

        private static double ToMilliseconds(long ticks)
        {
            return ticks * 1000.0 / Stopwatch.Frequency;
        }
    }
}
