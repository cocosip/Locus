using System.Diagnostics.Metrics;

namespace Locus.Storage
{
    internal static class QueueJournalMetrics
    {
        private static readonly Meter Meter = new Meter("Locus.Storage.QueueJournal", "1.0.0");
        private static readonly Counter<long> SequenceGapDetectedCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.sequence_gap.detected",
            unit: "{event}",
            description: "Number of queue journal sequence gaps detected by the projection pipeline.");
        private static readonly Counter<long> SequenceGapRecoveryAttemptedCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.sequence_gap.recovery_attempted",
            unit: "{event}",
            description: "Number of automatic orphan recovery attempts triggered after a queue journal sequence gap.");
        private static readonly Counter<long> SequenceGapRecoveryFailedCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.sequence_gap.recovery_failed",
            unit: "{event}",
            description: "Number of automatic orphan recovery attempts that failed after a queue journal sequence gap.");
        private static readonly Counter<long> CorruptTailDetectedCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.corrupt_tail.detected",
            unit: "{event}",
            description: "Number of corrupt queue journal tails detected during scan or read.");
        private static readonly Counter<long> CorruptTailAutoRepairedCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.corrupt_tail.auto_repaired",
            unit: "{event}",
            description: "Number of corrupt queue journal tails automatically truncated and repaired.");
        private static readonly Counter<long> ProjectionRecordFailedCounter = Meter.CreateCounter<long>(
            "locus.queue_projection.record.failed",
            unit: "{event}",
            description: "Number of queue projection record failures that were retried on a later cycle.");
        private static readonly Counter<long> ProjectionRecordQuarantinedCounter = Meter.CreateCounter<long>(
            "locus.queue_projection.record.quarantined",
            unit: "{event}",
            description: "Number of queue journal records quarantined to projection.deadletter.jsonl after exhausting the retry budget.");
        private static readonly Counter<long> AppendFailedCounter = Meter.CreateCounter<long>(
            "locus.queue_journal.append.failed",
            unit: "{event}",
            description: "Number of queue journal records whose append batch failed, including appends that no caller awaits.");

        public static void RecordSequenceGapDetected()
        {
            SequenceGapDetectedCounter.Add(1);
        }

        public static void RecordSequenceGapRecoveryAttempted()
        {
            SequenceGapRecoveryAttemptedCounter.Add(1);
        }

        public static void RecordSequenceGapRecoveryFailed()
        {
            SequenceGapRecoveryFailedCounter.Add(1);
        }

        public static void RecordCorruptTailDetected()
        {
            CorruptTailDetectedCounter.Add(1);
        }

        public static void RecordCorruptTailAutoRepaired()
        {
            CorruptTailAutoRepairedCounter.Add(1);
        }

        public static void RecordProjectionRecordFailed()
        {
            ProjectionRecordFailedCounter.Add(1);
        }

        public static void RecordProjectionRecordQuarantined()
        {
            ProjectionRecordQuarantinedCounter.Add(1);
        }

        public static void RecordAppendFailed(int recordCount)
        {
            if (recordCount > 0)
                AppendFailedCounter.Add(recordCount);
        }
    }
}
