using System.Collections.Generic;
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

        public static void RecordCorruptTailDetected(JournalFormat format, string stage)
        {
            CorruptTailDetectedCounter.Add(
                1,
                new KeyValuePair<string, object?>("format", format.ToString()),
                new KeyValuePair<string, object?>("stage", stage));
        }

        public static void RecordCorruptTailAutoRepaired(JournalFormat format)
        {
            CorruptTailAutoRepairedCounter.Add(
                1,
                new KeyValuePair<string, object?>("format", format.ToString()));
        }
    }
}
