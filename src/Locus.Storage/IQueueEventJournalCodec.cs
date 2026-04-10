using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Storage
{
    internal interface IQueueEventJournalCodec
    {
        JournalFormat Format { get; }

        byte[] SerializeRecord(QueueEventRecord record, long sequenceNumber);

        Task<QueueEventJournalCodecReadResult> ReadBatchAsync(Stream stream, long startOffset, int maxRecords, CancellationToken ct);

        QueueEventJournalCodecScanResult Scan(Stream stream);
    }

    internal sealed class QueueEventJournalCodecReadResult
    {
        public QueueEventJournalCodecReadResult(
            IReadOnlyList<QueueEventRecord> records,
            long nextOffset,
            bool reachedEndOfFile,
            bool encounteredCorruptTail)
        {
            Records = records;
            NextOffset = nextOffset;
            ReachedEndOfFile = reachedEndOfFile;
            EncounteredCorruptTail = encounteredCorruptTail;
        }

        public IReadOnlyList<QueueEventRecord> Records { get; }

        public long NextOffset { get; }

        public bool ReachedEndOfFile { get; }

        public bool EncounteredCorruptTail { get; }
    }

    internal sealed class QueueEventJournalCodecScanResult
    {
        public QueueEventJournalCodecScanResult(long lastValidOffset, long lastSequenceNumber, bool encounteredCorruptTail)
        {
            LastValidOffset = lastValidOffset;
            LastSequenceNumber = lastSequenceNumber;
            EncounteredCorruptTail = encounteredCorruptTail;
        }

        public long LastValidOffset { get; }

        public long LastSequenceNumber { get; }

        public bool EncounteredCorruptTail { get; }
    }
}
