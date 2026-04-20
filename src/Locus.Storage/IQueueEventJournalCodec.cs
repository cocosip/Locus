using System;
using System.Buffers;
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

        QueueEventJournalBatchBuffer SerializeBatch(IReadOnlyList<QueueEventRecord> records, long startingSequenceNumber);

        Task<QueueEventJournalCodecReadResult> ReadBatchAsync(Stream stream, long startOffset, int maxRecords, CancellationToken ct);

        QueueEventJournalCodecScanResult Scan(Stream stream);
    }

    internal sealed class QueueEventJournalBatchBuffer : IDisposable
    {
        private byte[]? _buffer;
        private readonly bool _pooled;

        public QueueEventJournalBatchBuffer(byte[] buffer, int length, bool pooled = true)
        {
            _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
            Length = length;
            _pooled = pooled;
        }

        public int Length { get; }

        public byte[] Buffer => _buffer ?? throw new ObjectDisposedException(nameof(QueueEventJournalBatchBuffer));

        public void Dispose()
        {
            var buffer = _buffer;
            _buffer = null;
            if (_pooled && buffer != null && buffer.Length > 0)
                ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal sealed class PooledByteBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private byte[]? _buffer;
        private readonly int _defaultBufferSize;

        public PooledByteBufferWriter(int initialCapacity = 256)
        {
            if (initialCapacity <= 0)
                initialCapacity = 256;

            _defaultBufferSize = initialCapacity;
            _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
        }

        public int WrittenCount { get; private set; }

        public ReadOnlySpan<byte> WrittenSpan
        {
            get
            {
                var buffer = _buffer ?? throw new ObjectDisposedException(nameof(PooledByteBufferWriter));
                return buffer.AsSpan(0, WrittenCount);
            }
        }

        public void Advance(int count)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            var buffer = _buffer ?? throw new ObjectDisposedException(nameof(PooledByteBufferWriter));
            if (WrittenCount > buffer.Length - count)
                throw new InvalidOperationException("Cannot advance beyond the available buffer space.");

            WrittenCount += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer!.AsMemory(WrittenCount);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer!.AsSpan(WrittenCount);
        }

        public void Clear()
        {
            WrittenCount = 0;
        }

        public void Write(ReadOnlySpan<byte> source)
        {
            source.CopyTo(GetSpan(source.Length));
            Advance(source.Length);
        }

        public void WriteByte(byte value)
        {
            var span = GetSpan(1);
            span[0] = value;
            Advance(1);
        }

        public QueueEventJournalBatchBuffer DetachBuffer()
        {
            var buffer = _buffer ?? throw new ObjectDisposedException(nameof(PooledByteBufferWriter));
            var writtenCount = WrittenCount;
            _buffer = null;
            WrittenCount = 0;
            return new QueueEventJournalBatchBuffer(buffer, writtenCount);
        }

        public void Dispose()
        {
            var buffer = _buffer;
            _buffer = null;
            WrittenCount = 0;
            if (buffer != null && buffer.Length > 0)
                ArrayPool<byte>.Shared.Return(buffer);
        }

        private void EnsureCapacity(int sizeHint)
        {
            var buffer = _buffer ?? throw new ObjectDisposedException(nameof(PooledByteBufferWriter));
            var requiredSize = sizeHint <= 0 ? _defaultBufferSize : sizeHint;
            if (buffer.Length - WrittenCount >= requiredSize)
                return;

            var newSize = Math.Max(buffer.Length * 2, WrittenCount + requiredSize);
            var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
            Buffer.BlockCopy(buffer, 0, newBuffer, 0, WrittenCount);
            ArrayPool<byte>.Shared.Return(buffer);
            _buffer = newBuffer;
        }
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
