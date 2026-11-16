using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Storage
{
    internal sealed class JsonQueueEventJournalCodec : IQueueEventJournalCodec
    {
        private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
        private const string PayloadCrcPropertyName = "\"payloadCrc32\":";
        private static readonly byte[] NullPayloadCrcToken = Utf8NoBom.GetBytes(PayloadCrcPropertyName + "null");
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
            TypeInfoResolver = new DefaultJsonTypeInfoResolver { Modifiers = { OmitNullImportOperationId } },
        };

        public JournalFormat Format => JournalFormat.JsonLines;

        public byte[] SerializeRecord(QueueEventRecord record, long sequenceNumber)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));

            record.SequenceNumber = sequenceNumber;
            record.PayloadCrc32 = null;

            // One serialization: the checksum is computed over the payload with a null checksum
            // field, which is exactly what the read path recomputes. The stored line is the same
            // bytes with the null token replaced by the checksum value.
            var payload = SerializePayload(record);
            var checksum = QueueEventCrc32.Compute(payload, 0, payload.Length);
            record.PayloadCrc32 = checksum;
            return WriteLine(payload, checksum);
        }

        public QueueEventJournalBatchBuffer SerializeBatch(IReadOnlyList<QueueEventRecord> records, long startingSequenceNumber)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            if (records.Count == 0)
                return new QueueEventJournalBatchBuffer(Array.Empty<byte>(), 0, pooled: false);

            using (var writer = new PooledByteBufferWriter(GetInitialBatchCapacity(records.Count)))
            {
                var sequenceNumber = startingSequenceNumber;
                for (var i = 0; i < records.Count; i++)
                {
                    var record = records[i] ?? throw new ArgumentNullException(nameof(records));
                    sequenceNumber++;
                    writer.Write(SerializeRecord(record, sequenceNumber));
                }

                return writer.DetachBuffer();
            }
        }

        public Task<QueueEventJournalCodecReadResult> ReadBatchAsync(Stream stream, long startOffset, int maxRecords, CancellationToken ct)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            return Task.Run(() => ReadBatchCore(stream, startOffset, maxRecords, ct), ct);
        }

        private QueueEventJournalCodecReadResult ReadBatchCore(Stream stream, long startOffset, int maxRecords, CancellationToken ct)
        {
            stream.Seek(startOffset, SeekOrigin.Begin);
            var records = new List<QueueEventRecord>(maxRecords);

            long nextOffset;
            using (var lines = new JournalLineReader(stream))
            {
                while (records.Count < maxRecords)
                {
                    ct.ThrowIfCancellationRequested();

                    var lineStartOffset = startOffset + lines.Consumed;
                    if (!lines.TryReadLine(out var line))
                        break;

                    if (line.Length == 0)
                        continue;

                    QueueEventRecord? record;
                    try
                    {
                        record = JsonSerializer.Deserialize<QueueEventRecord>(TrimUtf8Bom(line), JsonOptions);
                    }
                    catch (JsonException)
                    {
                        return new QueueEventJournalCodecReadResult(records, lineStartOffset, true, true);
                    }

                    if (record == null || !ValidatePayloadChecksum(record))
                        return new QueueEventJournalCodecReadResult(records, lineStartOffset, true, true);

                    records.Add(record);
                }

                nextOffset = startOffset + lines.Consumed;
            }

            if (nextOffset > stream.Length)
                nextOffset = stream.Length;

            return new QueueEventJournalCodecReadResult(records, nextOffset, nextOffset >= stream.Length, false);
        }

        public QueueEventJournalCodecScanResult Scan(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            stream.Seek(0, SeekOrigin.Begin);
            var lastSequenceNumber = 0L;

            long nextOffset;
            using (var lines = new JournalLineReader(stream))
            {
                while (true)
                {
                    var lineStartOffset = lines.Consumed;
                    if (!lines.TryReadLine(out var line))
                        break;

                    if (line.Length == 0)
                        continue;

                    QueueEventRecord? record;
                    try
                    {
                        record = JsonSerializer.Deserialize<QueueEventRecord>(TrimUtf8Bom(line), JsonOptions);
                    }
                    catch (JsonException)
                    {
                        return new QueueEventJournalCodecScanResult(lineStartOffset, lastSequenceNumber, true);
                    }

                    if (record == null || !ValidatePayloadChecksum(record))
                        return new QueueEventJournalCodecScanResult(lineStartOffset, lastSequenceNumber, true);

                    if (record.SequenceNumber.HasValue && record.SequenceNumber.Value > lastSequenceNumber)
                        lastSequenceNumber = record.SequenceNumber.Value;
                }

                nextOffset = lines.Consumed;
            }

            if (nextOffset > stream.Length)
                nextOffset = stream.Length;

            return new QueueEventJournalCodecScanResult(nextOffset, lastSequenceNumber, false);
        }

        public long NormalizeReadOffset(Stream stream, long startOffset)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            if (startOffset <= 0)
                return 0;

            if (startOffset >= stream.Length)
                return stream.Length;

            stream.Seek(0, SeekOrigin.Begin);

            using (var lines = new JournalLineReader(stream))
            {
                while (true)
                {
                    var lineStartOffset = lines.Consumed;
                    if (!lines.TryReadLine(out _))
                        break;

                    var nextOffset = lines.Consumed;
                    if (nextOffset > stream.Length)
                        nextOffset = stream.Length;

                    if (nextOffset >= startOffset)
                        return nextOffset == startOffset ? startOffset : lineStartOffset;
                }

                var tailOffset = lines.Consumed;
                return tailOffset > stream.Length ? stream.Length : tailOffset;
            }
        }

        private static uint ComputePayloadChecksum(QueueEventRecord record)
        {
            var payload = CloneRecord(record);
            payload.PayloadCrc32 = null;
            var bytes = SerializePayload(payload);
            return QueueEventCrc32.Compute(bytes, 0, bytes.Length);
        }

        /// <summary>
        /// Strips a leading UTF-8 byte order mark. The writer never emits one, but a journal that was
        /// hand-edited or produced by another tool used to be tolerated by the previous StreamReader
        /// based reader, and a BOM would otherwise fail JSON parsing as a corrupt record.
        /// </summary>
        private static ReadOnlySpan<byte> TrimUtf8Bom(ReadOnlySpan<byte> line)
        {
            return line.Length >= 3 && line[0] == 0xEF && line[1] == 0xBB && line[2] == 0xBF
                ? line.Slice(3)
                : line;
        }

        private static byte[] SerializePayload(QueueEventRecord record)
        {
            return JsonSerializer.SerializeToUtf8Bytes(record, JsonOptions);
        }

        /// <summary>
        /// Omits <c>importOperationId</c> when it is null and keeps every other property, matching
        /// the line shape written by earlier versions.
        /// </summary>
        private static void OmitNullImportOperationId(JsonTypeInfo typeInfo)
        {
            if (typeInfo.Type != typeof(QueueEventRecord))
                return;

            foreach (var property in typeInfo.Properties)
            {
                if (string.Equals(property.Name, "importOperationId", StringComparison.Ordinal))
                    property.ShouldSerialize = static (_, value) => value != null;
            }
        }

        private static byte[] WriteLine(byte[] payload, uint checksum)
        {
            var markerIndex = payload.AsSpan().IndexOf(NullPayloadCrcToken);
            if (markerIndex < 0)
            {
                // The checksum field was not serialized as a null token, so the payload already is
                // the line body the reader validates.
                return AppendLineTerminator(payload);
            }

            var replacement = Utf8NoBom.GetBytes(PayloadCrcPropertyName + checksum.ToString(CultureInfo.InvariantCulture));
            var tailStart = markerIndex + NullPayloadCrcToken.Length;
            var line = new byte[payload.Length - NullPayloadCrcToken.Length + replacement.Length + 1];
            Buffer.BlockCopy(payload, 0, line, 0, markerIndex);
            Buffer.BlockCopy(replacement, 0, line, markerIndex, replacement.Length);
            Buffer.BlockCopy(payload, tailStart, line, markerIndex + replacement.Length, payload.Length - tailStart);
            line[line.Length - 1] = (byte)'\n';
            return line;
        }

        private static byte[] AppendLineTerminator(byte[] payload)
        {
            var line = new byte[payload.Length + 1];
            Buffer.BlockCopy(payload, 0, line, 0, payload.Length);
            line[payload.Length] = (byte)'\n';
            return line;
        }

        private static bool ValidatePayloadChecksum(QueueEventRecord record)
        {
            if (!record.PayloadCrc32.HasValue)
                return true;

            return ComputePayloadChecksum(record) == record.PayloadCrc32.Value;
        }

        private static QueueEventRecord CloneRecord(QueueEventRecord record)
        {
            return new QueueEventRecord
            {
                SchemaVersion = record.SchemaVersion,
                EventId = record.EventId,
                TenantId = record.TenantId,
                FileKey = record.FileKey,
                EventType = record.EventType,
                OccurredAtUtc = record.OccurredAtUtc,
                SequenceNumber = record.SequenceNumber,
                PayloadCrc32 = record.PayloadCrc32,
                VolumeId = record.VolumeId,
                PhysicalPath = record.PhysicalPath,
                DirectoryPath = record.DirectoryPath,
                FileSize = record.FileSize,
                Status = record.Status,
                ProcessingStartTimeUtc = record.ProcessingStartTimeUtc,
                RetryCount = record.RetryCount,
                AvailableForProcessingAtUtc = record.AvailableForProcessingAtUtc,
                ErrorMessage = record.ErrorMessage,
                OriginalFileName = record.OriginalFileName,
                FileExtension = record.FileExtension,
                ImportOperationId = record.ImportOperationId,
            };
        }

        private static int GetInitialBatchCapacity(int recordCount)
        {
            return Math.Max(256, recordCount * 256);
        }

        /// <summary>
        /// Reads journal lines as raw UTF-8 byte ranges so read offsets advance by the bytes that
        /// were actually consumed. <see cref="StreamReader.ReadLine"/> hides whether a line ended
        /// with "\n" or "\r\n", which made offsets drift by one byte per line on CRLF journals.
        /// </summary>
        private sealed class JournalLineReader : IDisposable
        {
            private const int BufferSize = 16 * 1024;
            private readonly Stream _stream;
            private byte[]? _buffer;
            private int _start;
            private int _length;

            public JournalLineReader(Stream stream)
            {
                _stream = stream ?? throw new ArgumentNullException(nameof(stream));
                _buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
            }

            public long Consumed { get; private set; }

            public bool TryReadLine(out ReadOnlySpan<byte> line)
            {
                while (true)
                {
                    // Fill can compact or replace the pooled buffer, so always re-read it.
                    var buffer = _buffer ?? throw new ObjectDisposedException(nameof(JournalLineReader));
                    var available = _length - _start;
                    if (available > 0)
                    {
                        var terminatorIndex = Array.IndexOf(buffer, (byte)'\n', _start, available);
                        if (terminatorIndex >= 0)
                        {
                            var lineEnd = terminatorIndex;
                            if (lineEnd > _start && buffer[lineEnd - 1] == (byte)'\r')
                                lineEnd--;

                            var lineStart = _start;
                            Consumed += terminatorIndex - _start + 1;
                            _start = terminatorIndex + 1;
                            line = buffer.AsSpan(lineStart, lineEnd - lineStart);
                            return true;
                        }
                    }

                    if (!Fill())
                    {
                        if (_length == _start)
                        {
                            line = default;
                            return false;
                        }

                        // A final line without a terminator consumes exactly its own bytes.
                        var remainingBuffer = _buffer ?? throw new ObjectDisposedException(nameof(JournalLineReader));
                        var lineStart = _start;
                        var remaining = _length - _start;
                        Consumed += remaining;
                        _start = _length;
                        line = remainingBuffer.AsSpan(lineStart, remaining);
                        return true;
                    }
                }
            }

            public void Dispose()
            {
                var buffer = _buffer;
                _buffer = null;
                if (buffer != null)
                    ArrayPool<byte>.Shared.Return(buffer);
            }

            private bool Fill()
            {
                var buffer = _buffer ?? throw new ObjectDisposedException(nameof(JournalLineReader));
                if (_start > 0)
                {
                    Buffer.BlockCopy(buffer, _start, buffer, 0, _length - _start);
                    _length -= _start;
                    _start = 0;
                }

                if (_length == buffer.Length)
                {
                    var larger = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
                    Buffer.BlockCopy(buffer, 0, larger, 0, _length);
                    ArrayPool<byte>.Shared.Return(buffer);
                    _buffer = larger;
                    buffer = larger;
                }

                var read = _stream.Read(buffer, _length, buffer.Length - _length);
                if (read <= 0)
                    return false;

                _length += read;
                return true;
            }
        }
    }
}
