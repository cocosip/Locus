using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Storage
{
    internal sealed class BinaryQueueEventJournalCodec : IQueueEventJournalCodec
    {
        private const int RecordMagic = 0x474F4C51;
        private const short MinimumRecordVersion = 1;
        private const short RecordVersion = 2;
        private const short RecordFlags = 0;
        private const int HeaderSize = 24;
        private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);

        public JournalFormat Format => JournalFormat.BinaryV1;

        public byte[] SerializeRecord(QueueEventRecord record, long sequenceNumber)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));

            var layout = new RecordSerializationLayout(record);
            var buffer = new byte[HeaderSize + layout.PayloadLength];
            WriteRecord(record, sequenceNumber, layout, buffer, 0);
            return buffer;
        }

        public QueueEventJournalBatchBuffer SerializeBatch(IReadOnlyList<QueueEventRecord> records, long startingSequenceNumber)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            if (records.Count == 0)
                return new QueueEventJournalBatchBuffer(Array.Empty<byte>(), 0, pooled: false);

            var layouts = new RecordSerializationLayout[records.Count];
            var totalBytes = 0;
            for (var i = 0; i < records.Count; i++)
            {
                var record = records[i] ?? throw new ArgumentNullException(nameof(records));
                var layout = new RecordSerializationLayout(record);
                layouts[i] = layout;
                totalBytes = checked(totalBytes + HeaderSize + layout.PayloadLength);
            }

            var buffer = ArrayPool<byte>.Shared.Rent(totalBytes);
            var offset = 0;
            var sequenceNumber = startingSequenceNumber;
            for (var i = 0; i < records.Count; i++)
            {
                sequenceNumber++;
                offset += WriteRecord(records[i], sequenceNumber, layouts[i], buffer, offset);
            }

            return new QueueEventJournalBatchBuffer(buffer, totalBytes);
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
            var nextOffset = startOffset;

            using (var reader = new BinaryReader(stream, Utf8NoBom, leaveOpen: true))
            {
                while (records.Count < maxRecords)
                {
                    ct.ThrowIfCancellationRequested();

                    var recordStartOffset = nextOffset;
                    if (stream.Position >= stream.Length)
                        break;

                    if (!TryReadRecordHeader(stream, reader, out var header))
                        return new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true);

                    var payload = reader.ReadBytes(header.PayloadLength);
                    if (payload.Length != header.PayloadLength)
                        return new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true);

                    nextOffset = stream.Position;
                    if (QueueEventCrc32.Compute(payload, 0, payload.Length) != header.PayloadChecksum)
                        return new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true);

                    QueueEventRecord record;
                    try
                    {
                        record = ReadPayload(payload, header.Version, header.SequenceNumber, header.PayloadChecksum);
                    }
                    catch (Exception ex) when (IsCorruptPayload(ex))
                    {
                        return new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true);
                    }

                    records.Add(record);
                }
            }

            return new QueueEventJournalCodecReadResult(records, nextOffset, nextOffset >= stream.Length, false);
        }

        public QueueEventJournalCodecScanResult Scan(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            stream.Seek(0, SeekOrigin.Begin);
            var lastSequenceNumber = 0L;
            var nextOffset = 0L;

            using (var reader = new BinaryReader(stream, Utf8NoBom, leaveOpen: true))
            {
                while (stream.Position < stream.Length)
                {
                    var recordStartOffset = nextOffset;
                    if (!TryReadRecordHeader(stream, reader, out var header))
                        return new QueueEventJournalCodecScanResult(recordStartOffset, lastSequenceNumber, true);

                    if (!IsValidRecordPayload(stream, reader, header))
                        return new QueueEventJournalCodecScanResult(recordStartOffset, lastSequenceNumber, true);

                    if (header.SequenceNumber > lastSequenceNumber)
                        lastSequenceNumber = header.SequenceNumber;

                    nextOffset = stream.Position;
                }
            }

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
            var nextOffset = 0L;

            using (var reader = new BinaryReader(stream, Utf8NoBom, leaveOpen: true))
            {
                while (stream.Position < stream.Length && nextOffset < startOffset)
                {
                    var recordStartOffset = nextOffset;
                    if (!TryReadRecordHeader(stream, reader, out var header))
                        return recordStartOffset;

                    if (!IsValidRecordPayload(stream, reader, header))
                        return recordStartOffset;

                    var recordEndOffset = stream.Position;
                    if (recordEndOffset >= startOffset)
                        return recordEndOffset == startOffset ? startOffset : recordStartOffset;

                    nextOffset = recordEndOffset;
                }
            }

            return nextOffset;
        }

        public static bool MatchesMagic(byte[] prefix, int bytesRead)
        {
            if (prefix == null)
                throw new ArgumentNullException(nameof(prefix));

            if (bytesRead < sizeof(int))
                return false;

            var magic = BitConverter.ToInt32(prefix, 0);
            return magic == RecordMagic;
        }

        private static int WriteRecord(QueueEventRecord record, long sequenceNumber, RecordSerializationLayout layout, byte[] buffer, int recordOffset)
        {
            record.SequenceNumber = sequenceNumber;

            var payloadLength = layout.PayloadLength;
            var payloadOffset = recordOffset + HeaderSize;
            WritePayload(record, layout, buffer, ref payloadOffset);

            var checksum = QueueEventCrc32.Compute(buffer, recordOffset + HeaderSize, payloadLength);
            record.PayloadCrc32 = checksum;

            var headerOffset = recordOffset;
            WriteInt32(buffer, ref headerOffset, RecordMagic);
            WriteInt16(buffer, ref headerOffset, RecordVersion);
            WriteInt16(buffer, ref headerOffset, RecordFlags);
            WriteInt32(buffer, ref headerOffset, payloadLength);
            WriteInt64(buffer, ref headerOffset, sequenceNumber);
            WriteUInt32(buffer, ref headerOffset, checksum);
            return HeaderSize + payloadLength;
        }

        private static void WritePayload(QueueEventRecord record, RecordSerializationLayout layout, byte[] buffer, ref int offset)
        {
            WriteInt32(buffer, ref offset, record.SchemaVersion);
            WriteInt32(buffer, ref offset, (int)record.EventType);
            WriteInt64(buffer, ref offset, record.OccurredAtUtc.Ticks);
            WriteInt64(buffer, ref offset, record.FileSize ?? -1L);
            WriteInt32(buffer, ref offset, record.Status.HasValue ? (int)record.Status.Value : int.MinValue);
            WriteInt32(buffer, ref offset, record.RetryCount ?? int.MinValue);
            WriteInt64(buffer, ref offset, record.ProcessingStartTimeUtc?.Ticks ?? long.MinValue);
            WriteInt64(buffer, ref offset, record.AvailableForProcessingAtUtc?.Ticks ?? long.MinValue);
            WriteNullableString(buffer, ref offset, layout.EventId);
            WriteNullableString(buffer, ref offset, layout.TenantId);
            WriteNullableString(buffer, ref offset, layout.FileKey);
            WriteNullableString(buffer, ref offset, layout.VolumeId);
            WriteNullableString(buffer, ref offset, layout.PhysicalPath);
            WriteNullableString(buffer, ref offset, layout.DirectoryPath);
            WriteNullableString(buffer, ref offset, layout.ErrorMessage);
            WriteNullableString(buffer, ref offset, layout.OriginalFileName);
            WriteNullableString(buffer, ref offset, layout.FileExtension);
            WriteNullableString(buffer, ref offset, layout.ImportOperationId);
        }

        private static QueueEventRecord ReadPayload(byte[] payload, short recordVersion, long sequenceNumber, uint payloadChecksum)
        {
            using (var stream = new MemoryStream(payload, writable: false))
            using (var reader = new BinaryReader(stream, Utf8NoBom, leaveOpen: true))
            {
                var record = new QueueEventRecord
                {
                    SchemaVersion = reader.ReadInt32(),
                    EventType = (QueueEventType)reader.ReadInt32(),
                    OccurredAtUtc = new DateTime(reader.ReadInt64(), DateTimeKind.Utc),
                    FileSize = ReadNullableInt64(reader, -1L),
                    Status = ReadNullableStatus(reader),
                    RetryCount = ReadNullableInt32(reader, int.MinValue),
                    ProcessingStartTimeUtc = ReadNullableDateTime(reader),
                    AvailableForProcessingAtUtc = ReadNullableDateTime(reader),
                    EventId = ReadNullableString(reader) ?? Guid.NewGuid().ToString("N"),
                    TenantId = ReadNullableString(reader) ?? string.Empty,
                    FileKey = ReadNullableString(reader) ?? string.Empty,
                    VolumeId = ReadNullableString(reader),
                    PhysicalPath = ReadNullableString(reader),
                    DirectoryPath = ReadNullableString(reader),
                    ErrorMessage = ReadNullableString(reader),
                    OriginalFileName = ReadNullableString(reader),
                    FileExtension = ReadNullableString(reader),
                    ImportOperationId = recordVersion >= 2 ? ReadNullableString(reader) : null,
                    SequenceNumber = sequenceNumber,
                    PayloadCrc32 = payloadChecksum,
                };

                if (stream.Position != stream.Length)
                    throw new InvalidDataException("Binary queue journal payload contains unread bytes.");

                return record;
            }
        }

        private static void WriteNullableString(byte[] buffer, ref int offset, NullableStringEncodingInfo value)
        {
            buffer[offset++] = value.HasValue ? (byte)1 : (byte)0;
            if (!value.HasValue)
                return;

            Write7BitEncodedInt(buffer, ref offset, value.ByteCount);
            offset += Utf8NoBom.GetBytes(value.Value!, 0, value.Value!.Length, buffer, offset);
        }

        private static int Get7BitEncodedIntSize(int value)
        {
            var size = 1;
            uint remaining = (uint)value;
            while (remaining >= 0x80)
            {
                remaining >>= 7;
                size++;
            }

            return size;
        }

        private static void Write7BitEncodedInt(byte[] buffer, ref int offset, int value)
        {
            uint remaining = (uint)value;
            while (remaining >= 0x80)
            {
                buffer[offset++] = (byte)(remaining | 0x80);
                remaining >>= 7;
            }

            buffer[offset++] = (byte)remaining;
        }

        private static void WriteInt16(byte[] buffer, ref int offset, short value)
        {
            unchecked
            {
                buffer[offset++] = (byte)value;
                buffer[offset++] = (byte)(value >> 8);
            }
        }

        private static void WriteInt32(byte[] buffer, ref int offset, int value)
        {
            unchecked
            {
                buffer[offset++] = (byte)value;
                buffer[offset++] = (byte)(value >> 8);
                buffer[offset++] = (byte)(value >> 16);
                buffer[offset++] = (byte)(value >> 24);
            }
        }

        private static void WriteUInt32(byte[] buffer, ref int offset, uint value)
        {
            unchecked
            {
                buffer[offset++] = (byte)value;
                buffer[offset++] = (byte)(value >> 8);
                buffer[offset++] = (byte)(value >> 16);
                buffer[offset++] = (byte)(value >> 24);
            }
        }

        private static void WriteInt64(byte[] buffer, ref int offset, long value)
        {
            unchecked
            {
                buffer[offset++] = (byte)value;
                buffer[offset++] = (byte)(value >> 8);
                buffer[offset++] = (byte)(value >> 16);
                buffer[offset++] = (byte)(value >> 24);
                buffer[offset++] = (byte)(value >> 32);
                buffer[offset++] = (byte)(value >> 40);
                buffer[offset++] = (byte)(value >> 48);
                buffer[offset++] = (byte)(value >> 56);
            }
        }

        private static string? ReadNullableString(BinaryReader reader)
        {
            return reader.ReadBoolean() ? reader.ReadString() : null;
        }

        private static DateTime? ReadNullableDateTime(BinaryReader reader)
        {
            var ticks = reader.ReadInt64();
            if (ticks == long.MinValue)
                return null;

            return new DateTime(ticks, DateTimeKind.Utc);
        }

        private static long? ReadNullableInt64(BinaryReader reader, long nullSentinel)
        {
            var value = reader.ReadInt64();
            return value == nullSentinel ? (long?)null : value;
        }

        private static int? ReadNullableInt32(BinaryReader reader, int nullSentinel)
        {
            var value = reader.ReadInt32();
            return value == nullSentinel ? (int?)null : value;
        }

        private static FileProcessingStatus? ReadNullableStatus(BinaryReader reader)
        {
            var value = reader.ReadInt32();
            return value == int.MinValue ? (FileProcessingStatus?)null : (FileProcessingStatus)value;
        }

        private static bool IsCorruptPayload(Exception exception)
        {
            // Only the failures the payload readers raise for truncated or corrupt bytes count as a
            // corrupt tail. Anything else (I/O faults, allocation failures, unexpected bugs) must
            // propagate instead of silently truncating a healthy journal.
            return exception is EndOfStreamException
                || exception is InvalidDataException
                || exception is FormatException
                || exception is ArgumentOutOfRangeException;
        }

        /// <summary>
        /// Reads the fixed record header and validates it together with the remaining file length.
        /// Returns <c>false</c> when the next record cannot be parsed completely.
        /// </summary>
        private static bool TryReadRecordHeader(Stream stream, BinaryReader reader, out RecordHeader header)
        {
            header = default;
            if ((stream.Length - stream.Position) < HeaderSize)
                return false;

            int magic;
            short version;
            short flags;
            int payloadLength;
            long sequenceNumber;
            uint payloadChecksum;

            try
            {
                magic = reader.ReadInt32();
                version = reader.ReadInt16();
                flags = reader.ReadInt16();
                payloadLength = reader.ReadInt32();
                sequenceNumber = reader.ReadInt64();
                payloadChecksum = reader.ReadUInt32();
            }
            catch (EndOfStreamException)
            {
                return false;
            }

            if (magic != RecordMagic
                || version < MinimumRecordVersion
                || version > RecordVersion
                || flags != RecordFlags
                || payloadLength < 0
                || (stream.Length - stream.Position) < payloadLength)
            {
                return false;
            }

            header = new RecordHeader(version, payloadLength, sequenceNumber, payloadChecksum);
            return true;
        }

        /// <summary>
        /// Validates a record payload frame (length, CRC and payload structure) without building a
        /// <see cref="QueueEventRecord"/>, which the scan and offset-normalization paths discard.
        /// </summary>
        private static bool IsValidRecordPayload(Stream stream, BinaryReader reader, in RecordHeader header)
        {
            var payloadLength = header.PayloadLength;
            var buffer = ArrayPool<byte>.Shared.Rent(payloadLength == 0 ? 1 : payloadLength);
            try
            {
                var read = 0;
                while (read < payloadLength)
                {
                    var chunk = reader.Read(buffer, read, payloadLength - read);
                    if (chunk <= 0)
                        return false;

                    read += chunk;
                }

                if (QueueEventCrc32.Compute(buffer, 0, payloadLength) != header.PayloadChecksum)
                    return false;

                try
                {
                    ValidatePayload(buffer, payloadLength, header.Version);
                }
                catch (Exception ex) when (IsCorruptPayload(ex))
                {
                    return false;
                }

                return true;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        /// <summary>
        /// Walks a payload with the same layout the read path expects, without allocating the
        /// record, its strings or a reader over it.
        /// </summary>
        private static void ValidatePayload(byte[] payload, int payloadLength, short recordVersion)
        {
            var offset = 0;

            ReadInt32(payload, payloadLength, ref offset); // SchemaVersion
            ReadInt32(payload, payloadLength, ref offset); // EventType
            ValidateTimestamp(ReadInt64(payload, payloadLength, ref offset), nullable: false); // OccurredAtUtc
            ReadInt64(payload, payloadLength, ref offset); // FileSize
            ReadInt32(payload, payloadLength, ref offset); // Status
            ReadInt32(payload, payloadLength, ref offset); // RetryCount
            ValidateTimestamp(ReadInt64(payload, payloadLength, ref offset), nullable: true); // ProcessingStartTimeUtc
            ValidateTimestamp(ReadInt64(payload, payloadLength, ref offset), nullable: true); // AvailableForProcessingAtUtc

            // EventId, TenantId, FileKey, VolumeId, PhysicalPath, DirectoryPath, ErrorMessage,
            // OriginalFileName, FileExtension.
            for (var i = 0; i < 9; i++)
                SkipNullableString(payload, payloadLength, ref offset);

            if (recordVersion >= 2)
                SkipNullableString(payload, payloadLength, ref offset); // ImportOperationId

            if (offset != payloadLength)
                throw new InvalidDataException("Binary queue journal payload contains unread bytes.");
        }

        private static void ValidateTimestamp(long ticks, bool nullable)
        {
            if (nullable && ticks == long.MinValue)
                return;

            // The read path turns these ticks into a DateTime, so out-of-range values are corrupt.
            if (ticks < DateTime.MinValue.Ticks || ticks > DateTime.MaxValue.Ticks)
                throw new ArgumentOutOfRangeException(nameof(ticks), "Binary queue journal payload contains an invalid timestamp.");
        }

        private static void SkipNullableString(byte[] payload, int payloadLength, ref int offset)
        {
            if (ReadByte(payload, payloadLength, ref offset) == 0)
                return;

            var byteCount = Read7BitEncodedInt(payload, payloadLength, ref offset);
            if (byteCount < 0 || payloadLength - offset < byteCount)
                throw new EndOfStreamException("Binary queue journal payload is truncated.");

            offset += byteCount;
        }

        private static byte ReadByte(byte[] payload, int payloadLength, ref int offset)
        {
            if (payloadLength - offset < 1)
                throw new EndOfStreamException("Binary queue journal payload is truncated.");

            return payload[offset++];
        }

        private static int ReadInt32(byte[] payload, int payloadLength, ref int offset)
        {
            if (payloadLength - offset < sizeof(int))
                throw new EndOfStreamException("Binary queue journal payload is truncated.");

            var value = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(offset));
            offset += sizeof(int);
            return value;
        }

        private static long ReadInt64(byte[] payload, int payloadLength, ref int offset)
        {
            if (payloadLength - offset < sizeof(long))
                throw new EndOfStreamException("Binary queue journal payload is truncated.");

            var value = BinaryPrimitives.ReadInt64LittleEndian(payload.AsSpan(offset));
            offset += sizeof(long);
            return value;
        }

        private static int Read7BitEncodedInt(byte[] payload, int payloadLength, ref int offset)
        {
            var value = 0;
            var shift = 0;
            while (true)
            {
                var current = ReadByte(payload, payloadLength, ref offset);
                value |= (current & 0x7F) << shift;
                if ((current & 0x80) == 0)
                    return value;

                shift += 7;
                if (shift >= 35)
                    throw new FormatException("Binary queue journal payload contains an invalid string length.");
            }
        }

        private readonly struct RecordHeader
        {
            public RecordHeader(short version, int payloadLength, long sequenceNumber, uint payloadChecksum)
            {
                Version = version;
                PayloadLength = payloadLength;
                SequenceNumber = sequenceNumber;
                PayloadChecksum = payloadChecksum;
            }

            public short Version { get; }

            public int PayloadLength { get; }

            public long SequenceNumber { get; }

            public uint PayloadChecksum { get; }
        }

        private readonly struct RecordSerializationLayout
        {
            private const int FixedPayloadSize =
                sizeof(int) +
                sizeof(int) +
                sizeof(long) +
                sizeof(long) +
                sizeof(int) +
                sizeof(int) +
                sizeof(long) +
                sizeof(long);

            public RecordSerializationLayout(QueueEventRecord record)
            {
                EventId = new NullableStringEncodingInfo(record.EventId);
                TenantId = new NullableStringEncodingInfo(record.TenantId);
                FileKey = new NullableStringEncodingInfo(record.FileKey);
                VolumeId = new NullableStringEncodingInfo(record.VolumeId);
                PhysicalPath = new NullableStringEncodingInfo(record.PhysicalPath);
                DirectoryPath = new NullableStringEncodingInfo(record.DirectoryPath);
                ErrorMessage = new NullableStringEncodingInfo(record.ErrorMessage);
                OriginalFileName = new NullableStringEncodingInfo(record.OriginalFileName);
                FileExtension = new NullableStringEncodingInfo(record.FileExtension);
                ImportOperationId = new NullableStringEncodingInfo(record.ImportOperationId);
                PayloadLength =
                    FixedPayloadSize +
                    EventId.EncodedSize +
                    TenantId.EncodedSize +
                    FileKey.EncodedSize +
                    VolumeId.EncodedSize +
                    PhysicalPath.EncodedSize +
                    DirectoryPath.EncodedSize +
                    ErrorMessage.EncodedSize +
                    OriginalFileName.EncodedSize +
                    FileExtension.EncodedSize +
                    ImportOperationId.EncodedSize;
            }

            public NullableStringEncodingInfo EventId { get; }

            public NullableStringEncodingInfo TenantId { get; }

            public NullableStringEncodingInfo FileKey { get; }

            public NullableStringEncodingInfo VolumeId { get; }

            public NullableStringEncodingInfo PhysicalPath { get; }

            public NullableStringEncodingInfo DirectoryPath { get; }

            public NullableStringEncodingInfo ErrorMessage { get; }

            public NullableStringEncodingInfo OriginalFileName { get; }

            public NullableStringEncodingInfo FileExtension { get; }

            public NullableStringEncodingInfo ImportOperationId { get; }

            public int PayloadLength { get; }
        }

        private readonly struct NullableStringEncodingInfo
        {
            public NullableStringEncodingInfo(string? value)
            {
                Value = value;
                if (value == null)
                {
                    ByteCount = 0;
                    EncodedSize = 1;
                    return;
                }

                ByteCount = Utf8NoBom.GetByteCount(value);
                EncodedSize = 1 + Get7BitEncodedIntSize(ByteCount) + ByteCount;
            }

            public string? Value { get; }

            public bool HasValue => Value != null;

            public int ByteCount { get; }

            public int EncodedSize { get; }
        }
    }
}
