using System;
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
        private const short RecordVersion = 1;
        private const short RecordFlags = 0;
        private const int HeaderSize = 24;
        private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);

        public JournalFormat Format => JournalFormat.BinaryV1;

        public byte[] SerializeRecord(QueueEventRecord record, long sequenceNumber)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));

            record.SequenceNumber = sequenceNumber;
            var payloadLength = GetPayloadSize(record);
            var buffer = new byte[HeaderSize + payloadLength];
            var payloadOffset = HeaderSize;
            WritePayload(record, buffer, ref payloadOffset);

            var checksum = QueueEventCrc32.Compute(buffer, HeaderSize, payloadLength);
            record.PayloadCrc32 = checksum;

            var headerOffset = 0;
            WriteInt32(buffer, ref headerOffset, RecordMagic);
            WriteInt16(buffer, ref headerOffset, RecordVersion);
            WriteInt16(buffer, ref headerOffset, RecordFlags);
            WriteInt32(buffer, ref headerOffset, payloadLength);
            WriteInt64(buffer, ref headerOffset, sequenceNumber);
            WriteUInt32(buffer, ref headerOffset, checksum);

            return buffer;
        }

        public Task<QueueEventJournalCodecReadResult> ReadBatchAsync(Stream stream, long startOffset, int maxRecords, CancellationToken ct)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

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

                    if ((stream.Length - stream.Position) < HeaderSize)
                        return Task.FromResult(new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true));

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
                        return Task.FromResult(new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true));
                    }

                    if (magic != RecordMagic
                        || version != RecordVersion
                        || flags != RecordFlags
                        || payloadLength < 0
                        || (stream.Length - stream.Position) < payloadLength)
                    {
                        return Task.FromResult(new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true));
                    }

                    var payload = reader.ReadBytes(payloadLength);
                    if (payload.Length != payloadLength)
                        return Task.FromResult(new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true));

                    nextOffset = stream.Position;
                    if (QueueEventCrc32.Compute(payload, 0, payload.Length) != payloadChecksum)
                        return Task.FromResult(new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true));

                    QueueEventRecord record;
                    try
                    {
                        record = ReadPayload(payload, sequenceNumber, payloadChecksum);
                    }
                    catch (Exception) when (IsPayloadReadFailure())
                    {
                        return Task.FromResult(new QueueEventJournalCodecReadResult(records, recordStartOffset, true, true));
                    }

                    records.Add(record);
                }
            }

            return Task.FromResult(new QueueEventJournalCodecReadResult(records, nextOffset, nextOffset >= stream.Length, false));
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
                    if ((stream.Length - stream.Position) < HeaderSize)
                        return new QueueEventJournalCodecScanResult(recordStartOffset, lastSequenceNumber, true);

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
                        return new QueueEventJournalCodecScanResult(recordStartOffset, lastSequenceNumber, true);
                    }

                    if (magic != RecordMagic
                        || version != RecordVersion
                        || flags != RecordFlags
                        || payloadLength < 0
                        || (stream.Length - stream.Position) < payloadLength)
                    {
                        return new QueueEventJournalCodecScanResult(recordStartOffset, lastSequenceNumber, true);
                    }

                    var payload = reader.ReadBytes(payloadLength);
                    if (payload.Length != payloadLength)
                        return new QueueEventJournalCodecScanResult(recordStartOffset, lastSequenceNumber, true);

                    if (QueueEventCrc32.Compute(payload, 0, payload.Length) != payloadChecksum)
                        return new QueueEventJournalCodecScanResult(recordStartOffset, lastSequenceNumber, true);

                    try
                    {
                        ReadPayload(payload, sequenceNumber, payloadChecksum);
                    }
                    catch (Exception) when (IsPayloadReadFailure())
                    {
                        return new QueueEventJournalCodecScanResult(recordStartOffset, lastSequenceNumber, true);
                    }

                    if (sequenceNumber > lastSequenceNumber)
                        lastSequenceNumber = sequenceNumber;

                    nextOffset = stream.Position;
                }
            }

            return new QueueEventJournalCodecScanResult(nextOffset, lastSequenceNumber, false);
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

        private static int GetPayloadSize(QueueEventRecord record)
        {
            var total =
                sizeof(int) +
                sizeof(int) +
                sizeof(long) +
                sizeof(long) +
                sizeof(int) +
                sizeof(int) +
                sizeof(long) +
                sizeof(long);

            total += GetNullableStringSize(record.EventId);
            total += GetNullableStringSize(record.TenantId);
            total += GetNullableStringSize(record.FileKey);
            total += GetNullableStringSize(record.VolumeId);
            total += GetNullableStringSize(record.PhysicalPath);
            total += GetNullableStringSize(record.DirectoryPath);
            total += GetNullableStringSize(record.ErrorMessage);
            total += GetNullableStringSize(record.OriginalFileName);
            total += GetNullableStringSize(record.FileExtension);
            return total;
        }

        private static void WritePayload(QueueEventRecord record, byte[] buffer, ref int offset)
        {
            WriteInt32(buffer, ref offset, record.SchemaVersion);
            WriteInt32(buffer, ref offset, (int)record.EventType);
            WriteInt64(buffer, ref offset, record.OccurredAtUtc.Ticks);
            WriteInt64(buffer, ref offset, record.FileSize ?? -1L);
            WriteInt32(buffer, ref offset, record.Status.HasValue ? (int)record.Status.Value : int.MinValue);
            WriteInt32(buffer, ref offset, record.RetryCount ?? int.MinValue);
            WriteInt64(buffer, ref offset, record.ProcessingStartTimeUtc?.Ticks ?? long.MinValue);
            WriteInt64(buffer, ref offset, record.AvailableForProcessingAtUtc?.Ticks ?? long.MinValue);
            WriteNullableString(buffer, ref offset, record.EventId);
            WriteNullableString(buffer, ref offset, record.TenantId);
            WriteNullableString(buffer, ref offset, record.FileKey);
            WriteNullableString(buffer, ref offset, record.VolumeId);
            WriteNullableString(buffer, ref offset, record.PhysicalPath);
            WriteNullableString(buffer, ref offset, record.DirectoryPath);
            WriteNullableString(buffer, ref offset, record.ErrorMessage);
            WriteNullableString(buffer, ref offset, record.OriginalFileName);
            WriteNullableString(buffer, ref offset, record.FileExtension);
        }

        private static QueueEventRecord ReadPayload(byte[] payload, long sequenceNumber, uint payloadChecksum)
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
                    SequenceNumber = sequenceNumber,
                    PayloadCrc32 = payloadChecksum,
                };

                if (stream.Position != stream.Length)
                    throw new InvalidDataException("Binary queue journal payload contains unread bytes.");

                return record;
            }
        }

        private static int GetNullableStringSize(string? value)
        {
            if (value == null)
                return 1;

            var byteCount = Utf8NoBom.GetByteCount(value);
            return 1 + Get7BitEncodedIntSize(byteCount) + byteCount;
        }

        private static void WriteNullableString(byte[] buffer, ref int offset, string? value)
        {
            buffer[offset++] = value != null ? (byte)1 : (byte)0;
            if (value == null)
                return;

            var byteCount = Utf8NoBom.GetByteCount(value);
            Write7BitEncodedInt(buffer, ref offset, byteCount);
            offset += Utf8NoBom.GetBytes(value, 0, value.Length, buffer, offset);
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

        private static bool IsPayloadReadFailure()
        {
            return true;
        }
    }
}
