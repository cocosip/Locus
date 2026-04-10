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

            var payload = BuildPayload(record);
            var checksum = QueueEventCrc32.Compute(payload, 0, payload.Length);
            record.PayloadCrc32 = checksum;

            using (var stream = new MemoryStream(payload.Length + HeaderSize))
            using (var writer = new BinaryWriter(stream, Utf8NoBom, leaveOpen: true))
            {
                writer.Write(RecordMagic);
                writer.Write(RecordVersion);
                writer.Write(RecordFlags);
                writer.Write(payload.Length);
                writer.Write(sequenceNumber);
                writer.Write(checksum);
                writer.Write(payload);
                writer.Flush();
                return stream.ToArray();
            }
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

        private static byte[] BuildPayload(QueueEventRecord record)
        {
            using (var stream = new MemoryStream(512))
            using (var writer = new BinaryWriter(stream, Utf8NoBom, leaveOpen: true))
            {
                writer.Write(record.SchemaVersion);
                writer.Write((int)record.EventType);
                writer.Write(record.OccurredAtUtc.Ticks);
                writer.Write(record.FileSize ?? -1L);
                writer.Write(record.Status.HasValue ? (int)record.Status.Value : int.MinValue);
                writer.Write(record.RetryCount ?? int.MinValue);
                writer.Write(record.ProcessingStartTimeUtc?.Ticks ?? long.MinValue);
                writer.Write(record.AvailableForProcessingAtUtc?.Ticks ?? long.MinValue);
                WriteNullableString(writer, record.EventId);
                WriteNullableString(writer, record.TenantId);
                WriteNullableString(writer, record.FileKey);
                WriteNullableString(writer, record.VolumeId);
                WriteNullableString(writer, record.PhysicalPath);
                WriteNullableString(writer, record.DirectoryPath);
                WriteNullableString(writer, record.ErrorMessage);
                WriteNullableString(writer, record.OriginalFileName);
                WriteNullableString(writer, record.FileExtension);
                writer.Flush();
                return stream.ToArray();
            }
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

        private static void WriteNullableString(BinaryWriter writer, string? value)
        {
            writer.Write(value != null);
            if (value != null)
                writer.Write(value);
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
