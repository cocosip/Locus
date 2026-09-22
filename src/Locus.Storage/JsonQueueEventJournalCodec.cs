using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Storage
{
    internal sealed class JsonQueueEventJournalCodec : IQueueEventJournalCodec
    {
        private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
        };

        public JournalFormat Format => JournalFormat.JsonLines;

        public byte[] SerializeRecord(QueueEventRecord record, long sequenceNumber)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));

            record.SequenceNumber = sequenceNumber;
            record.PayloadCrc32 = ComputePayloadChecksum(record);
            var line = SerializePayload(record) + "\n";
            return Utf8NoBom.GetBytes(line);
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

        public async Task<QueueEventJournalCodecReadResult> ReadBatchAsync(Stream stream, long startOffset, int maxRecords, CancellationToken ct)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            stream.Seek(startOffset, SeekOrigin.Begin);
            var nextOffset = startOffset;
            var records = new List<QueueEventRecord>(maxRecords);

            using (var reader = new StreamReader(stream, Utf8NoBom, detectEncodingFromByteOrderMarks: true, bufferSize: 16 * 1024, leaveOpen: true))
            {
                while (records.Count < maxRecords)
                {
                    ct.ThrowIfCancellationRequested();

                    var lineStartOffset = nextOffset;
                    var line = await reader.ReadLineAsync().ConfigureAwait(false);
                    if (line == null)
                        break;

                    nextOffset += Utf8NoBom.GetByteCount(line) + 1;
                    if (nextOffset > stream.Length)
                        nextOffset = stream.Length;

                    if (line.Length == 0)
                        continue;

                    QueueEventRecord? record;
                    try
                    {
                        record = JsonSerializer.Deserialize<QueueEventRecord>(line, JsonOptions);
                    }
                    catch (JsonException)
                    {
                        return new QueueEventJournalCodecReadResult(records, lineStartOffset, true, true);
                    }

                    if (record == null || !ValidatePayloadChecksum(record))
                        return new QueueEventJournalCodecReadResult(records, lineStartOffset, true, true);

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
            var nextOffset = 0L;
            var lastSequenceNumber = 0L;

            using (var reader = new StreamReader(stream, Utf8NoBom, detectEncodingFromByteOrderMarks: true, bufferSize: 16 * 1024, leaveOpen: true))
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    var lineStartOffset = nextOffset;
                    nextOffset += Utf8NoBom.GetByteCount(line) + 1;
                    if (nextOffset > stream.Length)
                        nextOffset = stream.Length;

                    if (line.Length == 0)
                        continue;

                    QueueEventRecord? record;
                    try
                    {
                        record = JsonSerializer.Deserialize<QueueEventRecord>(line, JsonOptions);
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

            using (var reader = new StreamReader(stream, Utf8NoBom, detectEncodingFromByteOrderMarks: true, bufferSize: 16 * 1024, leaveOpen: true))
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    var lineStartOffset = nextOffset;
                    nextOffset += Utf8NoBom.GetByteCount(line) + 1;
                    if (nextOffset > stream.Length)
                        nextOffset = stream.Length;

                    if (nextOffset >= startOffset)
                        return nextOffset == startOffset ? startOffset : lineStartOffset;
                }
            }

            return nextOffset;
        }

        private static bool ValidatePayloadChecksum(QueueEventRecord record)
        {
            if (!record.PayloadCrc32.HasValue)
                return true;

            return ComputePayloadChecksum(record) == record.PayloadCrc32.Value;
        }

        private static uint ComputePayloadChecksum(QueueEventRecord record)
        {
            var payload = CloneRecord(record);
            payload.PayloadCrc32 = null;
            return QueueEventCrc32.Compute(SerializePayload(payload));
        }

        private static string SerializePayload(QueueEventRecord record)
        {
            var json = JsonSerializer.Serialize(record, JsonOptions);
            if (record.ImportOperationId != null)
                return json;

            using (var document = JsonDocument.Parse(json))
            using (var stream = new MemoryStream())
            using (var writer = new Utf8JsonWriter(stream))
            {
                writer.WriteStartObject();
                foreach (var property in document.RootElement.EnumerateObject())
                {
                    if (property.NameEquals("importOperationId")
                        && property.Value.ValueKind == JsonValueKind.Null)
                    {
                        continue;
                    }

                    property.WriteTo(writer);
                }
                writer.WriteEndObject();
                writer.Flush();
                return Utf8NoBom.GetString(stream.ToArray());
            }
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
    }
}
