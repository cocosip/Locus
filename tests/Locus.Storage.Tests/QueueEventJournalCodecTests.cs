using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Xunit;

namespace Locus.Storage.Tests;

public sealed class QueueEventJournalCodecTests
{
    private static QueueEventRecord CreateRecord(string fileKey)
    {
        return new QueueEventRecord
        {
            SchemaVersion = 1,
            EventId = Guid.NewGuid().ToString("N"),
            TenantId = "tenant-codec",
            FileKey = fileKey,
            EventType = QueueEventType.Accepted,
            OccurredAtUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        };
    }

    [Fact]
    public async Task ReadBatchAsync_WhenJournalStartsWithByteOrderMark_ReadsFirstRecord()
    {
        var codec = new JsonQueueEventJournalCodec();
        var line = codec.SerializeRecord(CreateRecord("file-1"), 1);

        var payload = new byte[3 + line.Length];
        payload[0] = 0xEF;
        payload[1] = 0xBB;
        payload[2] = 0xBF;
        Buffer.BlockCopy(line, 0, payload, 3, line.Length);

        using var stream = new MemoryStream(payload);
        var result = await codec.ReadBatchAsync(stream, 0, 10, CancellationToken.None);

        var record = Assert.Single(result.Records);
        Assert.Equal("file-1", record.FileKey);
        Assert.False(result.EncounteredCorruptTail);
    }

    [Fact]
    public async Task ReadBatchAsync_WhenLinesUseCrLf_ReportsOffsetsAtEndOfFile()
    {
        var codec = new JsonQueueEventJournalCodec();
        var first = codec.SerializeRecord(CreateRecord("file-1"), 1);
        var second = codec.SerializeRecord(CreateRecord("file-2"), 2);

        using var stream = new MemoryStream();
        // Replace the '\n' the codec appends with '\r\n' so offsets must account for two bytes.
        stream.Write(first, 0, first.Length - 1);
        stream.Write(new byte[] { (byte)'\r', (byte)'\n' });
        stream.Write(second, 0, second.Length - 1);
        stream.Write(new byte[] { (byte)'\r', (byte)'\n' });

        var length = stream.Length;
        stream.Position = 0;

        var result = await codec.ReadBatchAsync(stream, 0, 10, CancellationToken.None);

        Assert.Equal(2, result.Records.Count);
        Assert.Equal(length, result.NextOffset);
        Assert.True(result.ReachedEndOfFile);
        Assert.False(result.EncounteredCorruptTail);

        stream.Position = 0;
        var scan = codec.Scan(stream);

        Assert.Equal(length, scan.LastValidOffset);
        Assert.Equal(2, scan.LastSequenceNumber);
        Assert.False(scan.EncounteredCorruptTail);
    }
}
