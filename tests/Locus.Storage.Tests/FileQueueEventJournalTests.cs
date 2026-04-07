using System;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Locus.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class FileQueueEventJournalTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly string _queueDirectory;
        private readonly FileQueueEventJournal _journal;

        public FileQueueEventJournalTests()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _queueDirectory = Path.Combine(Path.GetTempPath(), $"locus-test-queue-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_queueDirectory);

            _journal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                _queueDirectory);
        }

        [Fact]
        public async Task AppendAndReadBatchAsync_RoundTripsTenantJournal()
        {
            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-001",
                FileKey = "file-001",
                EventType = QueueEventType.Accepted,
                DirectoryPath = "/a",
                PhysicalPath = @"D:\volume\tenant-001\a\file-001.dcm",
                FileExtension = ".dcm"
            }, CancellationToken.None);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-001",
                FileKey = "file-002",
                EventType = QueueEventType.ProcessingStarted,
                DirectoryPath = "/a",
                PhysicalPath = @"D:\volume\tenant-001\a\file-002.dcm",
                ProcessingStartTimeUtc = DateTime.UtcNow
            }, CancellationToken.None);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-002",
                FileKey = "file-003",
                EventType = QueueEventType.Accepted
            }, CancellationToken.None);

            var batch = await _journal.ReadBatchAsync("tenant-001", 0, 10, CancellationToken.None);
            var tenantIds = await _journal.GetTenantIdsAsync(CancellationToken.None);

            Assert.Equal(2, batch.Records.Count);
            Assert.Equal("file-001", batch.Records[0].FileKey);
            Assert.Equal("file-002", batch.Records[1].FileKey);
            Assert.True(batch.NextOffset > 0);
            Assert.True(batch.ReachedEndOfFile);
            Assert.Contains("tenant-001", tenantIds);
            Assert.Contains("tenant-002", tenantIds);
        }

        [Fact]
        public async Task CompactAsync_PreservesLogicalOffsetsAcrossCompaction()
        {
            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-001",
                FileKey = "file-001",
                EventType = QueueEventType.Accepted
            }, CancellationToken.None);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-001",
                FileKey = "file-002",
                EventType = QueueEventType.Accepted
            }, CancellationToken.None);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-001",
                FileKey = "file-003",
                EventType = QueueEventType.Accepted
            }, CancellationToken.None);

            var firstBatch = await _journal.ReadBatchAsync("tenant-001", 0, 2, CancellationToken.None);
            var compactedOffset = await _journal.CompactAsync("tenant-001", firstBatch.NextOffset, CancellationToken.None);
            var secondBatch = await _journal.ReadBatchAsync("tenant-001", compactedOffset, 10, CancellationToken.None);
            var baseOffset = await _journal.GetBaseOffsetAsync("tenant-001", CancellationToken.None);
            var tailOffset = await _journal.GetTailOffsetAsync("tenant-001", CancellationToken.None);

            Assert.Equal(firstBatch.NextOffset, compactedOffset);
            Assert.Equal(compactedOffset, baseOffset);
            Assert.True(tailOffset > baseOffset);
            Assert.Single(secondBatch.Records);
            Assert.Equal("file-003", secondBatch.Records[0].FileKey);
            Assert.True(secondBatch.NextOffset >= tailOffset);
        }

        [Fact]
        public async Task AppendAsync_AfterCompaction_KeepsLogicalOffsetsMonotonic()
        {
            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-001",
                FileKey = "file-001",
                EventType = QueueEventType.Accepted
            }, CancellationToken.None);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-001",
                FileKey = "file-002",
                EventType = QueueEventType.Accepted
            }, CancellationToken.None);

            var firstBatch = await _journal.ReadBatchAsync("tenant-001", 0, 1, CancellationToken.None);
            var compactedOffset = await _journal.CompactAsync("tenant-001", firstBatch.NextOffset, CancellationToken.None);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-001",
                FileKey = "file-003",
                EventType = QueueEventType.Accepted
            }, CancellationToken.None);

            var remainingBatch = await _journal.ReadBatchAsync("tenant-001", compactedOffset, 10, CancellationToken.None);
            var tailOffset = await _journal.GetTailOffsetAsync("tenant-001", CancellationToken.None);

            Assert.Equal(2, remainingBatch.Records.Count);
            Assert.Equal("file-002", remainingBatch.Records[0].FileKey);
            Assert.Equal("file-003", remainingBatch.Records[1].FileKey);
            Assert.True(remainingBatch.NextOffset > compactedOffset);
            Assert.Equal(tailOffset, remainingBatch.NextOffset);
        }

        public void Dispose()
        {
            try
            {
                if (_fileSystem.Directory.Exists(_queueDirectory))
                    _fileSystem.Directory.Delete(_queueDirectory, recursive: true);
            }
            catch
            {
            }
        }
    }
}
