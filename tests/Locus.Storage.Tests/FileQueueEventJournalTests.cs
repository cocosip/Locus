using System;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
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
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    JournalFormat = JournalFormat.BinaryV1,
                });
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
            Assert.Equal(1L, batch.Records[0].SequenceNumber);
            Assert.Equal(2L, batch.Records[1].SequenceNumber);
            Assert.True(batch.Records[0].PayloadCrc32.HasValue);
            Assert.True(batch.Records[1].PayloadCrc32.HasValue);
            Assert.True(batch.NextOffset > 0);
            Assert.True(batch.ReachedEndOfFile);
            Assert.Contains("tenant-001", tenantIds);
            Assert.Contains("tenant-002", tenantIds);
        }

        [Fact]
        public async Task AppendBatchAsync_AssignsSequentialSequenceNumbersAndPreservesOrder()
        {
            await _journal.AppendBatchAsync(new[]
            {
                new QueueEventRecord
                {
                    TenantId = "tenant-batch",
                    FileKey = "file-001",
                    EventType = QueueEventType.Accepted,
                    DirectoryPath = "/inbox",
                },
                new QueueEventRecord
                {
                    TenantId = "tenant-batch",
                    FileKey = "file-002",
                    EventType = QueueEventType.ProcessingStarted,
                    DirectoryPath = "/inbox",
                    ProcessingStartTimeUtc = DateTime.UtcNow,
                },
                new QueueEventRecord
                {
                    TenantId = "tenant-batch",
                    FileKey = "file-003",
                    EventType = QueueEventType.ProcessingCompleted,
                    DirectoryPath = "/inbox",
                }
            }, CancellationToken.None);

            var batch = await _journal.ReadBatchAsync("tenant-batch", 0, 10, CancellationToken.None);

            Assert.Equal(3, batch.Records.Count);
            Assert.Equal("file-001", batch.Records[0].FileKey);
            Assert.Equal("file-002", batch.Records[1].FileKey);
            Assert.Equal("file-003", batch.Records[2].FileKey);
            Assert.Equal(1L, batch.Records[0].SequenceNumber);
            Assert.Equal(2L, batch.Records[1].SequenceNumber);
            Assert.Equal(3L, batch.Records[2].SequenceNumber);
        }

        [Fact]
        public async Task AppendBatchAsync_WithJsonJournal_AssignsSequentialSequenceNumbersAndPreservesOrder()
        {
            var queueDirectory = Path.Combine(_queueDirectory, "json-batch");
            var journal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = queueDirectory,
                    JournalFormat = JournalFormat.JsonLines,
                });

            try
            {
                await journal.AppendBatchAsync(new[]
                {
                    new QueueEventRecord
                    {
                        TenantId = "tenant-json-batch",
                        FileKey = "file-001",
                        EventType = QueueEventType.Accepted,
                        DirectoryPath = "/json",
                    },
                    new QueueEventRecord
                    {
                        TenantId = "tenant-json-batch",
                        FileKey = "file-002",
                        EventType = QueueEventType.ProcessingStarted,
                        DirectoryPath = "/json",
                        ProcessingStartTimeUtc = DateTime.UtcNow,
                    },
                    new QueueEventRecord
                    {
                        TenantId = "tenant-json-batch",
                        FileKey = "file-003",
                        EventType = QueueEventType.ProcessingCompleted,
                        DirectoryPath = "/json",
                    }
                }, CancellationToken.None);

                var batch = await journal.ReadBatchAsync("tenant-json-batch", 0, 10, CancellationToken.None);

                Assert.Equal(3, batch.Records.Count);
                Assert.Equal("file-001", batch.Records[0].FileKey);
                Assert.Equal("file-002", batch.Records[1].FileKey);
                Assert.Equal("file-003", batch.Records[2].FileKey);
                Assert.Equal(1L, batch.Records[0].SequenceNumber);
                Assert.Equal(2L, batch.Records[1].SequenceNumber);
                Assert.Equal(3L, batch.Records[2].SequenceNumber);
            }
            finally
            {
                journal.Dispose();
            }
        }

        [Fact]
        public async Task AppendAsync_ConcurrentRequests_AssignsMonotonicSequenceNumbers()
        {
            var appendTasks = new Task[24];
            for (var i = 0; i < appendTasks.Length; i++)
            {
                var index = i;
                appendTasks[i] = _journal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-concurrent",
                    FileKey = "file-" + index.ToString("D2"),
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);
            }

            await Task.WhenAll(appendTasks);

            var batch = await _journal.ReadBatchAsync("tenant-concurrent", 0, 64, CancellationToken.None);

            Assert.Equal(appendTasks.Length, batch.Records.Count);
            for (var i = 0; i < batch.Records.Count; i++)
            {
                Assert.Equal(i + 1, batch.Records[i].SequenceNumber);
                Assert.True(batch.Records[i].PayloadCrc32.HasValue);
            }
        }

        [Fact]
        public async Task GetWritePathStatisticsSnapshot_TracksSingleAndMultiRecordAppendBatches()
        {
            var diagnostics = Assert.IsAssignableFrom<IQueueEventJournalWritePathDiagnostics>(_journal);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-stats",
                FileKey = "file-001",
                EventType = QueueEventType.Accepted,
            }, CancellationToken.None);

            await _journal.AppendBatchAsync(new[]
            {
                new QueueEventRecord
                {
                    TenantId = "tenant-stats",
                    FileKey = "file-002",
                    EventType = QueueEventType.ProcessingStarted,
                },
                new QueueEventRecord
                {
                    TenantId = "tenant-stats",
                    FileKey = "file-003",
                    EventType = QueueEventType.ProcessingCompleted,
                }
            }, CancellationToken.None);

            var snapshot = diagnostics.GetWritePathStatisticsSnapshot();
            Assert.Equal(2, snapshot.AppendBatchCount);
            Assert.Equal(1, snapshot.SingleRecordAppendBatchCount);
            Assert.Equal(1, snapshot.MultiRecordAppendBatchCount);
            Assert.Equal(3, snapshot.AppendedRecordCount);
            Assert.True(snapshot.AppendedBytes > 0);
            Assert.Equal(2, snapshot.FlushCount);
            Assert.True(snapshot.AppendTicks >= 0);
            Assert.True(snapshot.FlushTicks >= 0);
        }

        [Fact]
        public async Task AppendAsync_WithAsyncAckMode_ReturnsCompletedTaskAndStillPersistsRecords()
        {
            var queueDirectory = Path.Combine(_queueDirectory, "async-ack");
            var journal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = queueDirectory,
                    JournalFormat = JournalFormat.BinaryV1,
                    AckMode = QueueEventJournalAckMode.Async,
                    StateFlushDebounce = TimeSpan.Zero,
                });

            try
            {
                var singleTask = journal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-async",
                    FileKey = "file-001",
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);
                Assert.True(singleTask.IsCompletedSuccessfully);

                var batchTask = journal.AppendBatchAsync(new[]
                {
                    new QueueEventRecord
                    {
                        TenantId = "tenant-async",
                        FileKey = "file-002",
                        EventType = QueueEventType.ProcessingStarted,
                    },
                    new QueueEventRecord
                    {
                        TenantId = "tenant-async",
                        FileKey = "file-003",
                        EventType = QueueEventType.ProcessingCompleted,
                    }
                }, CancellationToken.None);
                Assert.True(batchTask.IsCompletedSuccessfully);
            }
            finally
            {
                journal.Dispose();
            }

            using var restartedJournal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = queueDirectory,
                    JournalFormat = JournalFormat.BinaryV1,
                    AckMode = QueueEventJournalAckMode.Async,
                });

            var batch = await restartedJournal.ReadBatchAsync("tenant-async", 0, 10, CancellationToken.None);
            Assert.Equal(3, batch.Records.Count);
            Assert.Equal("file-001", batch.Records[0].FileKey);
            Assert.Equal("file-002", batch.Records[1].FileKey);
            Assert.Equal("file-003", batch.Records[2].FileKey);
        }

        [Fact]
        public async Task AppendAsync_TenantIdWithTraversal_ThrowsArgumentException()
        {
            await Assert.ThrowsAsync<ArgumentException>(() => _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "../tenant-evil",
                FileKey = "file-001",
                EventType = QueueEventType.Accepted,
            }, CancellationToken.None));
        }

        [Fact]
        public async Task ReadBatchAsync_TenantIdWithPathSeparator_ThrowsArgumentException()
        {
            await Assert.ThrowsAsync<ArgumentException>(() => _journal.ReadBatchAsync("tenant/evil", 0, 10, CancellationToken.None));
        }

        [Fact]
        public async Task AppendAsync_PersistsJournalFormatAsStringInStateFile()
        {
            var journal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    JournalFormat = JournalFormat.BinaryV1,
                    StateFlushDebounce = TimeSpan.Zero,
                });

            try
            {
                await journal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-state-format",
                    FileKey = "file-001",
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);
            }
            finally
            {
                journal.Dispose();
            }

            var statePath = Path.Combine(_queueDirectory, "tenant-state-format", "queue.state.json");
            string stateJson;
            using (var stream = _fileSystem.File.Open(statePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var reader = new StreamReader(stream))
            {
                stateJson = reader.ReadToEnd();
            }

            Assert.Contains("\"format\":\"BinaryV1\"", stateJson);
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

        [Fact]
        public async Task AppendAsync_AfterCompaction_RestartRebuildsTailOffsetFromJournalLog()
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

            var restartedJournal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                _queueDirectory);
            try
            {
                var remainingBatch = await restartedJournal.ReadBatchAsync("tenant-001", compactedOffset, 10, CancellationToken.None);
                var baseOffset = await restartedJournal.GetBaseOffsetAsync("tenant-001", CancellationToken.None);
                var tailOffset = await restartedJournal.GetTailOffsetAsync("tenant-001", CancellationToken.None);

                Assert.Equal(compactedOffset, baseOffset);
                Assert.Equal(2, remainingBatch.Records.Count);
                Assert.Equal("file-002", remainingBatch.Records[0].FileKey);
                Assert.Equal("file-003", remainingBatch.Records[1].FileKey);
                Assert.Equal(tailOffset, remainingBatch.NextOffset);
            }
            finally
            {
                restartedJournal.Dispose();
            }
        }

        [Fact]
        public async Task AppendAsync_WithBinaryDefault_ReusesExistingJsonJournalWithoutMixingFormats()
        {
            var jsonJournal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    JournalFormat = JournalFormat.JsonLines,
                });

            try
            {
                await jsonJournal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-compat",
                    FileKey = "file-001",
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);
            }
            finally
            {
                jsonJournal.Dispose();
            }

            var binaryDefaultJournal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    JournalFormat = JournalFormat.BinaryV1,
                });

            try
            {
                await binaryDefaultJournal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-compat",
                    FileKey = "file-002",
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);

                var batch = await binaryDefaultJournal.ReadBatchAsync("tenant-compat", 0, 10, CancellationToken.None);
                var journalPath = Path.Combine(_queueDirectory, "tenant-compat", "queue.log");
                string[] lines;
                using (var stream = _fileSystem.File.Open(journalPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var reader = new StreamReader(stream))
                {
                    lines = reader
                        .ReadToEnd()
                        .Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);
                }

                Assert.Equal(2, batch.Records.Count);
                Assert.Equal("file-001", batch.Records[0].FileKey);
                Assert.Equal("file-002", batch.Records[1].FileKey);
                Assert.Equal(2, lines.Length);
                Assert.All(lines, line => Assert.StartsWith("{", line));
            }
            finally
            {
                binaryDefaultJournal.Dispose();
            }
        }

        [Fact]
        public async Task ReadBatchAsync_BinaryJournalWithCorruptTail_TruncatesTailAndAllowsFurtherAppends()
        {
            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-corrupt-tail",
                FileKey = "file-001",
                EventType = QueueEventType.Accepted,
            }, CancellationToken.None);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-corrupt-tail",
                FileKey = "file-002",
                EventType = QueueEventType.Accepted,
            }, CancellationToken.None);

            _journal.Dispose();

            var journalPath = Path.Combine(_queueDirectory, "tenant-corrupt-tail", "queue.log");
            using (var stream = _fileSystem.File.Open(journalPath, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                var corruptTail = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
                stream.Write(corruptTail, 0, corruptTail.Length);
            }

            var corruptedLength = _fileSystem.FileInfo.New(journalPath).Length;
            var restartedJournal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    JournalFormat = JournalFormat.BinaryV1,
                });

            try
            {
                QueueJournalMetricCapture? metrics = null;
                QueueEventReadBatch initialBatch;
                using (metrics = new QueueJournalMetricCapture())
                {
                    initialBatch = await restartedJournal.ReadBatchAsync("tenant-corrupt-tail", 0, 10, CancellationToken.None);

                    Assert.Equal(1, metrics.GetCount("locus.queue_journal.corrupt_tail.detected"));
                    Assert.Equal(1, metrics.GetCount("locus.queue_journal.corrupt_tail.auto_repaired"));
                }

                var repairedLength = _fileSystem.FileInfo.New(journalPath).Length;

                Assert.Equal(2, initialBatch.Records.Count);
                Assert.True(repairedLength < corruptedLength);

                await restartedJournal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-corrupt-tail",
                    FileKey = "file-003",
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);

                var finalBatch = await restartedJournal.ReadBatchAsync("tenant-corrupt-tail", 0, 10, CancellationToken.None);
                Assert.Equal(3, finalBatch.Records.Count);
                Assert.Equal(3L, finalBatch.Records[2].SequenceNumber);
            }
            finally
            {
                restartedJournal.Dispose();
            }
        }

        [Fact]
        public async Task ReadBatchAsync_BinaryJournalWithPersistedCorruptTail_TruncatesTailAndStopsRepeatedDetection()
        {
            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-persisted-corrupt-tail",
                FileKey = "file-001",
                EventType = QueueEventType.Accepted,
            }, CancellationToken.None);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-persisted-corrupt-tail",
                FileKey = "file-002",
                EventType = QueueEventType.Accepted,
            }, CancellationToken.None);

            _journal.Dispose();

            var journalPath = Path.Combine(_queueDirectory, "tenant-persisted-corrupt-tail", "queue.log");
            var statePath = Path.Combine(_queueDirectory, "tenant-persisted-corrupt-tail", "queue.state.json");
            using (var stream = _fileSystem.File.Open(journalPath, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                var corruptTail = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
                stream.Write(corruptTail, 0, corruptTail.Length);
            }

            var corruptedLength = _fileSystem.FileInfo.New(journalPath).Length;
            _fileSystem.File.WriteAllText(
                statePath,
                "{\"baseOffset\":0,\"tailOffset\":" + corruptedLength + ",\"lastSequenceNumber\":2,\"format\":\"BinaryV1\"}");

            var restartedJournal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    JournalFormat = JournalFormat.BinaryV1,
                });

            try
            {
                QueueEventReadBatch initialBatch;
                using (var metrics = new QueueJournalMetricCapture())
                {
                    initialBatch = await restartedJournal.ReadBatchAsync(
                        "tenant-persisted-corrupt-tail",
                        0,
                        10,
                        CancellationToken.None);

                    Assert.Equal(1, metrics.GetCount("locus.queue_journal.corrupt_tail.detected"));
                    Assert.Equal(1, metrics.GetCount("locus.queue_journal.corrupt_tail.auto_repaired"));
                }

                var repairedLength = _fileSystem.FileInfo.New(journalPath).Length;
                Assert.Equal(2, initialBatch.Records.Count);
                Assert.True(repairedLength < corruptedLength);

                using (var metrics = new QueueJournalMetricCapture())
                {
                    var secondBatch = await restartedJournal.ReadBatchAsync(
                        "tenant-persisted-corrupt-tail",
                        initialBatch.NextOffset,
                        10,
                        CancellationToken.None);

                    Assert.Empty(secondBatch.Records);
                    Assert.Equal(0, metrics.GetCount("locus.queue_journal.corrupt_tail.detected"));
                    Assert.Equal(0, metrics.GetCount("locus.queue_journal.corrupt_tail.auto_repaired"));
                }
            }
            finally
            {
                restartedJournal.Dispose();
            }
        }

        [Fact]
        public async Task ReadBatchAsync_BinaryJournalFromMiddleOfRecord_RewindsToValidBoundary()
        {
            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-misaligned-cursor",
                FileKey = "file-001",
                EventType = QueueEventType.Accepted,
            }, CancellationToken.None);

            await _journal.AppendAsync(new QueueEventRecord
            {
                TenantId = "tenant-misaligned-cursor",
                FileKey = "file-002",
                EventType = QueueEventType.Accepted,
            }, CancellationToken.None);

            var firstBatch = await _journal.ReadBatchAsync("tenant-misaligned-cursor", 0, 1, CancellationToken.None);
            var misalignedOffset = firstBatch.NextOffset + 1;

            QueueEventReadBatch recoveredBatch;
            using (var metrics = new QueueJournalMetricCapture())
            {
                recoveredBatch = await _journal.ReadBatchAsync(
                    "tenant-misaligned-cursor",
                    misalignedOffset,
                    10,
                    CancellationToken.None);

                Assert.Equal(1, metrics.GetCount("locus.queue_journal.corrupt_tail.detected"));
                Assert.Equal(0, metrics.GetCount("locus.queue_journal.corrupt_tail.auto_repaired"));
            }

            Assert.Single(recoveredBatch.Records);
            Assert.Equal("file-002", recoveredBatch.Records[0].FileKey);
            Assert.True(recoveredBatch.ReachedEndOfFile);
            Assert.True(recoveredBatch.NextOffset > misalignedOffset);
        }

        [Fact]
        public async Task ReadBatchAsync_JsonJournalFromMiddleOfLine_RewindsToValidBoundary()
        {
            var jsonJournal = new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    JournalFormat = JournalFormat.JsonLines,
                });

            try
            {
                await jsonJournal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-json-misaligned-cursor",
                    FileKey = "file-001",
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);

                await jsonJournal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-json-misaligned-cursor",
                    FileKey = "file-002",
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);

                var firstBatch = await jsonJournal.ReadBatchAsync("tenant-json-misaligned-cursor", 0, 1, CancellationToken.None);
                var misalignedOffset = firstBatch.NextOffset + 1;

                QueueEventReadBatch recoveredBatch;
                using (var metrics = new QueueJournalMetricCapture())
                {
                    recoveredBatch = await jsonJournal.ReadBatchAsync(
                        "tenant-json-misaligned-cursor",
                        misalignedOffset,
                        10,
                        CancellationToken.None);

                    Assert.Equal(1, metrics.GetCount("locus.queue_journal.corrupt_tail.detected"));
                    Assert.Equal(0, metrics.GetCount("locus.queue_journal.corrupt_tail.auto_repaired"));
                }

                Assert.Single(recoveredBatch.Records);
                Assert.Equal("file-002", recoveredBatch.Records[0].FileKey);
                Assert.True(recoveredBatch.ReachedEndOfFile);
                Assert.True(recoveredBatch.NextOffset > misalignedOffset);
            }
            finally
            {
                jsonJournal.Dispose();
            }
        }

        [Fact]
        public async Task ReadBatchAsync_MisalignedReadOffsetThatCanBeRecovered_LogsDebugWithoutWarning()
        {
            var logger = new Mock<ILogger<FileQueueEventJournal>>();
            var journal = new FileQueueEventJournal(
                _fileSystem,
                logger.Object,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    JournalFormat = JournalFormat.JsonLines,
                });

            try
            {
                await journal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-debug-misaligned-cursor",
                    FileKey = "file-001",
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);

                await journal.AppendAsync(new QueueEventRecord
                {
                    TenantId = "tenant-debug-misaligned-cursor",
                    FileKey = "file-002",
                    EventType = QueueEventType.Accepted,
                }, CancellationToken.None);

                var firstBatch = await journal.ReadBatchAsync("tenant-debug-misaligned-cursor", 0, 1, CancellationToken.None);
                var misalignedOffset = firstBatch.NextOffset + 1;

                var recoveredBatch = await journal.ReadBatchAsync(
                    "tenant-debug-misaligned-cursor",
                    misalignedOffset,
                    10,
                    CancellationToken.None);

                Assert.Single(recoveredBatch.Records);
                Assert.Equal(0, CountLogCalls(logger, LogLevel.Warning));
                Assert.Equal(1, CountLogCalls(logger, LogLevel.Debug));
            }
            finally
            {
                journal.Dispose();
            }
        }

        public void Dispose()
        {
            try
            {
                _journal.Dispose();
            }
            catch
            {
            }

            try
            {
                if (_fileSystem.Directory.Exists(_queueDirectory))
                    _fileSystem.Directory.Delete(_queueDirectory, recursive: true);
            }
            catch
            {
            }
        }

        private static int CountLogCalls(Mock<ILogger<FileQueueEventJournal>> logger, LogLevel level)
        {
            var count = 0;
            foreach (var invocation in logger.Invocations)
            {
                if (invocation.Method.Name == nameof(ILogger.Log)
                    && invocation.Arguments.Count > 0
                    && invocation.Arguments[0] is LogLevel logLevel
                    && logLevel == level)
                {
                    count++;
                }
            }

            return count;
        }
    }
}
