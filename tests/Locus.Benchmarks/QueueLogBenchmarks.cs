#nullable enable
using System;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Locus.Core.Models;
using Locus.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    /// <summary>
    /// Benchmarks for the current JSONL queue.log journal implementation.
    /// These establish a local baseline before evaluating any binary log format.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 2, iterationCount: 6)]
    public class QueueLogSingleRecordBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private string _rootDirectory = string.Empty;
        private FileQueueEventJournal _journal = null!;
        private int _sequence;

        [Params(QueueEventJournalAckMode.Durable, QueueEventJournalAckMode.Balanced, QueueEventJournalAckMode.Async)]
        public QueueEventJournalAckMode AckMode { get; set; }

        [Params(1, 2)]
        public int LingerMs { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-queuelog-single-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            CleanupJournal();
            _sequence = 0;

            _journal = new FileQueueEventJournal(
                _fileSystem,
                NullLogger<FileQueueEventJournal>.Instance,
                new QueueEventJournalOptions
                {
                    QueueDirectory = Path.Combine(_rootDirectory, "queue"),
                    JournalFormat = JournalFormat.JsonLines,
                    StateFlushDebounce = TimeSpan.FromSeconds(1),
                    AckMode = AckMode,
                    Linger = TimeSpan.FromMilliseconds(LingerMs),
                    MaxBatchRecords = 16,
                    MaxBatchBytes = 256 * 1024,
                    WriterIdleTimeout = TimeSpan.FromSeconds(30),
                    BalancedFlushWindow = TimeSpan.FromMilliseconds(5),
                });
        }

        [Benchmark(Baseline = true, Description = "queue.log single record append")]
        public Task AppendSingleRecord()
        {
            return _journal.AppendAsync(CreateRecord("bench-tenant"), CancellationToken.None);
        }

        public void Dispose()
        {
            CleanupJournal();
            try
            {
                if (_fileSystem.Directory.Exists(_rootDirectory))
                    _fileSystem.Directory.Delete(_rootDirectory, recursive: true);
            }
            catch
            {
                // Ignore benchmark cleanup failures.
            }
        }

        private QueueEventRecord CreateRecord(string tenantId)
        {
            var id = Interlocked.Increment(ref _sequence);
            return new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = $"single-{id:D8}",
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = "vol-001",
                PhysicalPath = $@"D:\bench\{tenantId}\incoming\single-{id:D8}.dcm",
                DirectoryPath = "/incoming",
                FileSize = 1024,
                Status = FileProcessingStatus.Pending,
                OriginalFileName = $"single-{id:D8}.dcm",
                FileExtension = ".dcm",
            };
        }

        private void CleanupJournal()
        {
            _journal?.Dispose();
            _journal = null!;

            var queuePath = Path.Combine(_rootDirectory, "queue");
            try
            {
                if (_fileSystem.Directory.Exists(queuePath))
                    _fileSystem.Directory.Delete(queuePath, recursive: true);
            }
            catch
            {
                // Ignore benchmark cleanup failures.
            }
        }
    }

    /// <summary>
    /// Measures queue.log throughput under same-tenant contention and multi-tenant fan-out.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 2, iterationCount: 6)]
    public class QueueLogBurstBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private string _rootDirectory = string.Empty;
        private FileQueueEventJournal _journal = null!;
        private int _sequence;

        [Params(QueueEventJournalAckMode.Durable, QueueEventJournalAckMode.Balanced)]
        public QueueEventJournalAckMode AckMode { get; set; }

        [Params(1, 2)]
        public int LingerMs { get; set; }

        [Params(8, 32)]
        public int Concurrency { get; set; }

        [Params(512)]
        public int TotalOperations { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-queuelog-burst-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            CleanupJournal();
            _sequence = 0;

            _journal = new FileQueueEventJournal(
                _fileSystem,
                NullLogger<FileQueueEventJournal>.Instance,
                new QueueEventJournalOptions
                {
                    QueueDirectory = Path.Combine(_rootDirectory, "queue"),
                    JournalFormat = JournalFormat.JsonLines,
                    StateFlushDebounce = TimeSpan.FromSeconds(1),
                    AckMode = AckMode,
                    Linger = TimeSpan.FromMilliseconds(LingerMs),
                    MaxBatchRecords = 16,
                    MaxBatchBytes = 256 * 1024,
                    WriterIdleTimeout = TimeSpan.FromSeconds(30),
                    BalancedFlushWindow = TimeSpan.FromMilliseconds(5),
                });
        }

        [Benchmark(Baseline = true, Description = "queue.log burst append (single tenant contention)")]
        public async Task SingleTenantBurst()
        {
            var perWorker = TotalOperations / Concurrency;
            var remainder = TotalOperations % Concurrency;
            var tasks = new Task[Concurrency];
            for (var worker = 0; worker < Concurrency; worker++)
            {
                var operations = perWorker + (worker < remainder ? 1 : 0);
                tasks[worker] = AppendBurstAsync("tenant-hot", operations);
            }

            await Task.WhenAll(tasks);
        }

        [Benchmark(Description = "queue.log burst append (multi-tenant fan-out)")]
        public async Task MultiTenantBurst()
        {
            var perWorker = TotalOperations / Concurrency;
            var remainder = TotalOperations % Concurrency;
            var tasks = new Task[Concurrency];
            for (var worker = 0; worker < Concurrency; worker++)
            {
                var operations = perWorker + (worker < remainder ? 1 : 0);
                var tenantId = $"tenant-{worker % 8:D2}";
                tasks[worker] = AppendBurstAsync(tenantId, operations);
            }

            await Task.WhenAll(tasks);
        }

        public void Dispose()
        {
            CleanupJournal();
            try
            {
                if (_fileSystem.Directory.Exists(_rootDirectory))
                    _fileSystem.Directory.Delete(_rootDirectory, recursive: true);
            }
            catch
            {
                // Ignore benchmark cleanup failures.
            }
        }

        private async Task AppendBurstAsync(string tenantId, int operations)
        {
            for (var i = 0; i < operations; i++)
                await _journal.AppendAsync(CreateRecord(tenantId), CancellationToken.None);
        }

        private QueueEventRecord CreateRecord(string tenantId)
        {
            var id = Interlocked.Increment(ref _sequence);
            return new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = $"burst-{id:D8}",
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = "vol-001",
                PhysicalPath = $@"D:\bench\{tenantId}\incoming\burst-{id:D8}.dcm",
                DirectoryPath = "/incoming",
                FileSize = 4096,
                Status = FileProcessingStatus.Pending,
                OriginalFileName = $"burst-{id:D8}.dcm",
                FileExtension = ".dcm",
            };
        }

        private void CleanupJournal()
        {
            _journal?.Dispose();
            _journal = null!;

            var queuePath = Path.Combine(_rootDirectory, "queue");
            try
            {
                if (_fileSystem.Directory.Exists(queuePath))
                    _fileSystem.Directory.Delete(queuePath, recursive: true);
            }
            catch
            {
                // Ignore benchmark cleanup failures.
            }
        }
    }
}
