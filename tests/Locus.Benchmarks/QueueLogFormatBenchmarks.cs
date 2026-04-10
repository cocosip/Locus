#nullable enable
using System;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Locus.Core.Models;
using Locus.Storage;

namespace Locus.Benchmarks
{
    /// <summary>
    /// Compares JSONL and binary queue.log append latency under the same single-writer model.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 2, iterationCount: 6)]
    public class QueueLogFormatSingleRecordBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private string _rootDirectory = string.Empty;
        private IQueueLogBenchmarkJournal _journal = null!;
        private int _sequence;

        [Params(QueueLogEncodingFormat.Json, QueueLogEncodingFormat.Binary)]
        public QueueLogEncodingFormat Format { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-queuelog-format-single-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            CleanupJournal();
            _sequence = 0;
            _journal = CreateJournal();
        }

        [Benchmark(Description = "queue.log format single append")]
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
            }
        }

        private IQueueLogBenchmarkJournal CreateJournal()
        {
            return new QueueLogBenchmarkJournal(
                _fileSystem,
                Path.Combine(_rootDirectory, "queue"),
                Format,
                QueueEventJournalAckMode.Durable,
                TimeSpan.FromMilliseconds(1),
                16,
                256 * 1024,
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMilliseconds(5));
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
            }
        }
    }

    /// <summary>
    /// Compares JSONL and binary queue.log throughput under burst pressure.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 2, iterationCount: 6)]
    public class QueueLogFormatBurstBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private string _rootDirectory = string.Empty;
        private IQueueLogBenchmarkJournal _journal = null!;
        private int _sequence;

        [Params(QueueLogEncodingFormat.Json, QueueLogEncodingFormat.Binary)]
        public QueueLogEncodingFormat Format { get; set; }

        [Params(QueueEventJournalAckMode.Durable, QueueEventJournalAckMode.Balanced)]
        public QueueEventJournalAckMode AckMode { get; set; }

        [Params(32)]
        public int Concurrency { get; set; }

        [Params(4096)]
        public int TotalOperations { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-queuelog-format-burst-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            CleanupJournal();
            _sequence = 0;
            _journal = CreateJournal();
        }

        [Benchmark(Description = "queue.log format burst (single tenant contention)")]
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

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        [Benchmark(Description = "queue.log format burst (multi-tenant fan-out)")]
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

            await Task.WhenAll(tasks).ConfigureAwait(false);
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
            }
        }

        private IQueueLogBenchmarkJournal CreateJournal()
        {
            return new QueueLogBenchmarkJournal(
                _fileSystem,
                Path.Combine(_rootDirectory, "queue"),
                Format,
                AckMode,
                TimeSpan.FromMilliseconds(1),
                16,
                256 * 1024,
                TimeSpan.FromSeconds(30),
                TimeSpan.FromMilliseconds(5));
        }

        private async Task AppendBurstAsync(string tenantId, int operations)
        {
            for (var i = 0; i < operations; i++)
                await _journal.AppendAsync(CreateRecord(tenantId), CancellationToken.None).ConfigureAwait(false);
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
            }
        }
    }
}
