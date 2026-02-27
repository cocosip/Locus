using System;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    /// <summary>
    /// Benchmarks for directory quota operations
    /// Tests atomic increment/decrement performance
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 3, iterationCount: 5)]
    public class DirectoryQuotaBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem;
        private DirectoryQuotaRepository _repository;
        private DirectoryQuotaManager _manager;
        private string _testDirectory;
        private readonly string _tenantId = "benchmark-tenant";

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _testDirectory = Path.Combine(Path.GetTempPath(), $"locus-quota-benchmark-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_testDirectory);

            var logger = NullLogger<DirectoryQuotaRepository>.Instance;
            _repository = new DirectoryQuotaRepository(_fileSystem, logger, _testDirectory);

            var managerLogger = NullLogger<DirectoryQuotaManager>.Instance;
            _manager = new DirectoryQuotaManager(_repository, managerLogger);

            // Pre-warm all fixed paths so hot-path benchmarks skip the first-load cost
            _manager.SetLimitAsync(_tenantId, "/bench-dir-with-limit", 1000, CancellationToken.None).GetAwaiter().GetResult();
            _manager.CanAddFileAsync(_tenantId, "/bench-dir-no-limit", CancellationToken.None).GetAwaiter().GetResult();
            _manager.CanAddFileAsync(_tenantId, "/bench-dir-with-limit", CancellationToken.None).GetAwaiter().GetResult();
            _manager.IncrementFileCountAsync(_tenantId, "/bench-dir-increment", CancellationToken.None).GetAwaiter().GetResult();
            _manager.IncrementFileCountAsync(_tenantId, "/bench-dir-decrement", CancellationToken.None).GetAwaiter().GetResult();
            _manager.SetLimitAsync(_tenantId, "/bench-dir-setlimit", 100, CancellationToken.None).GetAwaiter().GetResult();
            _manager.IncrementFileCountAsync(_tenantId, "/bench-dir-getcount", CancellationToken.None).GetAwaiter().GetResult();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _repository?.Dispose();
            try
            {
                if (_fileSystem.Directory.Exists(_testDirectory))
                    _fileSystem.Directory.Delete(_testDirectory, recursive: true);
            }
            catch { }
        }

        [Benchmark(Description = "Check can add file (no limit)")]
        public async Task CanAddFileAsync_NoLimit()
        {
            // Use fixed path to benchmark hot-path CAS (AtomicQuotaState already initialized)
            await _manager.CanAddFileAsync(_tenantId, "/bench-dir-no-limit", CancellationToken.None);
        }

        [Benchmark(Description = "Check can add file (with limit, under quota)")]
        public async Task CanAddFileAsync_WithLimit()
        {
            // Use fixed path — SetLimitAsync is called in IterationSetup, not here
            await _manager.CanAddFileAsync(_tenantId, "/bench-dir-with-limit", CancellationToken.None);
        }

        [Benchmark(Description = "Increment file count")]
        public async Task IncrementFileCountAsync()
        {
            // Use fixed path to benchmark hot-path lock-free CAS increment
            await _manager.IncrementFileCountAsync(_tenantId, "/bench-dir-increment", CancellationToken.None);
        }

        [Benchmark(Description = "Decrement file count")]
        public async Task DecrementFileCountAsync()
        {
            // Pre-increment to keep count positive, then decrement
            await _manager.IncrementFileCountAsync(_tenantId, "/bench-dir-decrement", CancellationToken.None);
            await _manager.DecrementFileCountAsync(_tenantId, "/bench-dir-decrement", CancellationToken.None);
        }

        [Benchmark(Description = "Set directory limit")]
        public async Task SetLimitAsync()
        {
            // Use fixed path — SetLimitAsync updates MaxCount and syncs AtomicQuotaState
            await _manager.SetLimitAsync(_tenantId, "/bench-dir-setlimit", 100, CancellationToken.None);
        }

        [Benchmark(Description = "Get file count")]
        public async Task GetFileCountAsync()
        {
            // Use fixed path — reads from AtomicQuotaState (hot path)
            await _manager.GetFileCountAsync(_tenantId, "/bench-dir-getcount", CancellationToken.None);
        }

        public void Dispose()
        {
            Cleanup();
        }
    }
}
