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
        private int _dirCounter;

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
            _dirCounter = 0;
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
            var dirPath = $"/dir-{Interlocked.Increment(ref _dirCounter)}";
            await _manager.CanAddFileAsync(_tenantId, dirPath, CancellationToken.None);
        }

        [Benchmark(Description = "Check can add file (with limit, under quota)")]
        public async Task CanAddFileAsync_WithLimit()
        {
            var dirPath = $"/limited-{Interlocked.Increment(ref _dirCounter)}";
            await _manager.SetLimitAsync(_tenantId, dirPath, 1000, CancellationToken.None);
            await _manager.CanAddFileAsync(_tenantId, dirPath, CancellationToken.None);
        }

        [Benchmark(Description = "Increment file count")]
        public async Task IncrementFileCountAsync()
        {
            var dirPath = $"/increment-{Interlocked.Increment(ref _dirCounter)}";
            await _manager.IncrementFileCountAsync(_tenantId, dirPath, CancellationToken.None);
        }

        [Benchmark(Description = "Decrement file count")]
        public async Task DecrementFileCountAsync()
        {
            var dirPath = $"/decrement-{Interlocked.Increment(ref _dirCounter)}";
            // Pre-increment
            await _manager.IncrementFileCountAsync(_tenantId, dirPath, CancellationToken.None);
            // Benchmark
            await _manager.DecrementFileCountAsync(_tenantId, dirPath, CancellationToken.None);
        }

        [Benchmark(Description = "Set directory limit")]
        public async Task SetLimitAsync()
        {
            var dirPath = $"/setlimit-{Interlocked.Increment(ref _dirCounter)}";
            await _manager.SetLimitAsync(_tenantId, dirPath, 100, CancellationToken.None);
        }

        [Benchmark(Description = "Get file count")]
        public async Task GetFileCountAsync()
        {
            var dirPath = "/getcount-dir";
            // Pre-populate
            await _manager.IncrementFileCountAsync(_tenantId, dirPath, CancellationToken.None);
            // Benchmark
            await _manager.GetFileCountAsync(_tenantId, dirPath, CancellationToken.None);
        }

        public void Dispose()
        {
            Cleanup();
        }
    }
}
