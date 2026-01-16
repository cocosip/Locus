using System;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    /// <summary>
    /// Benchmarks for MetadataRepository operations
    /// Tests LiteDB performance with in-memory caching
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 3, iterationCount: 5)]
    public class MetadataRepositoryBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem;
        private MetadataRepository _repository;
        private string _testDirectory;
        private readonly string _tenantId = "benchmark-tenant";
        private int _fileCounter;

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _testDirectory = Path.Combine(Path.GetTempPath(), $"locus-benchmark-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_testDirectory);

            var logger = NullLogger<MetadataRepository>.Instance;
            _repository = new MetadataRepository(_fileSystem, logger, _testDirectory);
            _fileCounter = 0;
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

        [Benchmark(Description = "AddOrUpdate single file metadata")]
        public async Task AddOrUpdateAsync_Single()
        {
            var fileKey = $"file-{Interlocked.Increment(ref _fileCounter)}";
            var metadata = CreateTestMetadata(fileKey);
            await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);
        }

        [Benchmark(Description = "Get file metadata (cache hit)")]
        public async Task GetAsync_CacheHit()
        {
            // Pre-populate
            var fileKey = "cached-file";
            var metadata = CreateTestMetadata(fileKey);
            await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);

            // Benchmark - should hit cache
            await _repository.GetAsync(_tenantId, fileKey, CancellationToken.None);
        }

        [Benchmark(Description = "Get file metadata (cache miss)")]
        public async Task GetAsync_CacheMiss()
        {
            var fileKey = $"file-{Interlocked.Increment(ref _fileCounter)}";
            var metadata = CreateTestMetadata(fileKey);
            await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);

            // Clear cache by creating new file to force eviction
            for (int i = 0; i < 100; i++)
            {
                await _repository.AddOrUpdateAsync(CreateTestMetadata($"evict-{i}"), CancellationToken.None);
            }

            // Benchmark - should miss cache and hit LiteDB
            await _repository.GetAsync(_tenantId, fileKey, CancellationToken.None);
        }

        [Benchmark(Description = "Batch insert 100 files")]
        public async Task BatchInsert_100Files()
        {
            for (int i = 0; i < 100; i++)
            {
                var fileKey = $"batch-{Interlocked.Increment(ref _fileCounter)}";
                var metadata = CreateTestMetadata(fileKey);
                await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);
            }
        }

        [Benchmark(Description = "Get pending files (10 files)")]
        public async Task GetNextPendingFileAsync()
        {
            // Pre-populate with pending files
            for (int i = 0; i < 10; i++)
            {
                var fileKey = $"pending-{Interlocked.Increment(ref _fileCounter)}";
                var metadata = CreateTestMetadata(fileKey);
                metadata.Status = FileProcessingStatus.Pending;
                await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);
            }

            // Benchmark
            await _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);
        }

        private FileMetadata CreateTestMetadata(string fileKey)
        {
            return new FileMetadata
            {
                FileKey = fileKey,
                TenantId = _tenantId,
                VolumeId = "vol-001",
                PhysicalPath = $"/test/{fileKey}.dat",
                DirectoryPath = "/",
                FileSize = 1024,
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            };
        }

        public void Dispose()
        {
            Cleanup();
        }
    }
}
