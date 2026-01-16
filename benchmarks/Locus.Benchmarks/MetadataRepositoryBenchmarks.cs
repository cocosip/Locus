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
    /// Benchmarks for MetadataRepository operations.
    /// Tests the performance of metadata write/read operations with LiteDB + memory cache.
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 1, iterationCount: 5)]
    public class MetadataRepositoryBenchmarks : IDisposable
    {
        private MetadataRepository? _repository;
        private string _tempDirectory = string.Empty;
        private const string TenantId = "benchmark-tenant";
        private int _fileCounter;

        [GlobalSetup]
        public void Setup()
        {
            // Create temporary directory for benchmark
            _tempDirectory = Path.Combine(Path.GetTempPath(), "locus-benchmarks", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_tempDirectory);

            // Initialize repository
            var fileSystem = new FileSystem();
            var logger = NullLogger<MetadataRepository>.Instance;
            _repository = new MetadataRepository(fileSystem, logger, _tempDirectory);

            _fileCounter = 0;
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _repository?.Dispose();

            if (Directory.Exists(_tempDirectory))
            {
                Directory.Delete(_tempDirectory, recursive: true);
            }
        }

        /// <summary>
        /// Benchmark: Single metadata write operation (LiteDB + memory cache)
        /// </summary>
        [Benchmark]
        public async Task WriteMetadata_Single()
        {
            var metadata = CreateFileMetadata();
            await _repository!.AddOrUpdateAsync(metadata, CancellationToken.None);
        }

        /// <summary>
        /// Benchmark: Read from memory cache (should be microseconds)
        /// </summary>
        [Benchmark]
        public async Task ReadMetadata_FromCache()
        {
            // First write a file
            var metadata = CreateFileMetadata();
            await _repository!.AddOrUpdateAsync(metadata, CancellationToken.None);

            // Then read it (from cache)
            await _repository.GetAsync(TenantId, metadata.FileKey, CancellationToken.None);
        }

        /// <summary>
        /// Benchmark: Get next pending file (atomic operation with status update)
        /// </summary>
        [Benchmark]
        public async Task GetNextPendingFile()
        {
            // Prepare: Write 10 pending files
            for (int i = 0; i < 10; i++)
            {
                var metadata = CreateFileMetadata();
                await _repository!.AddOrUpdateAsync(metadata, CancellationToken.None);
            }

            // Benchmark: Get next file (updates status to Processing)
            await _repository!.GetNextPendingFileAsync(TenantId, CancellationToken.None);
        }

        /// <summary>
        /// Benchmark: Batch operation (get 100 files at once)
        /// </summary>
        [Benchmark]
        public async Task GetNextPendingBatch_100Files()
        {
            // Prepare: Write 100 pending files
            for (int i = 0; i < 100; i++)
            {
                var metadata = CreateFileMetadata();
                await _repository!.AddOrUpdateAsync(metadata, CancellationToken.None);
            }

            // Benchmark: Get batch of 100 files
            await _repository!.GetNextPendingBatchAsync(TenantId, 100, CancellationToken.None);
        }

        private FileMetadata CreateFileMetadata()
        {
            var fileKey = $"file-{Interlocked.Increment(ref _fileCounter):D10}";
            return new FileMetadata
            {
                FileKey = fileKey,
                TenantId = TenantId,
                VolumeId = "vol-001",
                PhysicalPath = $"/storage/{TenantId}/{fileKey}",
                DirectoryPath = "/",
                FileSize = 1024 * 1024, // 1MB
                CreatedAt = DateTime.UtcNow,
                Status = FileProcessingStatus.Pending,
                RetryCount = 0
            };
        }

        public void Dispose()
        {
            Cleanup();
        }
    }
}
