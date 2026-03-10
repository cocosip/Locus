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
    ///
    /// Architecture notes:
    ///   - GetAsync only reads from the in-memory active-files cache (no SQLite fallback).
    ///     There is no traditional "cache miss → DB" path; a missing key returns null immediately.
    ///   - AddOrUpdateAsync is memory-first (O(1) ConcurrentDictionary update) with async
    ///     Write-Behind to SQLite; its cost does not grow with cache size.
    ///   - GetNextPendingFileAsync does a linear scan over the active cache for the tenant
    ///     (under a per-tenant SemaphoreSlim). Pool size is held stable at 100 files to keep
    ///     measurement conditions consistent across all invocations.
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
        public async Task Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _testDirectory = Path.Combine(Path.GetTempPath(), $"locus-benchmark-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_testDirectory);

            _repository = new MetadataRepository(_fileSystem, NullLogger<MetadataRepository>.Instance, _testDirectory);
            _fileCounter = 0;

            // Pre-populate the fixed file used by GetAsync_CacheHit (measured path is lookup only).
            await _repository.AddOrUpdateAsync(CreateTestMetadata("cached-file"), CancellationToken.None);

            // Pre-populate a stable pool of 100 pending files for GetNextPendingFileAsync.
            // The benchmark resets each claimed file back to Pending so the pool never shrinks.
            for (int i = 0; i < 100; i++)
            {
                var m = CreateTestMetadata($"pool-pending-{i}");
                m.Status = FileProcessingStatus.Pending;
                await _repository.AddOrUpdateAsync(m, CancellationToken.None);
            }
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
            await _repository.AddOrUpdateAsync(CreateTestMetadata(fileKey), CancellationToken.None);
        }

        [Benchmark(Description = "Get file metadata (cache hit)")]
        public async Task GetAsync_CacheHit()
        {
            // File pre-populated in GlobalSetup — only the ConcurrentDictionary lookup is measured.
            await _repository.GetAsync(_tenantId, "cached-file", CancellationToken.None);
        }

        [Benchmark(Description = "Get non-existent file (returns null)")]
        public async Task GetAsync_NotFound()
        {
            // Key absent from cache — ConcurrentDictionary.TryGetValue returns false immediately.
            // This is the actual "miss" cost in Write-Behind architecture (no SQLite fallback).
            await _repository.GetAsync(_tenantId, "nonexistent-key-xyz-000", CancellationToken.None);
        }

        [Benchmark(Description = "Batch insert 100 files")]
        public async Task BatchInsert_100Files()
        {
            for (int i = 0; i < 100; i++)
            {
                var fileKey = $"batch-{Interlocked.Increment(ref _fileCounter)}";
                await _repository.AddOrUpdateAsync(CreateTestMetadata(fileKey), CancellationToken.None);
            }
        }

        [Benchmark(Description = "Get next pending file (100-file pool, stable state)")]
        public async Task GetNextPendingFileAsync()
        {
            var file = await _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);
            if (file != null)
            {
                // Reset back to Pending so the pool stays at 100 files across all invocations.
                // The single AddOrUpdateAsync (memory-only, O(1)) is included in the measurement
                // but is negligible compared to the O(n) pending-file scan being benchmarked.
                file.Status = FileProcessingStatus.Pending;
                file.ProcessingStartTime = null;
                await _repository.AddOrUpdateAsync(file, CancellationToken.None);
            }
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
