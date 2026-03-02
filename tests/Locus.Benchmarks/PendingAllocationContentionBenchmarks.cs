#nullable enable
using System;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 1, iterationCount: 5)]
    public class PendingAllocationContentionBenchmarks : IDisposable
    {
        private const string TenantId = "phase7-pending-contention";

        private IFileSystem _fileSystem = null!;
        private MetadataRepository _repository = null!;
        private string _rootDirectory = string.Empty;
        private string _metadataDirectory = string.Empty;

        [Params(64)]
        public int WorkerCount;

        [Params(2048)]
        public int PendingPoolSize;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-pending-contention-{Guid.NewGuid():N}");
            _metadataDirectory = Path.Combine(_rootDirectory, "metadata");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            _repository?.Dispose();

            if (_fileSystem.Directory.Exists(_metadataDirectory))
                _fileSystem.Directory.Delete(_metadataDirectory, recursive: true);
            _fileSystem.Directory.CreateDirectory(_metadataDirectory);

            _repository = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                _metadataDirectory,
                enableBackgroundPersistence: false);

            var now = DateTime.UtcNow;
            for (var i = 0; i < PendingPoolSize; i++)
            {
                _repository.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = $"pending-{i:D6}",
                    TenantId = TenantId,
                    VolumeId = "vol-001",
                    PhysicalPath = $"/bench/pending-{i:D6}.dat",
                    DirectoryPath = "/bench",
                    FileSize = 1024,
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = now.AddTicks(i)
                }, CancellationToken.None).GetAwaiter().GetResult();
            }
        }

        [Benchmark(Description = "pending allocation contention (single-file API)")]
        public async Task AllocateSinglePendingFile_Concurrent()
        {
            var workers = Math.Max(1, WorkerCount);
            var tasks = Enumerable.Range(0, workers)
                .Select(_ => AllocateAndRequeueOneAsync())
                .ToArray();
            await Task.WhenAll(tasks);
        }

        [Benchmark(Description = "pending allocation contention (batch API)")]
        public async Task AllocatePendingBatch_Concurrent()
        {
            var workers = Math.Max(1, WorkerCount / 4);
            const int batchSize = 4;

            var tasks = Enumerable.Range(0, workers)
                .Select(_ => AllocateAndRequeueBatchAsync(batchSize))
                .ToArray();
            await Task.WhenAll(tasks);
        }

        public void Dispose()
        {
            _repository?.Dispose();
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

        private async Task AllocateAndRequeueOneAsync()
        {
            var allocated = await _repository.GetNextPendingFileAsync(TenantId, CancellationToken.None);
            if (allocated == null)
                return;

            allocated.Status = FileProcessingStatus.Pending;
            allocated.ProcessingStartTime = null;
            await _repository.AddOrUpdateAsync(allocated, CancellationToken.None);
        }

        private async Task AllocateAndRequeueBatchAsync(int batchSize)
        {
            var allocatedBatch = (await _repository.GetNextPendingBatchAsync(TenantId, batchSize, CancellationToken.None)).ToArray();
            for (var i = 0; i < allocatedBatch.Length; i++)
            {
                var file = allocatedBatch[i];
                file.Status = FileProcessingStatus.Pending;
                file.ProcessingStartTime = null;
                await _repository.AddOrUpdateAsync(file, CancellationToken.None);
            }
        }
    }
}
