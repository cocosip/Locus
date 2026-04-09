#nullable enable
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
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 1, iterationCount: 3)]
    public class MetadataRepositoryColdStartBenchmarks : IDisposable
    {
        private readonly string _tenantId = "phased-cold-start";
        private IFileSystem _fileSystem = null!;
        private string _rootDirectory = string.Empty;
        private string _metadataDirectory = string.Empty;
        private MetadataRepository? _startupRepository;
        private string _deadLetteredProbeKey = string.Empty;

        [Params(20_000)]
        public int ActiveFileCount;

        [Params(512, 2_000)]
        public int StartupLoadBatchSize;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-phased-{Guid.NewGuid():N}");
            _metadataDirectory = Path.Combine(_rootDirectory, "metadata");
            _fileSystem.Directory.CreateDirectory(_metadataDirectory);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            _startupRepository?.Dispose();
            _startupRepository = null;

            if (_fileSystem.Directory.Exists(_metadataDirectory))
                _fileSystem.Directory.Delete(_metadataDirectory, recursive: true);
            _fileSystem.Directory.CreateDirectory(_metadataDirectory);

            using var writer = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                _metadataDirectory,
                enableBackgroundPersistence: false,
                startupLoadBatchSize: StartupLoadBatchSize);

            var now = DateTime.UtcNow;
            _deadLetteredProbeKey = string.Empty;
            for (var i = 0; i < ActiveFileCount; i++)
            {
                var status = ResolveStatus(i);
                var metadata = new FileMetadata
                {
                    FileKey = $"cold-{i:D8}",
                    TenantId = _tenantId,
                    VolumeId = "vol-001",
                    PhysicalPath = $"/bench/cold-{i:D8}.dat",
                    DirectoryPath = "/bench",
                    FileSize = 256,
                    Status = status,
                    CreatedAt = now.AddTicks(i)
                };

                if (status == FileProcessingStatus.Processing)
                    metadata.ProcessingStartTime = now.AddMinutes(-30);
                if (status == FileProcessingStatus.PermanentlyFailed)
                    metadata.LastFailedAt = now.AddDays(-10);
                if (status == FileProcessingStatus.DeadLettered)
                {
                    metadata.DeadLetteredAt = now.AddDays(-7);
                    metadata.PhysicalPath = $"/deadletter/cold-{i:D8}.dat";
                    _deadLetteredProbeKey = metadata.FileKey;
                }

                writer.AddOrUpdateAsync(metadata, CancellationToken.None).GetAwaiter().GetResult();
            }
        }

        [Benchmark(Description = "cold-start metadata active-index load (includes dead-lettered)")]
        public async Task<int> ColdStart_LoadActiveIndexes()
        {
            _startupRepository = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                _metadataDirectory,
                enableBackgroundPersistence: false,
                startupLoadBatchSize: StartupLoadBatchSize);

            var allocated = await _startupRepository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);
            var timedOut = await _startupRepository.GetProcessingTimedOutAsync(
                _tenantId,
                DateTime.UtcNow.AddMinutes(-1),
                1,
                CancellationToken.None);
            var deadLettered = string.IsNullOrEmpty(_deadLetteredProbeKey)
                ? null
                : await _startupRepository.GetAsync(_tenantId, _deadLetteredProbeKey, CancellationToken.None);

            return (allocated != null ? 1 : 0) + timedOut.Count + (deadLettered != null ? 1 : 0);
        }

        public void Dispose()
        {
            _startupRepository?.Dispose();
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

        private static FileProcessingStatus ResolveStatus(int index)
        {
            var remainder = index % 10;
            if (remainder < 5)
                return FileProcessingStatus.Pending;
            if (remainder < 7)
                return FileProcessingStatus.Processing;
            if (remainder == 7)
                return FileProcessingStatus.Failed;
            if (remainder == 8)
                return FileProcessingStatus.PermanentlyFailed;

            return FileProcessingStatus.DeadLettered;
        }
    }
}
