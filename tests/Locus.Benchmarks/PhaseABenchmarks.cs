#nullable enable
using System;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Locus.Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 2, iterationCount: 5)]
    public class WriteBurstEnqueueBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private MetadataRepository _repository = null!;
        private string _rootDirectory = string.Empty;
        private readonly string _tenantId = "phasea-write-burst";
        private int _counter;

        [Params(2048)]
        public int OperationCount;

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-writeburst-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);

            _repository = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                Path.Combine(_rootDirectory, "metadata"),
                enableBackgroundPersistence: true,
                maxDrainBatchSize: 512,
                persistenceQueueSoftMergeThresholdPercent: 90,
                maxPersistenceQueueSize: 10_000);
        }

        [Benchmark(Description = "write-burst enqueue overhead")]
        public async Task WriteBurstEnqueueOverhead()
        {
            var workers = 32;
            var perWorker = OperationCount / workers;
            var tasks = new Task[workers];
            for (int worker = 0; worker < workers; worker++)
            {
                tasks[worker] = Task.Run(async () =>
                {
                    for (int i = 0; i < perWorker; i++)
                    {
                        var id = Interlocked.Increment(ref _counter);
                        var metadata = new FileMetadata
                        {
                            FileKey = $"burst-{id}",
                            TenantId = _tenantId,
                            VolumeId = "vol-001",
                            PhysicalPath = $"/bench/{id}.dat",
                            DirectoryPath = "/",
                            FileSize = 1024,
                            Status = FileProcessingStatus.Pending,
                            CreatedAt = DateTime.UtcNow
                        };
                        await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);
                    }
                });
            }

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
    }

    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 2, iterationCount: 8)]
    public class StalePendingQueueBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private MetadataRepository _repository = null!;
        private string _rootDirectory = string.Empty;
        private readonly string _tenantId = "phasea-stale-queue";

        [Params(1024)]
        public int StaleEntryCount;

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-stalequeue-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup]
        public void SetupIteration()
        {
            _repository?.Dispose();

            var metadataPath = Path.Combine(_rootDirectory, "metadata");
            if (_fileSystem.Directory.Exists(metadataPath))
                _fileSystem.Directory.Delete(metadataPath, recursive: true);

            _fileSystem.Directory.CreateDirectory(metadataPath);
            _repository = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                metadataPath,
                enableBackgroundPersistence: false);

            var metadata = new FileMetadata
            {
                FileKey = "stale-target",
                TenantId = _tenantId,
                VolumeId = "vol-001",
                PhysicalPath = "/bench/stale-target.dat",
                DirectoryPath = "/",
                FileSize = 1024,
                Status = FileProcessingStatus.Pending,
                CreatedAt = DateTime.UtcNow
            };

            // Repeated Pending updates create stale queue entries; only the latest generation remains valid.
            for (int i = 0; i < StaleEntryCount; i++)
            {
                metadata.CreatedAt = DateTime.UtcNow.AddTicks(i);
                _repository.AddOrUpdateAsync(metadata.Clone(), CancellationToken.None).GetAwaiter().GetResult();
            }
        }

        [Benchmark(Description = "stale-pending-queue dequeue")]
        public Task<FileMetadata?> DequeueFromStalePendingQueue()
        {
            return _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);
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
    }

    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 1, iterationCount: 3)]
    public class CleanupLargeTenantBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private MetadataRepository _metadataRepository = null!;
        private DirectoryQuotaRepository _quotaRepository = null!;
        private StorageCleanupService _cleanupService = null!;
        private string _rootDirectory = string.Empty;
        private readonly string _tenantId = "phasea-cleanup-large";

        [Params(1200)]
        public int ProcessingFileCount;

        [Params(800)]
        public int PermanentlyFailedFileCount;

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-cleanup-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup]
        public void SetupIterationAsync()
        {
            _metadataRepository?.Dispose();
            _quotaRepository?.Dispose();

            var metadataPath = Path.Combine(_rootDirectory, "metadata");
            var quotaPath = Path.Combine(_rootDirectory, "quota");
            if (_fileSystem.Directory.Exists(metadataPath))
                _fileSystem.Directory.Delete(metadataPath, recursive: true);
            if (_fileSystem.Directory.Exists(quotaPath))
                _fileSystem.Directory.Delete(quotaPath, recursive: true);
            _fileSystem.Directory.CreateDirectory(metadataPath);
            _fileSystem.Directory.CreateDirectory(quotaPath);

            _metadataRepository = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                metadataPath,
                enableBackgroundPersistence: false);
            _quotaRepository = new DirectoryQuotaRepository(
                _fileSystem,
                NullLogger<DirectoryQuotaRepository>.Instance,
                quotaPath);
            var tenantQuotaManager = new TenantQuotaManager(
                _quotaRepository,
                NullLogger<TenantQuotaManager>.Instance);

            _cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                tenantQuotaManager,
                _fileSystem,
                NullLogger<StorageCleanupService>.Instance,
                metadataPath,
                quotaPath,
                new CleanupOptions
                {
                    CleanupBatchSizePerTenant = 512
                });

            var volume = new Mock<IStorageVolume>();
            volume.Setup(v => v.VolumeId).Returns("vol-001");
            volume.Setup(v => v.MountPath).Returns(Path.Combine(_rootDirectory, "volume"));
            volume.Setup(v => v.DeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            _cleanupService.RegisterVolume(volume.Object);

            var now = DateTime.UtcNow;
            for (int i = 0; i < ProcessingFileCount; i++)
            {
                _metadataRepository.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = $"processing-{i}",
                    TenantId = _tenantId,
                    VolumeId = "vol-001",
                    PhysicalPath = $"/bench/processing-{i}.dat",
                    DirectoryPath = "/processing",
                    FileSize = 1024,
                    Status = FileProcessingStatus.Processing,
                    ProcessingStartTime = now.AddMinutes(-30),
                    CreatedAt = now.AddHours(-1)
                }, CancellationToken.None).GetAwaiter().GetResult();
            }

            for (int i = 0; i < PermanentlyFailedFileCount; i++)
            {
                _metadataRepository.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = $"failed-{i}",
                    TenantId = _tenantId,
                    VolumeId = "vol-001",
                    PhysicalPath = $"/bench/failed-{i}.dat",
                    DirectoryPath = "/failed",
                    FileSize = 1024,
                    Status = FileProcessingStatus.PermanentlyFailed,
                    LastFailedAt = now.AddDays(-14),
                    CreatedAt = now.AddDays(-30)
                }, CancellationToken.None).GetAwaiter().GetResult();
            }
        }

        [Benchmark(Description = "cleanup-large-tenant status cleanup")]
        public Task CleanupLargeTenantStatusCleanup()
        {
            return _cleanupService.CleanupFilesByStatusAsync(
                processingTimeout: TimeSpan.FromMinutes(5),
                failedRetentionPeriod: TimeSpan.FromDays(7),
                ct: CancellationToken.None);
        }

        public void Dispose()
        {
            _metadataRepository?.Dispose();
            _quotaRepository?.Dispose();
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
    }
}
