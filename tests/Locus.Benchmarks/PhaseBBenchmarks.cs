#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Runtime.InteropServices;
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
    [SimpleJob(warmupCount: 1, iterationCount: 3)]
    public class OrphanRebuildMemoryBenchmarks : IDisposable
    {
        private readonly string _tenantId = "phaseb-orphan-memory";
        private readonly StringComparer _pathComparer = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal;

        private IFileSystem _fileSystem = null!;
        private MetadataRepository _metadataRepository = null!;
        private DirectoryQuotaRepository _quotaRepository = null!;
        private StorageCleanupService _cleanupService = null!;
        private string _rootDirectory = string.Empty;
        private string _metadataDirectory = string.Empty;
        private string _quotaDirectory = string.Empty;
        private string _volumeDirectory = string.Empty;

        [Params(8_000)]
        public int FileCount;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-phaseb-orphan-{Guid.NewGuid():N}");
            _metadataDirectory = Path.Combine(_rootDirectory, "metadata");
            _quotaDirectory = Path.Combine(_rootDirectory, "quota");
            _volumeDirectory = Path.Combine(_rootDirectory, "volume");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup(Target = nameof(Baseline_FullHashSetPathScan))]
        public void IterationSetupBaseline()
        {
            PrepareDataset();
        }

        [IterationSetup(Target = nameof(Current_OnDemandPathLookup))]
        public void IterationSetupCurrent()
        {
            PrepareDataset();
        }

        [Benchmark(Baseline = true, Description = "orphan-rebuild baseline full HashSet")]
        public async Task Baseline_FullHashSetPathScan()
        {
            var allMetadata = await _metadataRepository.GetByTenantAsync(_tenantId, CancellationToken.None);
            var knownPaths = new HashSet<string>(_pathComparer);
            foreach (var metadata in allMetadata)
            {
                var normalized = NormalizePath(metadata.PhysicalPath);
                if (normalized != null)
                    knownPaths.Add(normalized);
            }

            var tenantPath = Path.Combine(_volumeDirectory, _tenantId);
            var knownFileHits = 0;
            foreach (var physicalPath in _fileSystem.Directory.EnumerateFiles(tenantPath, "*", SearchOption.AllDirectories))
            {
                var normalized = NormalizePath(physicalPath);
                if (normalized != null && knownPaths.Contains(normalized))
                    knownFileHits++;
            }

            if (knownFileHits != FileCount)
                throw new InvalidOperationException($"Expected {FileCount} known files but matched {knownFileHits}.");
        }

        [Benchmark(Description = "orphan-rebuild current on-demand lookup")]
        public Task Current_OnDemandPathLookup()
        {
            return _cleanupService.RecoverOrphanedFilesAsync(new BenchTenantContext(_tenantId), CancellationToken.None);
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

        private void PrepareDataset()
        {
            _metadataRepository?.Dispose();
            _quotaRepository?.Dispose();

            if (_fileSystem.Directory.Exists(_metadataDirectory))
                _fileSystem.Directory.Delete(_metadataDirectory, recursive: true);
            if (_fileSystem.Directory.Exists(_quotaDirectory))
                _fileSystem.Directory.Delete(_quotaDirectory, recursive: true);
            if (_fileSystem.Directory.Exists(_volumeDirectory))
                _fileSystem.Directory.Delete(_volumeDirectory, recursive: true);

            _fileSystem.Directory.CreateDirectory(_metadataDirectory);
            _fileSystem.Directory.CreateDirectory(_quotaDirectory);
            _fileSystem.Directory.CreateDirectory(_volumeDirectory);

            _metadataRepository = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                _metadataDirectory,
                enableBackgroundPersistence: false);
            _quotaRepository = new DirectoryQuotaRepository(
                _fileSystem,
                NullLogger<DirectoryQuotaRepository>.Instance,
                _quotaDirectory);
            var tenantQuotaManager = new TenantQuotaManager(
                _quotaRepository,
                NullLogger<TenantQuotaManager>.Instance);

            _cleanupService = new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                tenantQuotaManager,
                _fileSystem,
                NullLogger<StorageCleanupService>.Instance,
                _metadataDirectory,
                _quotaDirectory,
                new CleanupOptions
                {
                    MaxOrphanFilesPerRun = int.MaxValue,
                    OrphanRebuildLookupCacheSize = 8192
                });

            var volume = new Mock<IStorageVolume>();
            volume.Setup(v => v.VolumeId).Returns("vol-001");
            volume.Setup(v => v.MountPath).Returns(_volumeDirectory);
            _cleanupService.RegisterVolume(volume.Object);

            var tenantPath = Path.Combine(_volumeDirectory, _tenantId);
            _fileSystem.Directory.CreateDirectory(tenantPath);

            var now = DateTime.UtcNow;
            for (var i = 0; i < FileCount; i++)
            {
                var fileKey = $"known-{i:D6}";
                var path = Path.Combine(tenantPath, $"{fileKey}.dat");
                _fileSystem.File.WriteAllText(path, "benchmark");

                _metadataRepository.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = fileKey,
                    TenantId = _tenantId,
                    VolumeId = "vol-001",
                    PhysicalPath = path,
                    DirectoryPath = tenantPath,
                    FileSize = 9,
                    Status = FileProcessingStatus.Pending,
                    CreatedAt = now.AddTicks(i)
                }, CancellationToken.None).GetAwaiter().GetResult();
            }
        }

        private string? NormalizePath(string? path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return null;

            try
            {
                return _fileSystem.Path.GetFullPath(path);
            }
            catch
            {
                return path;
            }
        }

        private sealed class BenchTenantContext : ITenantContext
        {
            public BenchTenantContext(string tenantId)
            {
                TenantId = tenantId;
            }

            public string TenantId { get; }

            public TenantStatus Status => TenantStatus.Enabled;
        }
    }

    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 1, iterationCount: 4)]
    public class CompletionGuardContentionBenchmarks : IDisposable
    {
        private readonly string _tenantId = "phaseb-completion-contention";

        private IFileSystem _fileSystem = null!;
        private MetadataRepository _metadataRepository = null!;
        private StoragePool _storagePool = null!;
        private string _rootDirectory = string.Empty;
        private string _metadataDirectory = string.Empty;
        private string[] _fileKeys = Array.Empty<string>();
        private DateTime _processingStartTime;

        [Params(64, 256)]
        public int CompletionGuardStripeCount;

        [Params(8_192)]
        public int FileCount;

        [Params(256)]
        public int WorkerCount;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-phaseb-completion-{Guid.NewGuid():N}");
            _metadataDirectory = Path.Combine(_rootDirectory, "metadata");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
            _fileKeys = Enumerable.Range(0, FileCount).Select(i => $"complete-{i:D6}").ToArray();
        }

        [IterationSetup]
        public void IterationSetup()
        {
            _metadataRepository?.Dispose();

            if (_fileSystem.Directory.Exists(_metadataDirectory))
                _fileSystem.Directory.Delete(_metadataDirectory, recursive: true);
            _fileSystem.Directory.CreateDirectory(_metadataDirectory);

            _metadataRepository = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                _metadataDirectory,
                enableBackgroundPersistence: false);

            var tenantQuotaManager = new Mock<ITenantQuotaManager>();
            tenantQuotaManager
                .Setup(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var directoryQuotaManager = new Mock<IDirectoryQuotaManager>();
            directoryQuotaManager
                .Setup(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            directoryQuotaManager
                .Setup(m => m.IncrementFileCountAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var tenantManager = new Mock<ITenantManager>();
            var scheduler = new Mock<IFileScheduler>();
            scheduler
                .Setup(s => s.MarkAsCompletedAsync(It.IsAny<FileProcessingLease>(), It.IsAny<CancellationToken>()))
                .Returns(async (FileProcessingLease lease, CancellationToken token) =>
                {
                    var metadata = await _metadataRepository.GetAsync(lease.TenantId, lease.FileKey, token);
                    if (metadata != null)
                        await _metadataRepository.RemoveAsync(metadata.TenantId, lease.FileKey, token);
                });

            _storagePool = new StoragePool(
                _metadataRepository,
                tenantQuotaManager.Object,
                directoryQuotaManager.Object,
                tenantManager.Object,
                scheduler.Object,
                NullLogger<StoragePool>.Instance,
                CompletionGuardStripeCount);

            _processingStartTime = DateTime.UtcNow;
            foreach (var fileKey in _fileKeys)
            {
                _metadataRepository.AddOrUpdateAsync(new FileMetadata
                {
                    FileKey = fileKey,
                    TenantId = _tenantId,
                    VolumeId = "vol-001",
                    PhysicalPath = $"/bench/{fileKey}.dat",
                    DirectoryPath = "/bench",
                    FileSize = 1024,
                    Status = FileProcessingStatus.Processing,
                    CreatedAt = _processingStartTime,
                    ProcessingStartTime = _processingStartTime
                }, CancellationToken.None).GetAwaiter().GetResult();
            }
        }

        [Benchmark(Description = "completion contention by stripe count")]
        public async Task MarkAsCompleted_HighCardinalityContention()
        {
            var workerCount = Math.Max(1, WorkerCount);
            var chunkSize = (int)Math.Ceiling(_fileKeys.Length / (double)workerCount);
            var tasks = new Task[workerCount];

            for (var worker = 0; worker < workerCount; worker++)
            {
                var start = worker * chunkSize;
                var end = Math.Min(start + chunkSize, _fileKeys.Length);
                if (start >= end)
                {
                    tasks[worker] = Task.CompletedTask;
                    continue;
                }

                tasks[worker] = Task.Run(async () =>
                {
                    for (var i = start; i < end; i++)
                    {
                        await _storagePool.MarkAsCompletedAsync(new FileProcessingLease
                        {
                            TenantId = _tenantId,
                            FileKey = _fileKeys[i],
                            ProcessingStartTimeUtc = _processingStartTime
                        }, CancellationToken.None);
                    }
                });
            }

            await Task.WhenAll(tasks);
        }

        public void Dispose()
        {
            _metadataRepository?.Dispose();
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
    public class OrphanRebuildBudgetBenchmarks : IDisposable
    {
        private readonly string _tenantId = "phaseb-orphan-budget";

        private IFileSystem _fileSystem = null!;
        private MetadataRepository _metadataRepository = null!;
        private DirectoryQuotaRepository _quotaRepository = null!;
        private StorageCleanupService _singlePassCleanupService = null!;
        private StorageCleanupService _budgetedCleanupService = null!;
        private string _rootDirectory = string.Empty;
        private string _singlePassMetadataDirectory = string.Empty;
        private string _singlePassQuotaDirectory = string.Empty;
        private string _singlePassVolumeDirectory = string.Empty;
        private string _budgetedMetadataDirectory = string.Empty;
        private string _budgetedQuotaDirectory = string.Empty;
        private string _budgetedVolumeDirectory = string.Empty;

        [Params(4_096)]
        public int FileCount;

        [Params(128)]
        public int MaxOrphanFilesPerRun;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-phaseb-orphan-budget-{Guid.NewGuid():N}");
            _singlePassMetadataDirectory = Path.Combine(_rootDirectory, "single-metadata");
            _singlePassQuotaDirectory = Path.Combine(_rootDirectory, "single-quota");
            _singlePassVolumeDirectory = Path.Combine(_rootDirectory, "single-volume");
            _budgetedMetadataDirectory = Path.Combine(_rootDirectory, "budgeted-metadata");
            _budgetedQuotaDirectory = Path.Combine(_rootDirectory, "budgeted-quota");
            _budgetedVolumeDirectory = Path.Combine(_rootDirectory, "budgeted-volume");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup(Target = nameof(Current_SinglePassFullBudget))]
        public void IterationSetupSinglePass()
        {
            PrepareDataset(
                _singlePassMetadataDirectory,
                _singlePassQuotaDirectory,
                _singlePassVolumeDirectory,
                out _metadataRepository,
                out _quotaRepository,
                out _singlePassCleanupService,
                int.MaxValue);
        }

        [IterationSetup(Target = nameof(Current_BudgetedResumeUntilComplete))]
        public void IterationSetupBudgeted()
        {
            PrepareDataset(
                _budgetedMetadataDirectory,
                _budgetedQuotaDirectory,
                _budgetedVolumeDirectory,
                out _metadataRepository,
                out _quotaRepository,
                out _budgetedCleanupService,
                MaxOrphanFilesPerRun);
        }

        [Benchmark(Baseline = true, Description = "orphan-rebuild single-pass full budget")]
        public async Task Current_SinglePassFullBudget()
        {
            await _singlePassCleanupService.RecoverOrphanedFilesAsync(new BenchTenantContext(_tenantId), CancellationToken.None);

            var stats = await _singlePassCleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            if (stats.OrphanedFilesRecovered != FileCount)
                throw new InvalidOperationException($"Expected {FileCount} rebuilt files, actual {stats.OrphanedFilesRecovered}.");
        }

        [Benchmark(Description = "orphan-rebuild budgeted resume until complete")]
        public async Task Current_BudgetedResumeUntilComplete()
        {
            while (true)
            {
                await _budgetedCleanupService.RecoverOrphanedFilesAsync(new BenchTenantContext(_tenantId), CancellationToken.None);
                var stats = await _budgetedCleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
                if (stats.OrphanedFilesRecovered >= FileCount)
                    break;
            }
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

        private void PrepareDataset(
            string metadataDirectory,
            string quotaDirectory,
            string volumeDirectory,
            out MetadataRepository metadataRepository,
            out DirectoryQuotaRepository quotaRepository,
            out StorageCleanupService cleanupService,
            int maxOrphanFilesPerRun)
        {
            if (_fileSystem.Directory.Exists(metadataDirectory))
                _fileSystem.Directory.Delete(metadataDirectory, recursive: true);
            if (_fileSystem.Directory.Exists(quotaDirectory))
                _fileSystem.Directory.Delete(quotaDirectory, recursive: true);
            if (_fileSystem.Directory.Exists(volumeDirectory))
                _fileSystem.Directory.Delete(volumeDirectory, recursive: true);

            _fileSystem.Directory.CreateDirectory(metadataDirectory);
            _fileSystem.Directory.CreateDirectory(quotaDirectory);
            _fileSystem.Directory.CreateDirectory(volumeDirectory);

            metadataRepository = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                metadataDirectory,
                enableBackgroundPersistence: false);
            quotaRepository = new DirectoryQuotaRepository(
                _fileSystem,
                NullLogger<DirectoryQuotaRepository>.Instance,
                quotaDirectory);
            var tenantQuotaManager = new TenantQuotaManager(
                quotaRepository,
                NullLogger<TenantQuotaManager>.Instance);

            cleanupService = new StorageCleanupService(
                metadataRepository,
                quotaRepository,
                tenantQuotaManager,
                _fileSystem,
                NullLogger<StorageCleanupService>.Instance,
                metadataDirectory,
                quotaDirectory,
                new CleanupOptions
                {
                    MaxOrphanFilesPerRun = maxOrphanFilesPerRun,
                    OrphanRebuildLookupCacheSize = 8192
                });

            var volume = new Mock<IStorageVolume>();
            volume.Setup(v => v.VolumeId).Returns("vol-001");
            volume.Setup(v => v.MountPath).Returns(volumeDirectory);
            cleanupService.RegisterVolume(volume.Object);

            var tenantPath = Path.Combine(volumeDirectory, _tenantId);
            _fileSystem.Directory.CreateDirectory(tenantPath);

            for (var i = 0; i < FileCount; i++)
            {
                var fileKey = $"orphan-{i:D6}";
                var path = Path.Combine(tenantPath, $"{fileKey}.dat");
                _fileSystem.File.WriteAllText(path, "benchmark");
            }
        }

        private sealed class BenchTenantContext : ITenantContext
        {
            public BenchTenantContext(string tenantId)
            {
                TenantId = tenantId;
            }

            public string TenantId { get; }

            public TenantStatus Status => TenantStatus.Enabled;
        }
    }
}
