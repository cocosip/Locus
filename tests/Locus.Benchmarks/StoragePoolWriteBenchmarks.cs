using System;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.FileSystem;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Locus.Benchmarks
{
    /// <summary>
    /// End-to-end write throughput benchmarks for StoragePool.WriteFileAsync.
    /// Covers multiple DICOM-realistic file sizes: 100KB / 1MB / 10MB.
    /// Run in Release mode:
    ///   dotnet run -c Release -- --filter "StoragePoolWriteThroughput*"
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 3, iterationCount: 5)]
    public class StoragePoolWriteThroughputBenchmarks : IDisposable
    {
        /// <summary>100 KB / 1 MB / 10 MB — covers DICOM SR, CT slice, and MR volume.</summary>
        [Params(102400, 1048576, 10485760)]
        public int FileSize { get; set; }

        private IFileSystem _fileSystem;
        private MetadataRepository _metadataRepository;
        private DirectoryQuotaRepository _quotaRepository;
        private StoragePool _storagePool;
        private ITenantContext _tenant;
        private string _testDirectory;
        private byte[] _fileContent;

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _testDirectory = Path.Combine(Path.GetTempPath(), $"locus-write-bench-{testId}");
            var volumePath = Path.Combine(_testDirectory, "volume");
            _fileSystem.Directory.CreateDirectory(_testDirectory);
            _fileSystem.Directory.CreateDirectory(volumePath);

            var metadataLogger = NullLogger<MetadataRepository>.Instance;
            _metadataRepository = new MetadataRepository(
                _fileSystem, metadataLogger, Path.Combine(_testDirectory, "metadata"));

            var quotaLogger = NullLogger<DirectoryQuotaRepository>.Instance;
            _quotaRepository = new DirectoryQuotaRepository(
                _fileSystem, quotaLogger, Path.Combine(_testDirectory, "quota"));

            // Mock quota manager — isolates metadata + volume overhead only
            var tenantQuotaManager = new Mock<ITenantQuotaManager>();
            tenantQuotaManager
                .Setup(m => m.IncrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            tenantQuotaManager
                .Setup(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var tenantContext = new Mock<ITenantContext>();
            tenantContext.Setup(t => t.TenantId).Returns("bench-tenant");
            tenantContext.Setup(t => t.Status).Returns(TenantStatus.Enabled);

            var tenantManager = new Mock<ITenantManager>();
            tenantManager
                .Setup(m => m.GetTenantAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(tenantContext.Object);

            var fileScheduler = new Mock<IFileScheduler>();

            _storagePool = new StoragePool(
                _metadataRepository,
                tenantQuotaManager.Object,
                tenantManager.Object,
                fileScheduler.Object,
                NullLogger<StoragePool>.Instance);

            var volume = new LocalFileSystemVolume(
                _fileSystem,
                NullLogger<LocalFileSystemVolume>.Instance,
                "vol-001",
                volumePath);
            _storagePool.AddVolume(volume);

            _tenant = tenantContext.Object;
        }

        [IterationSetup]
        public void IterationSetup()
        {
            // Fill with non-zero pattern — avoids OS zero-page deduplication skewing results
            _fileContent = new byte[FileSize];
            for (int i = 0; i < FileSize; i++)
                _fileContent[i] = (byte)(i & 0xFF);
        }

        /// <summary>Single-threaded sequential write — establishes per-file latency baseline.</summary>
        [Benchmark(Baseline = true, Description = "Single-threaded write")]
        public async Task WriteFile_Sequential()
        {
            using var stream = new MemoryStream(_fileContent);
            await _storagePool.WriteFileAsync(_tenant, stream, "dicom-file.dcm", CancellationToken.None);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _metadataRepository?.Dispose();
            _quotaRepository?.Dispose();
            try
            {
                if (_fileSystem.Directory.Exists(_testDirectory))
                    _fileSystem.Directory.Delete(_testDirectory, recursive: true);
            }
            catch { }
        }

        public void Dispose() => Cleanup();
    }

    /// <summary>
    /// Concurrency benchmarks for StoragePool.WriteFileAsync.
    /// Fixed 1 MB file size (typical DICOM CT slice), varying concurrency level.
    /// Run in Release mode:
    ///   dotnet run -c Release -- --filter "StoragePoolConcurrency*"
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 3, iterationCount: 10)]
    public class StoragePoolConcurrencyBenchmarks : IDisposable
    {
        private const int FileSizeBytes = 1048576; // 1 MB — typical DICOM CT slice

        private IFileSystem _fileSystem;
        private MetadataRepository _metadataRepository;
        private DirectoryQuotaRepository _quotaRepository;
        private StoragePool _storagePool;
        private ITenantContext _tenant;
        private string _testDirectory;
        private byte[] _fileContent;

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _testDirectory = Path.Combine(Path.GetTempPath(), $"locus-conc-write-bench-{testId}");
            var volumePath = Path.Combine(_testDirectory, "volume");
            _fileSystem.Directory.CreateDirectory(_testDirectory);
            _fileSystem.Directory.CreateDirectory(volumePath);

            var metadataLogger = NullLogger<MetadataRepository>.Instance;
            _metadataRepository = new MetadataRepository(
                _fileSystem, metadataLogger, Path.Combine(_testDirectory, "metadata"));

            var quotaLogger = NullLogger<DirectoryQuotaRepository>.Instance;
            _quotaRepository = new DirectoryQuotaRepository(
                _fileSystem, quotaLogger, Path.Combine(_testDirectory, "quota"));

            var tenantQuotaManager = new Mock<ITenantQuotaManager>();
            tenantQuotaManager
                .Setup(m => m.IncrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            tenantQuotaManager
                .Setup(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var tenantContext = new Mock<ITenantContext>();
            tenantContext.Setup(t => t.TenantId).Returns("bench-tenant");
            tenantContext.Setup(t => t.Status).Returns(TenantStatus.Enabled);

            var tenantManager = new Mock<ITenantManager>();
            tenantManager
                .Setup(m => m.GetTenantAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(tenantContext.Object);

            var fileScheduler = new Mock<IFileScheduler>();

            _storagePool = new StoragePool(
                _metadataRepository,
                tenantQuotaManager.Object,
                tenantManager.Object,
                fileScheduler.Object,
                NullLogger<StoragePool>.Instance);

            var volume = new LocalFileSystemVolume(
                _fileSystem,
                NullLogger<LocalFileSystemVolume>.Instance,
                "vol-001",
                volumePath);
            _storagePool.AddVolume(volume);

            _tenant = tenantContext.Object;

            _fileContent = new byte[FileSizeBytes];
            for (int i = 0; i < FileSizeBytes; i++)
                _fileContent[i] = (byte)(i & 0xFF);
        }

        private async Task WriteConcurrently(int concurrency)
        {
            var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
            {
                using var stream = new MemoryStream(_fileContent);
                await _storagePool.WriteFileAsync(_tenant, stream, "dicom-file.dcm", CancellationToken.None);
            }));
            await Task.WhenAll(tasks);
        }

        /// <summary>Baseline: 1 writer — same as sequential but measured under the concurrency harness.</summary>
        [Benchmark(Baseline = true, Description = "1 writer (baseline)")]
        public async Task WriteFile_Concurrent1() => await WriteConcurrently(1);

        [Benchmark(Description = "10 concurrent writers")]
        public async Task WriteFile_Concurrent10() => await WriteConcurrently(10);

        [Benchmark(Description = "50 concurrent writers")]
        public async Task WriteFile_Concurrent50() => await WriteConcurrently(50);

        [Benchmark(Description = "100 concurrent writers")]
        public async Task WriteFile_Concurrent100() => await WriteConcurrently(100);

        [GlobalCleanup]
        public void Cleanup()
        {
            _metadataRepository?.Dispose();
            _quotaRepository?.Dispose();
            try
            {
                if (_fileSystem.Directory.Exists(_testDirectory))
                    _fileSystem.Directory.Delete(_testDirectory, recursive: true);
            }
            catch { }
        }

        public void Dispose() => Cleanup();
    }
}
