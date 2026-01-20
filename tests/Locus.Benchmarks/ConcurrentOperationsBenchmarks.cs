using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Text;
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
    /// Benchmarks for concurrent operations
    /// Tests multi-threaded read/write performance
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 2, iterationCount: 3)]
    public class ConcurrentOperationsBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem;
        private MetadataRepository _metadataRepository;
        private DirectoryQuotaRepository _quotaRepository;
        private StoragePool _storagePool;
        private IStorageVolume _volume;
        private ITenantContext _tenant;
        private string _testDirectory;
        private string _volumePath;

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _testDirectory = Path.Combine(Path.GetTempPath(), $"locus-concurrent-bench-{testId}");
            _volumePath = Path.Combine(_testDirectory, "volume");
            _fileSystem.Directory.CreateDirectory(_testDirectory);
            _fileSystem.Directory.CreateDirectory(_volumePath);

            // Setup repositories
            var metadataLogger = NullLogger<MetadataRepository>.Instance;
            _metadataRepository = new MetadataRepository(_fileSystem, metadataLogger, Path.Combine(_testDirectory, "metadata"));

            var quotaLogger = NullLogger<DirectoryQuotaRepository>.Instance;
            _quotaRepository = new DirectoryQuotaRepository(_fileSystem, quotaLogger, Path.Combine(_testDirectory, "quota"));

            // Setup mocks
            var tenantQuotaManager = new Mock<ITenantQuotaManager>();
            tenantQuotaManager.Setup(m => m.CanAddFileAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(true);
            tenantQuotaManager.Setup(m => m.IncrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            tenantQuotaManager.Setup(m => m.DecrementFileCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

            var tenantManager = new Mock<ITenantManager>();
            var tenantContext = new Mock<ITenantContext>();
            tenantContext.Setup(t => t.TenantId).Returns("benchmark-tenant");
            tenantContext.Setup(t => t.Status).Returns(TenantStatus.Enabled);
            tenantManager.Setup(m => m.GetTenantAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(tenantContext.Object);
            tenantManager.Setup(m => m.IsTenantEnabledAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(true);

            var fileScheduler = new Mock<IFileScheduler>();

            var poolLogger = NullLogger<StoragePool>.Instance;
            _storagePool = new StoragePool(_metadataRepository, tenantQuotaManager.Object, tenantManager.Object, fileScheduler.Object, poolLogger);

            // Setup volume
            var volumeLogger = NullLogger<LocalFileSystemVolume>.Instance;
            _volume = new LocalFileSystemVolume(_fileSystem, volumeLogger, "vol-001", _volumePath);
            _storagePool.AddVolume(_volume);

            _tenant = tenantContext.Object;
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

        [Benchmark(Description = "10 concurrent writes")]
        [Arguments(10)]
        public async Task ConcurrentWrites(int threadCount)
        {
            var tasks = new List<Task>();
            for (int i = 0; i < threadCount; i++)
            {
                int index = i;
                tasks.Add(Task.Run(async () =>
                {
                    var content = new MemoryStream(Encoding.UTF8.GetBytes($"Content {index}"));
                    await _storagePool.WriteFileAsync(_tenant, content, null, CancellationToken.None);
                }));
            }
            await Task.WhenAll(tasks);
        }

        [Benchmark(Description = "50 concurrent writes")]
        [Arguments(50)]
        public async Task ConcurrentWrites_50(int threadCount)
        {
            await ConcurrentWrites(threadCount);
        }

        [Benchmark(Description = "100 concurrent writes")]
        [Arguments(100)]
        public async Task ConcurrentWrites_100(int threadCount)
        {
            await ConcurrentWrites(threadCount);
        }

        [Benchmark(Description = "10 concurrent reads")]
        public async Task ConcurrentReads()
        {
            // Pre-populate files
            var fileKeys = new List<string>();
            for (int i = 0; i < 10; i++)
            {
                var content = new MemoryStream(Encoding.UTF8.GetBytes($"Read content {i}"));
                var key = await _storagePool.WriteFileAsync(_tenant, content, null, CancellationToken.None);
                fileKeys.Add(key);
            }

            // Benchmark concurrent reads
            var tasks = fileKeys.Select(key => Task.Run(async () =>
            {
                using var stream = await _storagePool.ReadFileAsync(_tenant, key, CancellationToken.None);
                using var reader = new StreamReader(stream);
                await reader.ReadToEndAsync();
            }));

            await Task.WhenAll(tasks);
        }

        [Benchmark(Description = "Mixed read/write operations (20 ops)")]
        public async Task MixedOperations()
        {
            // Pre-populate some files
            var fileKeys = new List<string>();
            for (int i = 0; i < 5; i++)
            {
                var content = new MemoryStream(Encoding.UTF8.GetBytes($"Existing {i}"));
                var key = await _storagePool.WriteFileAsync(_tenant, content, null, CancellationToken.None);
                fileKeys.Add(key);
            }

            // Benchmark: 10 writes + 10 reads
            var tasks = new List<Task>();

            // 10 concurrent writes
            for (int i = 0; i < 10; i++)
            {
                int index = i;
                tasks.Add(Task.Run(async () =>
                {
                    var content = new MemoryStream(Encoding.UTF8.GetBytes($"New {index}"));
                    await _storagePool.WriteFileAsync(_tenant, content, null, CancellationToken.None);
                }));
            }

            // 10 concurrent reads
            for (int i = 0; i < 10; i++)
            {
                int index = i % fileKeys.Count;
                tasks.Add(Task.Run(async () =>
                {
                    using var stream = await _storagePool.ReadFileAsync(_tenant, fileKeys[index], CancellationToken.None);
                    using var reader = new StreamReader(stream);
                    await reader.ReadToEndAsync();
                }));
            }

            await Task.WhenAll(tasks);
        }

        public void Dispose()
        {
            Cleanup();
        }
    }
}
