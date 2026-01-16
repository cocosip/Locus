using System;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Locus.MultiTenant;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    /// <summary>
    /// Benchmarks for TenantManager operations
    /// Tests tenant lookup and auto-creation performance
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 3, iterationCount: 5)]
    public class TenantManagerBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem;
        private TenantManager _managerWithAutoCreate;
        private TenantManager _managerWithoutAutoCreate;
        private string _testDirectory;
        private int _tenantCounter;

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _testDirectory = Path.Combine(Path.GetTempPath(), $"locus-tenant-bench-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_testDirectory);

            var logger = NullLogger<TenantManager>.Instance;

            // Manager with auto-create enabled
            _managerWithAutoCreate = new TenantManager(
                _fileSystem,
                logger,
                Path.Combine(_testDirectory, "auto"),
                cacheExpiration: TimeSpan.FromMinutes(5),
                autoCreateTenants: true);

            // Manager with auto-create disabled
            _managerWithoutAutoCreate = new TenantManager(
                _fileSystem,
                logger,
                Path.Combine(_testDirectory, "manual"),
                cacheExpiration: TimeSpan.FromMinutes(5),
                autoCreateTenants: false);

            // Pre-create some tenants for cache hit tests
            for (int i = 0; i < 10; i++)
            {
                _managerWithoutAutoCreate.CreateTenantAsync($"existing-{i}", CancellationToken.None).Wait();
            }

            _tenantCounter = 0;
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            try
            {
                if (_fileSystem.Directory.Exists(_testDirectory))
                    _fileSystem.Directory.Delete(_testDirectory, recursive: true);
            }
            catch { }
        }

        [Benchmark(Description = "Create tenant")]
        public async Task CreateTenantAsync()
        {
            var tenantId = $"tenant-{Interlocked.Increment(ref _tenantCounter)}";
            await _managerWithoutAutoCreate.CreateTenantAsync(tenantId, CancellationToken.None);
        }

        [Benchmark(Description = "Get tenant (cache hit)")]
        public async Task GetTenantAsync_CacheHit()
        {
            // Should hit cache
            await _managerWithoutAutoCreate.GetTenantAsync("existing-0", CancellationToken.None);
        }

        [Benchmark(Description = "Get tenant (cache miss, no auto-create)")]
        public async Task GetTenantAsync_CacheMiss()
        {
            try
            {
                await _managerWithoutAutoCreate.GetTenantAsync("nonexistent", CancellationToken.None);
            }
            catch (Core.Exceptions.TenantNotFoundException)
            {
                // Expected
            }
        }

        [Benchmark(Description = "Get tenant with auto-create")]
        public async Task GetTenantAsync_AutoCreate()
        {
            var tenantId = $"auto-{Interlocked.Increment(ref _tenantCounter)}";
            await _managerWithAutoCreate.GetTenantAsync(tenantId, CancellationToken.None);
        }

        [Benchmark(Description = "Check tenant enabled (cache hit)")]
        public async Task IsTenantEnabledAsync_CacheHit()
        {
            await _managerWithoutAutoCreate.IsTenantEnabledAsync("existing-0", CancellationToken.None);
        }

        [Benchmark(Description = "Enable tenant")]
        public async Task EnableTenantAsync()
        {
            var tenantId = $"enable-{Interlocked.Increment(ref _tenantCounter)}";
            await _managerWithoutAutoCreate.CreateTenantAsync(tenantId, CancellationToken.None);
            await _managerWithoutAutoCreate.DisableTenantAsync(tenantId, CancellationToken.None);
            // Benchmark
            await _managerWithoutAutoCreate.EnableTenantAsync(tenantId, CancellationToken.None);
        }

        [Benchmark(Description = "Disable tenant")]
        public async Task DisableTenantAsync()
        {
            var tenantId = $"disable-{Interlocked.Increment(ref _tenantCounter)}";
            await _managerWithoutAutoCreate.CreateTenantAsync(tenantId, CancellationToken.None);
            // Benchmark
            await _managerWithoutAutoCreate.DisableTenantAsync(tenantId, CancellationToken.None);
        }

        public void Dispose()
        {
            Cleanup();
        }
    }
}
