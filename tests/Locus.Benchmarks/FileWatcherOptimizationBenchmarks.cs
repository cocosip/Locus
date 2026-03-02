#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Locus.Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 1, iterationCount: 5)]
    public class FileWatcherLargeDirectoryBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private Mock<IStoragePool> _storagePool = null!;
        private Mock<ITenantManager> _tenantManager = null!;
        private FileWatcher _fileWatcher = null!;
        private FileWatcherConfiguration _configuration = null!;
        private string _rootDirectory = string.Empty;
        private string _watchDirectory = string.Empty;
        private string _configDirectory = string.Empty;
        private int _fileKeyCounter;

        [Params(2_000, 10_000)]
        public int FileCount;

        [Params(1, 8)]
        public int MaxConcurrentImports;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-fw-large-{Guid.NewGuid():N}");
            _watchDirectory = Path.Combine(_rootDirectory, "watch");
            _configDirectory = Path.Combine(_rootDirectory, "config");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            ResetDirectories();
            _storagePool = new Mock<IStoragePool>();
            _tenantManager = new Mock<ITenantManager>();
            _fileKeyCounter = 0;

            var tenant = new BenchTenantContext("tenant-large", TenantStatus.Enabled);
            _tenantManager.Setup(m => m.GetTenantAsync(tenant.TenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(tenant);
            _storagePool.Setup(m => m.WriteFileAsync(
                    It.IsAny<ITenantContext>(),
                    It.IsAny<Stream>(),
                    It.IsAny<string?>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => $"fw-large-{Interlocked.Increment(ref _fileKeyCounter):D8}");

            SeedFiles(_watchDirectory, FileCount);

            _fileWatcher = new FileWatcher(
                _fileSystem,
                _storagePool.Object,
                _tenantManager.Object,
                NullLogger<FileWatcher>.Instance,
                _configDirectory);

            _configuration = new FileWatcherConfiguration
            {
                WatcherId = "fw-large",
                TenantId = tenant.TenantId,
                WatchPath = _watchDirectory,
                Enabled = true,
                MultiTenantMode = false,
                IncludeSubdirectories = true,
                FilePatterns = new List<string> { "*.dcm" },
                MinFileAge = TimeSpan.Zero,
                MaxConcurrentImports = MaxConcurrentImports,
                PostImportAction = PostImportAction.Keep,
                FileStabilityCheckDelay = TimeSpan.Zero
            };
        }

        [Benchmark(Description = "point1 large-directory streamed scan")]
        public Task<FileWatcherScanResult> ScanLargeDirectory_Streamed()
        {
            return _fileWatcher.ScanNowAsync(_configuration, CancellationToken.None);
        }

        public void Dispose()
        {
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

        private void ResetDirectories()
        {
            if (_fileSystem.Directory.Exists(_watchDirectory))
                _fileSystem.Directory.Delete(_watchDirectory, recursive: true);
            if (_fileSystem.Directory.Exists(_configDirectory))
                _fileSystem.Directory.Delete(_configDirectory, recursive: true);
            _fileSystem.Directory.CreateDirectory(_watchDirectory);
            _fileSystem.Directory.CreateDirectory(_configDirectory);
        }

        private void SeedFiles(string rootPath, int count)
        {
            for (var i = 0; i < count; i++)
            {
                var shard = Path.Combine(rootPath, $"shard-{i % 32:D2}");
                _fileSystem.Directory.CreateDirectory(shard);
                var filePath = Path.Combine(shard, $"file-{i:D6}.dcm");
                _fileSystem.File.WriteAllText(filePath, "bench");
                _fileSystem.File.SetLastWriteTimeUtc(filePath, DateTime.UtcNow.AddMinutes(-10));
            }
        }
    }

    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 1, iterationCount: 5)]
    public class FileWatcherConfigurationDispatchBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private Mock<IStoragePool> _storagePool = null!;
        private Mock<ITenantManager> _tenantManager = null!;
        private FileWatcher _fileWatcher = null!;
        private FileWatcherConfiguration _configuration = null!;
        private string _rootDirectory = string.Empty;
        private string _watchDirectory = string.Empty;
        private string _configDirectory = string.Empty;
        private FieldInfo _cachedWatchersField = null!;
        private FieldInfo _watchersCacheExpiresField = null!;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-fw-config-{Guid.NewGuid():N}");
            _watchDirectory = Path.Combine(_rootDirectory, "watch");
            _configDirectory = Path.Combine(_rootDirectory, "config");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);

            _cachedWatchersField = typeof(FileWatcher).GetField("_cachedWatchers", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("_cachedWatchers field not found.");
            _watchersCacheExpiresField = typeof(FileWatcher).GetField("_watchersCacheExpiresAtTicks", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("_watchersCacheExpiresAtTicks field not found.");
        }

        [IterationSetup]
        public void IterationSetup()
        {
            if (_fileSystem.Directory.Exists(_watchDirectory))
                _fileSystem.Directory.Delete(_watchDirectory, recursive: true);
            if (_fileSystem.Directory.Exists(_configDirectory))
                _fileSystem.Directory.Delete(_configDirectory, recursive: true);
            _fileSystem.Directory.CreateDirectory(_watchDirectory);
            _fileSystem.Directory.CreateDirectory(_configDirectory);

            _storagePool = new Mock<IStoragePool>();
            _tenantManager = new Mock<ITenantManager>();

            var tenant = new BenchTenantContext("tenant-config", TenantStatus.Enabled);
            _tenantManager.Setup(m => m.GetTenantAsync(tenant.TenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(tenant);
            _storagePool.Setup(m => m.WriteFileAsync(
                    It.IsAny<ITenantContext>(),
                    It.IsAny<Stream>(),
                    It.IsAny<string?>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync("config-key");

            _fileWatcher = new FileWatcher(
                _fileSystem,
                _storagePool.Object,
                _tenantManager.Object,
                NullLogger<FileWatcher>.Instance,
                _configDirectory);

            _configuration = new FileWatcherConfiguration
            {
                WatcherId = "fw-config-dispatch",
                TenantId = tenant.TenantId,
                WatchPath = _watchDirectory,
                Enabled = true,
                MultiTenantMode = false,
                IncludeSubdirectories = true,
                FilePatterns = new List<string> { "*.dcm" },
                MinFileAge = TimeSpan.Zero,
                MaxConcurrentImports = 1,
                PostImportAction = PostImportAction.Keep,
                FileStabilityCheckDelay = TimeSpan.Zero
            };

            _fileWatcher.RegisterWatcherAsync(_configuration, CancellationToken.None).GetAwaiter().GetResult();
        }

        [Benchmark(Description = "point2 scan dispatch by watcher id (forced config load)")]
        public Task<FileWatcherScanResult> ScanNow_ByWatcherId_ForcedReload()
        {
            InvalidateWatcherCache();
            return _fileWatcher.ScanNowAsync(_configuration.WatcherId, CancellationToken.None);
        }

        [Benchmark(Description = "point2 scan dispatch by configuration snapshot")]
        public Task<FileWatcherScanResult> ScanNow_ByConfigurationSnapshot()
        {
            return _fileWatcher.ScanNowAsync(_configuration, CancellationToken.None);
        }

        public void Dispose()
        {
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

        private void InvalidateWatcherCache()
        {
            _cachedWatchersField.SetValue(_fileWatcher, null);
            _watchersCacheExpiresField.SetValue(_fileWatcher, 0L);
        }
    }

    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 1, iterationCount: 5)]
    public class FileWatcherMultiTenantStatusBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private Mock<IStoragePool> _storagePool = null!;
        private Mock<ITenantManager> _tenantManager = null!;
        private FileWatcher _fileWatcher = null!;
        private FileWatcherConfiguration _configuration = null!;
        private string _rootDirectory = string.Empty;
        private string _watchDirectory = string.Empty;
        private string _configDirectory = string.Empty;
        private Dictionary<string, ITenantContext> _tenantById = null!;
        private int _fileKeyCounter;

        [Params(40, 160)]
        public int TenantDirectoryCount;

        [Params(0, 50)]
        public int DisabledRatioPercent;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-fw-multitenant-{Guid.NewGuid():N}");
            _watchDirectory = Path.Combine(_rootDirectory, "watch");
            _configDirectory = Path.Combine(_rootDirectory, "config");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);
        }

        [IterationSetup]
        public void IterationSetup()
        {
            if (_fileSystem.Directory.Exists(_watchDirectory))
                _fileSystem.Directory.Delete(_watchDirectory, recursive: true);
            if (_fileSystem.Directory.Exists(_configDirectory))
                _fileSystem.Directory.Delete(_configDirectory, recursive: true);
            _fileSystem.Directory.CreateDirectory(_watchDirectory);
            _fileSystem.Directory.CreateDirectory(_configDirectory);

            _storagePool = new Mock<IStoragePool>();
            _tenantManager = new Mock<ITenantManager>();
            _tenantById = new Dictionary<string, ITenantContext>(StringComparer.Ordinal);
            _fileKeyCounter = 0;

            var disabledCount = (int)Math.Round(TenantDirectoryCount * (DisabledRatioPercent / 100.0));
            for (var i = 0; i < TenantDirectoryCount; i++)
            {
                var tenantId = $"tenant-{i:D4}";
                var tenantStatus = i < disabledCount ? TenantStatus.Disabled : TenantStatus.Enabled;
                var tenant = new BenchTenantContext(tenantId, tenantStatus);
                _tenantById[tenantId] = tenant;

                var tenantDir = Path.Combine(_watchDirectory, tenantId);
                _fileSystem.Directory.CreateDirectory(tenantDir);
                for (var f = 0; f < 4; f++)
                {
                    var filePath = Path.Combine(tenantDir, $"file-{f:D2}.dcm");
                    _fileSystem.File.WriteAllText(filePath, "bench");
                    _fileSystem.File.SetLastWriteTimeUtc(filePath, DateTime.UtcNow.AddMinutes(-10));
                }
            }

            _tenantManager.Setup(m => m.GetTenantAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>((tenantId, _) =>
                {
                    if (!_tenantById.TryGetValue(tenantId, out var tenant))
                        throw new InvalidOperationException($"Tenant not found: {tenantId}");
                    return Task.FromResult(tenant);
                });
            _tenantManager.Setup(m => m.IsTenantEnabledAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns<string, CancellationToken>((tenantId, _) =>
                {
                    var enabled = _tenantById.TryGetValue(tenantId, out var tenant)
                                  && tenant.Status == TenantStatus.Enabled;
                    return Task.FromResult(enabled);
                });
            _storagePool.Setup(m => m.WriteFileAsync(
                    It.IsAny<ITenantContext>(),
                    It.IsAny<Stream>(),
                    It.IsAny<string?>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => $"fw-multi-{Interlocked.Increment(ref _fileKeyCounter):D8}");

            _fileWatcher = new FileWatcher(
                _fileSystem,
                _storagePool.Object,
                _tenantManager.Object,
                NullLogger<FileWatcher>.Instance,
                _configDirectory);

            _configuration = new FileWatcherConfiguration
            {
                WatcherId = "fw-multi",
                WatchPath = _watchDirectory,
                Enabled = true,
                MultiTenantMode = true,
                IncludeSubdirectories = true,
                FilePatterns = new List<string> { "*.dcm" },
                MinFileAge = TimeSpan.Zero,
                MaxConcurrentImports = 8,
                PostImportAction = PostImportAction.Keep,
                FileStabilityCheckDelay = TimeSpan.Zero
            };
        }

        [Benchmark(Description = "point3 multi-tenant scan with status reuse")]
        public Task<FileWatcherScanResult> ScanMultiTenant_StatusReuse()
        {
            return _fileWatcher.ScanNowAsync(_configuration, CancellationToken.None);
        }

        public void Dispose()
        {
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
    [SimpleJob(warmupCount: 1, iterationCount: 5)]
    public class FileWatcherPrunePressureBenchmarks : IDisposable
    {
        private const int MaxImportedFilesCacheSize = 50_000;

        private IFileSystem _fileSystem = null!;
        private Mock<IStoragePool> _storagePool = null!;
        private Mock<ITenantManager> _tenantManager = null!;
        private FileWatcher _fileWatcher = null!;
        private FileWatcherConfiguration _configuration = null!;
        private string _rootDirectory = string.Empty;
        private string _watchDirectory = string.Empty;
        private string _configDirectory = string.Empty;
        private int _fileKeyCounter;
        private FieldInfo _importedFilesField = null!;

        [Params(60_000, 120_000)]
        public int ExistingImportedEntries;

        [Params(256, 1024)]
        public int NewFiles;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-fw-prune-{Guid.NewGuid():N}");
            _watchDirectory = Path.Combine(_rootDirectory, "watch");
            _configDirectory = Path.Combine(_rootDirectory, "config");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);

            _importedFilesField = typeof(FileWatcher).GetField("_importedFiles", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("_importedFiles field not found.");
        }

        [IterationSetup]
        public void IterationSetup()
        {
            if (_fileSystem.Directory.Exists(_watchDirectory))
                _fileSystem.Directory.Delete(_watchDirectory, recursive: true);
            if (_fileSystem.Directory.Exists(_configDirectory))
                _fileSystem.Directory.Delete(_configDirectory, recursive: true);
            _fileSystem.Directory.CreateDirectory(_watchDirectory);
            _fileSystem.Directory.CreateDirectory(_configDirectory);

            _storagePool = new Mock<IStoragePool>();
            _tenantManager = new Mock<ITenantManager>();
            _fileKeyCounter = 0;

            var tenant = new BenchTenantContext("tenant-prune", TenantStatus.Enabled);
            _tenantManager.Setup(m => m.GetTenantAsync(tenant.TenantId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(tenant);
            _storagePool.Setup(m => m.WriteFileAsync(
                    It.IsAny<ITenantContext>(),
                    It.IsAny<Stream>(),
                    It.IsAny<string?>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => $"fw-prune-{Interlocked.Increment(ref _fileKeyCounter):D8}");

            for (var i = 0; i < NewFiles; i++)
            {
                var filePath = Path.Combine(_watchDirectory, $"new-{i:D6}.dcm");
                _fileSystem.File.WriteAllText(filePath, "bench");
                _fileSystem.File.SetLastWriteTimeUtc(filePath, DateTime.UtcNow.AddMinutes(-10));
            }

            _fileWatcher = new FileWatcher(
                _fileSystem,
                _storagePool.Object,
                _tenantManager.Object,
                NullLogger<FileWatcher>.Instance,
                _configDirectory);

            SeedImportedHistoryPressure(_fileWatcher, ExistingImportedEntries);

            _configuration = new FileWatcherConfiguration
            {
                WatcherId = "fw-prune",
                TenantId = tenant.TenantId,
                WatchPath = _watchDirectory,
                Enabled = true,
                MultiTenantMode = false,
                IncludeSubdirectories = true,
                FilePatterns = new List<string> { "*.dcm" },
                MinFileAge = TimeSpan.Zero,
                MaxConcurrentImports = 8,
                PostImportAction = PostImportAction.Keep,
                FileStabilityCheckDelay = TimeSpan.Zero,
                EnableImportedFilesPruneThrottle = false,
                ImportedFilesPruneInterval = TimeSpan.Zero
            };
        }

        [Benchmark(Description = "point4 scan under imported-history prune pressure")]
        public Task<FileWatcherScanResult> ScanWithPrunePressure()
        {
            return _fileWatcher.ScanNowAsync(_configuration, CancellationToken.None);
        }

        public void Dispose()
        {
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

        private void SeedImportedHistoryPressure(FileWatcher fileWatcher, int targetCount)
        {
            var importedFiles = (ConcurrentDictionary<string, string>)_importedFilesField.GetValue(fileWatcher)!;
            var seedCount = Math.Max(MaxImportedFilesCacheSize, targetCount);
            for (var i = 0; i < seedCount; i++)
            {
                var stalePath = Path.Combine(_rootDirectory, "stale", $"stale-{i:D7}.dcm");
                importedFiles.TryAdd(stalePath, $"stale-key-{i:D7}");
            }
        }
    }

    internal sealed class BenchTenantContext : ITenantContext
    {
        public BenchTenantContext(string tenantId, TenantStatus status)
        {
            TenantId = tenantId;
            Status = status;
        }

        public string TenantId { get; }
        public TenantStatus Status { get; }
    }
}
