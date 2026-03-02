#nullable enable
using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Abstractions;
using System.Reflection;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Locus.FileSystem;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 2, iterationCount: 8)]
    public class DirectoryQuotaDirtyFlushBenchmarks : IDisposable
    {
        private const string TenantId = "phase5-dirty-flush";

        private IFileSystem _fileSystem = null!;
        private DirectoryQuotaRepository _repository = null!;
        private MethodInfo _flushDirtyMethod = null!;
        private string _rootDirectory = string.Empty;
        private string _quotaDirectory = string.Empty;
        private string[] _directoryPaths = Array.Empty<string>();

        [Params(20_000, 100_000)]
        public int TotalDirectories;

        [Params(32, 128, 512)]
        public int DirtyDirectories;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-phasec-dirty-flush-{Guid.NewGuid():N}");
            _quotaDirectory = Path.Combine(_rootDirectory, "quota");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);

            _repository = new DirectoryQuotaRepository(
                _fileSystem,
                NullLogger<DirectoryQuotaRepository>.Instance,
                _quotaDirectory,
                enableBackgroundFlush: false);

            _flushDirtyMethod = typeof(DirectoryQuotaRepository).GetMethod(
                "DoFlushAllDirtyCounters",
                BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("DoFlushAllDirtyCounters not found.");

            _directoryPaths = new string[TotalDirectories];
            for (var i = 0; i < TotalDirectories; i++)
            {
                var directoryPath = $"/bench/dir-{i:D6}";
                _directoryPaths[i] = directoryPath;
                _repository.TryIncrementAsync(TenantId, directoryPath, CancellationToken.None).GetAwaiter().GetResult();
            }

            // Baseline state: all counters clean, no indexed dirty keys.
            FlushDirtyCountersNow();
        }

        [IterationSetup]
        public void IterationSetup()
        {
            var dirtyCount = Math.Min(DirtyDirectories, _directoryPaths.Length);
            for (var i = 0; i < dirtyCount; i++)
            {
                _repository.TryIncrementAsync(TenantId, _directoryPaths[i], CancellationToken.None).GetAwaiter().GetResult();
            }
        }

        [Benchmark(Description = "directory quota flush (indexed sparse dirty set)")]
        public void FlushDirtyCounters_ForSparseDirtySet()
        {
            FlushDirtyCountersNow();
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

        private void FlushDirtyCountersNow()
        {
            _flushDirtyMethod.Invoke(_repository, null);
        }
    }

    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 2, iterationCount: 8)]
    public class KnownDirectoryTrimBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem = null!;
        private LocalFileSystemVolume _volume = null!;
        private Action<LocalFileSystemVolume, string> _trackKnownDirectory = null!;
        private ConcurrentDictionary<string, byte> _knownDirectories = null!;
        private string _rootDirectory = string.Empty;
        private int _directorySequence;

        [Params(256, 512, 2048)]
        public int CacheMaxEntries;

        [Params(2048, 4096, 8192)]
        public int DirectoryAddsPerOperation;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-bench-phasec-known-trim-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(_rootDirectory);

            _volume = new LocalFileSystemVolume(
                _fileSystem,
                NullLogger<LocalFileSystemVolume>.Instance,
                "vol-phasec-trim",
                _rootDirectory,
                knownDirectoryCacheMaxEntries: CacheMaxEntries);

            var trackMethod = typeof(LocalFileSystemVolume).GetMethod(
                "TrackKnownDirectory",
                BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("TrackKnownDirectory not found.");

            _trackKnownDirectory = CreateTrackDelegate(trackMethod);
            _knownDirectories = (ConcurrentDictionary<string, byte>)(typeof(LocalFileSystemVolume)
                .GetField("_knownDirectories", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(_volume)
                ?? throw new InvalidOperationException("_knownDirectories not found."));
        }

        [IterationSetup]
        public void IterationSetup()
        {
            _knownDirectories.Clear();
            _knownDirectories.TryAdd(_volume.MountPath, 0);
            _directorySequence = 0;
        }

        [Benchmark(Description = "known-directory cache trim under churn")]
        public void TrackKnownDirectory_TrimPressure()
        {
            var start = Interlocked.Add(ref _directorySequence, DirectoryAddsPerOperation) - DirectoryAddsPerOperation;
            for (var i = 0; i < DirectoryAddsPerOperation; i++)
            {
                var directory = Path.Combine(_volume.MountPath, $"tenant-{start + i:D6}");
                _trackKnownDirectory(_volume, directory);
            }
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

        private static Action<LocalFileSystemVolume, string> CreateTrackDelegate(MethodInfo trackMethod)
        {
            try
            {
                return (Action<LocalFileSystemVolume, string>)trackMethod.CreateDelegate(typeof(Action<LocalFileSystemVolume, string>));
            }
            catch
            {
                return (volume, directory) => trackMethod.Invoke(volume, new object[] { directory });
            }
        }
    }
}
