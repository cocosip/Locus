using System;
using System.IO;
using System.IO.Abstractions;
using BenchmarkDotNet.Attributes;
using Locus.FileSystem;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    /// <summary>
    /// Benchmarks isolating the cached fast-path cost of LocalFileSystemVolume health/space properties.
    ///
    /// Current implementation uses a 30-second TTL cache:
    /// - IsHealthy serves cached result in the hot path, refreshing probe asynchronously on expiry.
    /// - AvailableSpace/TotalCapacity serve cached drive metrics, refreshing on expiry.
    ///
    /// Run in Release mode:
    ///   dotnet run -c Release -- --filter "VolumeHealthCheck*"
    /// </summary>
    [MemoryDiagnoser]
    [SimpleJob(warmupCount: 3, iterationCount: 5)]
    public class VolumeHealthCheckBenchmarks : IDisposable
    {
        private IFileSystem _fileSystem;
        private LocalFileSystemVolume _volume;
        private string _testDirectory;

        [GlobalSetup]
        public void Setup()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _testDirectory = Path.Combine(Path.GetTempPath(), $"locus-health-bench-{testId}");
            _fileSystem.Directory.CreateDirectory(_testDirectory);

            _volume = new LocalFileSystemVolume(
                _fileSystem,
                NullLogger<LocalFileSystemVolume>.Instance,
                "vol-bench",
                _testDirectory);
        }

        /// <summary>
        /// Fast path: returns cached health status while TTL is valid.
        /// </summary>
        [Benchmark(Baseline = true, Description = "IsHealthy — TTL cached fast path")]
        public bool IsHealthy_PerCall() => _volume.IsHealthy;

        /// <summary>
        /// Fast path: returns cached available space while TTL is valid.
        /// </summary>
        [Benchmark(Description = "AvailableSpace — TTL cached fast path")]
        public long AvailableSpace_PerCall() => _volume.AvailableSpace;

        /// <summary>
        /// Fast path: returns cached total capacity while TTL is valid.
        /// </summary>
        [Benchmark(Description = "TotalCapacity — TTL cached fast path")]
        public long TotalCapacity_PerCall() => _volume.TotalCapacity;

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

        public void Dispose() => Cleanup();
    }
}
