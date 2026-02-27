using System;
using System.IO;
using System.IO.Abstractions;
using BenchmarkDotNet.Attributes;
using Locus.FileSystem;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    /// <summary>
    /// Benchmarks isolating the per-call cost of LocalFileSystemVolume health/space properties.
    ///
    /// Key finding: IsHealthy creates + writes + deletes a temp file on EVERY call.
    ///              AvailableSpace calls DriveInfo.GetDrives() on EVERY call.
    ///              Both are called on every WriteFileAsync via SelectVolumeForWrite().
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
        /// Current behavior: creates a unique temp file, writes "health check" text,
        /// then deletes it — full disk I/O on every call.
        /// This is invoked on every WriteFileAsync via SelectVolumeForWrite().
        /// </summary>
        [Benchmark(Baseline = true, Description = "IsHealthy — file write+delete per call (current behavior)")]
        public bool IsHealthy_PerCall() => _volume.IsHealthy;

        /// <summary>
        /// Current behavior: calls DriveInfo.GetDrives() on every call — system call.
        /// Called twice per WriteFileAsync (once in IsHealthy, once in OrderByDescending).
        /// </summary>
        [Benchmark(Description = "AvailableSpace — DriveInfo.GetDrives() per call (current behavior)")]
        public long AvailableSpace_PerCall() => _volume.AvailableSpace;

        /// <summary>
        /// For comparison: TotalCapacity also calls DriveInfo.GetDrives().
        /// </summary>
        [Benchmark(Description = "TotalCapacity — DriveInfo.GetDrives() per call")]
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
