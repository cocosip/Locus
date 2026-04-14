#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.FileSystem;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    internal static class DirectVolumeBreakdownCommand
    {
        private const string ScenarioName = "direct-volume-breakdown";
        private const string TenantId = "benchmark-tenant";

        public static bool ShouldRun(string[] args)
        {
            if (args.Length == 0)
                return false;

            if (string.Equals(args[0], ScenarioName, StringComparison.OrdinalIgnoreCase))
                return true;

            for (var i = 0; i < args.Length; i++)
            {
                if (!string.Equals(args[i], "--scenario", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (i + 1 >= args.Length)
                    return false;

                return string.Equals(args[i + 1], ScenarioName, StringComparison.OrdinalIgnoreCase);
            }

            return false;
        }

        public static async Task<int> RunAsync(string[] args)
        {
            if (!TryParseOptions(args, out var options, out var parseError))
            {
                if (!string.IsNullOrWhiteSpace(parseError))
                    Console.Error.WriteLine(parseError);

                PrintUsage();
                return 1;
            }

            Console.WriteLine(
                $"Running {ScenarioName}: writes={options.WriteCount}, warmup={options.WarmupCount}, sizes=[{string.Join(", ", options.FileSizes)}], includeNoFlush={options.IncludeNoFlushScenario}, streamModes=[{string.Join(", ", options.StreamModes.Select(mode => mode.Name))}]");
            Console.WriteLine();

            foreach (var fileSize in options.FileSizes)
            {
                Console.WriteLine($"=== File Size: {FormatBytes(fileSize)} ({fileSize} bytes) ===");
                foreach (var flushMode in BuildFlushModes(options))
                {
                    Console.WriteLine($"FlushMode={(flushMode ? "durable-flush" : "no-volume-flush")}");
                    foreach (var streamMode in options.StreamModes)
                    {
                        Console.WriteLine($"  StreamMode={streamMode.Name}");
                        foreach (var scenario in BuildDistributionScenarios())
                        {
                            using var context = CreateContext(fileSize, flushMode, scenario, streamMode);
                            await WarmupAsync(context, options.WarmupCount).ConfigureAwait(false);

                            context.Reset();

                            var stopwatch = Stopwatch.StartNew();
                            for (var i = 0; i < options.WriteCount; i++)
                                await context.WriteOnceAsync().ConfigureAwait(false);
                            stopwatch.Stop();

                            PrintResult(fileSize, options.WriteCount, flushMode, scenario, streamMode, stopwatch.Elapsed);
                        }
                    }

                    Console.WriteLine();
                }
            }

            return 0;
        }

        private static bool[] BuildFlushModes(DirectVolumeBreakdownOptions options)
        {
            if (options.IncludeNoFlushScenario)
                return new[] { true, false };

            return new[] { true };
        }

        private static DistributionScenario[] BuildDistributionScenarios()
        {
            return new[]
            {
                new DistributionScenario(
                    "depth0-no-shard",
                    shardingDepth: 0,
                    keyFactory: index => FormatSequentialFileKey(index),
                    precreateDirectories: false),
                new DistributionScenario(
                    "depth2-single-shard",
                    shardingDepth: 2,
                    keyFactory: index => "a1b2" + FormatSequentialFileKey(index).Substring(4),
                    precreateDirectories: false),
                new DistributionScenario(
                    "depth2-random-precreated",
                    shardingDepth: 2,
                    keyFactory: _ => Guid.NewGuid().ToString("N"),
                    precreateDirectories: true),
                new DistributionScenario(
                    "depth2-random-cold",
                    shardingDepth: 2,
                    keyFactory: _ => Guid.NewGuid().ToString("N"),
                    precreateDirectories: false),
            };
        }

        private static async Task WarmupAsync(ScenarioContext context, int warmupCount)
        {
            for (var i = 0; i < warmupCount; i++)
                await context.WriteOnceAsync().ConfigureAwait(false);

            context.Reset();
        }

        private static ScenarioContext CreateContext(
            int fileSize,
            bool forceFlushAfterWrite,
            DistributionScenario scenario,
            StreamMode streamMode)
        {
            var fileSystem = new System.IO.Abstractions.FileSystem();
            var rootDirectory = Path.Combine(
                Path.GetTempPath(),
                $"locus-direct-volume-{scenario.Name}-{streamMode.Name}-{fileSize}-{Guid.NewGuid():N}");
            fileSystem.Directory.CreateDirectory(rootDirectory);

            var mountPath = Path.Combine(rootDirectory, "volume");
            fileSystem.Directory.CreateDirectory(mountPath);

            var volume = new LocalFileSystemVolume(
                fileSystem,
                NullLogger<LocalFileSystemVolume>.Instance,
                "vol-001",
                mountPath,
                shardingDepth: scenario.ShardingDepth,
                forceFlushAfterWrite: forceFlushAfterWrite);

            var payload = CreatePayload(fileSize);
            var keys = new string[4096];
            for (var i = 0; i < keys.Length; i++)
                keys[i] = scenario.KeyFactory(i);

            if (scenario.PrecreateDirectories)
            {
                foreach (var directory in keys
                    .Select(key => Path.GetDirectoryName(volume.BuildPhysicalPath(TenantId, key)))
                    .Where(path => !string.IsNullOrWhiteSpace(path))
                    .Distinct(StringComparer.OrdinalIgnoreCase))
                {
                    fileSystem.Directory.CreateDirectory(directory!);
                }
            }

            return new ScenarioContext(fileSystem, rootDirectory, volume, payload, keys, scenario, streamMode);
        }

        private static byte[] CreatePayload(int fileSize)
        {
            var payload = new byte[fileSize];
            for (var i = 0; i < payload.Length; i++)
                payload[i] = (byte)(i & 0xFF);

            return payload;
        }

        private static void PrintResult(
            int fileSize,
            int writeCount,
            bool forceFlushAfterWrite,
            DistributionScenario scenario,
            StreamMode streamMode,
            TimeSpan elapsed)
        {
            var avgTicks = elapsed.Ticks / (double)writeCount;
            var throughput = elapsed.TotalSeconds > 0
                ? (fileSize * writeCount) / elapsed.TotalSeconds / 1024.0 / 1024.0
                : 0;

            Console.WriteLine(
                $"    Scenario={scenario.Name,-24} avg={FormatTime(avgTicks),10}  total={elapsed.TotalMilliseconds,10:F2} ms  throughput={throughput,8:F2} MiB/s  flush={forceFlushAfterWrite,-5}  stream={streamMode.Name,-16}  uniqueDirs={scenario.LastUniqueDirectoryCount,5}");
        }

        private static string FormatTime(double ticks)
        {
            var stopwatchTicks = ticks * Stopwatch.Frequency / TimeSpan.TicksPerSecond;
            var microseconds = stopwatchTicks * 1_000_000.0 / Stopwatch.Frequency;
            if (microseconds >= 1000)
                return $"{microseconds / 1000.0:F3} ms";

            return $"{microseconds:F1} us";
        }

        private static string FormatBytes(int bytes)
        {
            if (bytes >= 1024 * 1024)
                return $"{bytes / 1024.0 / 1024.0:F2} MiB";

            if (bytes >= 1024)
                return $"{bytes / 1024.0:F2} KiB";

            return $"{bytes} B";
        }

        private static string FormatSequentialFileKey(int value)
        {
            return value.ToString("x32", CultureInfo.InvariantCulture);
        }

        private static bool TryParseOptions(
            string[] args,
            out DirectVolumeBreakdownOptions options,
            out string? error)
        {
            options = new DirectVolumeBreakdownOptions
            {
                WriteCount = 1000,
                WarmupCount = 50,
                FileSizes = new[] { 4 * 1024, 16 * 1024 },
                IncludeNoFlushScenario = true,
                StreamModes = BuildDefaultStreamModes(),
            };
            error = null;

            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                if (string.Equals(arg, ScenarioName, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (string.Equals(arg, "--scenario", StringComparison.OrdinalIgnoreCase))
                {
                    i++;
                    continue;
                }

                if (string.Equals(arg, "--writes", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var writes))
                    {
                        error = "Missing or invalid value for --writes.";
                        return false;
                    }

                    options.WriteCount = writes;
                    continue;
                }

                if (string.Equals(arg, "--warmup", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var warmup))
                    {
                        error = "Missing or invalid value for --warmup.";
                        return false;
                    }

                    options.WarmupCount = warmup;
                    continue;
                }

                if (string.Equals(arg, "--sizes", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadValue(args, ref i, out var sizeValue))
                    {
                        error = "Missing value for --sizes.";
                        return false;
                    }

                    var parsedSizes = sizeValue
                        .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                        .Select(part => int.TryParse(part.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : -1)
                        .ToArray();
                    if (parsedSizes.Length == 0 || parsedSizes.Any(value => value <= 0))
                    {
                        error = "Invalid value for --sizes. Use a comma-separated byte list such as 4096,16384.";
                        return false;
                    }

                    options.FileSizes = parsedSizes;
                    continue;
                }

                if (string.Equals(arg, "--include-no-flush", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadBool(args, ref i, out var includeNoFlush))
                    {
                        error = "Missing or invalid value for --include-no-flush.";
                        return false;
                    }

                    options.IncludeNoFlushScenario = includeNoFlush;
                    continue;
                }

                if (string.Equals(arg, "--stream-modes", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadValue(args, ref i, out var streamModesValue))
                    {
                        error = "Missing value for --stream-modes.";
                        return false;
                    }

                    var parsedModes = streamModesValue
                        .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                        .Select(part => part.Trim())
                        .Where(part => !string.IsNullOrWhiteSpace(part))
                        .Select(TryParseStreamMode)
                        .ToArray();
                    if (parsedModes.Length == 0 || parsedModes.Any(mode => mode == null))
                    {
                        error = "Invalid value for --stream-modes. Use hidden-memory, visible-direct, visible-wrapped.";
                        return false;
                    }

                    options.StreamModes = parsedModes!
                        .Cast<StreamMode>()
                        .ToArray();
                    continue;
                }
            }

            if (options.WriteCount <= 0)
            {
                error = "--writes must be greater than zero.";
                return false;
            }

            if (options.WarmupCount < 0)
            {
                error = "--warmup must be zero or greater.";
                return false;
            }

            if (options.StreamModes == null || options.StreamModes.Length == 0)
            {
                error = "--stream-modes must contain at least one mode.";
                return false;
            }

            return true;
        }

        private static StreamMode[] BuildDefaultStreamModes()
        {
            return new[]
            {
                StreamMode.HiddenMemory,
                StreamMode.VisibleDirect,
                StreamMode.VisibleWrapped,
            };
        }

        private static StreamMode? TryParseStreamMode(string value)
        {
            if (string.Equals(value, StreamMode.HiddenMemory.Name, StringComparison.OrdinalIgnoreCase))
                return StreamMode.HiddenMemory;

            if (string.Equals(value, StreamMode.VisibleDirect.Name, StringComparison.OrdinalIgnoreCase))
                return StreamMode.VisibleDirect;

            if (string.Equals(value, StreamMode.VisibleWrapped.Name, StringComparison.OrdinalIgnoreCase))
                return StreamMode.VisibleWrapped;

            return null;
        }

        private static bool TryReadInt(string[] args, ref int index, out int value)
        {
            value = 0;
            if (!TryReadValue(args, ref index, out var rawValue))
                return false;

            return int.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
        }

        private static bool TryReadBool(string[] args, ref int index, out bool value)
        {
            value = false;
            if (!TryReadValue(args, ref index, out var rawValue))
                return false;

            return bool.TryParse(rawValue, out value);
        }

        private static bool TryReadValue(string[] args, ref int index, out string value)
        {
            value = string.Empty;
            if (index + 1 >= args.Length)
                return false;

            value = args[++index];
            return !string.IsNullOrWhiteSpace(value);
        }

        private static void PrintUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  dotnet run --project .\\tests\\Locus.Benchmarks\\Locus.Benchmarks.csproj -- direct-volume-breakdown [options]");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --writes <n>               Number of measured writes. Default: 1000");
            Console.WriteLine("  --warmup <n>               Number of warmup writes. Default: 50");
            Console.WriteLine("  --sizes <csv>              Comma-separated file sizes in bytes. Default: 4096,16384");
            Console.WriteLine("  --include-no-flush <bool>  Also run forceFlushAfterWrite=false. Default: true");
            Console.WriteLine("  --stream-modes <csv>       hidden-memory,visible-direct,visible-wrapped. Default: all");
        }

        private sealed class ScenarioContext : IDisposable
        {
            private readonly IFileSystem _fileSystem;
            private readonly byte[] _payload;
            private readonly string[] _keys;
            private readonly DistributionScenario _scenario;
            private readonly StreamMode _streamMode;
            private int _writeIndex;

            public ScenarioContext(
                IFileSystem fileSystem,
                string rootDirectory,
                LocalFileSystemVolume volume,
                byte[] payload,
                string[] keys,
                DistributionScenario scenario,
                StreamMode streamMode)
            {
                _fileSystem = fileSystem;
                RootDirectory = rootDirectory;
                Volume = volume;
                _payload = payload;
                _keys = keys;
                _scenario = scenario;
                _streamMode = streamMode;
            }

            public string RootDirectory { get; }

            public LocalFileSystemVolume Volume { get; }

            public async Task WriteOnceAsync()
            {
                var keyIndex = _writeIndex++ % _keys.Length;
                var fileKey = _keys[keyIndex];
                var physicalPath = Volume.BuildPhysicalPath(TenantId, fileKey);
                var directory = Path.GetDirectoryName(physicalPath);
                if (!string.IsNullOrWhiteSpace(directory))
                    _scenario.RecordDirectory(directory!);

                using var stream = _streamMode.CreateStream(_payload);
                await Volume.WriteAsync(physicalPath, stream, CancellationToken.None).ConfigureAwait(false);
            }

            public void Reset()
            {
                _writeIndex = 0;
                _scenario.Reset();
            }

            public void Dispose()
            {
                try
                {
                    if (_fileSystem.Directory.Exists(RootDirectory))
                        _fileSystem.Directory.Delete(RootDirectory, recursive: true);
                }
                catch
                {
                }
            }
        }

        private sealed class DistributionScenario
        {
            private readonly HashSet<string> _directories = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            public DistributionScenario(
                string name,
                int shardingDepth,
                Func<int, string> keyFactory,
                bool precreateDirectories)
            {
                Name = name;
                ShardingDepth = shardingDepth;
                KeyFactory = keyFactory;
                PrecreateDirectories = precreateDirectories;
            }

            public string Name { get; }

            public int ShardingDepth { get; }

            public Func<int, string> KeyFactory { get; }

            public bool PrecreateDirectories { get; }

            public int LastUniqueDirectoryCount => _directories.Count;

            public void RecordDirectory(string directory)
            {
                _directories.Add(directory);
            }

            public void Reset()
            {
                _directories.Clear();
            }
        }

        private sealed class StreamMode
        {
            public static readonly StreamMode HiddenMemory = new StreamMode(
                "hidden-memory",
                payload => new MemoryStream(payload, writable: false));

            public static readonly StreamMode VisibleDirect = new StreamMode(
                "visible-direct",
                payload => new MemoryStream(payload, 0, payload.Length, writable: false, publiclyVisible: true));

            public static readonly StreamMode VisibleWrapped = new StreamMode(
                "visible-wrapped",
                payload => new NonMemoryStreamWrapper(new MemoryStream(payload, 0, payload.Length, writable: false, publiclyVisible: true)));

            private readonly Func<byte[], Stream> _streamFactory;

            private StreamMode(string name, Func<byte[], Stream> streamFactory)
            {
                Name = name;
                _streamFactory = streamFactory;
            }

            public string Name { get; }

            public Stream CreateStream(byte[] payload) => _streamFactory(payload);
        }

        private sealed class NonMemoryStreamWrapper : Stream
        {
            private readonly Stream _inner;

            public NonMemoryStreamWrapper(Stream inner)
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            }

            public override bool CanRead => _inner.CanRead;

            public override bool CanSeek => _inner.CanSeek;

            public override bool CanWrite => _inner.CanWrite;

            public override long Length => _inner.Length;

            public override long Position
            {
                get => _inner.Position;
                set => _inner.Position = value;
            }

            public override void Flush() => _inner.Flush();

            public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

            public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

            public override void SetLength(long value) => _inner.SetLength(value);

            public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
                => _inner.ReadAsync(buffer, offset, count, cancellationToken);

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
                => _inner.WriteAsync(buffer, offset, count, cancellationToken);

            public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                    _inner.Dispose();

                base.Dispose(disposing);
            }
        }

        private sealed class DirectVolumeBreakdownOptions
        {
            public int WriteCount { get; set; }

            public int WarmupCount { get; set; }

            public int[] FileSizes { get; set; } = Array.Empty<int>();

            public bool IncludeNoFlushScenario { get; set; }

            public StreamMode[] StreamModes { get; set; } = Array.Empty<StreamMode>();
        }
    }
}
