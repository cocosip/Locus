#nullable enable
using System;
using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Locus.Benchmarks
{
    internal static class VolumeWritePhaseBreakdownCommand
    {
        private const string ScenarioName = "volume-write-phases";

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
                $"Running {ScenarioName}: writes={options.WriteCount}, warmup={options.WarmupCount}, sizes=[{string.Join(", ", options.FileSizes)}], includeNoFlush={options.IncludeNoFlushScenario}");
            Console.WriteLine();

            foreach (var fileSize in options.FileSizes)
            {
                Console.WriteLine($"=== File Size: {FormatBytes(fileSize)} ({fileSize} bytes) ===");
                foreach (var forceFlushAfterWrite in BuildFlushModes(options))
                {
                    Console.WriteLine($"FlushMode={(forceFlushAfterWrite ? "durable-flush" : "no-volume-flush")}");
                    foreach (var strategy in BuildStrategies())
                    {
                        using var context = new ScenarioContext(fileSize, forceFlushAfterWrite, strategy);
                        await context.WarmupAsync(options.WarmupCount).ConfigureAwait(false);
                        context.ResetMeasurements();

                        var totalStopwatch = Stopwatch.StartNew();
                        for (var i = 0; i < options.WriteCount; i++)
                            await context.WriteOnceAsync().ConfigureAwait(false);
                        totalStopwatch.Stop();

                        PrintResult(options.WriteCount, strategy, context.Measurements, totalStopwatch.ElapsedTicks);
                    }

                    Console.WriteLine();
                }
            }

            return 0;
        }

        private static bool[] BuildFlushModes(VolumeWritePhaseOptions options)
        {
            if (options.IncludeNoFlushScenario)
                return new[] { true, false };

            return new[] { true };
        }

        private static WriteStrategy[] BuildStrategies()
        {
            return new[]
            {
                new WriteStrategy(
                    "async-seq-80k-src-sync",
                    fileOptions: FileOptions.Asynchronous | FileOptions.SequentialScan,
                    useAsyncWrite: true,
                    useAsyncFlush: true,
                    copyBufferSize: 80 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
                new WriteStrategy(
                    "async-default-256k-src-sync",
                    fileOptions: FileOptions.Asynchronous,
                    useAsyncWrite: true,
                    useAsyncFlush: true,
                    copyBufferSize: 256 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
                new WriteStrategy(
                    "async-seq-256k-src-sync",
                    fileOptions: FileOptions.Asynchronous | FileOptions.SequentialScan,
                    useAsyncWrite: true,
                    useAsyncFlush: true,
                    copyBufferSize: 256 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
                new WriteStrategy(
                    "async-default-256k-src-async",
                    fileOptions: FileOptions.Asynchronous,
                    useAsyncWrite: true,
                    useAsyncFlush: true,
                    copyBufferSize: 256 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan | FileOptions.Asynchronous),
                new WriteStrategy(
                    "async-seq-256k-src-async",
                    fileOptions: FileOptions.Asynchronous | FileOptions.SequentialScan,
                    useAsyncWrite: true,
                    useAsyncFlush: true,
                    copyBufferSize: 256 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan | FileOptions.Asynchronous),
                new WriteStrategy(
                    "sync-default-256k-async-write",
                    fileOptions: FileOptions.None,
                    useAsyncWrite: true,
                    useAsyncFlush: true,
                    copyBufferSize: 256 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
                new WriteStrategy(
                    "sync-default-256k",
                    fileOptions: FileOptions.None,
                    useAsyncWrite: false,
                    useAsyncFlush: false,
                    copyBufferSize: 256 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
                new WriteStrategy(
                    "sync-seq-256k",
                    fileOptions: FileOptions.SequentialScan,
                    useAsyncWrite: false,
                    useAsyncFlush: false,
                    copyBufferSize: 256 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
                new WriteStrategy(
                    "sync-default-512k",
                    fileOptions: FileOptions.None,
                    useAsyncWrite: false,
                    useAsyncFlush: false,
                    copyBufferSize: 512 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
                new WriteStrategy(
                    "sync-seq-512k",
                    fileOptions: FileOptions.SequentialScan,
                    useAsyncWrite: false,
                    useAsyncFlush: false,
                    copyBufferSize: 512 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
                new WriteStrategy(
                    "sync-default-1m",
                    fileOptions: FileOptions.None,
                    useAsyncWrite: false,
                    useAsyncFlush: false,
                    copyBufferSize: 1024 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
                new WriteStrategy(
                    "sync-seq-1m",
                    fileOptions: FileOptions.SequentialScan,
                    useAsyncWrite: false,
                    useAsyncFlush: false,
                    copyBufferSize: 1024 * 1024,
                    sourceFileOptions: FileOptions.SequentialScan),
            };
        }

        private static bool TryParseOptions(
            string[] args,
            out VolumeWritePhaseOptions options,
            out string? error)
        {
            options = new VolumeWritePhaseOptions
            {
                WriteCount = 1000,
                WarmupCount = 50,
                FileSizes = new[] { 4 * 1024, 16 * 1024 },
                IncludeNoFlushScenario = true,
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

            return true;
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
            Console.WriteLine("  dotnet run --project .\\tests\\Locus.Benchmarks\\Locus.Benchmarks.csproj -- volume-write-phases [options]");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --writes <n>               Number of measured writes. Default: 1000");
            Console.WriteLine("  --warmup <n>               Number of warmup writes. Default: 50");
            Console.WriteLine("  --sizes <csv>              Comma-separated file sizes in bytes. Default: 4096,16384");
            Console.WriteLine("  --include-no-flush <bool>  Also run forceFlushAfterWrite=false. Default: true");
        }

        private static void PrintResult(
            int writeCount,
            WriteStrategy strategy,
            PhaseMeasurementSet measurements,
            long totalTicks)
        {
            Console.WriteLine($"  Strategy={strategy.Name}");
            PrintPhase("Total", totalTicks, writeCount, totalTicks);
            PrintPhase("Ensure dir", measurements.EnsureDirectoryTicks, writeCount, totalTicks);
            PrintPhase("Open stream", measurements.OpenStreamTicks, writeCount, totalTicks);
            PrintPhase("Copy/write", measurements.CopyWriteTicks, writeCount, totalTicks);
            PrintPhase("Flush", measurements.FlushTicks, writeCount, totalTicks);
            PrintPhase("Dispose", measurements.DisposeTicks, writeCount, totalTicks);

            var tracked = measurements.TotalTrackedTicks;
            var untracked = Math.Max(0L, totalTicks - tracked);
            PrintPhase("Untracked", untracked, writeCount, totalTicks);
        }

        private static void PrintPhase(string label, long ticks, int writeCount, long totalTicks)
        {
            var avgTicks = writeCount > 0 ? ticks / (double)writeCount : 0;
            var percent = totalTicks > 0 ? ticks * 100.0 / totalTicks : 0;
            Console.WriteLine(
                $"    {label,-12} avg={FormatStopwatchTicks(avgTicks),10}  share={percent,6:F2}%");
        }

        private static string FormatBytes(int bytes)
        {
            if (bytes >= 1024 * 1024)
                return $"{bytes / 1024.0 / 1024.0:F2} MiB";

            if (bytes >= 1024)
                return $"{bytes / 1024.0:F2} KiB";

            return $"{bytes} B";
        }

        private static string FormatStopwatchTicks(double ticks)
        {
            var microseconds = ticks * 1_000_000.0 / Stopwatch.Frequency;
            if (microseconds >= 1000)
                return $"{microseconds / 1000.0:F3} ms";

            return $"{microseconds:F1} us";
        }

        private sealed class ScenarioContext : IDisposable
        {
            private const int MaxKeys = 4096;

            private readonly string _rootDirectory;
            private readonly string _targetDirectory;
            private readonly string _sourceFilePath;
            private readonly byte[] _payload;
            private readonly string[] _fileKeys;
            private readonly bool _forceFlushAfterWrite;
            private readonly WriteStrategy _strategy;
            private int _writeIndex;

            public ScenarioContext(int fileSize, bool forceFlushAfterWrite, WriteStrategy strategy)
            {
                _forceFlushAfterWrite = forceFlushAfterWrite;
                _strategy = strategy;
                _rootDirectory = Path.Combine(
                    Path.GetTempPath(),
                    $"locus-volume-phases-{strategy.Name}-{fileSize}-{Guid.NewGuid():N}");
                _targetDirectory = Path.Combine(_rootDirectory, "volume", "benchmark-tenant", "a1", "b2");
                var sourceDirectory = Path.Combine(_rootDirectory, "source");
                Directory.CreateDirectory(_targetDirectory);
                Directory.CreateDirectory(sourceDirectory);

                _payload = new byte[fileSize];
                for (var i = 0; i < _payload.Length; i++)
                    _payload[i] = (byte)(i & 0xFF);

                _sourceFilePath = Path.Combine(sourceDirectory, "payload.bin");
                File.WriteAllBytes(_sourceFilePath, _payload);

                _fileKeys = new string[MaxKeys];
                for (var i = 0; i < _fileKeys.Length; i++)
                    _fileKeys[i] = $"{i.ToString("x32", CultureInfo.InvariantCulture)}.bin";
            }

            public PhaseMeasurementSet Measurements { get; } = new PhaseMeasurementSet();

            public async Task WarmupAsync(int warmupCount)
            {
                for (var i = 0; i < warmupCount; i++)
                    await WriteOnceAsync().ConfigureAwait(false);
            }

            public void ResetMeasurements()
            {
                Measurements.Reset();
                _writeIndex = 0;
            }

            public async Task WriteOnceAsync()
            {
                var fileName = _fileKeys[_writeIndex++ % _fileKeys.Length];
                var path = Path.Combine(_targetDirectory, fileName);

                var phaseStart = Stopwatch.GetTimestamp();
                Directory.CreateDirectory(_targetDirectory);
                Measurements.EnsureDirectoryTicks += Stopwatch.GetTimestamp() - phaseStart;

                using var source = OpenSourceStream();

                phaseStart = Stopwatch.GetTimestamp();
                var stream = CreateWriteStream(path, source.Length);
                Measurements.OpenStreamTicks += Stopwatch.GetTimestamp() - phaseStart;

                try
                {
                    phaseStart = Stopwatch.GetTimestamp();
                    await CopyToDestinationAsync(source, stream).ConfigureAwait(false);
                    Measurements.CopyWriteTicks += Stopwatch.GetTimestamp() - phaseStart;

                    if (_forceFlushAfterWrite)
                    {
                        phaseStart = Stopwatch.GetTimestamp();
                        if (_strategy.UseAsyncFlush)
                            await stream.FlushAsync().ConfigureAwait(false);
                        else
                            stream.Flush();
                        Measurements.FlushTicks += Stopwatch.GetTimestamp() - phaseStart;
                    }
                }
                finally
                {
                    phaseStart = Stopwatch.GetTimestamp();
                    stream.Dispose();
                    Measurements.DisposeTicks += Stopwatch.GetTimestamp() - phaseStart;
                }
            }

            private FileStream CreateWriteStream(string path, long expectedBytes)
            {
                var bufferSize = ResolveBufferSize(expectedBytes);
                return new FileStream(
                    path,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize,
                    _strategy.FileOptions);
            }

            private FileStream OpenSourceStream()
            {
                return new FileStream(
                    _sourceFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    ResolveBufferSize(_payload.Length),
                    _strategy.SourceFileOptions);
            }

            private async Task CopyToDestinationAsync(FileStream source, FileStream destination)
            {
                var exactLength = checked((int)(source.Length - source.Position));
                if (exactLength <= 0)
                    return;

                if (exactLength <= _strategy.CopyBufferSize)
                {
                    var buffer = ArrayPool<byte>.Shared.Rent(exactLength);
                    try
                    {
                        var totalRead = 0;
                        while (totalRead < exactLength)
                        {
                            var bytesRead = _strategy.UseAsyncWrite
                                ? await source.ReadAsync(buffer, totalRead, exactLength - totalRead).ConfigureAwait(false)
                                : source.Read(buffer, totalRead, exactLength - totalRead);
                            if (bytesRead <= 0)
                                break;

                            totalRead += bytesRead;
                        }

                        if (totalRead > 0)
                        {
                            if (_strategy.UseAsyncWrite)
                                await destination.WriteAsync(buffer, 0, totalRead).ConfigureAwait(false);
                            else
                                destination.Write(buffer, 0, totalRead);
                        }

                        return;
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }
                }

                var rented = ArrayPool<byte>.Shared.Rent(_strategy.CopyBufferSize);
                try
                {
                    while (true)
                    {
                        CancellationToken.None.ThrowIfCancellationRequested();
                        var bytesRead = _strategy.UseAsyncWrite
                            ? await source.ReadAsync(rented, 0, _strategy.CopyBufferSize).ConfigureAwait(false)
                            : source.Read(rented, 0, _strategy.CopyBufferSize);
                        if (bytesRead <= 0)
                            break;

                        if (_strategy.UseAsyncWrite)
                            await destination.WriteAsync(rented, 0, bytesRead).ConfigureAwait(false);
                        else
                            destination.Write(rented, 0, bytesRead);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }

            private static int ResolveBufferSize(long expectedBytes)
            {
                var candidate = expectedBytes > int.MaxValue ? 128 * 1024 : (int)expectedBytes;
                if (candidate < 4096)
                    candidate = 4096;

                return candidate < 128 * 1024 ? candidate : 128 * 1024;
            }

            public void Dispose()
            {
                try
                {
                    if (Directory.Exists(_rootDirectory))
                        Directory.Delete(_rootDirectory, recursive: true);
                }
                catch
                {
                }
            }
        }

        private sealed class PhaseMeasurementSet
        {
            public long EnsureDirectoryTicks;
            public long OpenStreamTicks;
            public long CopyWriteTicks;
            public long FlushTicks;
            public long DisposeTicks;

            public long TotalTrackedTicks =>
                EnsureDirectoryTicks
                + OpenStreamTicks
                + CopyWriteTicks
                + FlushTicks
                + DisposeTicks;

            public void Reset()
            {
                EnsureDirectoryTicks = 0;
                OpenStreamTicks = 0;
                CopyWriteTicks = 0;
                FlushTicks = 0;
                DisposeTicks = 0;
            }
        }

        private sealed class WriteStrategy
        {
            public WriteStrategy(string name, FileOptions fileOptions, bool useAsyncWrite, bool useAsyncFlush, int copyBufferSize, FileOptions sourceFileOptions)
            {
                Name = name;
                FileOptions = fileOptions;
                UseAsyncWrite = useAsyncWrite;
                UseAsyncFlush = useAsyncFlush;
                CopyBufferSize = copyBufferSize;
                SourceFileOptions = sourceFileOptions;
            }

            public string Name { get; }

            public FileOptions FileOptions { get; }

            public bool UseAsyncWrite { get; }

            public bool UseAsyncFlush { get; }

            public int CopyBufferSize { get; }

            public FileOptions SourceFileOptions { get; }
        }

        private sealed class VolumeWritePhaseOptions
        {
            public int WriteCount { get; set; }

            public int WarmupCount { get; set; }

            public int[] FileSizes { get; set; } = Array.Empty<int>();

            public bool IncludeNoFlushScenario { get; set; }
        }
    }
}
