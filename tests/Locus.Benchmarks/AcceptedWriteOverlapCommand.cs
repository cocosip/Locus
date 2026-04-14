#nullable enable
using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.FileSystem;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    internal static class AcceptedWriteOverlapCommand
    {
        private const string ScenarioName = "accepted-write-overlap";
        private const string TenantId = "benchmark-tenant";
        private const string OriginalFileName = "seq-file.dcm";

        public static bool ShouldRun(string[] args)
        {
            if (args.Length == 0)
                return false;

            if (string.Equals(args[0], ScenarioName, StringComparison.OrdinalIgnoreCase))
                return true;

            for (var i = 0; i < args.Length - 1; i++)
            {
                if (string.Equals(args[i], "--scenario", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(args[i + 1], ScenarioName, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        public static async Task<int> RunAsync(string[] args)
        {
            if (!TryParseOptions(args, out var options, out var error))
            {
                if (!string.IsNullOrWhiteSpace(error))
                    Console.Error.WriteLine(error);

                PrintUsage();
                return 1;
            }

            Console.WriteLine(
                $"Running {ScenarioName}: writes={options.WriteCount}, warmup={options.WarmupCount}, sizes=[{string.Join(", ", options.FileSizes)}], format={options.JournalFormat}, includeNoFlush={options.IncludeNoFlushScenario}, source={options.SourceKind}");
            Console.WriteLine();

            foreach (var fileSize in options.FileSizes)
            {
                Console.WriteLine($"=== File Size: {FormatBytes(fileSize)} ({fileSize} bytes) ===");
                foreach (var forceFlushAfterWrite in BuildFlushModes(options))
                {
                    Console.WriteLine($"FlushMode={(forceFlushAfterWrite ? "durable-flush" : "no-volume-flush")}");
                    var serial = await RunModeAsync(fileSize, forceFlushAfterWrite, options, PipelineMode.Serial).ConfigureAwait(false);
                    var overlap = await RunModeAsync(fileSize, forceFlushAfterWrite, options, PipelineMode.OverlapAccepted).ConfigureAwait(false);
                    PrintResult(serial);
                    PrintResult(overlap);
                    var delta = serial.TotalAvgTicks - overlap.TotalAvgTicks;
                    var deltaPercent = serial.TotalAvgTicks > 0 ? delta * 100.0 / serial.TotalAvgTicks : 0;
                    Console.WriteLine($"  Savings(serial -> overlap) avg={FormatTime(delta),10}  change={deltaPercent,6:F2}%");
                    Console.WriteLine();
                }
            }

            return 0;
        }

        private static async Task<ModeResult> RunModeAsync(
            int fileSize,
            bool forceFlushAfterWrite,
            Options options,
            PipelineMode mode)
        {
            using var context = new ScenarioContext(fileSize, forceFlushAfterWrite, options.JournalFormat, options.SourceKind);
            await context.WarmupAsync(options.WarmupCount, mode).ConfigureAwait(false);
            context.Reset();

            var totalStart = Stopwatch.GetTimestamp();
            for (var i = 0; i < options.WriteCount; i++)
                await context.ExecuteOnceAsync(mode).ConfigureAwait(false);

            return new ModeResult(mode, options.WriteCount, Stopwatch.GetTimestamp() - totalStart, context.Measurements.Clone());
        }

        private static bool[] BuildFlushModes(Options options)
        {
            return options.IncludeNoFlushScenario ? new[] { true, false } : new[] { true };
        }

        private static void PrintResult(ModeResult result)
        {
            Console.WriteLine($"  Mode={result.Name}");
            Console.WriteLine($"    Total              avg={FormatTime(result.TotalAvgTicks),10}");
            Console.WriteLine($"    Acceptance window  avg={FormatTime(result.Measurements.AcceptanceWindowTicks / (double)result.WriteCount),10}");
            Console.WriteLine($"    Volume write       avg={FormatTime(result.Measurements.VolumeWriteTicks / (double)result.WriteCount),10}");
            Console.WriteLine($"    Queue journal      avg={FormatTime(result.Measurements.QueueJournalTicks / (double)result.WriteCount),10}");
            Console.WriteLine($"    Projection enqueue avg={FormatTime(result.Measurements.ProjectionTicks / (double)result.WriteCount),10}");
        }

        private static bool TryParseOptions(string[] args, out Options options, out string? error)
        {
            options = new Options
            {
                WriteCount = 100,
                WarmupCount = 10,
                FileSizes = new[] { 256 * 1024, 1024 * 1024, 8 * 1024 * 1024 },
                JournalFormat = JournalFormat.BinaryV1,
                IncludeNoFlushScenario = true,
                SourceKind = SourceKind.File
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
                }
                else if (string.Equals(arg, "--warmup", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var warmup))
                    {
                        error = "Missing or invalid value for --warmup.";
                        return false;
                    }

                    options.WarmupCount = warmup;
                }
                else if (string.Equals(arg, "--sizes", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadValue(args, ref i, out var raw))
                    {
                        error = "Missing value for --sizes.";
                        return false;
                    }

                    options.FileSizes = raw.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                        .Select(v => int.TryParse(v.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : -1)
                        .ToArray();
                    if (options.FileSizes.Length == 0 || options.FileSizes.Any(v => v <= 0))
                    {
                        error = "Invalid value for --sizes.";
                        return false;
                    }
                }
                else if (string.Equals(arg, "--format", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadValue(args, ref i, out var raw))
                    {
                        error = "Missing value for --format.";
                        return false;
                    }

                    options.JournalFormat = string.Equals(raw, "json", StringComparison.OrdinalIgnoreCase)
                        ? JournalFormat.JsonLines
                        : JournalFormat.BinaryV1;
                }
                else if (string.Equals(arg, "--include-no-flush", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadBool(args, ref i, out var includeNoFlush))
                    {
                        error = "Missing or invalid value for --include-no-flush.";
                        return false;
                    }

                    options.IncludeNoFlushScenario = includeNoFlush;
                }
                else if (string.Equals(arg, "--source", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadValue(args, ref i, out var raw))
                    {
                        error = "Missing value for --source.";
                        return false;
                    }

                    options.SourceKind = string.Equals(raw, "memory", StringComparison.OrdinalIgnoreCase)
                        ? SourceKind.Memory
                        : SourceKind.File;
                }
            }

            return options.WriteCount > 0 && options.WarmupCount >= 0;
        }

        private static bool TryReadInt(string[] args, ref int index, out int value)
        {
            value = 0;
            return TryReadValue(args, ref index, out var raw) && int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
        }

        private static bool TryReadBool(string[] args, ref int index, out bool value)
        {
            value = false;
            return TryReadValue(args, ref index, out var raw) && bool.TryParse(raw, out value);
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
            Console.WriteLine("  dotnet run -c Release --project tests/Locus.Benchmarks -- accepted-write-overlap [options]");
        }

        private static string FormatTime(double ticks)
        {
            var microseconds = ticks * 1_000_000.0 / Stopwatch.Frequency;
            return microseconds >= 1000 ? $"{microseconds / 1000.0:F3} ms" : $"{microseconds:F1} us";
        }

        private static string FormatBytes(int bytes)
        {
            return bytes >= 1024 * 1024 ? $"{bytes / 1024.0 / 1024.0:F2} MiB"
                : bytes >= 1024 ? $"{bytes / 1024.0:F2} KiB"
                : $"{bytes} B";
        }

        private sealed class ScenarioContext : IDisposable
        {
            private readonly IFileSystem _fileSystem = new System.IO.Abstractions.FileSystem();
            private readonly string _rootDirectory;
            private readonly string _sourceFilePath;
            private readonly SourceKind _sourceKind;
            private readonly LocalFileSystemVolume _volume;
            private readonly FileQueueEventJournal _journal;
            private readonly MetadataRepository _metadataRepository;
            private readonly MetadataRepositoryQueueProjectionWriteStore _projectionWriteStore;
            private int _writeIndex;

            public ScenarioContext(int fileSize, bool forceFlushAfterWrite, JournalFormat journalFormat, SourceKind sourceKind)
            {
                _sourceKind = sourceKind;
                SourceLength = fileSize;
                Measurements = new Measurements();
                _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-overlap-{fileSize}-{Guid.NewGuid():N}");
                var volumeDirectory = Path.Combine(_rootDirectory, "volume");
                var queueDirectory = Path.Combine(_rootDirectory, "queue");
                var metadataDirectory = Path.Combine(_rootDirectory, "metadata");
                var sourceDirectory = Path.Combine(_rootDirectory, "source");
                Directory.CreateDirectory(volumeDirectory);
                Directory.CreateDirectory(queueDirectory);
                Directory.CreateDirectory(metadataDirectory);
                Directory.CreateDirectory(sourceDirectory);

                var payload = new byte[fileSize];
                for (var i = 0; i < payload.Length; i++)
                    payload[i] = (byte)(i & 0xFF);

                _sourceFilePath = Path.Combine(sourceDirectory, OriginalFileName);
                File.WriteAllBytes(_sourceFilePath, payload);
                _volume = new LocalFileSystemVolume(_fileSystem, NullLogger<LocalFileSystemVolume>.Instance, "vol-001", volumeDirectory, forceFlushAfterWrite: forceFlushAfterWrite);
                _journal = new FileQueueEventJournal(_fileSystem, NullLogger<FileQueueEventJournal>.Instance, new QueueEventJournalOptions
                {
                    QueueDirectory = queueDirectory,
                    JournalFormat = journalFormat,
                    AckMode = QueueEventJournalAckMode.Durable,
                    StateFlushDebounce = TimeSpan.FromSeconds(1),
                    Linger = TimeSpan.FromMilliseconds(1),
                    MaxBatchRecords = 16,
                    MaxBatchBytes = 256 * 1024,
                    WriterIdleTimeout = TimeSpan.FromSeconds(30),
                    BalancedFlushWindow = TimeSpan.FromMilliseconds(5),
                });
                _metadataRepository = new MetadataRepository(_fileSystem, NullLogger<MetadataRepository>.Instance, metadataDirectory);
                _projectionWriteStore = new MetadataRepositoryQueueProjectionWriteStore(_metadataRepository);
            }

            public int SourceLength { get; }

            public Measurements Measurements { get; }

            public async Task WarmupAsync(int count, PipelineMode mode)
            {
                if (_volume is IStorageVolumeWritePathWarmup warmup)
                    await warmup.WarmWritePathCacheAsync(CancellationToken.None).ConfigureAwait(false);

                for (var i = 0; i < count; i++)
                    await ExecuteOnceAsync(mode).ConfigureAwait(false);
            }

            public void Reset()
            {
                _writeIndex = 0;
                Measurements.Reset();
            }

            public async Task ExecuteOnceAsync(PipelineMode mode)
            {
                var metadata = CreateMetadata();
                using var stream = CreateInputStream();
                var acceptanceStart = Stopwatch.GetTimestamp();

                if (mode == PipelineMode.Serial)
                {
                    await MeasureAsync(v => Measurements.VolumeWriteTicks += v, () => _volume.WriteAsync(metadata.PhysicalPath, stream, CancellationToken.None)).ConfigureAwait(false);
                    await MeasureAsync(v => Measurements.QueueJournalTicks += v, () => _journal.AppendAsync(CreateAcceptedRecord(metadata), CancellationToken.None)).ConfigureAwait(false);
                }
                else
                {
                    var volumeTask = MeasureAsync(v => Measurements.VolumeWriteTicks += v, () => _volume.WriteAsync(metadata.PhysicalPath, stream, CancellationToken.None));
                    var journalTask = MeasureAsync(v => Measurements.QueueJournalTicks += v, () => _journal.AppendAsync(CreateAcceptedRecord(metadata), CancellationToken.None));
                    await Task.WhenAll(volumeTask, journalTask).ConfigureAwait(false);
                }

                Measurements.AcceptanceWindowTicks += Stopwatch.GetTimestamp() - acceptanceStart;
                await MeasureAsync(v => Measurements.ProjectionTicks += v, () => _projectionWriteStore.QueueProjectedFileAsync(metadata, CancellationToken.None)).ConfigureAwait(false);
            }

            private FileMetadata CreateMetadata()
            {
                var fileKey = (++_writeIndex).ToString("x32", CultureInfo.InvariantCulture);
                return new FileMetadata
                {
                    FileKey = fileKey,
                    TenantId = TenantId,
                    VolumeId = _volume.VolumeId,
                    PhysicalPath = _volume.BuildPhysicalPath(TenantId, fileKey, ".dcm"),
                    DirectoryPath = "/",
                    FileSize = SourceLength,
                    CreatedAt = DateTime.UtcNow,
                    Status = FileProcessingStatus.Pending,
                    RetryCount = 0,
                    OriginalFileName = OriginalFileName,
                    FileExtension = ".dcm"
                };
            }

            private static QueueEventRecord CreateAcceptedRecord(FileMetadata metadata)
            {
                return new QueueEventRecord
                {
                    TenantId = metadata.TenantId,
                    FileKey = metadata.FileKey,
                    EventType = QueueEventType.Accepted,
                    OccurredAtUtc = metadata.CreatedAt,
                    VolumeId = metadata.VolumeId,
                    PhysicalPath = metadata.PhysicalPath,
                    DirectoryPath = metadata.DirectoryPath,
                    FileSize = metadata.FileSize,
                    Status = metadata.Status,
                    RetryCount = metadata.RetryCount,
                    OriginalFileName = metadata.OriginalFileName,
                    FileExtension = metadata.FileExtension
                };
            }

            private Stream CreateInputStream()
            {
                if (_sourceKind == SourceKind.File)
                    return new FileStream(_sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 256 * 1024, FileOptions.SequentialScan);

                return new MemoryStream(_fileSystem.File.ReadAllBytes(_sourceFilePath), writable: false);
            }

            public void Dispose()
            {
                _journal.Dispose();
                _metadataRepository.Dispose();
                try
                {
                    if (_fileSystem.Directory.Exists(_rootDirectory))
                        _fileSystem.Directory.Delete(_rootDirectory, recursive: true);
                }
                catch
                {
                }
            }
        }

        private sealed class ModeResult
        {
            public ModeResult(PipelineMode mode, int writeCount, long totalTicks, Measurements measurements)
            {
                Mode = mode;
                WriteCount = writeCount;
                TotalTicks = totalTicks;
                Measurements = measurements;
            }

            public PipelineMode Mode { get; }
            public int WriteCount { get; }
            public long TotalTicks { get; }
            public Measurements Measurements { get; }
            public string Name => Mode == PipelineMode.Serial ? "serial-current" : "overlap-prototype";
            public double TotalAvgTicks => WriteCount > 0 ? TotalTicks / (double)WriteCount : 0;
        }

        private sealed class Measurements
        {
            public long AcceptanceWindowTicks { get; set; }
            public long VolumeWriteTicks { get; set; }
            public long QueueJournalTicks { get; set; }
            public long ProjectionTicks { get; set; }

            public Measurements Clone()
            {
                return new Measurements
                {
                    AcceptanceWindowTicks = AcceptanceWindowTicks,
                    VolumeWriteTicks = VolumeWriteTicks,
                    QueueJournalTicks = QueueJournalTicks,
                    ProjectionTicks = ProjectionTicks
                };
            }

            public void Reset()
            {
                AcceptanceWindowTicks = 0;
                VolumeWriteTicks = 0;
                QueueJournalTicks = 0;
                ProjectionTicks = 0;
            }
        }

        private sealed class Options
        {
            public int WriteCount { get; set; }
            public int WarmupCount { get; set; }
            public int[] FileSizes { get; set; } = Array.Empty<int>();
            public JournalFormat JournalFormat { get; set; }
            public bool IncludeNoFlushScenario { get; set; }
            public SourceKind SourceKind { get; set; }
        }

        private enum SourceKind
        {
            Memory,
            File
        }

        private enum PipelineMode
        {
            Serial,
            OverlapAccepted
        }

        private static async Task MeasureAsync(Action<long> record, Func<Task> action)
        {
            var started = Stopwatch.GetTimestamp();
            try
            {
                await action().ConfigureAwait(false);
            }
            finally
            {
                record(Stopwatch.GetTimestamp() - started);
            }
        }
    }
}
