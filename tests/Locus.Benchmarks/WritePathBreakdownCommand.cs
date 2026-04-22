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
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.FileSystem;
using Locus.MultiTenant;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Locus.Benchmarks
{
    internal static class WritePathBreakdownCommand
    {
        private const string ScenarioName = "write-path-breakdown";
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
                $"Running {ScenarioName}: writes={options.WriteCount}, warmup={options.WarmupCount}, sizes=[{string.Join(", ", options.FileSizes)}], format={options.JournalFormat}, ackModes=[{string.Join(", ", options.AckModes)}], includeNoFlush={options.IncludeNoFlushScenario}, source={options.SourceKind}, lingerMs={options.Linger.TotalMilliseconds:F0}, balancedFlushWindowMs={options.BalancedFlushWindow.TotalMilliseconds:F0}, maxBatchRecords={options.MaxBatchRecords}, maxBatchBytes={options.MaxBatchBytes}, volumeWriteBufferSize={options.VolumeWriteBufferSize}, volumeCopyBufferSize={options.VolumeCopyBufferSize}");
            Console.WriteLine();

            var scenarios = BuildScenarios(options);
            foreach (var fileSize in options.FileSizes)
            {
                Console.WriteLine($"=== File Size: {FormatBytes(fileSize)} ({fileSize} bytes) ===");
                foreach (var scenario in scenarios)
                {
                    using var context = await CreateContextAsync(fileSize, scenario, options).ConfigureAwait(false);
                    await WarmupAsync(context, options.WarmupCount).ConfigureAwait(false);

                    context.Measurements.Reset();

                    var totalStopwatch = Stopwatch.StartNew();
                    for (var i = 0; i < options.WriteCount; i++)
                        await context.WriteOnceAsync().ConfigureAwait(false);
                    totalStopwatch.Stop();

                    PrintResult(fileSize, scenario, options.WriteCount, totalStopwatch.ElapsedTicks, totalStopwatch.Elapsed, context.Measurements);
                    Console.WriteLine();
                }
            }

            return 0;
        }

        private static async Task WarmupAsync(ScenarioContext context, int warmupCount)
        {
            for (var i = 0; i < warmupCount; i++)
                await context.WriteOnceAsync().ConfigureAwait(false);
        }

        private static ScenarioDefinition[] BuildScenarios(WritePathBreakdownOptions options)
        {
            var scenarios = new List<ScenarioDefinition>();
            foreach (var ackMode in options.AckModes)
            {
                scenarios.Add(new ScenarioDefinition(
                    $"{ackMode.ToString().ToLowerInvariant()}-flush",
                    forceFlushAfterWrite: true,
                    journalFormat: options.JournalFormat,
                    ackMode: ackMode));

                if (options.IncludeNoFlushScenario)
                {
                    scenarios.Add(new ScenarioDefinition(
                        $"{ackMode.ToString().ToLowerInvariant()}-no-volume-flush",
                        forceFlushAfterWrite: false,
                        journalFormat: options.JournalFormat,
                        ackMode: ackMode));
                }
            }

            return scenarios.ToArray();
        }

        private static async Task<ScenarioContext> CreateContextAsync(int fileSize, ScenarioDefinition scenario, WritePathBreakdownOptions options)
        {
            var fileSystem = new System.IO.Abstractions.FileSystem();
            var rootDirectory = Path.Combine(
                Path.GetTempPath(),
                $"locus-write-breakdown-{scenario.Name}-{fileSize}-{Guid.NewGuid():N}");
            fileSystem.Directory.CreateDirectory(rootDirectory);

            var metadataDirectory = Path.Combine(rootDirectory, "metadata");
            var quotaDirectory = Path.Combine(rootDirectory, "quota");
            var tenantDirectory = Path.Combine(rootDirectory, "tenants");
            var queueDirectory = Path.Combine(rootDirectory, "queue");
            var volumeDirectory = Path.Combine(rootDirectory, "volume");

            fileSystem.Directory.CreateDirectory(volumeDirectory);

            var metadataRepository = new MetadataRepository(
                fileSystem,
                NullLogger<MetadataRepository>.Instance,
                metadataDirectory);
            var quotaRepository = new DirectoryQuotaRepository(
                fileSystem,
                NullLogger<DirectoryQuotaRepository>.Instance,
                quotaDirectory);

            var innerTenantManager = new TenantManager(
                fileSystem,
                NullLogger<TenantManager>.Instance,
                tenantDirectory,
                cacheExpiration: TimeSpan.FromMinutes(30),
                autoCreateTenants: false);
            await innerTenantManager.CreateTenantAsync(TenantId, CancellationToken.None).ConfigureAwait(false);

            var measurements = new MeasurementSet();
            var tenantManager = new MeasuringTenantManager(innerTenantManager, measurements.TenantValidation);
            var tenantQuotaManager = new MeasuringTenantQuotaManager(
                new TenantQuotaManager(quotaRepository, NullLogger<TenantQuotaManager>.Instance),
                measurements.TenantQuota);
            var directoryQuotaManager = new MeasuringDirectoryQuotaManager(
                new DirectoryQuotaManager(quotaRepository, NullLogger<DirectoryQuotaManager>.Instance),
                measurements.DirectoryQuota);

            var projectionWriteStore = new MeasuringProjectionWriteStore(
                new MetadataRepositoryQueueProjectionWriteStore(metadataRepository),
                measurements);

            var queueJournal = new MeasuringQueueEventJournal(
                new FileQueueEventJournal(
                    fileSystem,
                    NullLogger<FileQueueEventJournal>.Instance,
                    new QueueEventJournalOptions
                    {
                        QueueDirectory = queueDirectory,
                        JournalFormat = scenario.JournalFormat,
                        AckMode = scenario.AckMode,
                        StateFlushDebounce = TimeSpan.FromSeconds(1),
                        Linger = options.Linger,
                        MaxBatchRecords = options.MaxBatchRecords,
                        MaxBatchBytes = options.MaxBatchBytes,
                        WriterIdleTimeout = TimeSpan.FromSeconds(30),
                        BalancedFlushWindow = options.BalancedFlushWindow,
                    }),
                measurements.QueueJournal,
                measurements.QueueJournalAppend,
                measurements.QueueJournalFlush);

            var fileScheduler = new Mock<IFileScheduler>();
            var storagePool = new StoragePool(
                metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                tenantManager,
                fileScheduler.Object,
                NullLogger<StoragePool>.Instance,
                queueEventJournal: queueJournal,
                projectionWriteStore: projectionWriteStore,
                allowLegacyNonJournalMode: false);

            var volume = new MeasuringStorageVolume(
                new LocalFileSystemVolume(
                    fileSystem,
                    NullLogger<LocalFileSystemVolume>.Instance,
                    "vol-001",
                    volumeDirectory,
                    writeBufferSize: options.VolumeWriteBufferSize,
                    copyBufferSize: options.VolumeCopyBufferSize,
                    forceFlushAfterWrite: scenario.ForceFlushAfterWrite),
                measurements.VolumeWrite,
                measurements.VolumeWriteDirectoryPrepare,
                measurements.VolumeWriteOpenStream,
                measurements.VolumeWriteCopy,
                measurements.VolumeWriteCopyTo,
                measurements.VolumeWriteCopyLoop,
                measurements.VolumeWriteFlush);

            await storagePool.AddVolumeAsync(volume, initialDelayMs: 0, healthCheckDelayMs: 0, ct: CancellationToken.None)
                .ConfigureAwait(false);

            if (volume is IStorageVolumeWritePathWarmup writePathWarmup)
                await writePathWarmup.WarmWritePathCacheAsync(CancellationToken.None).ConfigureAwait(false);

            var tenant = await tenantManager.GetTenantAsync(TenantId, CancellationToken.None).ConfigureAwait(false);
            var payload = CreatePayload(fileSize);
            var sourceFilePath = Path.Combine(rootDirectory, "source.bin");
            fileSystem.File.WriteAllBytes(sourceFilePath, payload);

            return new ScenarioContext(
                fileSystem,
                rootDirectory,
                storagePool,
                tenant,
                sourceFilePath,
                payload,
                options.SourceKind,
                measurements,
                metadataRepository,
                quotaRepository,
                queueJournal);
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
            ScenarioDefinition scenario,
            int writeCount,
            long totalStopwatchTicks,
            TimeSpan elapsed,
            MeasurementSet measurements)
        {
            var totalTicks = totalStopwatchTicks;
            var trackedTicks = measurements.TotalTrackedTicks;
            var untrackedTicks = Math.Max(0L, totalTicks - trackedTicks);
            var throughput = elapsed.TotalSeconds > 0
                ? ((long)fileSize * writeCount) / elapsed.TotalSeconds / 1024.0 / 1024.0
                : 0;

            Console.WriteLine(
                $"Scenario={scenario.Name}, forceFlushAfterWrite={scenario.ForceFlushAfterWrite}, journalFormat={scenario.JournalFormat}, ackMode={scenario.AckMode}");
            Console.WriteLine(
                $"  Total:            avg={FormatTime(totalStopwatchTicks / (double)writeCount),10}  total={elapsed.TotalMilliseconds,10:F2} ms  throughput={throughput,8:F2} MiB/s");

            PrintPhase("Tenant validation", measurements.TenantValidation.TotalTicks, writeCount, totalTicks);
            PrintPhase("Tenant quota", measurements.TenantQuota.TotalTicks, writeCount, totalTicks);
            PrintPhase("Directory quota", measurements.DirectoryQuota.TotalTicks, writeCount, totalTicks);
            PrintPhase("Volume write", measurements.VolumeWrite.TotalTicks, writeCount, totalTicks);
            PrintPhase("Volume dir prep", measurements.VolumeWriteDirectoryPrepare.TotalTicks, writeCount, totalTicks);
            PrintPhase("Volume open", measurements.VolumeWriteOpenStream.TotalTicks, writeCount, totalTicks);
            PrintPhase("Volume copy", measurements.VolumeWriteCopy.TotalTicks, writeCount, totalTicks);
            PrintPhase("Volume CopyTo", measurements.VolumeWriteCopyTo.TotalTicks, writeCount, totalTicks);
            PrintPhase("Volume loop", measurements.VolumeWriteCopyLoop.TotalTicks, writeCount, totalTicks);
            PrintPhase("Volume flush", measurements.VolumeWriteFlush.TotalTicks, writeCount, totalTicks);
            PrintPhase("Queue journal", measurements.QueueJournal.TotalTicks, writeCount, totalTicks);
            PrintPhase("Queue append", measurements.QueueJournalAppend.TotalTicks, writeCount, totalTicks);
            PrintPhase("Queue flush", measurements.QueueJournalFlush.TotalTicks, writeCount, totalTicks);
            PrintPhase("Projection enqueue", measurements.ProjectionEnqueue.TotalTicks, writeCount, totalTicks);
            PrintPhase("Projection validate", measurements.ProjectionValidation.TotalTicks, writeCount, totalTicks);
            PrintPhase("Projection cache/index", measurements.ProjectionCacheAndIndex.TotalTicks, writeCount, totalTicks);
            PrintPhase("Projection cache mutate", measurements.ProjectionCacheMutation.TotalTicks, writeCount, totalTicks);
            PrintPhase("Projection path index", measurements.ProjectionPhysicalPathIndex.TotalTicks, writeCount, totalTicks);
            PrintPhase("Projection pending", measurements.ProjectionPendingQueue.TotalTicks, writeCount, totalTicks);
            PrintPhase("Projection status index", measurements.ProjectionStatusIndex.TotalTicks, writeCount, totalTicks);
            PrintPhase("Projection enqueue persistence", measurements.ProjectionPersistenceEnqueue.TotalTicks, writeCount, totalTicks);
            PrintPhase("Untracked", untrackedTicks, writeCount, totalTicks);
        }

        private static void PrintPhase(string label, long ticks, int writeCount, long totalTicks)
        {
            var avgTicks = writeCount > 0 ? ticks / (double)writeCount : 0;
            var percent = totalTicks > 0 ? ticks * 100.0 / totalTicks : 0;
            Console.WriteLine(
                $"  {label,-16} avg={FormatTime(avgTicks),10}  share={percent,6:F2}%");
        }

        private static string FormatTime(double ticks)
        {
            var microseconds = ticks * 1_000_000.0 / Stopwatch.Frequency;
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

        private static bool TryParseOptions(
            string[] args,
            out WritePathBreakdownOptions options,
            out string? error)
        {
            options = new WritePathBreakdownOptions
            {
                WriteCount = 300,
                WarmupCount = 30,
                FileSizes = new[] { 4 * 1024, 16 * 1024, 64 * 1024 },
                JournalFormat = JournalFormat.BinaryV1,
                IncludeNoFlushScenario = true,
                AckModes = new[] { QueueEventJournalAckMode.Durable },
                Linger = TimeSpan.FromMilliseconds(1),
                BalancedFlushWindow = TimeSpan.FromMilliseconds(5),
                MaxBatchRecords = 16,
                MaxBatchBytes = 256 * 1024,
                VolumeWriteBufferSize = 128 * 1024,
                VolumeCopyBufferSize = 256 * 1024,
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
                        error = "Invalid value for --sizes. Use a comma-separated byte list such as 4096,16384,65536.";
                        return false;
                    }

                    options.FileSizes = parsedSizes;
                    continue;
                }

                if (string.Equals(arg, "--format", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadValue(args, ref i, out var formatValue))
                    {
                        error = "Missing value for --format.";
                        return false;
                    }

                    if (string.Equals(formatValue, "binary", StringComparison.OrdinalIgnoreCase))
                    {
                        options.JournalFormat = JournalFormat.BinaryV1;
                    }
                    else if (string.Equals(formatValue, "json", StringComparison.OrdinalIgnoreCase))
                    {
                        options.JournalFormat = JournalFormat.JsonLines;
                    }
                    else
                    {
                        error = "Invalid value for --format. Supported values: binary, json.";
                        return false;
                    }

                    continue;
                }

                if (string.Equals(arg, "--ack-mode", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(arg, "--ack-modes", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadValue(args, ref i, out var rawAckModes))
                    {
                        error = "Missing value for --ack-mode.";
                        return false;
                    }

                    if (!TryParseAckModes(rawAckModes, out var ackModes))
                    {
                        error = "Invalid value for --ack-mode. Supported values: durable, balanced, async.";
                        return false;
                    }

                    options.AckModes = ackModes;
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

                if (string.Equals(arg, "--source", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadValue(args, ref i, out var sourceValue))
                    {
                        error = "Missing value for --source.";
                        return false;
                    }

                    if (string.Equals(sourceValue, "memory", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(sourceValue, "memory-hidden", StringComparison.OrdinalIgnoreCase))
                    {
                        options.SourceKind = SourceKind.HiddenMemory;
                    }
                    else if (string.Equals(sourceValue, "memory-visible", StringComparison.OrdinalIgnoreCase))
                    {
                        options.SourceKind = SourceKind.VisibleMemory;
                    }
                    else if (string.Equals(sourceValue, "file", StringComparison.OrdinalIgnoreCase))
                    {
                        options.SourceKind = SourceKind.File;
                    }
                    else
                    {
                        error = "Invalid value for --source. Supported values: memory, memory-hidden, memory-visible, file.";
                        return false;
                    }

                    continue;
                }

                if (string.Equals(arg, "--linger-ms", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var lingerMs) || lingerMs < 0)
                    {
                        error = "Missing or invalid value for --linger-ms.";
                        return false;
                    }

                    options.Linger = TimeSpan.FromMilliseconds(lingerMs);
                    continue;
                }

                if (string.Equals(arg, "--balanced-flush-window-ms", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var balancedFlushWindowMs) || balancedFlushWindowMs < 0)
                    {
                        error = "Missing or invalid value for --balanced-flush-window-ms.";
                        return false;
                    }

                    options.BalancedFlushWindow = TimeSpan.FromMilliseconds(balancedFlushWindowMs);
                    continue;
                }

                if (string.Equals(arg, "--max-batch-records", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var maxBatchRecords) || maxBatchRecords <= 0)
                    {
                        error = "Missing or invalid value for --max-batch-records.";
                        return false;
                    }

                    options.MaxBatchRecords = maxBatchRecords;
                    continue;
                }

                if (string.Equals(arg, "--max-batch-bytes", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var maxBatchBytes) || maxBatchBytes <= 0)
                    {
                        error = "Missing or invalid value for --max-batch-bytes.";
                        return false;
                    }

                    options.MaxBatchBytes = maxBatchBytes;
                    continue;
                }

                if (string.Equals(arg, "--volume-write-buffer-size", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var volumeWriteBufferSize) || volumeWriteBufferSize <= 0)
                    {
                        error = "Missing or invalid value for --volume-write-buffer-size.";
                        return false;
                    }

                    options.VolumeWriteBufferSize = volumeWriteBufferSize;
                    continue;
                }

                if (string.Equals(arg, "--volume-copy-buffer-size", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var volumeCopyBufferSize) || volumeCopyBufferSize <= 0)
                    {
                        error = "Missing or invalid value for --volume-copy-buffer-size.";
                        return false;
                    }

                    options.VolumeCopyBufferSize = volumeCopyBufferSize;
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
                error = "--warmup cannot be negative.";
                return false;
            }

            if (options.AckModes.Length == 0)
            {
                error = "--ack-mode must contain at least one mode.";
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

            index++;
            value = args[index];
            return true;
        }

        private static bool TryParseAckModes(string rawValue, out QueueEventJournalAckMode[] ackModes)
        {
            var parsedModes = rawValue
                .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(value => value.Trim())
                .Where(value => value.Length > 0)
                .Select(ParseAckMode)
                .ToArray();

            if (parsedModes.Length == 0 || parsedModes.Any(mode => !mode.HasValue))
            {
                ackModes = Array.Empty<QueueEventJournalAckMode>();
                return false;
            }

            ackModes = parsedModes
                .Select(mode => mode!.Value)
                .Distinct()
                .ToArray();
            return true;
        }

        private static QueueEventJournalAckMode? ParseAckMode(string value)
        {
            if (string.Equals(value, "durable", StringComparison.OrdinalIgnoreCase))
                return QueueEventJournalAckMode.Durable;

            if (string.Equals(value, "balanced", StringComparison.OrdinalIgnoreCase))
                return QueueEventJournalAckMode.Balanced;

            if (string.Equals(value, "async", StringComparison.OrdinalIgnoreCase))
                return QueueEventJournalAckMode.Async;

            return null;
        }

        private static void PrintUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  dotnet run -c Release --project tests/Locus.Benchmarks -- write-path-breakdown [options]");
            Console.WriteLine("Options:");
            Console.WriteLine("  --writes <count>              Number of measured sequential writes. Default: 300");
            Console.WriteLine("  --warmup <count>              Number of warmup writes before measurement. Default: 30");
            Console.WriteLine("  --sizes <csv>                 Comma-separated file sizes in bytes. Default: 4096,16384,65536");
            Console.WriteLine("  --format <binary|json>        Queue journal format. Default: binary");
            Console.WriteLine("  --ack-mode <csv>              Queue journal ack mode(s): durable,balanced,async. Default: durable");
            Console.WriteLine("  --include-no-flush <bool>     Also run forceFlushAfterWrite=false scenario. Default: true");
            Console.WriteLine("  --source <kind>               Benchmark input stream type: memory, memory-hidden, memory-visible, or file. Default: memory");
            Console.WriteLine("  --linger-ms <ms>              Queue journal linger window in milliseconds. Default: 1");
            Console.WriteLine("  --balanced-flush-window-ms <ms>  Balanced ack flush window in milliseconds. Default: 5");
            Console.WriteLine("  --max-batch-records <count>   Queue journal max coalesced records per batch. Default: 16");
            Console.WriteLine("  --max-batch-bytes <bytes>     Queue journal max coalesced bytes per batch. Default: 262144");
            Console.WriteLine("  --volume-write-buffer-size <bytes>  LocalFileSystemVolume write stream buffer size. Default: 131072");
            Console.WriteLine("  --volume-copy-buffer-size <bytes>   LocalFileSystemVolume pooled copy buffer size. Default: 262144");
        }

        private sealed class WritePathBreakdownOptions
        {
            public int WriteCount { get; set; }

            public int WarmupCount { get; set; }

            public int[] FileSizes { get; set; } = Array.Empty<int>();

            public JournalFormat JournalFormat { get; set; }

            public bool IncludeNoFlushScenario { get; set; }

            public QueueEventJournalAckMode[] AckModes { get; set; } = Array.Empty<QueueEventJournalAckMode>();

            public SourceKind SourceKind { get; set; } = SourceKind.HiddenMemory;

            public TimeSpan Linger { get; set; }

            public TimeSpan BalancedFlushWindow { get; set; }

            public int MaxBatchRecords { get; set; }

            public int MaxBatchBytes { get; set; }

            public int VolumeWriteBufferSize { get; set; }

            public int VolumeCopyBufferSize { get; set; }
        }

        private enum SourceKind
        {
            HiddenMemory,
            VisibleMemory,
            File
        }

        private sealed class ScenarioDefinition
        {
            public ScenarioDefinition(string name, bool forceFlushAfterWrite, JournalFormat journalFormat, QueueEventJournalAckMode ackMode)
            {
                Name = name;
                ForceFlushAfterWrite = forceFlushAfterWrite;
                JournalFormat = journalFormat;
                AckMode = ackMode;
            }

            public string Name { get; }

            public bool ForceFlushAfterWrite { get; }

            public JournalFormat JournalFormat { get; }

            public QueueEventJournalAckMode AckMode { get; }
        }

        private sealed class ScenarioContext : IDisposable
        {
            private readonly IFileSystem _fileSystem;
            private readonly string _rootDirectory;
            private readonly MetadataRepository _metadataRepository;
            private readonly DirectoryQuotaRepository _quotaRepository;
            private readonly IDisposable _queueJournal;
            private readonly byte[] _memoryPayload;
            public ScenarioContext(
                IFileSystem fileSystem,
                string rootDirectory,
                StoragePool storagePool,
                ITenantContext tenant,
                string sourceFilePath,
                byte[] memoryPayload,
                SourceKind sourceKind,
                MeasurementSet measurements,
                MetadataRepository metadataRepository,
                DirectoryQuotaRepository quotaRepository,
                IDisposable queueJournal)
            {
                _fileSystem = fileSystem;
                _rootDirectory = rootDirectory;
                StoragePool = storagePool;
                Tenant = tenant;
                SourceFilePath = sourceFilePath;
                _memoryPayload = memoryPayload;
                Source = sourceKind;
                Measurements = measurements;
                _metadataRepository = metadataRepository;
                _quotaRepository = quotaRepository;
                _queueJournal = queueJournal;
            }

            public StoragePool StoragePool { get; }

            public ITenantContext Tenant { get; }

            public string SourceFilePath { get; }

            public SourceKind Source { get; }

            public MeasurementSet Measurements { get; }

            public async Task WriteOnceAsync()
            {
                using var stream = CreateInputStream();
                await StoragePool.WriteFileAsync(Tenant, stream, "seq-file.dcm", CancellationToken.None).ConfigureAwait(false);
            }

            private Stream CreateInputStream()
            {
                if (Source == SourceKind.File)
                {
                    return new FileStream(
                        SourceFilePath,
                        FileMode.Open,
                        FileAccess.Read,
                        FileShare.Read,
                        256 * 1024,
                        FileOptions.SequentialScan);
                }

                if (Source == SourceKind.VisibleMemory)
                {
                    return new MemoryStream(
                        _memoryPayload,
                        0,
                        _memoryPayload.Length,
                        writable: false,
                        publiclyVisible: true);
                }

                return new MemoryStream(_memoryPayload, writable: false);
            }

            public void Dispose()
            {
                _queueJournal.Dispose();
                _metadataRepository.Dispose();
                _quotaRepository.Dispose();

                try
                {
                    if (_fileSystem.Directory.Exists(_rootDirectory))
                        _fileSystem.Directory.Delete(_rootDirectory, recursive: true);
                }
                catch
                {
                    // Ignore cleanup errors in benchmark helper.
                }
            }
        }

        private sealed class MeasurementSet
        {
            public MeasurementSet()
            {
                TenantValidation = new Measurement();
                TenantQuota = new Measurement();
                DirectoryQuota = new Measurement();
                VolumeWrite = new Measurement();
                VolumeWriteDirectoryPrepare = new Measurement();
                VolumeWriteOpenStream = new Measurement();
                VolumeWriteCopy = new Measurement();
                VolumeWriteCopyTo = new Measurement();
                VolumeWriteCopyLoop = new Measurement();
                VolumeWriteFlush = new Measurement();
                QueueJournal = new Measurement();
                QueueJournalAppend = new Measurement();
                QueueJournalFlush = new Measurement();
                ProjectionEnqueue = new Measurement();
                ProjectionValidation = new Measurement();
                ProjectionCacheAndIndex = new Measurement();
                ProjectionCacheMutation = new Measurement();
                ProjectionPhysicalPathIndex = new Measurement();
                ProjectionPendingQueue = new Measurement();
                ProjectionStatusIndex = new Measurement();
                ProjectionPersistenceEnqueue = new Measurement();
            }

            public Measurement TenantValidation { get; }

            public Measurement TenantQuota { get; }

            public Measurement DirectoryQuota { get; }

            public Measurement VolumeWrite { get; }

            public Measurement VolumeWriteDirectoryPrepare { get; }

            public Measurement VolumeWriteOpenStream { get; }

            public Measurement VolumeWriteCopy { get; }

            public Measurement VolumeWriteCopyTo { get; }

            public Measurement VolumeWriteCopyLoop { get; }

            public Measurement VolumeWriteFlush { get; }

            public Measurement QueueJournal { get; }

            public Measurement QueueJournalAppend { get; }

            public Measurement QueueJournalFlush { get; }

            public Measurement ProjectionEnqueue { get; }

            public Measurement ProjectionValidation { get; }

            public Measurement ProjectionCacheAndIndex { get; }

            public Measurement ProjectionCacheMutation { get; }

            public Measurement ProjectionPhysicalPathIndex { get; }

            public Measurement ProjectionPendingQueue { get; }

            public Measurement ProjectionStatusIndex { get; }

            public Measurement ProjectionPersistenceEnqueue { get; }

            public long TotalTrackedTicks =>
                TenantValidation.TotalTicks
                + TenantQuota.TotalTicks
                + DirectoryQuota.TotalTicks
                + VolumeWrite.TotalTicks
                + QueueJournal.TotalTicks
                + ProjectionEnqueue.TotalTicks;

            public void Reset()
            {
                TenantValidation.Reset();
                TenantQuota.Reset();
                DirectoryQuota.Reset();
                VolumeWrite.Reset();
                VolumeWriteDirectoryPrepare.Reset();
                VolumeWriteOpenStream.Reset();
                VolumeWriteCopy.Reset();
                VolumeWriteCopyTo.Reset();
                VolumeWriteCopyLoop.Reset();
                VolumeWriteFlush.Reset();
                QueueJournal.Reset();
                QueueJournalAppend.Reset();
                QueueJournalFlush.Reset();
                ProjectionEnqueue.Reset();
                ProjectionValidation.Reset();
                ProjectionCacheAndIndex.Reset();
                ProjectionCacheMutation.Reset();
                ProjectionPhysicalPathIndex.Reset();
                ProjectionPendingQueue.Reset();
                ProjectionStatusIndex.Reset();
                ProjectionPersistenceEnqueue.Reset();
            }
        }

        private sealed class Measurement
        {
            private long _totalTicks;

            public long TotalTicks => Interlocked.Read(ref _totalTicks);

            public void Add(long ticks)
            {
                Interlocked.Add(ref _totalTicks, ticks);
            }

            public void Reset()
            {
                Interlocked.Exchange(ref _totalTicks, 0);
            }
        }

        private sealed class MeasuringTenantManager : ITenantManager
        {
            private readonly ITenantManager _inner;
            private readonly Measurement _measurement;

            public MeasuringTenantManager(ITenantManager inner, Measurement measurement)
            {
                _inner = inner;
                _measurement = measurement;
            }

            public Task<ITenantContext> GetTenantAsync(string tenantId, CancellationToken ct = default)
            {
                return MeasureAsync(() => _inner.GetTenantAsync(tenantId, ct));
            }

            public Task<ITenantContext?> TryGetTenantAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.TryGetTenantAsync(tenantId, ct);
            }

            public Task<bool> IsTenantEnabledAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.IsTenantEnabledAsync(tenantId, ct);
            }

            public Task EnableTenantAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.EnableTenantAsync(tenantId, ct);
            }

            public Task DisableTenantAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.DisableTenantAsync(tenantId, ct);
            }

            public Task CreateTenantAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.CreateTenantAsync(tenantId, ct);
            }

            public Task<IEnumerable<ITenantContext>> GetAllTenantsAsync(CancellationToken ct = default)
            {
                return _inner.GetAllTenantsAsync(ct);
            }

            private async Task<T> MeasureAsync<T>(Func<Task<T>> operation)
            {
                var started = Stopwatch.GetTimestamp();
                try
                {
                    return await operation().ConfigureAwait(false);
                }
                finally
                {
                    _measurement.Add(Stopwatch.GetTimestamp() - started);
                }
            }
        }

        private sealed class MeasuringTenantQuotaManager : ITenantQuotaManager
        {
            private readonly ITenantQuotaManager _inner;
            private readonly Measurement _measurement;

            public MeasuringTenantQuotaManager(ITenantQuotaManager inner, Measurement measurement)
            {
                _inner = inner;
                _measurement = measurement;
            }

            public Task<bool> CanAddFileAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.CanAddFileAsync(tenantId, ct);
            }

            public Task IncrementFileCountAsync(string tenantId, CancellationToken ct = default)
            {
                return MeasureAsync(() => _inner.IncrementFileCountAsync(tenantId, ct));
            }

            public Task DecrementFileCountAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.DecrementFileCountAsync(tenantId, ct);
            }

            public Task<int> GetFileCountAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.GetFileCountAsync(tenantId, ct);
            }

            public Task<int> GetEffectiveLimitAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.GetEffectiveLimitAsync(tenantId, ct);
            }

            public Task SetTenantLimitAsync(string tenantId, int maxFiles, CancellationToken ct = default)
            {
                return _inner.SetTenantLimitAsync(tenantId, maxFiles, ct);
            }

            public Task RemoveTenantLimitAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.RemoveTenantLimitAsync(tenantId, ct);
            }

            public Task SetGlobalLimitAsync(int maxFiles, CancellationToken ct = default)
            {
                return _inner.SetGlobalLimitAsync(maxFiles, ct);
            }

            public Task<int> GetGlobalLimitAsync(CancellationToken ct = default)
            {
                return _inner.GetGlobalLimitAsync(ct);
            }

            private async Task MeasureAsync(Func<Task> operation)
            {
                var started = Stopwatch.GetTimestamp();
                try
                {
                    await operation().ConfigureAwait(false);
                }
                finally
                {
                    _measurement.Add(Stopwatch.GetTimestamp() - started);
                }
            }
        }

        private sealed class MeasuringDirectoryQuotaManager : IDirectoryQuotaManager
        {
            private readonly IDirectoryQuotaManager _inner;
            private readonly Measurement _measurement;

            public MeasuringDirectoryQuotaManager(IDirectoryQuotaManager inner, Measurement measurement)
            {
                _inner = inner;
                _measurement = measurement;
            }

            public Task<bool> CanAddFileAsync(string tenantId, string directoryPath, CancellationToken ct = default)
            {
                return _inner.CanAddFileAsync(tenantId, directoryPath, ct);
            }

            public Task IncrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct = default)
            {
                return MeasureAsync(() => _inner.IncrementFileCountAsync(tenantId, directoryPath, ct));
            }

            public Task DecrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct = default)
            {
                return _inner.DecrementFileCountAsync(tenantId, directoryPath, ct);
            }

            public Task<int> GetFileCountAsync(string tenantId, string directoryPath, CancellationToken ct = default)
            {
                return _inner.GetFileCountAsync(tenantId, directoryPath, ct);
            }

            public Task<int> GetLimitAsync(string tenantId, string directoryPath, CancellationToken ct = default)
            {
                return _inner.GetLimitAsync(tenantId, directoryPath, ct);
            }

            public Task SetLimitAsync(string tenantId, string directoryPath, int maxFiles, CancellationToken ct = default)
            {
                return _inner.SetLimitAsync(tenantId, directoryPath, maxFiles, ct);
            }

            private async Task MeasureAsync(Func<Task> operation)
            {
                var started = Stopwatch.GetTimestamp();
                try
                {
                    await operation().ConfigureAwait(false);
                }
                finally
                {
                    _measurement.Add(Stopwatch.GetTimestamp() - started);
                }
            }
        }

        private sealed class MeasuringStorageVolume : IStorageVolume, IStorageVolumeHealthProbe, IStorageVolumeWritePathWarmup
        {
            private readonly IStorageVolume _inner;
            private readonly Measurement _measurement;
            private readonly Measurement _directoryPrepareMeasurement;
            private readonly Measurement _openStreamMeasurement;
            private readonly Measurement _copyMeasurement;
            private readonly Measurement _copyToMeasurement;
            private readonly Measurement _copyLoopMeasurement;
            private readonly Measurement _flushMeasurement;

            public MeasuringStorageVolume(
                IStorageVolume inner,
                Measurement measurement,
                Measurement directoryPrepareMeasurement,
                Measurement openStreamMeasurement,
                Measurement copyMeasurement,
                Measurement copyToMeasurement,
                Measurement copyLoopMeasurement,
                Measurement flushMeasurement)
            {
                _inner = inner;
                _measurement = measurement;
                _directoryPrepareMeasurement = directoryPrepareMeasurement;
                _openStreamMeasurement = openStreamMeasurement;
                _copyMeasurement = copyMeasurement;
                _copyToMeasurement = copyToMeasurement;
                _copyLoopMeasurement = copyLoopMeasurement;
                _flushMeasurement = flushMeasurement;
            }

            public string VolumeId => _inner.VolumeId;

            public string MountPath => _inner.MountPath;

            public long TotalCapacity => _inner.TotalCapacity;

            public long AvailableSpace => _inner.AvailableSpace;

            public bool IsHealthy => _inner.IsHealthy;

            public int ShardingDepth => _inner.ShardingDepth;

            public Task<Stream> ReadAsync(string path, CancellationToken ct = default)
            {
                return _inner.ReadAsync(path, ct);
            }

            public async Task WriteAsync(string path, Stream content, CancellationToken ct = default)
            {
                var beforeSnapshot = (_inner as IStorageVolumeWritePathDiagnostics)?.GetWritePathStatisticsSnapshot();
                var started = Stopwatch.GetTimestamp();
                try
                {
                    await _inner.WriteAsync(path, content, ct).ConfigureAwait(false);
                }
                finally
                {
                    _measurement.Add(Stopwatch.GetTimestamp() - started);
                    var afterSnapshot = (_inner as IStorageVolumeWritePathDiagnostics)?.GetWritePathStatisticsSnapshot();
                    if (beforeSnapshot != null && afterSnapshot != null)
                    {
                        _directoryPrepareMeasurement.Add(Math.Max(0L, afterSnapshot.DirectoryPreparationTicks - beforeSnapshot.DirectoryPreparationTicks));
                        _openStreamMeasurement.Add(Math.Max(0L, afterSnapshot.OpenStreamTicks - beforeSnapshot.OpenStreamTicks));
                        _copyMeasurement.Add(Math.Max(0L, afterSnapshot.CopyTicks - beforeSnapshot.CopyTicks));
                        _copyToMeasurement.Add(Math.Max(0L, afterSnapshot.SynchronousSeekableFileCopyToTicks - beforeSnapshot.SynchronousSeekableFileCopyToTicks));
                        _copyLoopMeasurement.Add(Math.Max(0L, afterSnapshot.SynchronousSeekableFileLoopTicks - beforeSnapshot.SynchronousSeekableFileLoopTicks));
                        _flushMeasurement.Add(Math.Max(0L, afterSnapshot.FlushTicks - beforeSnapshot.FlushTicks));
                    }
                }
            }

            public Task DeleteAsync(string path, CancellationToken ct = default)
            {
                return _inner.DeleteAsync(path, ct);
            }

            public string BuildPhysicalPath(string tenantId, string fileKey, string? fileExtension)
            {
                return _inner.BuildPhysicalPath(tenantId, fileKey, fileExtension);
            }

            public bool ProbeHealth()
            {
                return _inner is IStorageVolumeHealthProbe healthProbe
                    ? healthProbe.ProbeHealth()
                    : _inner.IsHealthy;
            }

            public Task WarmWritePathCacheAsync(CancellationToken ct = default)
            {
                if (_inner is IStorageVolumeWritePathWarmup warmup)
                    return warmup.WarmWritePathCacheAsync(ct);

                return Task.CompletedTask;
            }
        }

        private sealed class MeasuringQueueEventJournal : IQueueEventJournal, IDisposable
        {
            private readonly IQueueEventJournal _inner;
            private readonly Measurement _measurement;
            private readonly Measurement _appendMeasurement;
            private readonly Measurement _flushMeasurement;

            public MeasuringQueueEventJournal(
                IQueueEventJournal inner,
                Measurement measurement,
                Measurement appendMeasurement,
                Measurement flushMeasurement)
            {
                _inner = inner;
                _measurement = measurement;
                _appendMeasurement = appendMeasurement;
                _flushMeasurement = flushMeasurement;
            }

            public async Task AppendAsync(QueueEventRecord record, CancellationToken ct = default)
            {
                var beforeSnapshot = (_inner as IQueueEventJournalWritePathDiagnostics)?.GetWritePathStatisticsSnapshot();
                var started = Stopwatch.GetTimestamp();
                try
                {
                    await _inner.AppendAsync(record, ct).ConfigureAwait(false);
                }
                finally
                {
                    _measurement.Add(Stopwatch.GetTimestamp() - started);
                    RecordSubphases(beforeSnapshot);
                }
            }

            public async Task AppendBatchAsync(IReadOnlyList<QueueEventRecord> records, CancellationToken ct = default)
            {
                var beforeSnapshot = (_inner as IQueueEventJournalWritePathDiagnostics)?.GetWritePathStatisticsSnapshot();
                var started = Stopwatch.GetTimestamp();
                try
                {
                    await _inner.AppendBatchAsync(records, ct).ConfigureAwait(false);
                }
                finally
                {
                    _measurement.Add(Stopwatch.GetTimestamp() - started);
                    RecordSubphases(beforeSnapshot);
                }
            }

            public Task<QueueEventReadBatch> ReadBatchAsync(string tenantId, long offset, int maxRecords, CancellationToken ct = default)
            {
                return _inner.ReadBatchAsync(tenantId, offset, maxRecords, ct);
            }

            public Task<long> CompactAsync(string tenantId, long processedOffset, CancellationToken ct = default)
            {
                return _inner.CompactAsync(tenantId, processedOffset, ct);
            }

            public Task<long> GetTailOffsetAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.GetTailOffsetAsync(tenantId, ct);
            }

            public Task<long> GetBaseOffsetAsync(string tenantId, CancellationToken ct = default)
            {
                return _inner.GetBaseOffsetAsync(tenantId, ct);
            }

            public Task<IReadOnlyList<string>> GetTenantIdsAsync(CancellationToken ct = default)
            {
                return _inner.GetTenantIdsAsync(ct);
            }

            public void Dispose()
            {
                if (_inner is IDisposable disposable)
                    disposable.Dispose();
            }

            private void RecordSubphases(QueueJournalWritePathStatistics? beforeSnapshot)
            {
                var afterSnapshot = (_inner as IQueueEventJournalWritePathDiagnostics)?.GetWritePathStatisticsSnapshot();
                if (beforeSnapshot == null || afterSnapshot == null)
                    return;

                _appendMeasurement.Add(Math.Max(0L, afterSnapshot.AppendTicks - beforeSnapshot.AppendTicks));
                _flushMeasurement.Add(Math.Max(0L, afterSnapshot.FlushTicks - beforeSnapshot.FlushTicks));
            }
        }

        private sealed class MeasuringProjectionWriteStore : IQueueProjectionWriteStore
        {
            private readonly IQueueProjectionWriteStore _inner;
            private readonly MeasurementSet _measurements;

            public MeasuringProjectionWriteStore(IQueueProjectionWriteStore inner, MeasurementSet measurements)
            {
                _inner = inner;
                _measurements = measurements;
            }

            public async Task QueueProjectedFileAsync(FileMetadata metadata, CancellationToken ct = default)
            {
                var beforeSnapshot = (_inner as IQueueProjectionWritePathDiagnostics)?.GetWritePathStatisticsSnapshot();
                var started = Stopwatch.GetTimestamp();
                try
                {
                    await _inner.QueueProjectedFileAsync(metadata, ct).ConfigureAwait(false);
                }
                finally
                {
                    _measurements.ProjectionEnqueue.Add(Stopwatch.GetTimestamp() - started);
                    RecordSubphases(beforeSnapshot);
                }
            }

            private void RecordSubphases(QueueProjectionWritePathStatistics? beforeSnapshot)
            {
                var afterSnapshot = (_inner as IQueueProjectionWritePathDiagnostics)?.GetWritePathStatisticsSnapshot();
                if (beforeSnapshot == null || afterSnapshot == null)
                    return;

                _measurements.ProjectionValidation.Add(Math.Max(0L, afterSnapshot.ValidationTicks - beforeSnapshot.ValidationTicks));
                _measurements.ProjectionCacheAndIndex.Add(Math.Max(0L, afterSnapshot.CacheAndIndexTicks - beforeSnapshot.CacheAndIndexTicks));
                _measurements.ProjectionCacheMutation.Add(Math.Max(0L, afterSnapshot.CacheMutationTicks - beforeSnapshot.CacheMutationTicks));
                _measurements.ProjectionPhysicalPathIndex.Add(Math.Max(0L, afterSnapshot.PhysicalPathIndexTicks - beforeSnapshot.PhysicalPathIndexTicks));
                _measurements.ProjectionPendingQueue.Add(Math.Max(0L, afterSnapshot.PendingQueueTicks - beforeSnapshot.PendingQueueTicks));
                _measurements.ProjectionStatusIndex.Add(Math.Max(0L, afterSnapshot.StatusIndexTicks - beforeSnapshot.StatusIndexTicks));
                _measurements.ProjectionPersistenceEnqueue.Add(Math.Max(0L, afterSnapshot.PersistenceEnqueueTicks - beforeSnapshot.PersistenceEnqueueTicks));
            }
        }
    }
}
