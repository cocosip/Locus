#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Globalization;
using System.IO;
using System.IO.Abstractions;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Locus.Benchmarks
{
    internal static class OrphanRebuildPeakMemoryCommand
    {
        private const string ScenarioName = "orphan-rebuild-peak";

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
                $"Running {ScenarioName} mode={options.Mode}, files={options.FileCount}, metadataRatio={options.MetadataRatio:F3}, cacheSize={options.LookupCacheSize}");

            if (options.FileCount < 100_000)
                Console.WriteLine("Warning: fileCount < 100000; this run is useful for smoke testing, not peak-memory proof.");

            var runResults = new List<ScenarioRunResult>();
            if (options.Mode == ScenarioMode.Baseline || options.Mode == ScenarioMode.Both)
                runResults.Add(await RunScenarioAsync(ScenarioMode.Baseline, options));

            if (options.Mode == ScenarioMode.Current || options.Mode == ScenarioMode.Both)
                runResults.Add(await RunScenarioAsync(ScenarioMode.Current, options));

            Console.WriteLine();
            Console.WriteLine("=== Peak Memory Results (GC Heap Size) ===");
            foreach (var result in runResults)
            {
                var peakText = result.PeakGcHeapDisplayValue.HasValue
                    ? $"{result.PeakGcHeapDisplayValue.Value:F3} {result.PeakGcHeapDisplayUnit}"
                    : "n/a";
                var peakBytesText = result.PeakGcHeapBytesEstimate.HasValue
                    ? $"{result.PeakGcHeapBytesEstimate.Value / 1024.0 / 1024.0:F2} MiB (estimated)"
                    : "n/a";

                Console.WriteLine(
                    $"{result.Mode,-8} elapsed={result.Elapsed.TotalSeconds,8:F2}s  samples={result.CounterSampleCount,4}  peak={peakText,-16}  peakBytes={peakBytesText,-22}  knownHits={result.KnownFileHits,8}  rebuilt={result.RebuiltCount,8}");
            }

            if (runResults.Count == 2)
            {
                var baseline = runResults[0];
                var current = runResults[1];
                if (baseline.PeakGcHeapBytesEstimate.HasValue
                    && current.PeakGcHeapBytesEstimate.HasValue
                    && baseline.PeakGcHeapBytesEstimate.Value > 0)
                {
                    var delta = (current.PeakGcHeapBytesEstimate.Value - baseline.PeakGcHeapBytesEstimate.Value)
                        / (double)baseline.PeakGcHeapBytesEstimate.Value;
                    Console.WriteLine($"Delta(current-vs-baseline)={delta:P2}");
                }
                else if (baseline.PeakGcHeapDisplayValue.HasValue
                    && current.PeakGcHeapDisplayValue.HasValue
                    && baseline.PeakGcHeapDisplayValue.Value > 0)
                {
                    var delta = (current.PeakGcHeapDisplayValue.Value - baseline.PeakGcHeapDisplayValue.Value)
                        / baseline.PeakGcHeapDisplayValue.Value;
                    Console.WriteLine($"Delta(current-vs-baseline)={delta:P2} (display units)");
                }
                else
                {
                    Console.WriteLine("Delta(current-vs-baseline)=n/a (counter samples unavailable)");
                }
            }

            Console.WriteLine();
            Console.WriteLine("Tip: for external validation, attach dotnet-counters to the printed PID during attach delay:");
            Console.WriteLine("  dotnet-counters monitor -p <pid> --counters System.Runtime[gc-heap-size]");

            return 0;
        }

        private static async Task<ScenarioRunResult> RunScenarioAsync(ScenarioMode mode, OrphanPeakOptions options)
        {
            using var context = CreateScenarioContext(mode, options);
            var dataset = SeedDataset(mode, context, options);

            if (options.AttachDelaySeconds > 0)
            {
                Console.WriteLine(
                    $"[{mode}] dataset ready. pid={Environment.ProcessId}. Attach tools now; waiting {options.AttachDelaySeconds}s...");
                await Task.Delay(TimeSpan.FromSeconds(options.AttachDelaySeconds));
            }

            ForceFullGc();

            using var heapSampler = new ManagedHeapSampler(TimeSpan.FromMilliseconds(200));
            using var listener = new RuntimeGcHeapCounterListener(options.CounterIntervalSeconds);
            var stopwatch = Stopwatch.StartNew();

            WorkloadResult workloadResult;
            switch (mode)
            {
                case ScenarioMode.Baseline:
                    workloadResult = await RunBaselineAsync(context);
                    break;
                case ScenarioMode.Current:
                    workloadResult = await RunCurrentAsync(context);
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported scenario mode: {mode}.");
            }

            if (options.PostRunDelaySeconds > 0)
                await Task.Delay(TimeSpan.FromSeconds(options.PostRunDelaySeconds));

            stopwatch.Stop();

            var counterSnapshot = listener.GetSnapshot();
            var samplerSnapshot = heapSampler.GetSnapshot();
            if (counterSnapshot.SampleCount == 0)
            {
                Console.WriteLine(
                    $"[{mode}] warning: no gc-heap-size counter sample captured; using in-process heap sampler result.");
            }

            var peakDisplayValue = counterSnapshot.PeakDisplayValue;
            var peakDisplayUnit = counterSnapshot.PeakDisplayUnit;
            var peakBytesEstimate = counterSnapshot.PeakBytesEstimate;
            var sampleCount = counterSnapshot.SampleCount;

            if (!peakBytesEstimate.HasValue || peakBytesEstimate.Value <= 0)
            {
                peakBytesEstimate = samplerSnapshot.PeakBytes;
                peakDisplayValue = samplerSnapshot.PeakBytes / 1024.0 / 1024.0;
                peakDisplayUnit = "MiB-poller";
                sampleCount = samplerSnapshot.SampleCount;
            }

            if (mode == ScenarioMode.Baseline && workloadResult.KnownFileHits != dataset.KnownCount)
            {
                throw new InvalidOperationException(
                    $"Baseline mismatch: expected known hits {dataset.KnownCount}, actual {workloadResult.KnownFileHits}.");
            }

            if (mode == ScenarioMode.Current && workloadResult.RebuiltCount != dataset.OrphanCount)
            {
                throw new InvalidOperationException(
                    $"Current mismatch: expected rebuilt {dataset.OrphanCount}, actual {workloadResult.RebuiltCount}.");
            }

            return new ScenarioRunResult(
                mode.ToString().ToLowerInvariant(),
                stopwatch.Elapsed,
                peakDisplayValue,
                peakDisplayUnit,
                peakBytesEstimate,
                sampleCount,
                workloadResult.KnownFileHits,
                workloadResult.RebuiltCount);
        }

        private static ScenarioContext CreateScenarioContext(ScenarioMode mode, OrphanPeakOptions options)
        {
            var fileSystem = new System.IO.Abstractions.FileSystem();
            var root = Path.Combine(Path.GetTempPath(), $"locus-orphan-peak-{mode}-{Guid.NewGuid():N}");
            var metadataDir = Path.Combine(root, "metadata");
            var quotaDir = Path.Combine(root, "quota");
            var volumeDir = Path.Combine(root, "volume");
            fileSystem.Directory.CreateDirectory(metadataDir);
            fileSystem.Directory.CreateDirectory(quotaDir);
            fileSystem.Directory.CreateDirectory(volumeDir);

            var metadataRepository = new MetadataRepository(
                fileSystem,
                NullLogger<MetadataRepository>.Instance,
                metadataDir,
                enableBackgroundPersistence: false);
            var quotaRepository = new DirectoryQuotaRepository(
                fileSystem,
                NullLogger<DirectoryQuotaRepository>.Instance,
                quotaDir);
            var tenantQuotaManager = new TenantQuotaManager(
                quotaRepository,
                NullLogger<TenantQuotaManager>.Instance);
            var cleanupService = new StorageCleanupService(
                metadataRepository,
                quotaRepository,
                tenantQuotaManager,
                fileSystem,
                NullLogger<StorageCleanupService>.Instance,
                metadataDir,
                quotaDir,
                new CleanupOptions
                {
                    MaxOrphanFilesPerRun = options.MaxOrphanFilesPerRun,
                    OrphanRebuildLookupCacheSize = options.LookupCacheSize
                });

            var volume = new Mock<IStorageVolume>();
            volume.Setup(v => v.VolumeId).Returns("vol-001");
            volume.Setup(v => v.MountPath).Returns(volumeDir);
            cleanupService.RegisterVolume(volume.Object);

            var tenantPath = Path.Combine(volumeDir, options.TenantId);
            fileSystem.Directory.CreateDirectory(tenantPath);

            return new ScenarioContext(
                fileSystem,
                metadataRepository,
                quotaRepository,
                cleanupService,
                root,
                tenantPath,
                options.TenantId,
                options.DirectoryShardCount);
        }

        private static DatasetCounts SeedDataset(ScenarioMode mode, ScenarioContext context, OrphanPeakOptions options)
        {
            var knownTargetCount = (int)Math.Round(options.FileCount * options.MetadataRatio, MidpointRounding.AwayFromZero);
            knownTargetCount = Math.Clamp(knownTargetCount, 0, options.FileCount);
            var orphanTargetCount = options.FileCount - knownTargetCount;

            Console.WriteLine(
                $"[{mode}] seeding tenant={context.TenantId}, known={knownTargetCount}, orphan={orphanTargetCount}, shardCount={context.DirectoryShardCount}");

            var now = DateTime.UtcNow;
            for (var i = 0; i < options.FileCount; i++)
            {
                var physicalPath = GetPhysicalPath(context, i);
                var directoryPath = context.FileSystem.Path.GetDirectoryName(physicalPath)!;
                context.FileSystem.Directory.CreateDirectory(directoryPath);
                context.FileSystem.File.WriteAllBytes(physicalPath, Array.Empty<byte>());

                if (i < knownTargetCount)
                {
                    context.MetadataRepository
                        .AddOrUpdateAsync(new FileMetadata
                        {
                            FileKey = $"known-{i:D8}",
                            TenantId = context.TenantId,
                            VolumeId = "vol-001",
                            PhysicalPath = physicalPath,
                            DirectoryPath = directoryPath,
                            FileSize = 0,
                            Status = FileProcessingStatus.Pending,
                            CreatedAt = now.AddTicks(i)
                        }, CancellationToken.None)
                        .GetAwaiter()
                        .GetResult();
                }

                if ((i + 1) % 10_000 == 0)
                    Console.WriteLine($"seed progress {i + 1}/{options.FileCount}");
            }

            return new DatasetCounts(knownTargetCount, orphanTargetCount);
        }

        private static async Task<WorkloadResult> RunBaselineAsync(ScenarioContext context)
        {
            var comparer = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal;
            var allMetadata = await context.MetadataRepository.GetByTenantAsync(context.TenantId, CancellationToken.None);
            var knownPaths = new HashSet<string>(comparer);
            foreach (var metadata in allMetadata)
            {
                var normalized = NormalizePath(context.FileSystem, metadata.PhysicalPath);
                if (normalized != null)
                    knownPaths.Add(normalized);
            }

            var knownHits = 0;
            foreach (var physicalPath in context.FileSystem.Directory.EnumerateFiles(context.TenantPath, "*", SearchOption.AllDirectories))
            {
                var normalized = NormalizePath(context.FileSystem, physicalPath);
                if (normalized != null && knownPaths.Contains(normalized))
                    knownHits++;
            }

            return new WorkloadResult(knownHits, rebuiltCount: 0);
        }

        private static async Task<WorkloadResult> RunCurrentAsync(ScenarioContext context)
        {
            var before = await context.CleanupService.GetCleanupStatisticsAsync(CancellationToken.None);
            await context.CleanupService.CleanupOrphanedFilesAsync(new BenchTenantContext(context.TenantId), CancellationToken.None);
            var after = await context.CleanupService.GetCleanupStatisticsAsync(CancellationToken.None);

            var allMetadata = await context.MetadataRepository.GetByTenantAsync(context.TenantId, CancellationToken.None);
            var knownHits = 0;
            foreach (var _ in allMetadata)
                knownHits++;

            return new WorkloadResult(
                knownHits,
                rebuiltCount: Math.Max(0, after.OrphanedFilesRemoved - before.OrphanedFilesRemoved));
        }

        private static string GetPhysicalPath(ScenarioContext context, int index)
        {
            var shard = index % context.DirectoryShardCount;
            var nested = (index / context.DirectoryShardCount) % context.DirectoryShardCount;
            return context.FileSystem.Path.Combine(
                context.TenantPath,
                $"shard-{shard:D4}",
                $"sub-{nested:D4}",
                $"file-{index:D8}.dat");
        }

        private static string? NormalizePath(IFileSystem fileSystem, string? path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return null;

            try
            {
                return fileSystem.Path.GetFullPath(path);
            }
            catch
            {
                return path;
            }
        }

        private static void ForceFullGc()
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        }

        private static bool TryParseOptions(string[] args, out OrphanPeakOptions options, out string? error)
        {
            options = new OrphanPeakOptions();
            error = null;

            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                if (string.Equals(arg, ScenarioName, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (string.Equals(arg, "--help", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(arg, "-h", StringComparison.OrdinalIgnoreCase))
                {
                    error = null;
                    return false;
                }

                if (string.Equals(arg, "--scenario", StringComparison.OrdinalIgnoreCase))
                {
                    i++;
                    if (i >= args.Length)
                    {
                        error = "--scenario requires a value.";
                        return false;
                    }

                    if (!string.Equals(args[i], ScenarioName, StringComparison.OrdinalIgnoreCase))
                    {
                        error = $"Unsupported scenario: {args[i]}.";
                        return false;
                    }

                    continue;
                }

                if (string.Equals(arg, "--mode", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadNext(args, ref i, out var value, out error))
                        return false;

                    if (!Enum.TryParse<ScenarioMode>(value, true, out var mode))
                    {
                        error = $"Invalid --mode value '{value}'. Use baseline/current/both.";
                        return false;
                    }

                    options.Mode = mode;
                    continue;
                }

                if (string.Equals(arg, "--file-count", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt32(args, ref i, out var fileCount, out error))
                        return false;

                    if (fileCount <= 0)
                    {
                        error = "--file-count must be > 0.";
                        return false;
                    }

                    options.FileCount = fileCount;
                    continue;
                }

                if (string.Equals(arg, "--metadata-ratio", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadDouble(args, ref i, out var metadataRatio, out error))
                        return false;

                    if (metadataRatio < 0 || metadataRatio > 1)
                    {
                        error = "--metadata-ratio must be between 0 and 1.";
                        return false;
                    }

                    options.MetadataRatio = metadataRatio;
                    continue;
                }

                if (string.Equals(arg, "--lookup-cache-size", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt32(args, ref i, out var lookupCacheSize, out error))
                        return false;

                    if (lookupCacheSize < 0)
                    {
                        error = "--lookup-cache-size must be >= 0.";
                        return false;
                    }

                    options.LookupCacheSize = lookupCacheSize;
                    continue;
                }

                if (string.Equals(arg, "--attach-delay-sec", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt32(args, ref i, out var attachDelaySeconds, out error))
                        return false;

                    if (attachDelaySeconds < 0)
                    {
                        error = "--attach-delay-sec must be >= 0.";
                        return false;
                    }

                    options.AttachDelaySeconds = attachDelaySeconds;
                    continue;
                }

                if (string.Equals(arg, "--counter-interval-sec", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt32(args, ref i, out var counterIntervalSeconds, out error))
                        return false;

                    if (counterIntervalSeconds <= 0)
                    {
                        error = "--counter-interval-sec must be > 0.";
                        return false;
                    }

                    options.CounterIntervalSeconds = counterIntervalSeconds;
                    continue;
                }

                if (string.Equals(arg, "--post-run-delay-sec", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt32(args, ref i, out var postRunDelaySeconds, out error))
                        return false;

                    if (postRunDelaySeconds < 0)
                    {
                        error = "--post-run-delay-sec must be >= 0.";
                        return false;
                    }

                    options.PostRunDelaySeconds = postRunDelaySeconds;
                    continue;
                }

                if (string.Equals(arg, "--max-orphan-files-per-run", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt32(args, ref i, out var maxOrphanFilesPerRun, out error))
                        return false;

                    if (maxOrphanFilesPerRun <= 0)
                    {
                        error = "--max-orphan-files-per-run must be > 0.";
                        return false;
                    }

                    options.MaxOrphanFilesPerRun = maxOrphanFilesPerRun;
                    continue;
                }

                if (string.Equals(arg, "--shard-count", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt32(args, ref i, out var directoryShardCount, out error))
                        return false;

                    if (directoryShardCount <= 0)
                    {
                        error = "--shard-count must be > 0.";
                        return false;
                    }

                    options.DirectoryShardCount = directoryShardCount;
                    continue;
                }

                if (string.Equals(arg, "--tenant-id", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadNext(args, ref i, out var tenantId, out error))
                        return false;

                    if (string.IsNullOrWhiteSpace(tenantId))
                    {
                        error = "--tenant-id cannot be empty.";
                        return false;
                    }

                    options.TenantId = tenantId;
                    continue;
                }

                error = $"Unknown argument: {arg}";
                return false;
            }

            return true;
        }

        private static bool TryReadInt32(string[] args, ref int index, out int value, out string? error)
        {
            value = 0;
            error = null;

            if (!TryReadNext(args, ref index, out var raw, out error))
                return false;

            if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
            {
                error = $"Invalid integer value: {raw}";
                return false;
            }

            return true;
        }

        private static bool TryReadDouble(string[] args, ref int index, out double value, out string? error)
        {
            value = 0;
            error = null;

            if (!TryReadNext(args, ref index, out var raw, out error))
                return false;

            if (!double.TryParse(raw, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out value))
            {
                error = $"Invalid double value: {raw}";
                return false;
            }

            return true;
        }

        private static bool TryReadNext(string[] args, ref int index, out string value, out string? error)
        {
            value = string.Empty;
            error = null;

            index++;
            if (index >= args.Length)
            {
                error = $"Missing value for argument '{args[index - 1]}'.";
                return false;
            }

            value = args[index];
            return true;
        }

        private static void PrintUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  dotnet run -c Release --project tests/Locus.Benchmarks -- --scenario orphan-rebuild-peak [options]");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --mode baseline|current|both        Scenario mode (default: both)");
            Console.WriteLine("  --file-count <int>                  Total physical files to seed (default: 120000)");
            Console.WriteLine("  --metadata-ratio <0..1>             Fraction of files with metadata (default: 0.90)");
            Console.WriteLine("  --lookup-cache-size <int>           Cleanup lookup cache size (default: 8192)");
            Console.WriteLine("  --max-orphan-files-per-run <int>    Cleanup scan budget (default: int.MaxValue)");
            Console.WriteLine("  --shard-count <int>                 Tenant directory shard count (default: 1024)");
            Console.WriteLine("  --attach-delay-sec <int>            Delay before workload, for tool attach (default: 10)");
            Console.WriteLine("  --counter-interval-sec <int>        EventCounter sampling interval (default: 1)");
            Console.WriteLine("  --post-run-delay-sec <int>          Keep process alive after run for sampling (default: 2)");
            Console.WriteLine("  --tenant-id <string>                Tenant id used in the run (default: peak-orphan-memory)");
        }

        private sealed class ScenarioContext : IDisposable
        {
            public ScenarioContext(
                IFileSystem fileSystem,
                MetadataRepository metadataRepository,
                DirectoryQuotaRepository quotaRepository,
                StorageCleanupService cleanupService,
                string rootDirectory,
                string tenantPath,
                string tenantId,
                int directoryShardCount)
            {
                FileSystem = fileSystem;
                MetadataRepository = metadataRepository;
                QuotaRepository = quotaRepository;
                CleanupService = cleanupService;
                RootDirectory = rootDirectory;
                TenantPath = tenantPath;
                TenantId = tenantId;
                DirectoryShardCount = directoryShardCount;
            }

            public IFileSystem FileSystem { get; }

            public MetadataRepository MetadataRepository { get; }

            public DirectoryQuotaRepository QuotaRepository { get; }

            public StorageCleanupService CleanupService { get; }

            public string RootDirectory { get; }

            public string TenantPath { get; }

            public string TenantId { get; }

            public int DirectoryShardCount { get; }

            public void Dispose()
            {
                MetadataRepository.Dispose();
                QuotaRepository.Dispose();

                try
                {
                    if (FileSystem.Directory.Exists(RootDirectory))
                        FileSystem.Directory.Delete(RootDirectory, recursive: true);
                }
                catch
                {
                    // Ignore cleanup errors in benchmark helper.
                }
            }
        }

        private sealed class RuntimeGcHeapCounterListener : EventListener
        {
            private readonly string _intervalSeconds;
            private readonly object _gate = new object();
            private double _peakDisplayValue;
            private string _peakDisplayUnit = "counter-units";
            private long? _peakBytesEstimate;
            private int _sampleCount;

            public RuntimeGcHeapCounterListener(int intervalSeconds)
            {
                _intervalSeconds = intervalSeconds.ToString(CultureInfo.InvariantCulture);
            }

            public CounterSnapshot GetSnapshot()
            {
                lock (_gate)
                {
                    return new CounterSnapshot(
                        _sampleCount > 0 ? _peakDisplayValue : (double?)null,
                        _peakDisplayUnit,
                        _peakBytesEstimate,
                        _sampleCount);
                }
            }

            protected override void OnEventSourceCreated(EventSource eventSource)
            {
                if (!string.Equals(eventSource.Name, "System.Runtime", StringComparison.Ordinal))
                    return;

                EnableEvents(
                    eventSource,
                    EventLevel.Informational,
                    EventKeywords.None,
                    new Dictionary<string, string?>
                    {
                        ["EventCounterIntervalSec"] = _intervalSeconds
                    });
            }

            protected override void OnEventWritten(EventWrittenEventArgs eventData)
            {
                if (!string.Equals(eventData.EventName, "EventCounters", StringComparison.Ordinal))
                    return;

                if (eventData.Payload == null || eventData.Payload.Count == 0)
                    return;

                if (eventData.Payload[0] is not IDictionary<string, object> payload)
                    return;

                if (!TryGetString(payload, "Name", out var counterName))
                    return;

                if (!string.Equals(counterName, "gc-heap-size", StringComparison.OrdinalIgnoreCase)
                    && !string.Equals(counterName, "gc-heap-size-bytes", StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }

                if (!TryGetDouble(payload, "Mean", out var value)
                    && !TryGetDouble(payload, "Increment", out value))
                {
                    return;
                }

                var displayUnit = "counter-units";
                if (TryGetString(payload, "DisplayUnits", out var parsedUnit))
                    displayUnit = parsedUnit;

                long? bytesEstimate = null;
                if (string.Equals(counterName, "gc-heap-size-bytes", StringComparison.OrdinalIgnoreCase))
                {
                    bytesEstimate = (long)value;
                    displayUnit = "bytes";
                }
                else
                {
                    bytesEstimate = ConvertDisplayValueToBytes(value, displayUnit);
                }

                lock (_gate)
                {
                    _sampleCount++;
                    if (_sampleCount == 1 || value > _peakDisplayValue)
                    {
                        _peakDisplayValue = value;
                        _peakDisplayUnit = displayUnit;
                        _peakBytesEstimate = bytesEstimate;
                    }
                }
            }

            private static long? ConvertDisplayValueToBytes(double value, string? displayUnit)
            {
                if (double.IsNaN(value) || double.IsInfinity(value) || value < 0)
                    return null;

                if (string.IsNullOrWhiteSpace(displayUnit))
                    return null;

                switch (displayUnit.Trim().ToUpperInvariant())
                {
                    case "B":
                    case "BYTE":
                    case "BYTES":
                        return (long)value;
                    case "KB":
                    case "KIB":
                        return (long)(value * 1024.0);
                    case "MB":
                    case "MIB":
                        return (long)(value * 1024.0 * 1024.0);
                    case "GB":
                    case "GIB":
                        return (long)(value * 1024.0 * 1024.0 * 1024.0);
                    default:
                        return null;
                }
            }

            private static bool TryGetString(IDictionary<string, object> payload, string key, out string value)
            {
                value = string.Empty;
                if (!payload.TryGetValue(key, out var boxed) || boxed == null)
                    return false;

                value = Convert.ToString(boxed, CultureInfo.InvariantCulture) ?? string.Empty;
                return !string.IsNullOrWhiteSpace(value);
            }

            private static bool TryGetDouble(IDictionary<string, object> payload, string key, out double value)
            {
                value = 0;
                if (!payload.TryGetValue(key, out var boxed) || boxed == null)
                    return false;

                switch (boxed)
                {
                    case double d:
                        value = d;
                        return true;
                    case float f:
                        value = f;
                        return true;
                    case int i:
                        value = i;
                        return true;
                    case long l:
                        value = l;
                        return true;
                    case decimal m:
                        value = (double)m;
                        return true;
                    default:
                        var text = Convert.ToString(boxed, CultureInfo.InvariantCulture);
                        if (!string.IsNullOrWhiteSpace(text)
                            && double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out value))
                        {
                            return true;
                        }

                        return false;
                }
            }
        }

        private sealed class ManagedHeapSampler : IDisposable
        {
            private readonly CancellationTokenSource _cts = new CancellationTokenSource();
            private readonly Task _samplingTask;
            private long _peakBytes;
            private int _sampleCount;

            public ManagedHeapSampler(TimeSpan samplingInterval)
            {
                if (samplingInterval <= TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException(nameof(samplingInterval));

                _samplingTask = Task.Run(async () =>
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        SampleOnce();
                        try
                        {
                            await Task.Delay(samplingInterval, _cts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                    }
                });
            }

            public HeapSamplerSnapshot GetSnapshot()
            {
                return new HeapSamplerSnapshot(
                    peakBytes: Volatile.Read(ref _peakBytes),
                    sampleCount: Volatile.Read(ref _sampleCount));
            }

            public void Dispose()
            {
                _cts.Cancel();
                try
                {
                    _samplingTask.GetAwaiter().GetResult();
                }
                catch (OperationCanceledException)
                {
                    // Expected on shutdown.
                }
                finally
                {
                    _cts.Dispose();
                }
            }

            private void SampleOnce()
            {
                var info = GC.GetGCMemoryInfo();
                var heapSize = info.HeapSizeBytes;
                var liveSize = GC.GetTotalMemory(forceFullCollection: false);
                var sample = Math.Max(heapSize, liveSize);

                Interlocked.Increment(ref _sampleCount);
                UpdatePeak(sample);
            }

            private void UpdatePeak(long sample)
            {
                while (true)
                {
                    var observed = Volatile.Read(ref _peakBytes);
                    if (sample <= observed)
                        return;

                    if (Interlocked.CompareExchange(ref _peakBytes, sample, observed) == observed)
                        return;
                }
            }
        }

        private sealed class BenchTenantContext : ITenantContext
        {
            public BenchTenantContext(string tenantId)
            {
                TenantId = tenantId;
            }

            public string TenantId { get; }

            public TenantStatus Status => TenantStatus.Enabled;
        }

        private sealed class OrphanPeakOptions
        {
            public ScenarioMode Mode { get; set; } = ScenarioMode.Both;

            public int FileCount { get; set; } = 120_000;

            public double MetadataRatio { get; set; } = 0.90;

            public int LookupCacheSize { get; set; } = 8_192;

            public int AttachDelaySeconds { get; set; } = 10;

            public int CounterIntervalSeconds { get; set; } = 1;

            public int PostRunDelaySeconds { get; set; } = 2;

            public int MaxOrphanFilesPerRun { get; set; } = int.MaxValue;

            public int DirectoryShardCount { get; set; } = 1_024;

            public string TenantId { get; set; } = "peak-orphan-memory";
        }

        private readonly struct WorkloadResult
        {
            public WorkloadResult(int knownFileHits, int rebuiltCount)
            {
                KnownFileHits = knownFileHits;
                RebuiltCount = rebuiltCount;
            }

            public int KnownFileHits { get; }

            public int RebuiltCount { get; }
        }

        private readonly struct CounterSnapshot
        {
            public CounterSnapshot(double? peakDisplayValue, string peakDisplayUnit, long? peakBytesEstimate, int sampleCount)
            {
                PeakDisplayValue = peakDisplayValue;
                PeakDisplayUnit = peakDisplayUnit;
                PeakBytesEstimate = peakBytesEstimate;
                SampleCount = sampleCount;
            }

            public double? PeakDisplayValue { get; }

            public string PeakDisplayUnit { get; }

            public long? PeakBytesEstimate { get; }

            public int SampleCount { get; }
        }

        private readonly struct HeapSamplerSnapshot
        {
            public HeapSamplerSnapshot(long peakBytes, int sampleCount)
            {
                PeakBytes = peakBytes;
                SampleCount = sampleCount;
            }

            public long PeakBytes { get; }

            public int SampleCount { get; }
        }

        private readonly struct ScenarioRunResult
        {
            public ScenarioRunResult(
                string mode,
                TimeSpan elapsed,
                double? peakGcHeapDisplayValue,
                string peakGcHeapDisplayUnit,
                long? peakGcHeapBytesEstimate,
                int counterSampleCount,
                int knownFileHits,
                int rebuiltCount)
            {
                Mode = mode;
                Elapsed = elapsed;
                PeakGcHeapDisplayValue = peakGcHeapDisplayValue;
                PeakGcHeapDisplayUnit = peakGcHeapDisplayUnit;
                PeakGcHeapBytesEstimate = peakGcHeapBytesEstimate;
                CounterSampleCount = counterSampleCount;
                KnownFileHits = knownFileHits;
                RebuiltCount = rebuiltCount;
            }

            public string Mode { get; }

            public TimeSpan Elapsed { get; }

            public double? PeakGcHeapDisplayValue { get; }

            public string PeakGcHeapDisplayUnit { get; }

            public long? PeakGcHeapBytesEstimate { get; }

            public int CounterSampleCount { get; }

            public int KnownFileHits { get; }

            public int RebuiltCount { get; }
        }

        private readonly struct DatasetCounts
        {
            public DatasetCounts(int knownCount, int orphanCount)
            {
                KnownCount = knownCount;
                OrphanCount = orphanCount;
            }

            public int KnownCount { get; }

            public int OrphanCount { get; }
        }

        internal enum ScenarioMode
        {
            Baseline,
            Current,
            Both
        }
    }
}
