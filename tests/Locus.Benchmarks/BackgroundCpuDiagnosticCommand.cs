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
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    internal static class BackgroundCpuDiagnosticCommand
    {
        private const string ScenarioName = "background-cpu-diagnostic";
        private const string VolumeId = "vol-001";

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
                    return true;
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
                $"Running {ScenarioName}: tenants={options.TenantCount}, journalRecordsPerTenant={options.JournalRecordsPerTenant}, orphanFilesPerTenant={options.OrphanFilesPerTenant}, junkFilesPerTenant={options.JunkFilesPerTenant}, projectionSeconds={options.ProjectionRunSeconds}");

            using var context = await CreateContextAsync(options).ConfigureAwait(false);
            var seed = await SeedAsync(context, options).ConfigureAwait(false);

            Console.WriteLine(
                $"Seeded: journalRecords={seed.JournalRecords}, orphanFiles={seed.OrphanFiles}, junkFiles={seed.JunkFiles}, root={context.RootDirectory}");

            var beforeCleanup = await context.CleanupService.GetCleanupStatisticsAsync(CancellationToken.None).ConfigureAwait(false);
            var projectionResult = await RunProjectionAsync(context, options).ConfigureAwait(false);

            var junk = await MeasureAsync(
                "junk-sweep",
                () => context.CleanupService.CleanupAllEmptyDirectoriesAsync(CancellationToken.None)).ConfigureAwait(false);

            var recovery = await MeasureAsync(
                "orphan-recovery",
                () => context.CleanupService.RecoverAllOrphanedFilesAsync(CancellationToken.None)).ConfigureAwait(false);

            var reentry = await MeasureReentrySkipAsync(context).ConfigureAwait(false);
            var afterCleanup = await context.CleanupService.GetCleanupStatisticsAsync(CancellationToken.None).ConfigureAwait(false);

            Console.WriteLine();
            Console.WriteLine("=== Background CPU Diagnostic ===");
            PrintMeasurement("Projection", projectionResult.Elapsed, projectionResult.Cpu, projectionResult.CpuRatio);
            Console.WriteLine($"  Projected tenants: {projectionResult.ProjectedTenants}/{options.TenantCount}");
            Console.WriteLine($"  Projected records: {projectionResult.ProjectedFiles}/{seed.JournalRecords}");
            Console.WriteLine($"  Max lag bytes:     {projectionResult.MaxLagBytes}");
            PrintMeasurement("Junk sweep", junk.Elapsed, junk.Cpu, junk.CpuRatio);
            PrintMeasurement("Orphan recovery", recovery.Elapsed, recovery.Cpu, recovery.CpuRatio);
            PrintMeasurement("Reentry skip", reentry.Elapsed, reentry.Cpu, reentry.CpuRatio);
            Console.WriteLine($"  Junk removed:      {afterCleanup.EmptyDirectoriesRemoved - beforeCleanup.EmptyDirectoriesRemoved}");
            Console.WriteLine($"  Orphans recovered: {afterCleanup.OrphanedFilesRecovered - beforeCleanup.OrphanedFilesRecovered}");

            if (!options.KeepDirectory)
                Console.WriteLine("Temporary diagnostic directory removed.");
            else
                Console.WriteLine($"Temporary diagnostic directory kept: {context.RootDirectory}");

            return 0;
        }

        private static async Task<DiagnosticContext> CreateContextAsync(Options options)
        {
            var fileSystem = new System.IO.Abstractions.FileSystem();
            var root = string.IsNullOrWhiteSpace(options.RootDirectory)
                ? Path.Combine(Path.GetTempPath(), "locus-background-cpu-" + Guid.NewGuid().ToString("N"))
                : Path.GetFullPath(options.RootDirectory);
            var metadataDir = Path.Combine(root, "metadata");
            var quotaDir = Path.Combine(root, "quota");
            var queueDir = Path.Combine(root, "queue");
            var volumeDir = Path.Combine(root, "volume");

            fileSystem.Directory.CreateDirectory(metadataDir);
            fileSystem.Directory.CreateDirectory(quotaDir);
            fileSystem.Directory.CreateDirectory(queueDir);
            fileSystem.Directory.CreateDirectory(volumeDir);

            var metadataRepository = new MetadataRepository(
                fileSystem,
                NullLogger<MetadataRepository>.Instance,
                metadataDir,
                enableBackgroundPersistence: false);
            var quotaRepository = new DirectoryQuotaRepository(
                fileSystem,
                NullLogger<DirectoryQuotaRepository>.Instance,
                quotaDir,
                enableBackgroundFlush: false);
            var tenantQuotaManager = new TenantQuotaManager(
                quotaRepository,
                NullLogger<TenantQuotaManager>.Instance);
            var directoryQuotaManager = new DirectoryQuotaManager(
                quotaRepository,
                NullLogger<DirectoryQuotaManager>.Instance);
            var quotaMaintenanceStore = new DirectoryQuotaRepositoryProjectionMaintenanceStore(quotaRepository);

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
                    OrphanRebuildLookupCacheSize = options.OrphanLookupCacheSize
                },
                directoryQuotaManager: directoryQuotaManager,
                allowLegacyNonJournalMode: true);
            var volume = new DiagnosticStorageVolume(volumeDir);
            cleanupService.RegisterVolume(volume);

            var journalOptions = new QueueEventJournalOptions
            {
                QueueDirectory = queueDir,
                JournalFormat = JournalFormat.BinaryV1,
                AckMode = QueueEventJournalAckMode.Durable,
                EnableProjection = true,
                MaxRecordsPerTenantPerCycle = options.MaxRecordsPerTenantPerCycle,
                MaxTenantsPerCycle = options.MaxTenantsPerCycle,
                BusyCycleDelay = TimeSpan.FromMilliseconds(options.BusyDelayMs),
                IdleCycleDelay = TimeSpan.FromMilliseconds(options.IdleDelayMs),
                MaxProjectionTimePerCycle = TimeSpan.FromMilliseconds(options.MaxProjectionMs),
                EnableAutomaticSnapshots = false,
                EnableCompaction = false
            };
            var journal = new FileQueueEventJournal(
                fileSystem,
                NullLogger<FileQueueEventJournal>.Instance,
                journalOptions);
            var projectionService = new QueueEventProjectionService(
                journal,
                metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                fileSystem,
                journalOptions,
                NullLogger<QueueEventProjectionService>.Instance,
                cleanupService,
                quotaMaintenanceStore: quotaMaintenanceStore);

            await Task.CompletedTask.ConfigureAwait(false);
            return new DiagnosticContext(
                fileSystem,
                root,
                volumeDir,
                metadataRepository,
                quotaRepository,
                journal,
                cleanupService,
                projectionService,
                options.KeepDirectory);
        }

        private static async Task<SeedResult> SeedAsync(DiagnosticContext context, Options options)
        {
            var buffer = new byte[Math.Max(1, options.FileSizeBytes)];
            new Random(17).NextBytes(buffer);
            var journalRecords = 0;
            var orphanFiles = 0;
            var junkFiles = 0;

            for (var tenantIndex = 0; tenantIndex < options.TenantCount; tenantIndex++)
            {
                var tenantId = GetTenantId(tenantIndex);
                var tenantRoot = Path.Combine(context.VolumeDirectory, tenantId);
                context.FileSystem.Directory.CreateDirectory(tenantRoot);

                for (var recordIndex = 0; recordIndex < options.JournalRecordsPerTenant; recordIndex++)
                {
                    var fileKey = $"journal-{tenantIndex:D4}-{recordIndex:D8}";
                    var physicalPath = Path.Combine(tenantRoot, "journal", fileKey + ".dat");
                    context.FileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
                    await context.FileSystem.File.WriteAllBytesAsync(physicalPath, buffer).ConfigureAwait(false);
                    await context.Journal.AppendAsync(new QueueEventRecord
                    {
                        TenantId = tenantId,
                        FileKey = fileKey,
                        EventType = QueueEventType.Accepted,
                        OccurredAtUtc = DateTime.UtcNow,
                        VolumeId = VolumeId,
                        PhysicalPath = physicalPath,
                        DirectoryPath = "/journal",
                        FileSize = buffer.Length,
                        Status = FileProcessingStatus.Pending,
                        RetryCount = 0,
                        OriginalFileName = fileKey + ".dat",
                        FileExtension = ".dat"
                    }, CancellationToken.None).ConfigureAwait(false);
                    journalRecords++;
                }

                for (var orphanIndex = 0; orphanIndex < options.OrphanFilesPerTenant; orphanIndex++)
                {
                    var fileKey = $"orphan-{tenantIndex:D4}-{orphanIndex:D8}";
                    var physicalPath = Path.Combine(tenantRoot, "orphan", fileKey + ".dat");
                    context.FileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
                    await context.FileSystem.File.WriteAllBytesAsync(physicalPath, buffer).ConfigureAwait(false);
                    orphanFiles++;
                }

                for (var junkIndex = 0; junkIndex < options.JunkFilesPerTenant; junkIndex++)
                {
                    var junkDir = Path.Combine(tenantRoot, "junk", junkIndex.ToString("D4", CultureInfo.InvariantCulture));
                    context.FileSystem.Directory.CreateDirectory(junkDir);
                    await context.FileSystem.File.WriteAllTextAsync(Path.Combine(junkDir, ".DS_Store"), "junk").ConfigureAwait(false);
                    junkFiles++;
                }
            }

            return new SeedResult(journalRecords, orphanFiles, junkFiles);
        }

        private static async Task<ProjectionResult> RunProjectionAsync(DiagnosticContext context, Options options)
        {
            var measurement = await MeasureAsync("projection", async () =>
            {
                await context.ProjectionService.StartAsync(CancellationToken.None).ConfigureAwait(false);
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(options.ProjectionRunSeconds)).ConfigureAwait(false);
                }
                finally
                {
                    await context.ProjectionService.StopAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }).ConfigureAwait(false);

            var projectedTenants = 0;
            var projectedFiles = 0;
            long maxLagBytes = 0;
            var projectedJournalFiles = (await context.MetadataRepository
                    .GetAllAsync(CancellationToken.None)
                    .ConfigureAwait(false))
                .Where(m => m.FileKey.StartsWith("journal-", StringComparison.Ordinal))
                .GroupBy(m => m.TenantId, StringComparer.Ordinal)
                .ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);

            for (var tenantIndex = 0; tenantIndex < options.TenantCount; tenantIndex++)
            {
                var tenantId = GetTenantId(tenantIndex);
                var state = await context.ProjectionService
                    .GetTenantStateAsync(tenantId, CancellationToken.None)
                    .ConfigureAwait(false);
                maxLagBytes = Math.Max(maxLagBytes, state.LagBytes);

                projectedJournalFiles.TryGetValue(tenantId, out var tenantFiles);
                if (tenantFiles > 0)
                    projectedTenants++;

                projectedFiles += tenantFiles;
            }

            return new ProjectionResult(
                measurement.Elapsed,
                measurement.Cpu,
                measurement.CpuRatio,
                projectedTenants,
                projectedFiles,
                maxLagBytes);
        }

        private static async Task<Measurement> MeasureReentrySkipAsync(DiagnosticContext context)
        {
            var gateField = typeof(StorageCleanupService).GetField(
                "_volumeScanGate",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            var gate = (SemaphoreSlim?)gateField?.GetValue(context.CleanupService)
                ?? throw new InvalidOperationException("Could not find StorageCleanupService volume scan gate.");

            await gate.WaitAsync().ConfigureAwait(false);
            try
            {
                return await MeasureAsync(
                    "reentry-skip",
                    () => context.CleanupService.RecoverAllOrphanedFilesAsync(CancellationToken.None)).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
        }

        private static async Task<Measurement> MeasureAsync(string name, Func<Task> action)
        {
            var process = Process.GetCurrentProcess();
            process.Refresh();
            var cpuBefore = process.TotalProcessorTime;
            var stopwatch = Stopwatch.StartNew();

            await action().ConfigureAwait(false);

            stopwatch.Stop();
            process.Refresh();
            var cpu = process.TotalProcessorTime - cpuBefore;
            var ratio = stopwatch.Elapsed.TotalMilliseconds <= 0
                ? 0
                : cpu.TotalMilliseconds / (stopwatch.Elapsed.TotalMilliseconds * Environment.ProcessorCount);
            return new Measurement(name, stopwatch.Elapsed, cpu, ratio);
        }

        private static void PrintMeasurement(string label, TimeSpan elapsed, TimeSpan cpu, double cpuRatio)
        {
            Console.WriteLine(
                $"  {label,-16} elapsed={elapsed.TotalSeconds,8:F3}s cpu={cpu.TotalSeconds,8:F3}s processCpu={cpuRatio:P1}");
        }

        private static string GetTenantId(int index)
        {
            return "tenant-" + index.ToString("D4", CultureInfo.InvariantCulture);
        }

        private static bool TryParseOptions(string[] args, out Options options, out string? error)
        {
            options = new Options();
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

                if (!TryReadValue(args, ref i, out var optionName, out var value))
                {
                    error = $"Missing value for argument '{arg}'.";
                    return false;
                }

                switch (optionName.ToLowerInvariant())
                {
                    case "--tenants":
                        if (!TryReadPositiveInt(value, out options.TenantCount))
                            return Fail("--tenants must be a positive integer.", out error);
                        break;
                    case "--records-per-tenant":
                        if (!TryReadNonNegativeInt(value, out options.JournalRecordsPerTenant))
                            return Fail("--records-per-tenant must be a non-negative integer.", out error);
                        break;
                    case "--orphans-per-tenant":
                        if (!TryReadNonNegativeInt(value, out options.OrphanFilesPerTenant))
                            return Fail("--orphans-per-tenant must be a non-negative integer.", out error);
                        break;
                    case "--junk-per-tenant":
                        if (!TryReadNonNegativeInt(value, out options.JunkFilesPerTenant))
                            return Fail("--junk-per-tenant must be a non-negative integer.", out error);
                        break;
                    case "--projection-seconds":
                        if (!TryReadPositiveInt(value, out options.ProjectionRunSeconds))
                            return Fail("--projection-seconds must be a positive integer.", out error);
                        break;
                    case "--max-tenants-per-cycle":
                        if (!TryReadPositiveInt(value, out options.MaxTenantsPerCycle))
                            return Fail("--max-tenants-per-cycle must be a positive integer.", out error);
                        break;
                    case "--max-records-per-tenant":
                        if (!TryReadPositiveInt(value, out options.MaxRecordsPerTenantPerCycle))
                            return Fail("--max-records-per-tenant must be a positive integer.", out error);
                        break;
                    case "--busy-delay-ms":
                        if (!TryReadNonNegativeInt(value, out options.BusyDelayMs))
                            return Fail("--busy-delay-ms must be a non-negative integer.", out error);
                        break;
                    case "--idle-delay-ms":
                        if (!TryReadNonNegativeInt(value, out options.IdleDelayMs))
                            return Fail("--idle-delay-ms must be a non-negative integer.", out error);
                        break;
                    case "--max-projection-ms":
                        if (!TryReadPositiveInt(value, out options.MaxProjectionMs))
                            return Fail("--max-projection-ms must be a positive integer.", out error);
                        break;
                    case "--max-orphans-per-run":
                        if (!TryReadPositiveInt(value, out options.MaxOrphanFilesPerRun))
                            return Fail("--max-orphans-per-run must be a positive integer.", out error);
                        break;
                    case "--orphan-cache":
                        if (!TryReadNonNegativeInt(value, out options.OrphanLookupCacheSize))
                            return Fail("--orphan-cache must be a non-negative integer.", out error);
                        break;
                    case "--file-size":
                        if (!TryReadPositiveInt(value, out options.FileSizeBytes))
                            return Fail("--file-size must be a positive integer.", out error);
                        break;
                    case "--root":
                        options.RootDirectory = value;
                        break;
                    case "--keep":
                        if (!bool.TryParse(value, out options.KeepDirectory))
                            return Fail("--keep must be true or false.", out error);
                        break;
                    default:
                        error = $"Unknown argument '{optionName}'.";
                        return false;
                }
            }

            return true;
        }

        private static bool TryReadValue(string[] args, ref int index, out string optionName, out string value)
        {
            var arg = args[index];
            var equalsIndex = arg.IndexOf('=');
            if (equalsIndex >= 0)
            {
                optionName = arg.Substring(0, equalsIndex);
                value = arg.Substring(equalsIndex + 1);
                return true;
            }

            optionName = arg;
            if (string.Equals(arg, "--keep", StringComparison.OrdinalIgnoreCase))
            {
                value = "true";
                return true;
            }

            if (index + 1 >= args.Length)
            {
                value = string.Empty;
                return false;
            }

            value = args[++index];
            return true;
        }

        private static bool TryReadPositiveInt(string value, out int result)
        {
            return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out result) && result > 0;
        }

        private static bool TryReadNonNegativeInt(string value, out int result)
        {
            return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out result) && result >= 0;
        }

        private static bool Fail(string message, out string? error)
        {
            error = message;
            return false;
        }

        private static void PrintUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  dotnet run -c Release --project tests/Locus.Benchmarks -- background-cpu-diagnostic [options]");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --tenants <count>                 Tenants to seed. Default: 24");
            Console.WriteLine("  --records-per-tenant <count>      Accepted journal records per tenant. Default: 64");
            Console.WriteLine("  --orphans-per-tenant <count>      Physical orphan files per tenant. Default: 16");
            Console.WriteLine("  --junk-per-tenant <count>         Junk files per tenant. Default: 8");
            Console.WriteLine("  --projection-seconds <seconds>    How long to run projector. Default: 8");
            Console.WriteLine("  --max-tenants-per-cycle <count>   Projector tenant batch cap. Default: 4");
            Console.WriteLine("  --max-records-per-tenant <count>  Projector per-tenant record cap. Default: 32");
            Console.WriteLine("  --busy-delay-ms <ms>              Projector busy delay. Default: 100");
            Console.WriteLine("  --idle-delay-ms <ms>              Projector idle delay. Default: 300");
            Console.WriteLine("  --max-projection-ms <ms>          Per-cycle projection time budget. Default: 1000");
            Console.WriteLine("  --max-orphans-per-run <count>     Orphan scan budget. Default: 5000");
            Console.WriteLine("  --orphan-cache <count>            Orphan lookup cache size. Default: 32768");
            Console.WriteLine("  --file-size <bytes>               Seed file size. Default: 128");
            Console.WriteLine("  --root <path>                     Temporary root directory. Default: OS temp");
            Console.WriteLine("  --keep[=true|false]               Keep temporary root after run. Default: false");
        }

        private sealed class Options
        {
            public int TenantCount = 24;
            public int JournalRecordsPerTenant = 64;
            public int OrphanFilesPerTenant = 16;
            public int JunkFilesPerTenant = 8;
            public int ProjectionRunSeconds = 8;
            public int MaxTenantsPerCycle = 4;
            public int MaxRecordsPerTenantPerCycle = 32;
            public int BusyDelayMs = 100;
            public int IdleDelayMs = 300;
            public int MaxProjectionMs = 1000;
            public int MaxOrphanFilesPerRun = 5000;
            public int OrphanLookupCacheSize = 32768;
            public int FileSizeBytes = 128;
            public string? RootDirectory;
            public bool KeepDirectory;
        }

        private sealed class DiagnosticContext : IDisposable
        {
            private readonly bool _keepDirectory;

            public DiagnosticContext(
                IFileSystem fileSystem,
                string rootDirectory,
                string volumeDirectory,
                MetadataRepository metadataRepository,
                DirectoryQuotaRepository quotaRepository,
                FileQueueEventJournal journal,
                StorageCleanupService cleanupService,
                QueueEventProjectionService projectionService,
                bool keepDirectory)
            {
                FileSystem = fileSystem;
                RootDirectory = rootDirectory;
                VolumeDirectory = volumeDirectory;
                MetadataRepository = metadataRepository;
                QuotaRepository = quotaRepository;
                Journal = journal;
                CleanupService = cleanupService;
                ProjectionService = projectionService;
                _keepDirectory = keepDirectory;
            }

            public IFileSystem FileSystem { get; }
            public string RootDirectory { get; }
            public string VolumeDirectory { get; }
            public MetadataRepository MetadataRepository { get; }
            public DirectoryQuotaRepository QuotaRepository { get; }
            public FileQueueEventJournal Journal { get; }
            public StorageCleanupService CleanupService { get; }
            public QueueEventProjectionService ProjectionService { get; }

            public void Dispose()
            {
                ProjectionService.Dispose();
                Journal.Dispose();
                MetadataRepository.Dispose();
                QuotaRepository.Dispose();

                if (_keepDirectory)
                    return;

                try
                {
                    Directory.Delete(RootDirectory, recursive: true);
                }
                catch
                {
                    // Ignore temporary diagnostic cleanup failures.
                }
            }
        }

        private sealed class DiagnosticStorageVolume : IStorageVolume
        {
            public DiagnosticStorageVolume(string mountPath)
            {
                MountPath = mountPath;
            }

            public string VolumeId => BackgroundCpuDiagnosticCommand.VolumeId;
            public string MountPath { get; }
            public long TotalCapacity => long.MaxValue;
            public long AvailableSpace => long.MaxValue;
            public bool IsHealthy => true;
            public int ShardingDepth => 0;

            public Task<Stream> ReadAsync(string path, CancellationToken ct = default)
            {
                return Task.FromResult<Stream>(File.OpenRead(path));
            }

            public async Task WriteAsync(string path, Stream content, CancellationToken ct = default)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                using var stream = File.Create(path);
                await content.CopyToAsync(stream, 81920, ct).ConfigureAwait(false);
            }

            public Task DeleteAsync(string path, CancellationToken ct = default)
            {
                if (File.Exists(path))
                    File.Delete(path);

                return Task.CompletedTask;
            }

            public string BuildPhysicalPath(string tenantId, string fileKey, string? fileExtension)
            {
                return Path.Combine(MountPath, tenantId, fileKey + fileExtension);
            }
        }

        private sealed record SeedResult(int JournalRecords, int OrphanFiles, int JunkFiles);
        private sealed record Measurement(string Name, TimeSpan Elapsed, TimeSpan Cpu, double CpuRatio);
        private sealed record ProjectionResult(
            TimeSpan Elapsed,
            TimeSpan Cpu,
            double CpuRatio,
            int ProjectedTenants,
            int ProjectedFiles,
            long MaxLagBytes);
    }
}
