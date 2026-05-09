#nullable enable
using System;
using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Abstractions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    internal static class MetadataColdStartMemoryCommand
    {
        private const string ScenarioName = "metadata-cold-start-memory";
        private const string DefaultTenantId = "memory-tenant";

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

            var fileSystem = new System.IO.Abstractions.FileSystem();
            var root = string.IsNullOrWhiteSpace(options.RootDirectory)
                ? Path.Combine(Path.GetTempPath(), $"locus-metadata-cold-start-{Guid.NewGuid():N}")
                : Path.GetFullPath(options.RootDirectory!);
            var metadataDirectory = Path.Combine(root, "metadata");

            Console.WriteLine(
                $"Running {ScenarioName}: cold={options.ColdCount}, hot={options.HotCount}, batchSize={options.StartupLoadBatchSize}, tenant={options.TenantId}");
            Console.WriteLine($"Root={root}");

            try
            {
                if (Directory.Exists(root) && !options.KeepDirectory)
                    Directory.Delete(root, recursive: true);

                Directory.CreateDirectory(metadataDirectory);
                InitializeTenantDatabase(fileSystem, metadataDirectory, options);

                var dbPath = Path.Combine(metadataDirectory, options.TenantId, "metadata.db");
                var seedStopwatch = Stopwatch.StartNew();
                SeedRows(dbPath, options);
                seedStopwatch.Stop();

                var counts = ReadCounts(dbPath);
                Console.WriteLine(
                    $"Seeded rows in {seedStopwatch.Elapsed.TotalSeconds:F2}s: total={counts.Total}, hot={counts.Hot}, cold={counts.Cold}, completed={counts.Completed}");

                if (options.AttachDelaySeconds > 0)
                {
                    Console.WriteLine(
                        $"Dataset ready. pid={Environment.ProcessId}. Attach profiler now; waiting {options.AttachDelaySeconds}s...");
                    await Task.Delay(TimeSpan.FromSeconds(options.AttachDelaySeconds)).ConfigureAwait(false);
                }

                ForceFullGc();
                var before = MemorySnapshot.Capture("before-startup");
                var startupStopwatch = Stopwatch.StartNew();

                using var repository = new MetadataRepository(
                    fileSystem,
                    NullLogger<MetadataRepository>.Instance,
                    metadataDirectory,
                    enableBackgroundPersistence: false,
                    startupLoadBatchSize: options.StartupLoadBatchSize);

                var firstPending = await repository
                    .GetNextPendingFileAsync(options.TenantId, CancellationToken.None)
                    .ConfigureAwait(false);
                startupStopwatch.Stop();

                var afterStartup = MemorySnapshot.Capture("after-startup-load");
                var hotCachedCount = CountHotCacheEntries(repository, options.TenantId);
                var coldProbe = await repository
                    .GetCompletedOlderThanAsync(
                        options.TenantId,
                        DateTime.UtcNow.AddDays(1),
                        Math.Max(1, Math.Min(options.ColdQueryLimit, options.ColdCount)),
                        null,
                        CancellationToken.None)
                    .ConfigureAwait(false);
                var afterColdQuery = MemorySnapshot.Capture("after-cold-page-query");

                Console.WriteLine();
                Console.WriteLine("=== Metadata Cold Start Memory Result ===");
                PrintSnapshot(before);
                PrintSnapshot(afterStartup, before);
                PrintSnapshot(afterColdQuery, before);
                Console.WriteLine();
                Console.WriteLine($"StartupElapsed={startupStopwatch.Elapsed.TotalSeconds:F3}s");
                Console.WriteLine($"HotCacheEntries={hotCachedCount}");
                Console.WriteLine($"ExpectedHotRows={counts.Hot}");
                Console.WriteLine($"FirstPending={(firstPending == null ? "null" : firstPending.FileKey)}");
                Console.WriteLine($"ColdPageQueryRows={coldProbe.Count}");
                Console.WriteLine($"ColdRowsRemainInSQLite={counts.Cold}");

                if (hotCachedCount != counts.Hot)
                {
                    Console.Error.WriteLine(
                        $"FAILED: hot cache count mismatch. expected={counts.Hot}, actual={hotCachedCount}");
                    return 2;
                }

                Console.WriteLine("PASS: cold metadata rows remained in SQLite and were not loaded into the hot cache.");
                return 0;
            }
            finally
            {
                if (!options.KeepDirectory)
                {
                    try
                    {
                        if (Directory.Exists(root))
                            Directory.Delete(root, recursive: true);
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"Warning: failed to delete benchmark root {root}: {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine($"Kept benchmark root: {root}");
                }
            }
        }

        private static void InitializeTenantDatabase(
            IFileSystem fileSystem,
            string metadataDirectory,
            Options options)
        {
            using var repository = new MetadataRepository(
                fileSystem,
                NullLogger<MetadataRepository>.Instance,
                metadataDirectory,
                enableBackgroundPersistence: false,
                startupLoadBatchSize: options.StartupLoadBatchSize);
            repository.GetByTenantAsync(options.TenantId, CancellationToken.None).GetAwaiter().GetResult();
        }

        private static void SeedRows(string dbPath, Options options)
        {
            using var conn = new SqliteConnection($"Data Source={dbPath};Pooling=False");
            conn.Open();

            ExecuteNonQuery(conn, "PRAGMA journal_mode=MEMORY;");
            ExecuteNonQuery(conn, "PRAGMA synchronous=OFF;");
            ExecuteNonQuery(conn, "DELETE FROM files;");

            using var transaction = conn.BeginTransaction();
            using var cmd = conn.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = @"
INSERT INTO files (
    file_key, tenant_id, volume_id, physical_path, directory_path, file_size,
    created_at, status, retry_count, last_failed_at, last_error,
    processing_start_time, completed_at, delete_succeeded_at, dead_lettered_at, available_for_processing_at,
    original_file_name, file_extension, metadata_json)
VALUES (
    @file_key, @tenant_id, @volume_id, @physical_path, @directory_path, @file_size,
    @created_at, @status, @retry_count, @last_failed_at, @last_error,
    @processing_start_time, @completed_at, @delete_succeeded_at, @dead_lettered_at, @available_for_processing_at,
    @original_file_name, @file_extension, @metadata_json);";

            var fileKey = cmd.Parameters.Add("@file_key", SqliteType.Text);
            var tenantId = cmd.Parameters.Add("@tenant_id", SqliteType.Text);
            var volumeId = cmd.Parameters.Add("@volume_id", SqliteType.Text);
            var physicalPath = cmd.Parameters.Add("@physical_path", SqliteType.Text);
            var directoryPath = cmd.Parameters.Add("@directory_path", SqliteType.Text);
            var fileSize = cmd.Parameters.Add("@file_size", SqliteType.Integer);
            var createdAt = cmd.Parameters.Add("@created_at", SqliteType.Text);
            var status = cmd.Parameters.Add("@status", SqliteType.Integer);
            var retryCount = cmd.Parameters.Add("@retry_count", SqliteType.Integer);
            var lastFailedAt = cmd.Parameters.Add("@last_failed_at", SqliteType.Text);
            var lastError = cmd.Parameters.Add("@last_error", SqliteType.Text);
            var processingStartTime = cmd.Parameters.Add("@processing_start_time", SqliteType.Text);
            var completedAt = cmd.Parameters.Add("@completed_at", SqliteType.Text);
            var deleteSucceededAt = cmd.Parameters.Add("@delete_succeeded_at", SqliteType.Text);
            var deadLetteredAt = cmd.Parameters.Add("@dead_lettered_at", SqliteType.Text);
            var availableForProcessingAt = cmd.Parameters.Add("@available_for_processing_at", SqliteType.Text);
            var originalFileName = cmd.Parameters.Add("@original_file_name", SqliteType.Text);
            var fileExtension = cmd.Parameters.Add("@file_extension", SqliteType.Text);
            var metadataJson = cmd.Parameters.Add("@metadata_json", SqliteType.Text);

            tenantId.Value = options.TenantId;
            volumeId.Value = "vol-001";
            directoryPath.Value = "/bench";
            fileSize.Value = 256;
            retryCount.Value = 0;
            lastError.Value = DBNull.Value;
            availableForProcessingAt.Value = DBNull.Value;
            originalFileName.Value = DBNull.Value;
            fileExtension.Value = ".dat";
            metadataJson.Value = DBNull.Value;

            var now = DateTime.UtcNow;
            for (var i = 0; i < options.ColdCount; i++)
            {
                var coldStatus = ResolveColdStatus(i);
                var timestamp = now.AddSeconds(-i - 1).ToString("O", CultureInfo.InvariantCulture);
                fileKey.Value = $"cold-{i:D8}";
                physicalPath.Value = $"/bench/cold-{i:D8}.dat";
                createdAt.Value = now.AddTicks(i).ToString("O", CultureInfo.InvariantCulture);
                status.Value = (int)coldStatus;
                processingStartTime.Value = DBNull.Value;
                lastFailedAt.Value = coldStatus == FileProcessingStatus.PermanentlyFailed
                    ? timestamp
                    : DBNull.Value;
                completedAt.Value = coldStatus == FileProcessingStatus.Completed
                    || coldStatus == FileProcessingStatus.DeleteRequested
                    || coldStatus == FileProcessingStatus.DeleteSucceeded
                        ? timestamp
                        : DBNull.Value;
                deleteSucceededAt.Value = coldStatus == FileProcessingStatus.DeleteSucceeded
                    ? timestamp
                    : DBNull.Value;
                deadLetteredAt.Value = coldStatus == FileProcessingStatus.DeadLettered
                    ? timestamp
                    : DBNull.Value;
                cmd.ExecuteNonQuery();
            }

            for (var i = 0; i < options.HotCount; i++)
            {
                var hotStatus = ResolveHotStatus(i);
                var timestamp = now.AddMinutes(-10).ToString("O", CultureInfo.InvariantCulture);
                fileKey.Value = $"hot-{i:D8}";
                physicalPath.Value = $"/bench/hot-{i:D8}.dat";
                createdAt.Value = now.AddTicks(options.ColdCount + i).ToString("O", CultureInfo.InvariantCulture);
                status.Value = (int)hotStatus;
                processingStartTime.Value = hotStatus == FileProcessingStatus.Processing
                    ? timestamp
                    : DBNull.Value;
                lastFailedAt.Value = hotStatus == FileProcessingStatus.Failed
                    ? timestamp
                    : DBNull.Value;
                completedAt.Value = DBNull.Value;
                deleteSucceededAt.Value = DBNull.Value;
                deadLetteredAt.Value = DBNull.Value;
                cmd.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        private static DatasetCounts ReadCounts(string dbPath)
        {
            using var conn = new SqliteConnection($"Data Source={dbPath};Pooling=False");
            conn.Open();

            var total = ExecuteScalarLong(conn, "SELECT COUNT(1) FROM files;");
            var hot = ExecuteScalarLong(
                conn,
                $"SELECT COUNT(1) FROM files WHERE status IN ({(int)FileProcessingStatus.Pending},{(int)FileProcessingStatus.Processing},{(int)FileProcessingStatus.Failed});");
            var completed = ExecuteScalarLong(
                conn,
                $"SELECT COUNT(1) FROM files WHERE status = {(int)FileProcessingStatus.Completed};");

            return new DatasetCounts(total, hot, total - hot, completed);
        }

        private static long ExecuteScalarLong(SqliteConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            return Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
        }

        private static void ExecuteNonQuery(SqliteConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        private static FileProcessingStatus ResolveColdStatus(int index)
        {
            return (index % 5) switch
            {
                0 => FileProcessingStatus.Completed,
                1 => FileProcessingStatus.DeleteRequested,
                2 => FileProcessingStatus.DeleteSucceeded,
                3 => FileProcessingStatus.PermanentlyFailed,
                _ => FileProcessingStatus.DeadLettered
            };
        }

        private static FileProcessingStatus ResolveHotStatus(int index)
        {
            return (index % 3) switch
            {
                0 => FileProcessingStatus.Pending,
                1 => FileProcessingStatus.Processing,
                _ => FileProcessingStatus.Failed
            };
        }

        private static long CountHotCacheEntries(MetadataRepository repository, string tenantId)
        {
            var field = typeof(MetadataRepository).GetField(
                "_activeFiles",
                BindingFlags.Instance | BindingFlags.NonPublic);
            if (field == null)
                throw new InvalidOperationException("Unable to find MetadataRepository._activeFiles field.");

            var activeFiles = field.GetValue(repository) as IDictionary;
            if (activeFiles == null || !activeFiles.Contains(tenantId))
                return 0;

            var tenantCache = activeFiles[tenantId] as ICollection;
            if (tenantCache == null)
                throw new InvalidOperationException("Unable to read tenant hot cache collection.");

            return tenantCache.Count;
        }

        private static void ForceFullGc()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        private static void PrintSnapshot(MemorySnapshot snapshot, MemorySnapshot? baseline = null)
        {
            var delta = baseline == null
                ? string.Empty
                : $" deltaPrivate={FormatBytes(snapshot.PrivateBytes - baseline.Value.PrivateBytes),12} deltaWorking={FormatBytes(snapshot.WorkingSetBytes - baseline.Value.WorkingSetBytes),12} deltaManaged={FormatBytes(snapshot.ManagedBytes - baseline.Value.ManagedBytes),12}";
            Console.WriteLine(
                $"{snapshot.Name,-22} private={FormatBytes(snapshot.PrivateBytes),12} working={FormatBytes(snapshot.WorkingSetBytes),12} managed={FormatBytes(snapshot.ManagedBytes),12}{delta}");
        }

        private static string FormatBytes(long bytes)
        {
            var sign = bytes < 0 ? "-" : string.Empty;
            var value = Math.Abs(bytes);
            if (value >= 1024L * 1024L * 1024L)
                return $"{sign}{value / 1024.0 / 1024.0 / 1024.0:F2} GiB";
            if (value >= 1024L * 1024L)
                return $"{sign}{value / 1024.0 / 1024.0:F2} MiB";
            if (value >= 1024L)
                return $"{sign}{value / 1024.0:F2} KiB";

            return $"{sign}{value} B";
        }

        private static bool TryParseOptions(string[] args, out Options options, out string? error)
        {
            options = new Options();
            error = null;

            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                var optionName = arg;
                if (string.Equals(arg, ScenarioName, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (string.Equals(arg, "--scenario", StringComparison.OrdinalIgnoreCase))
                {
                    i++;
                    continue;
                }

                if (!TryReadValue(args, ref i, out optionName, out var value))
                {
                    error = $"Missing value for argument '{arg}'.";
                    return false;
                }

                switch (optionName.ToLowerInvariant())
                {
                    case "--cold":
                    case "--cold-count":
                        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out options.ColdCount) || options.ColdCount < 0)
                        {
                            error = "--cold must be a non-negative integer.";
                            return false;
                        }
                        break;

                    case "--hot":
                    case "--hot-count":
                        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out options.HotCount) || options.HotCount < 0)
                        {
                            error = "--hot must be a non-negative integer.";
                            return false;
                        }
                        break;

                    case "--batch":
                    case "--startup-load-batch-size":
                        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out options.StartupLoadBatchSize) || options.StartupLoadBatchSize <= 0)
                        {
                            error = "--batch must be a positive integer.";
                            return false;
                        }
                        break;

                    case "--tenant":
                        if (string.IsNullOrWhiteSpace(value))
                        {
                            error = "--tenant cannot be empty.";
                            return false;
                        }
                        options.TenantId = value;
                        break;

                    case "--root":
                        options.RootDirectory = value;
                        break;

                    case "--keep":
                        options.KeepDirectory = bool.Parse(value);
                        break;

                    case "--attach-delay":
                        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out options.AttachDelaySeconds) || options.AttachDelaySeconds < 0)
                        {
                            error = "--attach-delay must be a non-negative integer.";
                            return false;
                        }
                        break;

                    case "--cold-query-limit":
                        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out options.ColdQueryLimit) || options.ColdQueryLimit <= 0)
                        {
                            error = "--cold-query-limit must be a positive integer.";
                            return false;
                        }
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

        private static void PrintUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  dotnet run -c Release --project tests/Locus.Benchmarks -- metadata-cold-start-memory [options]");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --cold <count>              Cold rows to seed. Default: 900000");
            Console.WriteLine("  --hot <count>               Hot rows to seed. Default: 100");
            Console.WriteLine("  --batch <count>             Startup load batch size. Default: 2000");
            Console.WriteLine("  --tenant <id>               Tenant id. Default: memory-tenant");
            Console.WriteLine("  --root <path>               Optional benchmark root directory");
            Console.WriteLine("  --keep                      Keep generated SQLite files after the run");
            Console.WriteLine("  --attach-delay <seconds>    Wait after seeding so external profilers can attach");
            Console.WriteLine("  --cold-query-limit <count>  Cold cleanup page probe size. Default: 100");
        }

        private sealed class Options
        {
            public int ColdCount = 900_000;
            public int HotCount = 100;
            public int StartupLoadBatchSize = 2_000;
            public string TenantId = DefaultTenantId;
            public string? RootDirectory;
            public bool KeepDirectory;
            public int AttachDelaySeconds;
            public int ColdQueryLimit = 100;
        }

        private readonly struct DatasetCounts
        {
            public DatasetCounts(long total, long hot, long cold, long completed)
            {
                Total = total;
                Hot = hot;
                Cold = cold;
                Completed = completed;
            }

            public long Total { get; }
            public long Hot { get; }
            public long Cold { get; }
            public long Completed { get; }
        }

        private readonly struct MemorySnapshot
        {
            private MemorySnapshot(string name, long privateBytes, long workingSetBytes, long managedBytes)
            {
                Name = name;
                PrivateBytes = privateBytes;
                WorkingSetBytes = workingSetBytes;
                ManagedBytes = managedBytes;
            }

            public string Name { get; }
            public long PrivateBytes { get; }
            public long WorkingSetBytes { get; }
            public long ManagedBytes { get; }

            public static MemorySnapshot Capture(string name)
            {
                using var process = Process.GetCurrentProcess();
                process.Refresh();
                return new MemorySnapshot(
                    name,
                    process.PrivateMemorySize64,
                    process.WorkingSet64,
                    GC.GetTotalMemory(forceFullCollection: false));
            }
        }
    }
}
