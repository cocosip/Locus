#nullable enable
using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Benchmarks
{
    internal static class AsyncJournalDrainCommand
    {
        private const string ScenarioName = "async-journal-drain";
        private const string TenantId = "benchmark-tenant";

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
                $"Running {ScenarioName}: writes={options.WriteCount}, format={options.JournalFormat}, lingerMs={options.Linger.TotalMilliseconds:F0}, maxBatchRecords={options.MaxBatchRecords}, maxBatchBytes={options.MaxBatchBytes}, stateFlushDebounceMs={options.StateFlushDebounce.TotalMilliseconds:F0}");

            var result = await RunAsync(options).ConfigureAwait(false);
            Console.WriteLine($"  Submit total:     {FormatTime(result.SubmitTicks)}");
            Console.WriteLine($"  Drain total:      {FormatTime(result.DrainTicks)}");
            Console.WriteLine($"  Total:            {FormatTime(result.TotalTicks)}");
            Console.WriteLine($"  Records/sec:      {result.RecordsPerSecond:F0}");
            Console.WriteLine($"  Append batches:   {result.AppendBatchCount}");
            Console.WriteLine($"  Appended records: {result.AppendedRecordCount}");
            Console.WriteLine($"  Appended bytes:   {FormatBytes(result.AppendedBytes)}");
            Console.WriteLine($"  Append avg:       {FormatTime(result.AppendAverageTicks)}");
            Console.WriteLine($"  Flush count:      {result.FlushCount}");
            Console.WriteLine($"  Flush avg:        {FormatTime(result.FlushAverageTicks)}");
            return 0;
        }

        private static async Task<Result> RunAsync(Options options)
        {
            var root = Path.Combine(Path.GetTempPath(), "locus-async-journal-drain-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(root);

            try
            {
                using var journal = new FileQueueEventJournal(
                    new System.IO.Abstractions.FileSystem(),
                    NullLogger<FileQueueEventJournal>.Instance,
                    new QueueEventJournalOptions
                    {
                        QueueDirectory = Path.Combine(root, "queue"),
                        JournalFormat = options.JournalFormat,
                        AckMode = QueueEventJournalAckMode.Async,
                        EnableProjection = false,
                        StateFlushDebounce = options.StateFlushDebounce,
                        Linger = options.Linger,
                        MaxBatchRecords = options.MaxBatchRecords,
                        MaxBatchBytes = options.MaxBatchBytes,
                        WriterIdleTimeout = TimeSpan.FromSeconds(30),
                    });

                var diagnostics = (IQueueEventJournalWritePathDiagnostics)journal;
                var submitStarted = Stopwatch.GetTimestamp();
                for (var i = 0; i < options.WriteCount; i++)
                    await journal.AppendAsync(CreateRecord(i), CancellationToken.None).ConfigureAwait(false);

                var submitTicks = Stopwatch.GetTimestamp() - submitStarted;
                var drainStarted = Stopwatch.GetTimestamp();
                while (diagnostics.GetWritePathStatisticsSnapshot().AppendedRecordCount < options.WriteCount)
                    await Task.Delay(1).ConfigureAwait(false);

                var drainTicks = Stopwatch.GetTimestamp() - drainStarted;
                var snapshot = diagnostics.GetWritePathStatisticsSnapshot();
                return new Result(options.WriteCount, submitTicks, drainTicks, snapshot);
            }
            finally
            {
                try
                {
                    Directory.Delete(root, recursive: true);
                }
                catch
                {
                    // Ignore temporary benchmark cleanup failures.
                }
            }
        }

        private static QueueEventRecord CreateRecord(int index)
        {
            return new QueueEventRecord
            {
                TenantId = TenantId,
                FileKey = "file-" + index.ToString("D8", CultureInfo.InvariantCulture),
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow,
                DirectoryPath = "/incoming",
                PhysicalPath = @"D:\benchmark\incoming\file-" + index.ToString("D8", CultureInfo.InvariantCulture) + ".dcm",
                FileSize = 1024,
                Status = FileProcessingStatus.Pending,
                OriginalFileName = "file-" + index.ToString("D8", CultureInfo.InvariantCulture) + ".dcm",
                FileExtension = ".dcm",
            };
        }

        private static bool TryParseOptions(string[] args, out Options options, out string? error)
        {
            options = new Options
            {
                WriteCount = 20_000,
                JournalFormat = JournalFormat.BinaryV1,
                Linger = TimeSpan.Zero,
                MaxBatchRecords = 16,
                MaxBatchBytes = 256 * 1024,
                StateFlushDebounce = TimeSpan.FromSeconds(1),
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
                    if (!TryReadInt(args, ref i, out var writes) || writes <= 0)
                    {
                        error = "Missing or invalid value for --writes.";
                        return false;
                    }

                    options.WriteCount = writes;
                    continue;
                }

                if (string.Equals(arg, "--format", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadValue(args, ref i, out var raw))
                    {
                        error = "Missing value for --format.";
                        return false;
                    }

                    if (string.Equals(raw, "binary", StringComparison.OrdinalIgnoreCase))
                    {
                        options.JournalFormat = JournalFormat.BinaryV1;
                    }
                    else if (string.Equals(raw, "json", StringComparison.OrdinalIgnoreCase))
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

                if (string.Equals(arg, "--state-flush-debounce-ms", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryReadInt(args, ref i, out var debounceMs) || debounceMs < 0)
                    {
                        error = "Missing or invalid value for --state-flush-debounce-ms.";
                        return false;
                    }

                    options.StateFlushDebounce = TimeSpan.FromMilliseconds(debounceMs);
                    continue;
                }
            }

            return true;
        }

        private static bool TryReadInt(string[] args, ref int index, out int value)
        {
            value = 0;
            return TryReadValue(args, ref index, out var rawValue)
                && int.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
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
            Console.WriteLine("  dotnet run -c Release --project tests/Locus.Benchmarks -- async-journal-drain [options]");
        }

        private static string FormatTime(double ticks)
        {
            var microseconds = ticks * 1_000_000.0 / Stopwatch.Frequency;
            return microseconds >= 1000
                ? $"{microseconds / 1000.0:F3} ms"
                : $"{microseconds:F1} us";
        }

        private static string FormatBytes(long bytes)
        {
            return bytes >= 1024 * 1024
                ? $"{bytes / 1024.0 / 1024.0:F2} MiB"
                : bytes >= 1024
                    ? $"{bytes / 1024.0:F2} KiB"
                    : $"{bytes} B";
        }

        private sealed class Options
        {
            public int WriteCount { get; set; }

            public JournalFormat JournalFormat { get; set; }

            public TimeSpan Linger { get; set; }

            public int MaxBatchRecords { get; set; }

            public int MaxBatchBytes { get; set; }

            public TimeSpan StateFlushDebounce { get; set; }
        }

        private sealed class Result
        {
            public Result(int writeCount, long submitTicks, long drainTicks, QueueJournalWritePathStatistics snapshot)
            {
                WriteCount = writeCount;
                SubmitTicks = submitTicks;
                DrainTicks = drainTicks;
                AppendBatchCount = snapshot.AppendBatchCount;
                AppendedRecordCount = snapshot.AppendedRecordCount;
                AppendedBytes = snapshot.AppendedBytes;
                AppendAverageTicks = snapshot.AppendBatchCount == 0 ? 0 : snapshot.AppendTicks / (double)snapshot.AppendBatchCount;
                FlushCount = snapshot.FlushCount;
                FlushAverageTicks = snapshot.FlushCount == 0 ? 0 : snapshot.FlushTicks / (double)snapshot.FlushCount;
            }

            public int WriteCount { get; }

            public long SubmitTicks { get; }

            public long DrainTicks { get; }

            public long TotalTicks => SubmitTicks + DrainTicks;

            public double RecordsPerSecond => WriteCount / (TotalTicks / (double)Stopwatch.Frequency);

            public long AppendBatchCount { get; }

            public long AppendedRecordCount { get; }

            public long AppendedBytes { get; }

            public double AppendAverageTicks { get; }

            public long FlushCount { get; }

            public double FlushAverageTicks { get; }
        }
    }
}
