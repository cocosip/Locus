using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Storage.Statistics
{
    /// <summary>
    /// Periodically outputs Locus statistics through configured sinks.
    /// </summary>
    public class LocusStatisticsOutputService : BackgroundService
    {
        private readonly ILocusStatisticsReader _reader;
        private readonly LocusStatisticsOptions _options;
        private readonly ILogger<LocusStatisticsOutputService> _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="LocusStatisticsOutputService"/> class.
        /// </summary>
        public LocusStatisticsOutputService(
            ILocusStatisticsReader reader,
            LocusStatisticsOptions options,
            ILogger<LocusStatisticsOutputService> logger)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!_options.Enabled || !_options.Output.Enabled)
                return;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_options.Output.Interval, stoppingToken).ConfigureAwait(false);
                    await WriteSnapshotAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to output Locus statistics summary.");
                }
            }
        }

        /// <summary>
        /// Writes one statistics snapshot.
        /// </summary>
        protected Task WriteSnapshotAsync(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            var now = DateTimeOffset.UtcNow;
            var snapshot = _reader.GetSnapshot(new LocusStatisticsQuery
            {
                From = now - _options.Output.QueryWindow,
                To = now
            });

            if (!_options.Output.IncludeEmptySnapshots && IsEmpty(snapshot))
                return Task.CompletedTask;

            _logger.LogInformation(
                "Locus statistics summary: Window={Window}, WriteFiles={WriteFiles}, WriteMBps={WriteMBps:F3}, WriteBytes={WriteBytes}, DequeuedFiles={DequeuedFiles}, ReadFiles={ReadFiles}, CompletedFiles={CompletedFiles}, SqliteOps={SqliteOps}, WatcherImportedFiles={WatcherImportedFiles}, WatcherImportedBytes={WatcherImportedBytes}",
                _options.Output.QueryWindow,
                snapshot.WriteFileCount,
                snapshot.WriteMegabytesPerSecond,
                snapshot.WriteBytes,
                snapshot.DequeuedFileCount,
                snapshot.ReadFileCount,
                snapshot.CompletedFileCount,
                snapshot.SqlitePersistedOperationCount,
                snapshot.WatcherImportedFileCount,
                snapshot.WatcherImportedBytes);

            return Task.CompletedTask;
        }

        private static bool IsEmpty(LocusStatisticsSnapshot snapshot)
        {
            return snapshot.WriteFileCount == 0
                && snapshot.WriteBytes == 0
                && snapshot.DequeuedFileCount == 0
                && snapshot.ReadFileCount == 0
                && snapshot.CompletedFileCount == 0
                && snapshot.SqlitePersistedOperationCount == 0
                && snapshot.WatcherImportedFileCount == 0
                && snapshot.WatcherImportedBytes == 0;
        }
    }
}
