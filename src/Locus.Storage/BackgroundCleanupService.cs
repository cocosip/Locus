using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Background service that periodically runs cleanup tasks.
    /// </summary>
    public class BackgroundCleanupService : BackgroundService
    {
        private readonly IStorageCleanupService _cleanupService;
        private readonly ILogger<BackgroundCleanupService> _logger;
        private readonly CleanupOptions _options;

        /// <summary>
        /// Initializes a new instance of the <see cref="BackgroundCleanupService"/> class.
        /// </summary>
        public BackgroundCleanupService(
            IStorageCleanupService cleanupService,
            CleanupOptions options,
            ILogger<BackgroundCleanupService> logger)
        {
            _cleanupService = cleanupService ?? throw new ArgumentNullException(nameof(cleanupService));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("BackgroundCleanupService started");

            // Wait for initial delay before first cleanup
            await Task.Delay(_options.InitialDelay, stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Starting scheduled cleanup tasks");

                    // 1. Cleanup empty directories (if enabled)
                    if (_options.CleanupEmptyDirectories)
                    {
                        await _cleanupService.CleanupAllEmptyDirectoriesAsync(stoppingToken);
                    }

                    // 2. Cleanup timed-out processing files (if enabled)
                    if (_options.CleanupTimedOutFiles && _options.ProcessingTimeout.HasValue)
                    {
                        await _cleanupService.CleanupTimedOutProcessingFilesAsync(
                            _options.ProcessingTimeout.Value, stoppingToken);
                    }

                    // 3. Cleanup permanently failed files (if enabled)
                    if (_options.CleanupPermanentlyFailedFiles && _options.FailedFileRetentionPeriod.HasValue)
                    {
                        await _cleanupService.CleanupPermanentlyFailedFilesAsync(
                            _options.FailedFileRetentionPeriod.Value, stoppingToken);
                    }

                    // 4. Cleanup completed file records (if enabled)
                    if (_options.CleanupCompletedRecords && _options.CompletedRecordRetentionPeriod.HasValue)
                    {
                        await _cleanupService.CleanupCompletedFileRecordsAsync(
                            _options.CompletedRecordRetentionPeriod.Value, stoppingToken);
                    }

                    // Get and log statistics
                    var stats = await _cleanupService.GetCleanupStatisticsAsync(stoppingToken);
                    _logger.LogInformation(
                        "Cleanup completed: " +
                        "EmptyDirs={EmptyDirs}, " +
                        "FailedFiles={FailedFiles}, " +
                        "TimedOut={TimedOut}, " +
                        "CompletedRecords={CompletedRecords}, " +
                        "OrphanedFiles={OrphanedFiles}, " +
                        "SpaceFreed={SpaceFreedMB} MB",
                        stats.EmptyDirectoriesRemoved,
                        stats.PermanentlyFailedFilesRemoved,
                        stats.TimedOutFilesReset,
                        stats.CompletedRecordsRemoved,
                        stats.OrphanedFilesRemoved,
                        stats.SpaceFreed / 1024 / 1024);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error during scheduled cleanup");
                }

                // Wait for next cleanup interval
                await Task.Delay(_options.CleanupInterval, stoppingToken);
            }

            _logger.LogInformation("BackgroundCleanupService stopped");
        }
    }

    /// <summary>
    /// Configuration options for the background cleanup service.
    /// </summary>
    public class CleanupOptions
    {
        /// <summary>
        /// Gets or sets the interval between cleanup runs.
        /// Default: 1 hour.
        /// </summary>
        public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);

        /// <summary>
        /// Gets or sets the initial delay before the first cleanup run.
        /// Default: 1 minute.
        /// </summary>
        public TimeSpan InitialDelay { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Gets or sets whether to cleanup empty directories.
        /// Default: true.
        /// </summary>
        public bool CleanupEmptyDirectories { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to cleanup timed-out processing files.
        /// Default: true.
        /// </summary>
        public bool CleanupTimedOutFiles { get; set; } = true;

        /// <summary>
        /// Gets or sets the processing timeout threshold.
        /// Files in Processing status longer than this will be reset to Pending.
        /// Default: 30 minutes.
        /// </summary>
        public TimeSpan? ProcessingTimeout { get; set; } = TimeSpan.FromMinutes(30);

        /// <summary>
        /// Gets or sets whether to cleanup permanently failed files.
        /// Default: true.
        /// </summary>
        public bool CleanupPermanentlyFailedFiles { get; set; } = true;

        /// <summary>
        /// Gets or sets the retention period for permanently failed files.
        /// Default: 7 days.
        /// </summary>
        public TimeSpan? FailedFileRetentionPeriod { get; set; } = TimeSpan.FromDays(7);

        /// <summary>
        /// Gets or sets whether to cleanup completed file records.
        /// Default: false (keep records indefinitely).
        /// </summary>
        public bool CleanupCompletedRecords { get; set; } = false;

        /// <summary>
        /// Gets or sets the retention period for completed file records.
        /// Default: 30 days.
        /// </summary>
        public TimeSpan? CompletedRecordRetentionPeriod { get; set; } = TimeSpan.FromDays(30);
    }
}
