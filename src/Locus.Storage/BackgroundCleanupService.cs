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
        private readonly IFileScheduler _fileScheduler;
        private readonly ILogger<BackgroundCleanupService> _logger;
        private readonly CleanupOptions _options;
        // Initialized to UtcNow so the first optimization is deferred by a full
        // DatabaseOptimizationInterval rather than running immediately on startup.
        private DateTime _lastDatabaseOptimization;

        /// <summary>
        /// Initializes a new instance of the <see cref="BackgroundCleanupService"/> class.
        /// </summary>
        public BackgroundCleanupService(
            IStorageCleanupService cleanupService,
            IFileScheduler fileScheduler,
            CleanupOptions options,
            ILogger<BackgroundCleanupService> logger)
        {
            _cleanupService = cleanupService ?? throw new ArgumentNullException(nameof(cleanupService));
            _fileScheduler = fileScheduler ?? throw new ArgumentNullException(nameof(fileScheduler));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _lastDatabaseOptimization = DateTime.UtcNow;
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken = default)
        {
            if (!_options.Enabled)
            {
                _logger.LogInformation("BackgroundCleanupService is disabled by CleanupOptions.Enabled");
                return;
            }

            _logger.LogInformation("BackgroundCleanupService started");

            // Wait for initial delay before first cleanup
            await Task.Delay(_options.InitialDelay, stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Starting scheduled cleanup tasks");

                    // 1. Cleanup junk files (Thumbs.db, .DS_Store, etc.)
                    // Note: This no longer deletes empty directories, only the junk files within them.
                    await _cleanupService.CleanupAllEmptyDirectoriesAsync(stoppingToken);

                    // 2. Remove stale metadata whose physical files are already missing.
                    // This closes the persistence-loss gap where SQLite still references files that
                    // no longer exist on disk after an incomplete shutdown or failed delete flush.
                    var orphanedMetadataRemoved = await _fileScheduler.CleanupOrphanedMetadataAsync(stoppingToken);
                    if (orphanedMetadataRemoved > 0)
                    {
                        _logger.LogInformation(
                            "Removed {Count} orphaned metadata record(s) whose physical files were missing",
                            orphanedMetadataRemoved);
                    }

                    // 3. Orphan recovery is now handled by OrphanFileRecoveryService (independent BackgroundService).

                    // 4. Low-rate cleanup for completed files that are ready for physical deletion.
                    if (_options.CleanupCompletedFiles && _options.CompletedFileRetentionPeriod.HasValue)
                    {
                        await _cleanupService.CleanupCompletedFilesAsync(
                            _options.CompletedFileRetentionPeriod.Value,
                            stoppingToken);
                    }

                    // 5-6. Combined single-pass cleanup: timed-out and permanently-failed files.
                    // Uses a single GetAllAsync call instead of one per status category.
                    await _cleanupService.CleanupFilesByStatusAsync(
                        _options.CleanupTimedOutFiles ? _options.ProcessingTimeout : null,
                        _options.PermanentlyFailedDisposition == PermanentlyFailedDisposition.Keep
                            ? null
                            : _options.FailedFileRetentionPeriod,
                        stoppingToken);

                    // 6. Remove stale corruption backup files left over from rebuild operations.
                    if (_options.CleanupInvalidDatabaseBackups)
                    {
                        var (filesRemoved, spaceFreed) = await _cleanupService.CleanupInvalidDatabaseFilesAsync(stoppingToken);
                        if (filesRemoved > 0)
                        {
                            _logger.LogInformation(
                                "Removed {FilesRemoved} stale SQLite corruption backup file(s), freed {SpaceFreedMB:F2} MB",
                                filesRemoved,
                                spaceFreed / 1024.0 / 1024.0);
                        }
                    }

                    // 7. Optimize databases (if enabled and due)
                    if (_options.OptimizeDatabases && ShouldOptimizeDatabases())
                    {
                        _logger.LogInformation("Starting scheduled database optimization...");

                        try
                        {
                            var optimizationResult = await _cleanupService.OptimizeDatabasesAsync(stoppingToken);
                            _lastDatabaseOptimization = DateTime.UtcNow;

                            _logger.LogInformation(
                                "Database optimization completed: " +
                                "Databases={TotalDatabases} (Metadata={Metadata}, Quota={Quota}), " +
                                "SpaceReclaimed={SpaceReclaimedMB:F2} MB ({PercentageReclaimed:F1}%), " +
                                "Before={BeforeMB:F2} MB, After={AfterMB:F2} MB",
                                optimizationResult.MetadataDatabasesOptimized + optimizationResult.QuotaDatabasesOptimized,
                                optimizationResult.MetadataDatabasesOptimized,
                                optimizationResult.QuotaDatabasesOptimized,
                                optimizationResult.SpaceReclaimedMB,
                                optimizationResult.PercentageReclaimed,
                                optimizationResult.SizeBefore / 1024.0 / 1024.0,
                                optimizationResult.SizeAfter / 1024.0 / 1024.0);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error during database optimization");
                        }
                    }

                    // Get and log statistics
                    var stats = await _cleanupService.GetCleanupStatisticsAsync(stoppingToken);
                    _logger.LogInformation(
                        "Cleanup completed: " +
                        "EmptyDirs={EmptyDirs}, " +
                        "FailedFiles={FailedFiles}, " +
                        "TimedOut={TimedOut}, " +
                        "CompletedRecords={CompletedRecords}, " +
                        "OrphanedFilesRemoved={OrphanedFilesRemoved}, " +
                        "OrphanedFilesRecovered={OrphanedFilesRecovered}, " +
                        "SpaceFreed={SpaceFreedMB} MB",
                        stats.EmptyDirectoriesRemoved,
                        stats.PermanentlyFailedFilesRemoved,
                        stats.TimedOutFilesReset,
                        stats.CompletedRecordsRemoved,
                        stats.OrphanedFilesRemoved,
                        stats.OrphanedFilesRecovered,
                        stats.SpaceFreed / 1024 / 1024);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error during scheduled cleanup");
                }

                // Wait for next cleanup interval
                try
                {
                    await Task.Delay(_options.CleanupInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }

            _logger.LogInformation("BackgroundCleanupService stopped");
        }

        /// <summary>
        /// Determines whether database optimization should run based on the configured interval.
        /// </summary>
        private bool ShouldOptimizeDatabases()
        {
            if (!_options.DatabaseOptimizationInterval.HasValue)
                return false;

            var timeSinceLastOptimization = DateTime.UtcNow - _lastDatabaseOptimization;
            return timeSinceLastOptimization >= _options.DatabaseOptimizationInterval.Value;
        }
    }

    /// <summary>
    /// Configuration options for the background cleanup service.
    /// </summary>
    public class CleanupOptions
    {
        /// <summary>
        /// Gets or sets whether the background cleanup hosted service is enabled.
        /// Default: true.
        /// </summary>
        public bool Enabled { get; set; } = true;

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
        /// Gets or sets whether to cleanup timed-out processing files.
        /// Default: true.
        /// </summary>
        public bool CleanupTimedOutFiles { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to cleanup completed files in the background.
        /// Default: true.
        /// </summary>
        public bool CleanupCompletedFiles { get; set; } = true;

        /// <summary>
        /// Gets or sets the processing timeout threshold.
        /// Files in Processing status longer than this will be reset to Pending.
        /// Default: 30 minutes.
        /// </summary>
        public TimeSpan? ProcessingTimeout { get; set; } = TimeSpan.FromMinutes(30);

        /// <summary>
        /// Gets or sets how permanently failed files are handled after the retention period elapses.
        /// Default: MoveToDeadLetter.
        /// </summary>
        public PermanentlyFailedDisposition PermanentlyFailedDisposition { get; set; } = PermanentlyFailedDisposition.MoveToDeadLetter;

        /// <summary>
        /// Gets or sets the retention period before a completed file is reaped physically.
        /// Default: zero, meaning the next cleanup cycle may delete it.
        /// </summary>
        public TimeSpan? CompletedFileRetentionPeriod { get; set; } = TimeSpan.Zero;

        /// <summary>
        /// Gets or sets whether to remove stale SQLite corruption backup files (*.corrupted.*).
        /// Default: true.
        /// </summary>
        public bool CleanupInvalidDatabaseBackups { get; set; } = true;

        /// <summary>
        /// Gets or sets the retention period for permanently failed files.
        /// Default: 3 days.
        /// </summary>
        public TimeSpan? FailedFileRetentionPeriod { get; set; } = TimeSpan.FromDays(3);

        /// <summary>
        /// Gets or sets dead-letter storage options used when permanently failed files are moved
        /// out of the active queue instead of being deleted.
        /// </summary>
        public DeadLetterOptions DeadLetter { get; set; } = new DeadLetterOptions();

        /// <summary>
        /// Gets or sets whether to optimize (shrink) SQLite databases periodically.
        /// Database optimization reclaims space from deleted records by running VACUUM on the database files.
        /// This is a heavy operation and should be run during low-activity periods.
        /// Default: true.
        /// </summary>
        public bool OptimizeDatabases { get; set; } = true;

        /// <summary>
        /// Gets or sets the interval between database optimization runs.
        /// Database optimization is independent of the regular cleanup interval.
        /// Recommended: Daily (1 day) for high-frequency delete operations, weekly (7 days) for normal usage.
        /// Default: 1 day.
        /// </summary>
        public TimeSpan? DatabaseOptimizationInterval { get; set; } = TimeSpan.FromDays(1);

        /// <summary>
        /// Gets or sets the per-tenant batch size for status-based cleanup.
        /// Default: 500.
        /// </summary>
        public int CleanupBatchSizePerTenant { get; set; } = 500;

        /// <summary>
        /// Gets or sets the maximum number of physical files to scan per orphan-rebuild run
        /// (per tenant per volume). Set to a non-positive value for unlimited scanning.
        /// Default: 5000.
        /// </summary>
        public int MaxOrphanFilesPerRun { get; set; } = 5000;

        /// <summary>
        /// Gets or sets the maximum number of physical-path lookup results cached in memory
        /// during a single orphan-rebuild run (per tenant per volume).
        /// Set to 0 to disable the cache.
        /// Default: 8192.
        /// </summary>
        public int OrphanRebuildLookupCacheSize { get; set; } = 8192;

        /// <summary>
        /// Gets or sets the number of tenant databases to optimize before pausing briefly.
        /// Default: 10.
        /// </summary>
        public int DatabaseOptimizationTenantBatchSize { get; set; } = 10;

        /// <summary>
        /// Gets or sets the pause duration between optimization batches.
        /// Default: 200 milliseconds.
        /// </summary>
        public TimeSpan DatabaseOptimizationPauseBetweenBatches { get; set; } = TimeSpan.FromMilliseconds(200);
    }

    /// <summary>
    /// Defines how permanently failed files are handled once the retention period elapses.
    /// </summary>
    public enum PermanentlyFailedDisposition
    {
        /// <summary>
        /// Keep permanently failed files in place for manual intervention.
        /// </summary>
        Keep = 0,

        /// <summary>
        /// Move permanently failed files to a dead-letter area and remove them from active quotas.
        /// </summary>
        MoveToDeadLetter = 1,

        /// <summary>
        /// Delete permanently failed files physically and remove their metadata.
        /// </summary>
        Delete = 2,
    }

    /// <summary>
    /// Configures how permanently failed files are stored when they are moved to dead letter.
    /// </summary>
    public sealed class DeadLetterOptions
    {
        /// <summary>
        /// Gets or sets the dead-letter root path.
        /// Relative paths are resolved under the owning volume mount path.
        /// Default: ".deadletter".
        /// </summary>
        public string RootPath { get; set; } = ".deadletter";

        /// <summary>
        /// Gets or sets whether tenant identifiers are included under the dead-letter root.
        /// Default: true.
        /// </summary>
        public bool IncludeTenantInPath { get; set; } = true;

        /// <summary>
        /// Gets or sets whether a yyyyMMdd partition directory is inserted before shard segments.
        /// Default: true.
        /// </summary>
        public bool IncludeDatePartition { get; set; } = true;

        /// <summary>
        /// Gets or sets the shard depth under the dead-letter root.
        /// Default: 2.
        /// </summary>
        public int ShardingDepth { get; set; } = 2;
    }
}
