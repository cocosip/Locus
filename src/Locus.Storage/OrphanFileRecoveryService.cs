using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Background service that periodically recovers orphaned files (physical files on disk
    /// whose metadata was lost, e.g. due to process crash during write-behind persistence).
    /// Unlike cleanup, recovery rebuilds metadata and re-queues files as Pending.
    /// </summary>
    public class OrphanFileRecoveryService : BackgroundService
    {
        private readonly IStorageCleanupService _cleanupService;
        private readonly ILogger<OrphanFileRecoveryService> _logger;
        private readonly OrphanRecoveryOptions _options;

        /// <summary>
        /// Initializes a new instance of the <see cref="OrphanFileRecoveryService"/> class.
        /// </summary>
        public OrphanFileRecoveryService(
            IStorageCleanupService cleanupService,
            OrphanRecoveryOptions options,
            ILogger<OrphanFileRecoveryService> logger)
        {
            _cleanupService = cleanupService ?? throw new ArgumentNullException(nameof(cleanupService));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken = default)
        {
            _logger.LogInformation("OrphanFileRecoveryService started (InitialDelay={InitialDelay}, RecoveryInterval={RecoveryInterval})",
                _options.InitialDelay, _options.RecoveryInterval);

            // Short initial delay to allow volumes to mount
            if (_options.InitialDelay > TimeSpan.Zero)
                await Task.Delay(_options.InitialDelay, stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var statsBefore = await _cleanupService.GetCleanupStatisticsAsync(stoppingToken);
                    var countBefore = statsBefore.OrphanedFilesRecovered;

                    _logger.LogInformation("Starting orphaned file recovery scan");

                    await _cleanupService.RecoverAllOrphanedFilesAsync(stoppingToken);

                    var statsAfter = await _cleanupService.GetCleanupStatisticsAsync(stoppingToken);
                    var recovered = statsAfter.OrphanedFilesRecovered - countBefore;
                    _logger.LogInformation(
                        "Orphaned file recovery completed: {OrphanedFilesRecovered} file(s) recovered this run, {TotalRecovered} total since startup",
                        recovered, statsAfter.OrphanedFilesRecovered);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error during orphaned file recovery");
                }

                await Task.Delay(_options.RecoveryInterval, stoppingToken);
            }

            _logger.LogInformation("OrphanFileRecoveryService stopped");
        }
    }

    /// <summary>
    /// Configuration options for the orphan file recovery service.
    /// </summary>
    public class OrphanRecoveryOptions
    {
        /// <summary>
        /// Gets or sets the interval between recovery scans.
        /// Default: 30 minutes.
        /// </summary>
        public TimeSpan RecoveryInterval { get; set; } = TimeSpan.FromMinutes(30);

        /// <summary>
        /// Gets or sets the initial delay before the first recovery scan.
        /// Allows time for storage volumes to mount before scanning.
        /// Default: 10 seconds.
        /// </summary>
        public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(10);
    }
}
