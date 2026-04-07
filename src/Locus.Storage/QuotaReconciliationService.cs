using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Background maintenance service that reconciles quota counters from metadata snapshots.
    /// This service is intended for explicit maintenance windows and should remain disabled
    /// during normal runtime unless quota drift repair is required.
    /// </summary>
    public class QuotaReconciliationService : BackgroundService
    {
        private readonly IStorageCleanupService _cleanupService;
        private readonly ILogger<QuotaReconciliationService> _logger;
        private readonly QuotaReconciliationOptions _options;

        /// <summary>
        /// Initializes a new instance of the <see cref="QuotaReconciliationService"/> class.
        /// </summary>
        public QuotaReconciliationService(
            IStorageCleanupService cleanupService,
            QuotaReconciliationOptions options,
            ILogger<QuotaReconciliationService> logger)
        {
            _cleanupService = cleanupService ?? throw new ArgumentNullException(nameof(cleanupService));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "QuotaReconciliationService started (RunOnStartup={RunOnStartup}, Interval={Interval})",
                _options.RunOnStartup,
                _options.Interval);

            var delayBeforeFirstRun = _options.RunOnStartup
                ? _options.InitialDelay
                : _options.Interval;

            if (delayBeforeFirstRun > TimeSpan.Zero)
                await Task.Delay(delayBeforeFirstRun, stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Starting quota reconciliation maintenance run");
                    await _cleanupService.ReconcileAllQuotaCountsAsync(stoppingToken);
                    _logger.LogInformation("Quota reconciliation maintenance run completed");
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error during quota reconciliation maintenance run");
                }

                await Task.Delay(_options.Interval, stoppingToken);
            }

            _logger.LogInformation("QuotaReconciliationService stopped");
        }
    }

    /// <summary>
    /// Configuration options for the quota reconciliation maintenance service.
    /// </summary>
    public class QuotaReconciliationOptions
    {
        /// <summary>
        /// Gets or sets a value indicating whether reconciliation should run shortly after startup.
        /// Default: false.
        /// </summary>
        public bool RunOnStartup { get; set; }

        /// <summary>
        /// Gets or sets the initial delay before the first reconciliation run when <see cref="RunOnStartup"/> is enabled.
        /// Default: 30 seconds.
        /// </summary>
        public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Gets or sets the interval between reconciliation runs.
        /// Default: 6 hours.
        /// </summary>
        public TimeSpan Interval { get; set; } = TimeSpan.FromHours(6);
    }
}
