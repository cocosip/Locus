using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Background service that periodically scans registered file watchers and imports files.
    /// </summary>
    public class BackgroundFileWatcherService : BackgroundService
    {
        private readonly IFileWatcher _fileWatcher;
        private readonly IFileWatcherOptionsManager _optionsManager;
        private readonly ILogger<BackgroundFileWatcherService> _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="BackgroundFileWatcherService"/> class.
        /// </summary>
        public BackgroundFileWatcherService(
            IFileWatcher fileWatcher,
            IFileWatcherOptionsManager optionsManager,
            ILogger<BackgroundFileWatcherService> logger)
        {
            _fileWatcher = fileWatcher ?? throw new ArgumentNullException(nameof(fileWatcher));
            _optionsManager = optionsManager ?? throw new ArgumentNullException(nameof(optionsManager));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Background File Watcher Service started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Check global enable/disable switch
                    var isEnabled = await _optionsManager.IsServiceEnabledAsync(stoppingToken);
                    if (!isEnabled)
                    {
                        var disabledOptions = await _optionsManager.GetOptionsAsync(stoppingToken);
                        _logger.LogDebug("File watcher service is globally disabled, checking again in {Interval}",
                            disabledOptions.DisabledCheckInterval);
                        await Task.Delay(disabledOptions.DisabledCheckInterval, stoppingToken);
                        continue;
                    }

                    await ScanAllWatchersAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error during file watcher scan cycle");
                }

                // Wait before next scan cycle
                // Use the shortest polling interval from all watchers, or default to configured interval
                var options = await _optionsManager.GetOptionsAsync(stoppingToken);
                var watchers = await _fileWatcher.GetAllWatchersAsync(stoppingToken);
                var minInterval = watchers
                    .Where(w => w.Enabled)
                    .Select(w => w.PollingInterval)
                    .DefaultIfEmpty(options.DefaultPollingInterval)
                    .Min();

                // Enforce minimum interval limit
                if (minInterval < options.MinimumPollingInterval)
                {
                    _logger.LogWarning(
                        "Polling interval {Interval} is less than minimum {MinInterval}, using minimum",
                        minInterval, options.MinimumPollingInterval);
                    minInterval = options.MinimumPollingInterval;
                }

                _logger.LogDebug("Next scan cycle in {Interval}", minInterval);
                await Task.Delay(minInterval, stoppingToken);
            }

            _logger.LogInformation("Background File Watcher Service stopped");
        }

        private async Task ScanAllWatchersAsync(CancellationToken ct)
        {
            var watchers = await _fileWatcher.GetAllWatchersAsync(ct);
            var enabledWatchers = watchers.Where(w => w.Enabled).ToList();

            if (!enabledWatchers.Any())
            {
                _logger.LogDebug("No enabled watchers found, skipping scan");
                return;
            }

            _logger.LogInformation("Scanning {Count} enabled watchers", enabledWatchers.Count);

            foreach (var watcher in enabledWatchers)
            {
                if (ct.IsCancellationRequested)
                    break;

                try
                {
                    _logger.LogDebug("Scanning watcher {WatcherId} ({Mode} mode) at path {WatchPath}",
                        watcher.WatcherId,
                        watcher.MultiTenantMode ? "multi-tenant" : "single-tenant",
                        watcher.WatchPath);

                    var result = await _fileWatcher.ScanNowAsync(watcher.WatcherId, ct);

                    if (result.FilesImported > 0 || result.FilesFailed > 0)
                    {
                        _logger.LogInformation(
                            "Watcher {WatcherId} scan completed: {Discovered} discovered, {Imported} imported, {Skipped} skipped, {Failed} failed, {Bytes} bytes",
                            watcher.WatcherId,
                            result.FilesDiscovered,
                            result.FilesImported,
                            result.FilesSkipped,
                            result.FilesFailed,
                            result.BytesImported);

                        if (result.Errors.Any())
                        {
                            foreach (var error in result.Errors.Take(5)) // Log first 5 errors
                            {
                                _logger.LogWarning("Watcher {WatcherId} error: {Error}", watcher.WatcherId, error);
                            }

                            if (result.Errors.Count > 5)
                            {
                                _logger.LogWarning("Watcher {WatcherId} had {Count} more errors", watcher.WatcherId, result.Errors.Count - 5);
                            }
                        }
                    }
                    else if (result.FilesDiscovered > 0)
                    {
                        _logger.LogDebug("Watcher {WatcherId} found {Count} files (all skipped)", watcher.WatcherId, result.FilesDiscovered);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error scanning watcher {WatcherId}", watcher.WatcherId);
                }
            }
        }
    }
}
