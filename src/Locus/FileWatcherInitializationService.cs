using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus
{
    /// <summary>
    /// Background service that initializes pre-configured file watchers on application startup.
    /// </summary>
    internal class FileWatcherInitializationService : IHostedService
    {
        private readonly IFileWatcher _fileWatcher;
        private readonly ILogger<FileWatcherInitializationService> _logger;
        private readonly LocusOptions _options;

        public FileWatcherInitializationService(
            IFileWatcher fileWatcher,
            ILogger<FileWatcherInitializationService> logger,
            LocusOptions options)
        {
            _fileWatcher = fileWatcher ?? throw new ArgumentNullException(nameof(fileWatcher));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Initializing file watchers...");

            try
            {
                foreach (var watcherConfig in _options.FileWatchers)
                {
                    try
                    {
                        // Register the file watcher configuration
                        await _fileWatcher.RegisterWatcherAsync(watcherConfig, cancellationToken);

                        _logger.LogInformation("Registered file watcher: {WatcherId} monitoring {WatchPath} (MultiTenant: {MultiTenant})",
                            watcherConfig.WatcherId,
                            watcherConfig.WatchPath,
                            watcherConfig.MultiTenantMode);

                        // Perform initial scan if enabled
                        if (watcherConfig.Enabled)
                        {
                            var importedCount = await _fileWatcher.ScanNowAsync(watcherConfig.WatcherId, cancellationToken);
                            _logger.LogInformation("Initial scan completed for watcher {WatcherId}: {Count} files imported",
                                watcherConfig.WatcherId, importedCount);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to initialize file watcher: {WatcherId}", watcherConfig.WatcherId);
                        // Continue with other watchers
                    }
                }

                _logger.LogInformation("File watcher initialization completed. {Count} watchers registered.", _options.FileWatchers.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize file watchers");
                throw;
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
