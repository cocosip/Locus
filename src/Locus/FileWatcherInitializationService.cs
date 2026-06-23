using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus
{
    /// <summary>
    /// Background service that initializes pre-configured file watchers on application startup.
    /// </summary>
    internal class FileWatcherInitializationService : BackgroundService
    {
        private readonly IFileWatcher _fileWatcher;
        private readonly ILogger<FileWatcherInitializationService> _logger;
        private readonly LocusOptions _options;
        private readonly LocusStartupCoordinator _startupCoordinator;

        public FileWatcherInitializationService(
            IFileWatcher fileWatcher,
            ILogger<FileWatcherInitializationService> logger,
            LocusOptions options,
            LocusStartupCoordinator startupCoordinator)
        {
            _fileWatcher = fileWatcher ?? throw new ArgumentNullException(nameof(fileWatcher));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _startupCoordinator = startupCoordinator ?? throw new ArgumentNullException(nameof(startupCoordinator));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Initializing file watchers...");

            try
            {
                await _startupCoordinator.WaitForRuntimeReadyAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            var registeredCount = 0;

            try
            {
                foreach (var watcherConfig in _options.FileWatchers)
                {
                    try
                    {
                        // Skip disabled watchers during initialization
                        if (!watcherConfig.Enabled)
                        {
                            _logger.LogInformation("Skipping disabled file watcher: {WatcherId}", watcherConfig.WatcherId);
                            continue;
                        }

                        _logger.LogDebug("Initializing file watcher: {WatcherId}, Enabled: {Enabled}, WatchPath: {WatchPath}, TenantId: {TenantId}, MultiTenantMode: {MultiTenantMode}",
                            watcherConfig.WatcherId, watcherConfig.Enabled, watcherConfig.WatchPath, watcherConfig.TenantId, watcherConfig.MultiTenantMode);

                        // Register the file watcher configuration
                        await _fileWatcher.RegisterWatcherAsync(watcherConfig, stoppingToken);

                        _logger.LogInformation("Registered file watcher: {WatcherId} monitoring {WatchPath} (MultiTenant: {MultiTenant})",
                            watcherConfig.WatcherId,
                            watcherConfig.WatchPath,
                            watcherConfig.MultiTenantMode);

                        // Perform initial scan
                        var importedCount = await _fileWatcher.ScanNowAsync(watcherConfig.WatcherId, stoppingToken);
                        _logger.LogInformation("Initial scan completed for watcher {WatcherId}: {Count} files imported",
                            watcherConfig.WatcherId, importedCount);

                        registeredCount++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,
                            "Failed to initialize file watcher: {WatcherId}. Exception Type: {ExceptionType}, Message: {Message}, InnerException: {InnerException}",
                            watcherConfig.WatcherId,
                            ex.GetType().FullName,
                            ex.Message,
                            ex.InnerException?.Message ?? "None");
                        // Continue with other watchers
                    }
                }

                _logger.LogInformation("File watcher initialization completed. {EnabledCount}/{TotalCount} watchers registered.",
                    registeredCount, _options.FileWatchers.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize file watchers");
                throw;
            }
        }
    }
}
