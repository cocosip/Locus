using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
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
        private readonly ConcurrentDictionary<string, DateTime> _nextScanDueByWatcherId;
        private readonly ConcurrentDictionary<string, byte> _warnedIntervalWatcherIds;
        private readonly ConcurrentDictionary<string, int> _recentWatcherErrorHashes;

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
            _nextScanDueByWatcherId = new ConcurrentDictionary<string, DateTime>(StringComparer.Ordinal);
            _warnedIntervalWatcherIds = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
            _recentWatcherErrorHashes = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken = default)
        {
            _logger.LogInformation("Background File Watcher Service started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var options = await _optionsManager.GetOptionsAsync(stoppingToken);
                    if (!options.Enabled)
                    {
                        _logger.LogDebug("File watcher service is globally disabled, checking again in {Interval}",
                            options.DisabledCheckInterval);
                        // Clear schedule so we recalculate next-due times when re-enabled.
                        // Dedup dictionaries are intentionally NOT cleared: we want to
                        // avoid re-emitting warnings for already-seen conditions.
                        _nextScanDueByWatcherId.Clear();
                        await Task.Delay(options.DisabledCheckInterval, stoppingToken);
                        continue;
                    }

                    var delay = await ScanDueWatchersAsync(options, stoppingToken);
                    _logger.LogDebug("Next watcher scan cycle in {Interval}", delay);
                    await Task.Delay(delay, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error during file watcher scan cycle");
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }

            _logger.LogInformation("Background File Watcher Service stopped");
        }

        private async Task<TimeSpan> ScanDueWatchersAsync(FileWatcherOptions options, CancellationToken ct = default)
        {
            var watchers = await _fileWatcher.GetAllWatchersAsync(ct);
            var enabledWatchers = watchers.Where(w => w.Enabled).ToList();

            if (!enabledWatchers.Any())
            {
                _logger.LogDebug("No enabled watchers found, skipping scan");
                _nextScanDueByWatcherId.Clear();
                return options.DefaultPollingInterval;
            }

            PruneSchedule(enabledWatchers);

            var now = DateTime.UtcNow;
            var dueWatchers = enabledWatchers
                .Where(w => now >= GetNextDueUtc(w, options, now))
                .ToList();

            if (!dueWatchers.Any())
                return GetDelayUntilNextDue(enabledWatchers, options, now);

            _logger.LogDebug("Scanning {DueCount} due watchers (enabled total: {EnabledCount})",
                dueWatchers.Count, enabledWatchers.Count);

            var maxParallel = Math.Max(1, options.MaxParallelWatcherScans);
            if (maxParallel == 1 || dueWatchers.Count == 1)
            {
                foreach (var watcher in dueWatchers)
                {
                    if (ct.IsCancellationRequested)
                        break;
                    await ScanWatcherAsync(watcher, options, ct);
                }
            }
            else
            {
                var queue = Channel.CreateBounded<FileWatcherConfiguration>(new BoundedChannelOptions(dueWatchers.Count)
                {
                    SingleReader = false,
                    SingleWriter = true,
                    FullMode = BoundedChannelFullMode.Wait,
                    AllowSynchronousContinuations = false
                });

                foreach (var watcher in dueWatchers)
                {
                    if (!queue.Writer.TryWrite(watcher))
                        await queue.Writer.WriteAsync(watcher, ct);
                }
                queue.Writer.Complete();

                var workerCount = Math.Min(maxParallel, dueWatchers.Count);
                var workers = Enumerable.Range(0, workerCount).Select(_ => Task.Run(async () =>
                {
                    while (!ct.IsCancellationRequested && await queue.Reader.WaitToReadAsync(ct))
                    {
                        if (!queue.Reader.TryRead(out var watcher))
                            continue;

                        await ScanWatcherAsync(watcher, options, ct);
                    }
                }, ct)).ToArray();

                await Task.WhenAll(workers);
            }

            return GetDelayUntilNextDue(enabledWatchers, options, DateTime.UtcNow);
        }

        private void PruneSchedule(List<FileWatcherConfiguration> enabledWatchers)
        {
            var enabledIds = new HashSet<string>(enabledWatchers.Select(w => w.WatcherId), StringComparer.Ordinal);
            var staleIds = _nextScanDueByWatcherId.Keys.Where(id => !enabledIds.Contains(id)).ToList();

            foreach (var watcherId in staleIds)
            {
                _nextScanDueByWatcherId.TryRemove(watcherId, out _);
                _recentWatcherErrorHashes.TryRemove(watcherId, out _);
                _warnedIntervalWatcherIds.TryRemove(watcherId, out _);
            }
        }

        private DateTime GetNextDueUtc(FileWatcherConfiguration watcher, FileWatcherOptions options, DateTime now)
        {
            var interval = NormalizePollingInterval(watcher, options);
            var maxAllowedDue = now.Add(interval);

            var dueUtc = _nextScanDueByWatcherId.GetOrAdd(watcher.WatcherId, now);
            if (dueUtc > maxAllowedDue)
            {
                dueUtc = maxAllowedDue;
                _nextScanDueByWatcherId[watcher.WatcherId] = dueUtc;
            }

            return dueUtc;
        }

        private TimeSpan GetDelayUntilNextDue(
            List<FileWatcherConfiguration> enabledWatchers,
            FileWatcherOptions options,
            DateTime now)
        {
            var nextDueUtc = enabledWatchers
                .Select(w => GetNextDueUtc(w, options, now))
                .DefaultIfEmpty(now.Add(options.DefaultPollingInterval))
                .Min();

            var delay = nextDueUtc - now;
            return delay > TimeSpan.Zero ? delay : TimeSpan.FromMilliseconds(200);
        }

        private TimeSpan NormalizePollingInterval(FileWatcherConfiguration watcher, FileWatcherOptions options)
        {
            var interval = watcher.PollingInterval > TimeSpan.Zero
                ? watcher.PollingInterval
                : options.DefaultPollingInterval;

            if (interval < options.MinimumPollingInterval)
            {
                if (_warnedIntervalWatcherIds.TryAdd(watcher.WatcherId, 0))
                {
                    _logger.LogWarning(
                        "Watcher {WatcherId} polling interval {Interval} is below minimum {MinInterval}; using minimum",
                        watcher.WatcherId, interval, options.MinimumPollingInterval);
                }
                else
                {
                    _logger.LogDebug(
                        "Watcher {WatcherId} polling interval {Interval} is below minimum {MinInterval}; using minimum",
                        watcher.WatcherId, interval, options.MinimumPollingInterval);
                }
                interval = options.MinimumPollingInterval;
            }

            if (interval > options.MaximumPollingInterval)
            {
                if (_warnedIntervalWatcherIds.TryAdd(watcher.WatcherId, 0))
                {
                    _logger.LogWarning(
                        "Watcher {WatcherId} polling interval {Interval} is above maximum {MaxInterval}; using maximum",
                        watcher.WatcherId, interval, options.MaximumPollingInterval);
                }
                else
                {
                    _logger.LogDebug(
                        "Watcher {WatcherId} polling interval {Interval} is above maximum {MaxInterval}; using maximum",
                        watcher.WatcherId, interval, options.MaximumPollingInterval);
                }
                interval = options.MaximumPollingInterval;
            }

            return interval;
        }

        private async Task ScanWatcherAsync(
            FileWatcherConfiguration watcher,
            FileWatcherOptions options,
            CancellationToken ct = default)
        {
            try
            {
                _logger.LogDebug("Scanning watcher {WatcherId} ({Mode} mode) at path {WatchPath}",
                    watcher.WatcherId,
                    watcher.MultiTenantMode ? "multi-tenant" : "single-tenant",
                    watcher.WatchPath);

                var result = await _fileWatcher.ScanNowAsync(watcher, ct);

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
                        var distinctErrors = result.Errors.Distinct().ToList();
                        var errorHash = GetErrorHash(distinctErrors);

                        var isRepeat = false;
                        _recentWatcherErrorHashes.AddOrUpdate(
                            watcher.WatcherId,
                            _ => { isRepeat = false; return errorHash; },
                            (_, previousHash) =>
                            {
                                isRepeat = previousHash == errorHash;
                                return isRepeat ? previousHash : errorHash;
                            });

                        var level = isRepeat ? LogLevel.Debug : LogLevel.Warning;
                        var displayErrors = distinctErrors.Take(5).ToList();
                        foreach (var error in displayErrors)
                        {
                            _logger.Log(level, "Watcher {WatcherId} error: {Error}", watcher.WatcherId, error);
                        }

                        if (distinctErrors.Count > 5)
                        {
                            _logger.Log(level, "Watcher {WatcherId} had {Count} more distinct errors",
                                watcher.WatcherId, distinctErrors.Count - 5);
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
            finally
            {
                var interval = NormalizePollingInterval(watcher, options);
                _nextScanDueByWatcherId[watcher.WatcherId] = DateTime.UtcNow.Add(interval);
            }
        }

        private static int GetErrorHash(IList<string> errors)
        {
            // FNV-1a per string for platform-stable hashing, XOR for order independence.
            // string.GetHashCode() is not used because it differs across .NET runtimes.
            var hash = 0;
            foreach (var error in errors)
            {
                if (error == null) continue;
                var errorHash = unchecked((int)0x811C9DC5);
                foreach (char c in error)
                    errorHash = unchecked((errorHash ^ c) * (int)0x01000193);
                hash ^= errorHash;
            }
            // Mix in the count so different numbers of errors produce different hashes
            hash ^= errors.Count * 31;
            return hash;
        }
    }
}
