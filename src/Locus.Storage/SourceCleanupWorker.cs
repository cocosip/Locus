using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Processes source-file cleanup independently from the import scan.
    /// </summary>
    public sealed class SourceCleanupWorker : BackgroundService
    {
        private readonly ISourceCleanupStore _store;
        private readonly IFileSystem _fileSystem;
        private readonly SourceCleanupOptions _options;
        private readonly ILogger<SourceCleanupWorker> _logger;
        private readonly LocusStartupCoordinator _startupCoordinator;
        private readonly IFileWatcher? _fileWatcher;
        private readonly IFileWatcherOptionsManager? _fileWatcherOptionsManager;

        /// <summary>
        /// Initializes a new source cleanup worker.
        /// </summary>
        /// <param name="store">The durable source cleanup store.</param>
        /// <param name="fileSystem">The file system used for source operations.</param>
        /// <param name="options">The source cleanup worker options.</param>
        /// <param name="logger">The worker logger.</param>
        /// <param name="startupCoordinator">Coordinates startup with the storage runtime.</param>
        /// <param name="fileWatcher">Provides the current watcher configurations.</param>
        /// <param name="fileWatcherOptionsManager">Provides the global watcher options.</param>
        public SourceCleanupWorker(
            ISourceCleanupStore store,
            IFileSystem fileSystem,
            SourceCleanupOptions options,
            ILogger<SourceCleanupWorker> logger,
            LocusStartupCoordinator? startupCoordinator = null,
            IFileWatcher? fileWatcher = null,
            IFileWatcherOptionsManager? fileWatcherOptionsManager = null)
        {
            _store = store ?? throw new ArgumentNullException(nameof(store));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _startupCoordinator = startupCoordinator ?? LocusStartupCoordinator.Ready;
            _fileWatcher = fileWatcher;
            _fileWatcherOptionsManager = fileWatcherOptionsManager;
        }

        /// <inheritdoc />
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Yield();
            try
            {
                await _startupCoordinator.WaitForRuntimeReadyAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await ProcessDueJobsAsync(stoppingToken).ConfigureAwait(false);
                    await Task.Delay(NormalizePollingInterval(), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Source cleanup worker cycle failed");
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).ConfigureAwait(false);
                }
            }
        }

        internal async Task ProcessDueJobsAsync(CancellationToken ct)
        {
            if (!_options.Enabled || !await IsRuntimeEnabledAsync(ct).ConfigureAwait(false))
                return;

            var jobs = await _store.GetDueAsync(DateTime.UtcNow, Math.Max(1, _options.MaxConcurrentActions), ct)
                .ConfigureAwait(false);
            if (jobs.Count == 0)
                return;

            using (var semaphore = new SemaphoreSlim(Math.Max(1, _options.MaxConcurrentActions)))
            {
                var tasks = jobs.Select(job => ProcessClaimedJobAsync(job, semaphore, ct)).ToArray();
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }

        private async Task<bool> IsRuntimeEnabledAsync(CancellationToken ct)
        {
            // Direct worker construction remains useful for isolated callers. Production
            // registration supplies both services and therefore applies the runtime gate.
            if (_fileWatcher == null || _fileWatcherOptionsManager == null)
                return true;

            var options = await _fileWatcherOptionsManager.GetOptionsAsync(ct).ConfigureAwait(false);
            if (!options.Enabled)
                return false;

            var watchers = await _fileWatcher.GetAllWatchersAsync(ct).ConfigureAwait(false);
            return watchers != null && watchers.Any(watcher => watcher.Enabled);
        }

        private async Task ProcessClaimedJobAsync(SourceCleanupJob job, SemaphoreSlim semaphore, CancellationToken ct)
        {
            await semaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var now = DateTime.UtcNow;
                if (!await _store.TryClaimAsync(job.Id, now, now.AddMinutes(5), ct).ConfigureAwait(false))
                    return;

                try
                {
                    if (job.State == SourceCleanupJobState.MovePending)
                    {
                        if (await TryMoveToFailureDirectoryAsync(job, ct).ConfigureAwait(false))
                            await _store.RemoveAsync(job.Id, ct).ConfigureAwait(false);
                        else
                            await RecordFailureAsync(job, "Failed to move source file to the failure directory.", movePending: true, ct).ConfigureAwait(false);
                        return;
                    }

                    if (job.Action == SourceCleanupJobAction.Keep)
                        return;

                    if (!_fileSystem.File.Exists(job.SourcePath))
                    {
                        await _store.RemoveAsync(job.Id, ct).ConfigureAwait(false);
                        return;
                    }

                    if (!FingerprintMetadataMatches(job))
                    {
                        await _store.RemoveAsync(job.Id, ct).ConfigureAwait(false);
                        return;
                    }

                    if (job.Action == SourceCleanupJobAction.Move)
                        await MoveAsync(job.SourcePath, job.MoveTargetPath, ct).ConfigureAwait(false);
                    else
                        _fileSystem.File.Delete(job.SourcePath);

                    await _store.RemoveAsync(job.Id, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    await RecordFailureAsync(job, ex.Message, movePending: false, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        private async Task RecordFailureAsync(SourceCleanupJob job, string error, bool movePending, CancellationToken ct)
        {
            job.AttemptCount++;
            job.LastError = error;
            job.UpdatedAtUtc = DateTime.UtcNow;

            if (job.AttemptCount >= Math.Max(1, job.MaxAttempts))
            {
                if (!string.IsNullOrWhiteSpace(job.FailureDirectory)
                    && !await TryMoveToFailureDirectoryAsync(job, ct).ConfigureAwait(false))
                {
                    job.State = SourceCleanupJobState.MovePending;
                    job.NextAttemptUtc = DateTime.UtcNow.Add(CalculateDelay(job));
                }
                else if (!string.IsNullOrWhiteSpace(job.FailureDirectory))
                {
                    await _store.RemoveAsync(job.Id, ct).ConfigureAwait(false);
                    return;
                }
                else
                {
                    job.State = SourceCleanupJobState.Failed;
                    job.NextAttemptUtc = null;
                }
            }
            else
            {
                job.State = movePending ? SourceCleanupJobState.MovePending : SourceCleanupJobState.Retrying;
                job.NextAttemptUtc = DateTime.UtcNow.Add(CalculateDelay(job));
            }

            await _store.UpdateAsync(job, ct).ConfigureAwait(false);
            _logger.LogWarning(
                "Source cleanup failed for {SourcePath}; state={State}, attempt={AttemptCount}, next={NextAttemptUtc}",
                job.SourcePath, job.State, job.AttemptCount, job.NextAttemptUtc);
        }

        private async Task<bool> TryMoveToFailureDirectoryAsync(SourceCleanupJob job, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            if (!_fileSystem.File.Exists(job.SourcePath))
                return true;

            // A producer may have reused the path while this job was waiting. Treat the
            // old job as complete rather than moving the replacement file to quarantine.
            if (!FingerprintMetadataMatches(job))
                return true;

            if (string.IsNullOrWhiteSpace(job.FailureDirectory))
                return false;

            var failureDirectory = job.FailureDirectory!;
            var directory = _fileSystem.Path.Combine(failureDirectory, job.WatcherId);
            var fileName = _fileSystem.Path.GetFileName(job.SourcePath);
            var target = _fileSystem.Path.Combine(directory, fileName);
            if (_fileSystem.File.Exists(target))
                target = _fileSystem.Path.Combine(directory, job.FileKey + "-" + fileName);

            try
            {
                _fileSystem.Directory.CreateDirectory(directory);
                _fileSystem.File.Move(job.SourcePath, target);
                return true;
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }
            catch (ArgumentException)
            {
                return false;
            }
            catch (NotSupportedException)
            {
                return false;
            }
        }

        private Task MoveAsync(string sourcePath, string? targetPath, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            if (string.IsNullOrWhiteSpace(targetPath))
                throw new InvalidOperationException("Move cleanup action has no target path.");

            var destinationPath = targetPath!;
            var directory = _fileSystem.Path.GetDirectoryName(destinationPath);
            if (directory is string directoryPath)
                _fileSystem.Directory.CreateDirectory(directoryPath);
            _fileSystem.File.Move(sourcePath, destinationPath);
            return Task.CompletedTask;
        }

        private bool FingerprintMetadataMatches(SourceCleanupJob job)
        {
            var parts = job.Fingerprint.Split(':');
            if (parts.Length < 4 || !parts[0].Equals("fp", StringComparison.Ordinal)
                || !long.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var size)
                || !long.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var lastWriteTicks))
                return false;

            try
            {
                var info = _fileSystem.FileInfo.New(job.SourcePath);
                if (info.Length != size || info.LastWriteTimeUtc.Ticks != lastWriteTicks)
                    return false;

                if (string.Equals(parts[1], "v1", StringComparison.Ordinal))
                    return true;

                if (parts.Length < 5
                    || !long.TryParse(parts[4], NumberStyles.Integer, CultureInfo.InvariantCulture, out var creationTicks))
                    return false;

                var creationTime = info.CreationTimeUtc == DateTime.MinValue
                    ? info.LastWriteTimeUtc
                    : info.CreationTimeUtc;
                if (creationTime.Ticks != creationTicks)
                    return false;

                if (string.Equals(parts[1], "v2", StringComparison.Ordinal))
                    return true;

                if (!string.Equals(parts[1], "v3", StringComparison.Ordinal)
                    || parts.Length != 6
                    || string.IsNullOrWhiteSpace(parts[5]))
                    return false;

                return string.Equals(parts[5], ComputeContentSampleHash(job.SourcePath, info.Length), StringComparison.Ordinal);
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }
        }

        private string ComputeContentSampleHash(string filePath, long fileSize)
        {
            const int sampleSize = 4 * 1024;
            using (var source = _fileSystem.File.OpenRead(filePath))
            using (var samples = new MemoryStream(sampleSize * 3))
            {
                var positions = new[]
                {
                    0L,
                    Math.Max(0L, (fileSize - sampleSize) / 2L),
                    Math.Max(0L, fileSize - sampleSize)
                };
                var buffer = new byte[sampleSize];
                var visitedPositions = new HashSet<long>();

                foreach (var position in positions)
                {
                    if (!visitedPositions.Add(position))
                        continue;

                    source.Position = position;
                    var remaining = (int)Math.Min(sampleSize, Math.Max(0L, fileSize - position));
                    while (remaining > 0)
                    {
                        var read = source.Read(buffer, 0, Math.Min(buffer.Length, remaining));
                        if (read == 0)
                            break;

                        samples.Write(buffer, 0, read);
                        remaining -= read;
                    }
                }

                using (var sha256 = SHA256.Create())
                    return Convert.ToBase64String(sha256.ComputeHash(samples.ToArray()));
            }
        }

        private TimeSpan CalculateDelay(SourceCleanupJob job)
        {
            var initial = job.RetryInitialDelay > TimeSpan.Zero ? job.RetryInitialDelay : TimeSpan.Zero;
            var maximum = job.RetryMaxDelay > TimeSpan.Zero ? job.RetryMaxDelay : initial;
            if (initial <= TimeSpan.Zero)
                return TimeSpan.Zero;

            var exponent = Math.Min(Math.Max(0, job.AttemptCount - 1), 30);
            var ticks = initial.Ticks > long.MaxValue / (1L << exponent)
                ? long.MaxValue
                : initial.Ticks * (1L << exponent);
            return TimeSpan.FromTicks(Math.Min(ticks, maximum.Ticks));
        }

        private TimeSpan NormalizePollingInterval()
        {
            return _options.PollingInterval > TimeSpan.Zero
                ? _options.PollingInterval
                : TimeSpan.FromSeconds(5);
        }
    }
}
