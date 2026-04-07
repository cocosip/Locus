using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Replays durable queue events into the existing metadata/quota projections in bounded batches.
    /// </summary>
    public class QueueEventProjectionService : BackgroundService, IQueueProjectionMaintenanceService
    {
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
        };

        private readonly IQueueEventJournal _journal;
        private readonly MetadataRepository _metadataRepository;
        private readonly ITenantQuotaManager _tenantQuotaManager;
        private readonly IDirectoryQuotaManager _directoryQuotaManager;
        private readonly IFileSystem _fileSystem;
        private readonly QueueEventJournalOptions _options;
        private readonly ILogger<QueueEventProjectionService> _logger;
        private readonly IStorageCleanupService? _storageCleanupService;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _tenantProjectionLocks;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueEventProjectionService"/> class.
        /// </summary>
        public QueueEventProjectionService(
            IQueueEventJournal journal,
            MetadataRepository metadataRepository,
            ITenantQuotaManager tenantQuotaManager,
            IDirectoryQuotaManager directoryQuotaManager,
            IFileSystem fileSystem,
            QueueEventJournalOptions options,
            ILogger<QueueEventProjectionService> logger,
            IStorageCleanupService? storageCleanupService = null)
        {
            _journal = journal ?? throw new ArgumentNullException(nameof(journal));
            _metadataRepository = metadataRepository ?? throw new ArgumentNullException(nameof(metadataRepository));
            _tenantQuotaManager = tenantQuotaManager ?? throw new ArgumentNullException(nameof(tenantQuotaManager));
            _directoryQuotaManager = directoryQuotaManager ?? throw new ArgumentNullException(nameof(directoryQuotaManager));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _storageCleanupService = storageCleanupService;
            _tenantProjectionLocks = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.Ordinal);
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "QueueEventProjectionService started (QueueDirectory={QueueDirectory}, MaxRecordsPerTenantPerCycle={MaxRecordsPerTenantPerCycle}, MaxTenantsPerCycle={MaxTenantsPerCycle})",
                _options.QueueDirectory,
                _options.MaxRecordsPerTenantPerCycle,
                _options.MaxTenantsPerCycle);

            while (!stoppingToken.IsCancellationRequested)
            {
                var cycleStopwatch = Stopwatch.StartNew();
                var processedAny = false;

                try
                {
                    var tenantIds = await _journal.GetTenantIdsAsync(stoppingToken).ConfigureAwait(false);
                    var tenantsProcessed = 0;
                    foreach (var tenantId in tenantIds)
                    {
                        stoppingToken.ThrowIfCancellationRequested();

                        if (tenantsProcessed >= _options.MaxTenantsPerCycle)
                            break;

                        if (cycleStopwatch.Elapsed >= _options.MaxProjectionTimePerCycle)
                            break;

                        var processedForTenant = await ProjectTenantAsync(tenantId, stoppingToken).ConfigureAwait(false);
                        processedAny |= processedForTenant;
                        tenantsProcessed++;
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Queue event projection cycle failed");
                }

                var delay = processedAny ? _options.BusyCycleDelay : _options.IdleCycleDelay;
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }

            _logger.LogInformation("QueueEventProjectionService stopped");
        }

        /// <inheritdoc/>
        public async Task<QueueProjectionTenantState> GetTenantStateAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);
            return await BuildTenantStateAsync(tenantId, ct).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<QueueProjectionTenantState> ReplayTenantAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var projectionLock = GetTenantProjectionLock(tenantId);
            await projectionLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                while (await ProjectTenantCoreAsync(tenantId, ct).ConfigureAwait(false))
                {
                }

                return await BuildTenantStateAsync(tenantId, ct).ConfigureAwait(false);
            }
            finally
            {
                projectionLock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task<QueueProjectionTenantState> SnapshotTenantAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var projectionLock = GetTenantProjectionLock(tenantId);
            await projectionLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cursorOffset = await GetEffectiveCursorOffsetAsync(tenantId, ct).ConfigureAwait(false);
                await SaveSnapshotAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
                return await BuildTenantStateAsync(tenantId, ct).ConfigureAwait(false);
            }
            finally
            {
                projectionLock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task<QueueProjectionTenantState> RebuildTenantAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var projectionLock = GetTenantProjectionLock(tenantId);
            await projectionLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await ClearTenantProjectionAsync(tenantId, ct).ConfigureAwait(false);
                var restoredCursorOffset = await TryRestoreSnapshotAsync(tenantId, ct).ConfigureAwait(false);
                var cursorOffset = await NormalizeCursorOffsetAsync(tenantId, restoredCursorOffset ?? 0, ct).ConfigureAwait(false);
                await SaveCursorAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);

                while (await ProjectTenantCoreAsync(tenantId, ct).ConfigureAwait(false))
                {
                }

                if (_storageCleanupService != null)
                {
                    try
                    {
                        await _storageCleanupService.ReconcileQuotaCountsAsync(tenantId, ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(
                            ex,
                            "Projection rebuild restored metadata for tenant {TenantId}, but quota reconciliation failed",
                            tenantId);
                    }
                }

                return await BuildTenantStateAsync(tenantId, ct).ConfigureAwait(false);
            }
            finally
            {
                projectionLock.Release();
            }
        }

        private async Task<bool> ProjectTenantAsync(string tenantId, CancellationToken ct)
        {
            ValidateTenantId(tenantId);

            var projectionLock = GetTenantProjectionLock(tenantId);
            await projectionLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                return await ProjectTenantCoreAsync(tenantId, ct).ConfigureAwait(false);
            }
            finally
            {
                projectionLock.Release();
            }
        }

        private async Task<bool> ProjectTenantCoreAsync(string tenantId, CancellationToken ct)
        {
            var offset = await LoadCursorAsync(tenantId, ct).ConfigureAwait(false);
            var batch = await _journal.ReadBatchAsync(tenantId, offset, _options.MaxRecordsPerTenantPerCycle, ct).ConfigureAwait(false);
            if (batch.Records.Count == 0)
            {
                if (batch.NextOffset > offset)
                {
                    await SaveCursorAsync(tenantId, batch.NextOffset, ct).ConfigureAwait(false);
                    return true;
                }

                return false;
            }

            foreach (var record in batch.Records)
            {
                ct.ThrowIfCancellationRequested();
                await ProjectRecordAsync(record, ct).ConfigureAwait(false);
            }

            var cursorOffset = batch.NextOffset;
            if (_options.EnableCompaction
                && batch.ReachedEndOfFile
                && await ShouldCompactTenantAsync(tenantId, cursorOffset, ct).ConfigureAwait(false))
            {
                await SaveSnapshotAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
                cursorOffset = await _journal.CompactAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
            }

            await SaveCursorAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
            return true;
        }

        private async Task ProjectRecordAsync(QueueEventRecord record, CancellationToken ct)
        {
            switch (record.EventType)
            {
                case QueueEventType.Accepted:
                    await ProjectAcceptedAsync(record, ct).ConfigureAwait(false);
                    return;
                case QueueEventType.ProcessingStarted:
                    await ProjectProcessingStartedAsync(record, ct).ConfigureAwait(false);
                    return;
                case QueueEventType.ProcessingFailed:
                    await ProjectProcessingFailedAsync(record, ct).ConfigureAwait(false);
                    return;
                case QueueEventType.ProcessingCompleted:
                    await ProjectProcessingCompletedAsync(record, ct).ConfigureAwait(false);
                    return;
                case QueueEventType.DeleteRequested:
                    await ProjectDeleteRequestedAsync(record, ct).ConfigureAwait(false);
                    return;
                case QueueEventType.DeleteSucceeded:
                    await ProjectDeleteSucceededAsync(record, ct).ConfigureAwait(false);
                    return;
                default:
                    return;
            }
        }

        private async Task ProjectAcceptedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await _metadataRepository.GetByFileKeyAsync(record.FileKey, ct).ConfigureAwait(false);
            if (existing != null)
                return;

            await EnsureMetadataExistsAsync(record, record.Status ?? FileProcessingStatus.Pending, ct).ConfigureAwait(false);
        }

        private async Task ProjectProcessingStartedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var eventProcessingStart = record.ProcessingStartTimeUtc ?? record.OccurredAtUtc;
            var existing = await _metadataRepository.GetByFileKeyAsync(record.FileKey, ct).ConfigureAwait(false);
            if (existing == null)
            {
                existing = await EnsureMetadataExistsAsync(record, FileProcessingStatus.Processing, ct).ConfigureAwait(false);
                if (existing == null)
                    return;
            }

            if (ShouldSkipStartedProjection(existing, eventProcessingStart))
                return;

            var updated = existing.Clone();
            updated.Status = FileProcessingStatus.Processing;
            updated.ProcessingStartTime = eventProcessingStart;
            updated.CompletedAt = null;
            updated.DeleteSucceededAt = null;
            updated.AvailableForProcessingAt = null;
            updated.LastError = record.ErrorMessage ?? updated.LastError;

            await _metadataRepository.AddOrUpdateDirectAsync(updated, ct).ConfigureAwait(false);
        }

        private async Task ProjectProcessingFailedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var status = record.Status ?? FileProcessingStatus.Pending;
            var existing = await _metadataRepository.GetByFileKeyAsync(record.FileKey, ct).ConfigureAwait(false);
            if (existing == null)
            {
                existing = await EnsureMetadataExistsAsync(record, status, ct).ConfigureAwait(false);
                if (existing == null)
                    return;
            }

            if (ShouldSkipFailedProjection(existing, record))
                return;

            var updated = existing.Clone();
            updated.Status = status;
            updated.ProcessingStartTime = null;
            updated.CompletedAt = null;
            updated.DeleteSucceededAt = null;
            updated.RetryCount = record.RetryCount ?? updated.RetryCount;
            updated.LastError = record.ErrorMessage;
            updated.LastFailedAt = record.OccurredAtUtc;
            updated.AvailableForProcessingAt = status == FileProcessingStatus.Pending
                ? record.AvailableForProcessingAtUtc
                : null;

            await _metadataRepository.AddOrUpdateDirectAsync(updated, ct).ConfigureAwait(false);
        }

        private async Task ProjectProcessingCompletedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await _metadataRepository.GetByFileKeyAsync(record.FileKey, ct).ConfigureAwait(false);
            if (existing == null)
            {
                existing = await EnsureMetadataExistsAsync(record, FileProcessingStatus.Completed, ct).ConfigureAwait(false);
                if (existing == null)
                    return;
            }

            if (ShouldSkipCompletedProjection(existing, record))
                return;

            var updated = existing.Clone();
            updated.Status = FileProcessingStatus.Completed;
            updated.ProcessingStartTime = null;
            updated.CompletedAt = record.OccurredAtUtc;
            updated.DeleteSucceededAt = null;
            updated.AvailableForProcessingAt = null;
            updated.LastError = null;

            await _metadataRepository.AddOrUpdateDirectAsync(updated, ct).ConfigureAwait(false);
        }

        private async Task ProjectDeleteRequestedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await _metadataRepository.GetByFileKeyAsync(record.FileKey, ct).ConfigureAwait(false);
            if (existing == null)
            {
                await EnsureMetadataExistsAsync(record, FileProcessingStatus.Completed, ct).ConfigureAwait(false);
                return;
            }

            if (existing.Status == FileProcessingStatus.Completed
                && existing.CompletedAt.HasValue
                && !existing.ProcessingStartTime.HasValue
                && !existing.AvailableForProcessingAt.HasValue
                && string.IsNullOrEmpty(existing.LastError))
            {
                return;
            }

            var updated = existing.Clone();
            updated.Status = FileProcessingStatus.Completed;
            updated.ProcessingStartTime = null;
            updated.CompletedAt = existing.CompletedAt ?? record.OccurredAtUtc;
            updated.DeleteSucceededAt = null;
            updated.AvailableForProcessingAt = null;
            updated.LastError = null;

            await _metadataRepository.AddOrUpdateDirectAsync(updated, ct).ConfigureAwait(false);
        }

        private async Task ProjectDeleteSucceededAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await _metadataRepository.GetByFileKeyAsync(record.FileKey, ct).ConfigureAwait(false);
            if (existing == null)
                return;

            var directoryPath = DirectoryPathNormalizer.Normalize(existing.DirectoryPath);
            var tenantQuotaDecremented = false;
            var directoryQuotaDecremented = false;

            try
            {
                await _directoryQuotaManager.DecrementFileCountAsync(existing.TenantId, directoryPath, default).ConfigureAwait(false);
                directoryQuotaDecremented = true;

                await _tenantQuotaManager.DecrementFileCountAsync(existing.TenantId, default).ConfigureAwait(false);
                tenantQuotaDecremented = true;

                var metadataRemoved = await _metadataRepository.RemoveDirectAsync(existing.TenantId, existing.FileKey, ct).ConfigureAwait(false);
                if (!metadataRemoved)
                    throw new InvalidOperationException($"Failed to remove projected metadata for deleted file {existing.FileKey}.");
            }
            catch
            {
                await RollbackDeleteSucceededProjectionAsync(existing.TenantId, directoryPath, tenantQuotaDecremented, directoryQuotaDecremented).ConfigureAwait(false);
                throw;
            }
        }

        private async Task RollbackDeleteSucceededProjectionAsync(
            string tenantId,
            string directoryPath,
            bool tenantQuotaDecremented,
            bool directoryQuotaDecremented)
        {
            if (tenantQuotaDecremented)
            {
                try
                {
                    await CompensateTenantQuotaIncrementAsync(tenantId).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to rollback projected tenant quota decrement for tenant {TenantId}", tenantId);
                }
            }

            if (directoryQuotaDecremented)
            {
                try
                {
                    await CompensateDirectoryQuotaIncrementAsync(tenantId, directoryPath).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to rollback projected directory quota decrement for tenant {TenantId}, directory {DirectoryPath}",
                        tenantId,
                        directoryPath);
                }
            }
        }

        private async Task<FileMetadata?> EnsureMetadataExistsAsync(
            QueueEventRecord record,
            FileProcessingStatus fallbackStatus,
            CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(record.PhysicalPath) || !_fileSystem.File.Exists(record.PhysicalPath))
                return null;

            var metadata = BuildMetadata(record, fallbackStatus);
            var tenantQuotaCompensated = false;
            var directoryQuotaCompensated = false;
            try
            {
                await CompensateTenantQuotaIncrementAsync(record.TenantId).ConfigureAwait(false);
                tenantQuotaCompensated = true;

                await CompensateDirectoryQuotaIncrementAsync(record.TenantId, metadata.DirectoryPath).ConfigureAwait(false);
                directoryQuotaCompensated = true;

                await _metadataRepository.AddOrUpdateDirectAsync(metadata, ct).ConfigureAwait(false);
                return metadata;
            }
            catch
            {
                await RollbackProjectionAsync(record.TenantId, metadata.DirectoryPath, tenantQuotaCompensated, directoryQuotaCompensated).ConfigureAwait(false);
                throw;
            }
        }

        private static FileMetadata BuildMetadata(QueueEventRecord record, FileProcessingStatus fallbackStatus)
        {
            var normalizedDirectoryPath = NormalizeDirectoryPath(record.DirectoryPath);

            return new FileMetadata
            {
                FileKey = record.FileKey,
                TenantId = record.TenantId,
                VolumeId = record.VolumeId ?? string.Empty,
                PhysicalPath = record.PhysicalPath ?? string.Empty,
                DirectoryPath = normalizedDirectoryPath,
                FileSize = record.FileSize ?? 0,
                CreatedAt = record.OccurredAtUtc,
                Status = record.Status ?? fallbackStatus,
                RetryCount = record.RetryCount ?? 0,
                ProcessingStartTime = record.ProcessingStartTimeUtc,
                CompletedAt = IsCompletedEvent(record) ? record.OccurredAtUtc : null,
                AvailableForProcessingAt = record.AvailableForProcessingAtUtc,
                LastError = record.ErrorMessage,
                LastFailedAt = record.EventType == QueueEventType.ProcessingFailed ? record.OccurredAtUtc : null,
                OriginalFileName = record.OriginalFileName,
                FileExtension = record.FileExtension,
            };
        }

        private bool ShouldSkipStartedProjection(FileMetadata existing, DateTime eventProcessingStart)
        {
            if (existing.ProcessingStartTime.HasValue && existing.ProcessingStartTime.Value >= eventProcessingStart)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingStarted projection for file {FileKey}: existing lease {ExistingLease} >= event lease {EventLease}",
                    existing.FileKey,
                    existing.ProcessingStartTime.Value,
                    eventProcessingStart);
                return true;
            }

            if (existing.LastFailedAt.HasValue && existing.LastFailedAt.Value >= eventProcessingStart)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingStarted projection for file {FileKey}: existing failure {LastFailedAt} >= event lease {EventLease}",
                    existing.FileKey,
                    existing.LastFailedAt.Value,
                    eventProcessingStart);
                return true;
            }

            if (existing.CompletedAt.HasValue && existing.CompletedAt.Value >= eventProcessingStart)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingStarted projection for file {FileKey}: existing completion {CompletedAt} >= event lease {EventLease}",
                    existing.FileKey,
                    existing.CompletedAt.Value,
                    eventProcessingStart);
                return true;
            }

            return false;
        }

        private bool ShouldSkipFailedProjection(FileMetadata existing, QueueEventRecord record)
        {
            var eventProcessingStart = record.ProcessingStartTimeUtc ?? record.OccurredAtUtc;
            if (existing.ProcessingStartTime.HasValue && existing.ProcessingStartTime.Value > eventProcessingStart)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingFailed projection for file {FileKey}: existing lease {ExistingLease} > event lease {EventLease}",
                    existing.FileKey,
                    existing.ProcessingStartTime.Value,
                    eventProcessingStart);
                return true;
            }

            if (existing.LastFailedAt.HasValue && existing.LastFailedAt.Value >= record.OccurredAtUtc)
            {
                _logger.LogDebug(
                    "Skipping duplicate/stale ProcessingFailed projection for file {FileKey}: existing failure {LastFailedAt} >= event failure {EventFailure}",
                    existing.FileKey,
                    existing.LastFailedAt.Value,
                    record.OccurredAtUtc);
                return true;
            }

            if (existing.CompletedAt.HasValue && existing.CompletedAt.Value >= record.OccurredAtUtc)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingFailed projection for file {FileKey}: existing completion {CompletedAt} >= event failure {EventFailure}",
                    existing.FileKey,
                    existing.CompletedAt.Value,
                    record.OccurredAtUtc);
                return true;
            }

            return false;
        }

        private bool ShouldSkipCompletedProjection(FileMetadata existing, QueueEventRecord record)
        {
            var eventProcessingStart = record.ProcessingStartTimeUtc ?? record.OccurredAtUtc;
            if (existing.ProcessingStartTime.HasValue && existing.ProcessingStartTime.Value > eventProcessingStart)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingCompleted projection for file {FileKey}: existing lease {ExistingLease} > event lease {EventLease}",
                    existing.FileKey,
                    existing.ProcessingStartTime.Value,
                    eventProcessingStart);
                return true;
            }

            if (existing.LastFailedAt.HasValue && existing.LastFailedAt.Value >= record.OccurredAtUtc)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingCompleted projection for file {FileKey}: existing failure {LastFailedAt} >= event completion {EventCompletion}",
                    existing.FileKey,
                    existing.LastFailedAt.Value,
                    record.OccurredAtUtc);
                return true;
            }

            if (existing.CompletedAt.HasValue && existing.CompletedAt.Value >= record.OccurredAtUtc)
            {
                _logger.LogDebug(
                    "Skipping duplicate/stale ProcessingCompleted projection for file {FileKey}: existing completion {CompletedAt} >= event completion {EventCompletion}",
                    existing.FileKey,
                    existing.CompletedAt.Value,
                    record.OccurredAtUtc);
                return true;
            }

            return false;
        }

        private static bool IsCompletedEvent(QueueEventRecord record)
        {
            return record.EventType == QueueEventType.ProcessingCompleted
                || record.EventType == QueueEventType.DeleteRequested
                || record.EventType == QueueEventType.DeleteSucceeded
                || record.Status == FileProcessingStatus.Completed;
        }

        private async Task<long> LoadCursorAsync(string tenantId, CancellationToken ct)
        {
            var path = GetCursorPath(tenantId);
            if (!_fileSystem.File.Exists(path))
                return 0;

            using (var stream = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var cursor = await JsonSerializer.DeserializeAsync<ProjectionCursor>(stream, JsonOptions, ct).ConfigureAwait(false);
                return cursor?.Offset ?? 0;
            }
        }

        private async Task SaveCursorAsync(string tenantId, long offset, CancellationToken ct)
        {
            var path = GetCursorPath(tenantId);
            var directory = _fileSystem.Path.GetDirectoryName(path);
            if (directory is string cursorDirectory && cursorDirectory.Length > 0)
            {
                if (!_fileSystem.Directory.Exists(cursorDirectory))
                    _fileSystem.Directory.CreateDirectory(cursorDirectory);
            }

            var tempPath = path + ".tmp";
            using (var stream = _fileSystem.File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await JsonSerializer.SerializeAsync(stream, new ProjectionCursor { Offset = offset }, JsonOptions, ct).ConfigureAwait(false);
                await stream.FlushAsync(ct).ConfigureAwait(false);
            }

            if (_fileSystem.File.Exists(path))
                _fileSystem.File.Delete(path);

            _fileSystem.File.Move(tempPath, path);
        }

        private SemaphoreSlim GetTenantProjectionLock(string tenantId)
        {
            return _tenantProjectionLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
        }

        private string GetCursorPath(string tenantId)
        {
            return _fileSystem.Path.Combine(_options.QueueDirectory, tenantId, "projector.cursor.json");
        }

        private string GetJournalPath(string tenantId)
        {
            return _fileSystem.Path.Combine(_options.QueueDirectory, tenantId, "queue.log");
        }

        private string GetSnapshotPath(string tenantId)
        {
            return _fileSystem.Path.Combine(_options.QueueDirectory, tenantId, "projection.snapshot.json");
        }

        private static string NormalizeDirectoryPath(string? directoryPath)
        {
            return DirectoryPathNormalizer.Normalize(directoryPath);
        }

        private async Task<bool> ShouldCompactTenantAsync(string tenantId, long cursorOffset, CancellationToken ct)
        {
            var tailOffset = await _journal.GetTailOffsetAsync(tenantId, ct).ConfigureAwait(false);
            var baseOffset = await _journal.GetBaseOffsetAsync(tenantId, ct).ConfigureAwait(false);
            return tailOffset >= cursorOffset
                && cursorOffset >= baseOffset
                && (cursorOffset - baseOffset) >= _options.MinBytesBeforeCompaction;
        }

        private async Task<QueueProjectionTenantState> BuildTenantStateAsync(string tenantId, CancellationToken ct)
        {
            var cursorOffset = await GetEffectiveCursorOffsetAsync(tenantId, ct).ConfigureAwait(false);
            var journalSizeBytes = await _journal.GetTailOffsetAsync(tenantId, ct).ConfigureAwait(false);
            var snapshot = await LoadSnapshotAsync(tenantId, ct).ConfigureAwait(false);
            return new QueueProjectionTenantState
            {
                TenantId = tenantId,
                CursorOffset = cursorOffset,
                JournalSizeBytes = journalSizeBytes,
                LagBytes = Math.Max(0, journalSizeBytes - cursorOffset),
                HasSnapshot = snapshot != null,
                SnapshotCreatedAtUtc = snapshot?.CreatedAtUtc,
                SnapshotCursorOffset = snapshot?.CursorOffset ?? 0,
                SnapshotFileCount = snapshot?.Files.Count ?? 0
            };
        }

        private async Task<long> GetEffectiveCursorOffsetAsync(string tenantId, CancellationToken ct)
        {
            var cursorOffset = await LoadCursorAsync(tenantId, ct).ConfigureAwait(false);
            return await NormalizeCursorOffsetAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
        }

        private async Task<long> NormalizeCursorOffsetAsync(string tenantId, long cursorOffset, CancellationToken ct)
        {
            var baseOffset = await _journal.GetBaseOffsetAsync(tenantId, ct).ConfigureAwait(false);
            return cursorOffset < baseOffset ? baseOffset : cursorOffset;
        }

        private async Task SaveSnapshotAsync(string tenantId, long cursorOffset, CancellationToken ct)
        {
            var snapshotPath = GetSnapshotPath(tenantId);
            var snapshotDirectory = _fileSystem.Path.GetDirectoryName(snapshotPath);
            if (snapshotDirectory is string directory && directory.Length > 0 && !_fileSystem.Directory.Exists(directory))
                _fileSystem.Directory.CreateDirectory(directory);

            var metadata = await _metadataRepository.GetByTenantAsync(tenantId, ct).ConfigureAwait(false);
            var snapshot = new ProjectionSnapshot
            {
                TenantId = tenantId,
                CreatedAtUtc = DateTime.UtcNow,
                CursorOffset = cursorOffset,
                Files = metadata
                    .OrderBy(item => item.CreatedAt)
                    .ThenBy(item => item.FileKey, StringComparer.Ordinal)
                    .Select(item => item.Clone())
                    .ToList()
            };

            var tempPath = snapshotPath + ".tmp";
            using (var stream = _fileSystem.File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await JsonSerializer.SerializeAsync(stream, snapshot, JsonOptions, ct).ConfigureAwait(false);
                await stream.FlushAsync(ct).ConfigureAwait(false);
            }

            if (_fileSystem.File.Exists(snapshotPath))
                _fileSystem.File.Delete(snapshotPath);

            _fileSystem.File.Move(tempPath, snapshotPath);
        }

        private async Task<long?> TryRestoreSnapshotAsync(string tenantId, CancellationToken ct)
        {
            var snapshot = await LoadSnapshotAsync(tenantId, ct).ConfigureAwait(false);
            if (snapshot == null)
                return null;

            foreach (var file in snapshot.Files)
            {
                ct.ThrowIfCancellationRequested();
                await _metadataRepository.AddOrUpdateDirectAsync(file, ct).ConfigureAwait(false);
            }

            return snapshot.CursorOffset;
        }

        private async Task<ProjectionSnapshot?> LoadSnapshotAsync(string tenantId, CancellationToken ct)
        {
            var snapshotPath = GetSnapshotPath(tenantId);
            if (!_fileSystem.File.Exists(snapshotPath))
                return null;

            try
            {
                using (var stream = _fileSystem.File.Open(snapshotPath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    var snapshot = await JsonSerializer.DeserializeAsync<ProjectionSnapshot>(stream, JsonOptions, ct).ConfigureAwait(false);
                    if (snapshot == null || !string.Equals(snapshot.TenantId, tenantId, StringComparison.Ordinal))
                        return null;

                    snapshot.Files ??= new System.Collections.Generic.List<FileMetadata>();
                    return snapshot;
                }
            }
            catch (Exception ex) when (ex is IOException || ex is JsonException || ex is UnauthorizedAccessException)
            {
                _logger.LogWarning(ex, "Failed to load projection snapshot for tenant {TenantId}", tenantId);
                return null;
            }
        }

        private async Task ClearTenantProjectionAsync(string tenantId, CancellationToken ct)
        {
            var metadataEntries = await _metadataRepository.GetByTenantAsync(tenantId, ct).ConfigureAwait(false);
            foreach (var metadata in metadataEntries)
            {
                ct.ThrowIfCancellationRequested();
                await _metadataRepository.RemoveDirectAsync(tenantId, metadata.FileKey, ct).ConfigureAwait(false);
            }
        }

        private static void ValidateTenantId(string tenantId)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));
        }

        private async Task CompensateTenantQuotaIncrementAsync(string tenantId)
        {
            if (_tenantQuotaManager is ITenantQuotaCompensationManager tenantQuotaCompensationManager)
            {
                await tenantQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, default).ConfigureAwait(false);
                return;
            }

            await _tenantQuotaManager.IncrementFileCountAsync(tenantId, default).ConfigureAwait(false);
        }

        private async Task CompensateDirectoryQuotaIncrementAsync(string tenantId, string directoryPath)
        {
            if (_directoryQuotaManager is IDirectoryQuotaCompensationManager directoryQuotaCompensationManager)
            {
                await directoryQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
                return;
            }

            await _directoryQuotaManager.IncrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
        }

        private async Task RollbackProjectionAsync(
            string tenantId,
            string directoryPath,
            bool tenantQuotaCompensated,
            bool directoryQuotaCompensated)
        {
            if (directoryQuotaCompensated)
            {
                try
                {
                    await _directoryQuotaManager.DecrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to rollback projected directory quota increment for tenant {TenantId}, directory {DirectoryPath}",
                        tenantId,
                        directoryPath);
                }
            }

            if (tenantQuotaCompensated)
            {
                try
                {
                    await _tenantQuotaManager.DecrementFileCountAsync(tenantId, default).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to rollback projected tenant quota increment for tenant {TenantId}",
                        tenantId);
                }
            }
        }

        private sealed class ProjectionCursor
        {
            public long Offset { get; set; }
        }

        private sealed class ProjectionSnapshot
        {
            public string TenantId { get; set; } = string.Empty;

            public DateTime CreatedAtUtc { get; set; }

            public long CursorOffset { get; set; }

            public System.Collections.Generic.List<FileMetadata> Files { get; set; } = new System.Collections.Generic.List<FileMetadata>();
        }
    }
}
