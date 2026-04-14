using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.IO;
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
        private static readonly EventId SequenceGapDetectedEventId = new EventId(4201, nameof(SequenceGapDetectedEventId));
        private static readonly EventId SequenceGapRecoveryFailedEventId = new EventId(4202, nameof(SequenceGapRecoveryFailedEventId));
        private static readonly TimeSpan CursorPersistenceDebounce = TimeSpan.FromSeconds(1);
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
        };

        private readonly IQueueEventJournal _journal;
        private readonly IQueueProjectionStore _projectionStore;
        private readonly IQueueProjectionBatchStore? _projectionBatchStore;
        private readonly ITenantQuotaManager _tenantQuotaManager;
        private readonly IDirectoryQuotaManager _directoryQuotaManager;
        private readonly IFileSystem _fileSystem;
        private readonly QueueEventJournalOptions _options;
        private readonly ILogger<QueueEventProjectionService> _logger;
        private readonly IStorageCleanupService? _storageCleanupService;
        private readonly IQuotaProjectionMaintenanceStore? _quotaMaintenanceStore;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _tenantProjectionLocks;
        private readonly ConcurrentDictionary<string, CachedProjectionCursor> _cursorCache;
        private readonly AsyncLocal<IQueueProjectionBatch?> _currentProjectionBatch;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueEventProjectionService"/> class.
        /// </summary>
        public QueueEventProjectionService(
            IQueueEventJournal journal,
            MetadataRepository? metadataRepository,
            ITenantQuotaManager tenantQuotaManager,
            IDirectoryQuotaManager directoryQuotaManager,
            IFileSystem fileSystem,
            QueueEventJournalOptions options,
            ILogger<QueueEventProjectionService> logger,
            IStorageCleanupService? storageCleanupService = null,
            IQueueProjectionStore? projectionStore = null,
            IQuotaProjectionMaintenanceStore? quotaMaintenanceStore = null)
        {
            _journal = journal ?? throw new ArgumentNullException(nameof(journal));
            _projectionStore = projectionStore
                ?? (metadataRepository != null
                    ? new MetadataRepositoryQueueProjectionStore(metadataRepository)
                    : throw new ArgumentNullException(nameof(metadataRepository)));
            _projectionBatchStore = _projectionStore as IQueueProjectionBatchStore;
            _tenantQuotaManager = tenantQuotaManager ?? throw new ArgumentNullException(nameof(tenantQuotaManager));
            _directoryQuotaManager = directoryQuotaManager ?? throw new ArgumentNullException(nameof(directoryQuotaManager));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _storageCleanupService = storageCleanupService;
            _quotaMaintenanceStore = quotaMaintenanceStore;
            _tenantProjectionLocks = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.Ordinal);
            _cursorCache = new ConcurrentDictionary<string, CachedProjectionCursor>(StringComparer.Ordinal);
            _currentProjectionBatch = new AsyncLocal<IQueueProjectionBatch?>();
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
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                await FlushDirtyCursorStatesAsync(force: true, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to flush debounced projection cursor state during shutdown");
            }
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

            ProjectionMaintenanceWork? maintenanceWork = null;
            long cursorOffset;
            var projectionLock = GetTenantProjectionLock(tenantId);
            await projectionLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                cursorOffset = await GetEffectiveCursorOffsetAsync(tenantId, ct).ConfigureAwait(false);
                try
                {
                    ProjectionCycleResult cycleResult;
                    while ((cycleResult = await ProjectTenantCoreAsync(tenantId, ct).ConfigureAwait(false)).AdvancedCursorOffset.HasValue)
                    {
                        cursorOffset = cycleResult.AdvancedCursorOffset.Value;
                        maintenanceWork = cycleResult.MaintenanceWork;
                    }
                }
                catch (DeferredProjectionException ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Deferred queue projection replay for tenant {TenantId} because the physical file could not be verified",
                        tenantId);
                }

                await FlushDirtyCursorStateIfNeededAsync(tenantId, force: true, CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                projectionLock.Release();
            }

            if (maintenanceWork != null)
                await ExecuteProjectionMaintenanceAsync(maintenanceWork, ct).ConfigureAwait(false);

            return await BuildTenantStateAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<QueueProjectionTenantState> SnapshotTenantAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            ProjectionSnapshotCapture snapshotCapture;
            var projectionLock = GetTenantProjectionLock(tenantId);
            await projectionLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cursorOffset = await GetEffectiveCursorOffsetAsync(tenantId, ct).ConfigureAwait(false);
                snapshotCapture = await CaptureProjectionSnapshotAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
            }
            finally
            {
                projectionLock.Release();
            }

            await PersistSnapshotAsync(snapshotCapture, ct).ConfigureAwait(false);
            return await BuildTenantStateAsync(tenantId, ct).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<QueueProjectionTenantState> RebuildTenantAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            ProjectionMaintenanceWork? maintenanceWork = null;
            long cursorOffset;
            var projectionLock = GetTenantProjectionLock(tenantId);
            await projectionLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await ClearTenantProjectionAsync(tenantId, ct).ConfigureAwait(false);
                if (_quotaMaintenanceStore != null)
                    await ResetQuotaProjectionAsync(tenantId, ct).ConfigureAwait(false);

                var restoredCursorOffset = await TryRestoreSnapshotAsync(tenantId, ct).ConfigureAwait(false);
                cursorOffset = await NormalizeCursorOffsetAsync(tenantId, restoredCursorOffset ?? 0, ct).ConfigureAwait(false);
                await SaveCursorAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);

                try
                {
                    try
                    {
                        ProjectionCycleResult cycleResult;
                        while ((cycleResult = await ProjectTenantCoreAsync(tenantId, ct).ConfigureAwait(false)).AdvancedCursorOffset.HasValue)
                        {
                            cursorOffset = cycleResult.AdvancedCursorOffset.Value;
                            maintenanceWork = cycleResult.MaintenanceWork;
                        }
                    }
                    catch (DeferredProjectionException ex)
                    {
                        _logger.LogWarning(
                            ex,
                            "Deferred queue projection rebuild for tenant {TenantId} because the physical file could not be verified",
                            tenantId);
                    }

                    await ReconcileQuotaCountsAfterRebuildAsync(tenantId, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (!(ex is OperationCanceledException && ct.IsCancellationRequested))
                {
                    await TryReconcileQuotaCountsAfterFailedRebuildAsync(tenantId).ConfigureAwait(false);
                    throw;
                }

                await FlushDirtyCursorStateIfNeededAsync(tenantId, force: true, CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                projectionLock.Release();
            }

            if (maintenanceWork != null)
                await ExecuteProjectionMaintenanceAsync(maintenanceWork, ct).ConfigureAwait(false);

            return await BuildTenantStateAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
        }

        private async Task<bool> ProjectTenantAsync(string tenantId, CancellationToken ct)
        {
            ValidateTenantId(tenantId);

            ProjectionCycleResult? cycleResult = null;
            var projectionLock = GetTenantProjectionLock(tenantId);
            await projectionLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                try
                {
                    cycleResult = await ProjectTenantCoreAsync(tenantId, ct).ConfigureAwait(false);
                }
                catch (DeferredProjectionException ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Deferred queue projection for tenant {TenantId} because the physical file could not be verified",
                        tenantId);
                    return false;
                }
            }
            finally
            {
                projectionLock.Release();
            }

            if (cycleResult?.MaintenanceWork != null)
                await ExecuteProjectionMaintenanceAsync(cycleResult.MaintenanceWork, ct).ConfigureAwait(false);

            return cycleResult?.AdvancedCursorOffset.HasValue == true;
        }

        private async Task<ProjectionCycleResult> ProjectTenantCoreAsync(string tenantId, CancellationToken ct)
        {
            var cursor = await LoadCursorStateAsync(tenantId, ct).ConfigureAwait(false);
            var offset = cursor.Offset;
            var batch = await _journal.ReadBatchAsync(tenantId, offset, _options.MaxRecordsPerTenantPerCycle, ct).ConfigureAwait(false);
            if (batch.Records.Count == 0)
            {
                await FlushDirtyCursorStateIfNeededAsync(tenantId, force: false, CancellationToken.None).ConfigureAwait(false);
                if (batch.NextOffset > offset)
                {
                    cursor.Offset = batch.NextOffset;
                    await QueueCursorStateAsync(tenantId, cursor, CancellationToken.None).ConfigureAwait(false);
                    return new ProjectionCycleResult(batch.NextOffset, maintenanceWork: null);
                }

                return new ProjectionCycleResult(null, maintenanceWork: null);
            }

            var projectionBatch = BeginProjectionBatch(tenantId);
            try
            {
                foreach (var record in batch.Records)
                {
                    ct.ThrowIfCancellationRequested();
                    await ApplySequenceTrackingAsync(tenantId, cursor, record, ct).ConfigureAwait(false);
                    await ProjectRecordAsync(record, ct).ConfigureAwait(false);
                }

                if (projectionBatch != null)
                    await projectionBatch.FlushAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                EndProjectionBatch(projectionBatch);
            }

            var cursorOffset = batch.NextOffset;
            ProjectionMaintenanceWork? maintenanceWork = null;
            if (_options.EnableCompaction
                && batch.ReachedEndOfFile
                && await ShouldCompactTenantAsync(tenantId, cursorOffset, ct).ConfigureAwait(false))
            {
                var snapshotCapture = await CaptureProjectionSnapshotAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
                maintenanceWork = new ProjectionMaintenanceWork(snapshotCapture, compactAfterSnapshot: true);
            }

            if (maintenanceWork == null
                && batch.ReachedEndOfFile
                && await ShouldSaveAutomaticSnapshotAsync(tenantId, cursorOffset, ct).ConfigureAwait(false))
            {
                var snapshotCapture = await CaptureProjectionSnapshotAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
                maintenanceWork = new ProjectionMaintenanceWork(snapshotCapture, compactAfterSnapshot: false);
            }

            cursor.Offset = cursorOffset;
            await QueueCursorStateAsync(tenantId, cursor, CancellationToken.None).ConfigureAwait(false);
            return new ProjectionCycleResult(cursorOffset, maintenanceWork);
        }

        private IQueueProjectionBatch? BeginProjectionBatch(string tenantId)
        {
            if (_projectionBatchStore == null)
                return null;

            if (_currentProjectionBatch.Value != null)
                throw new InvalidOperationException("Nested projection batches are not supported.");

            var batch = _projectionBatchStore.BeginBatch(tenantId);
            _currentProjectionBatch.Value = batch;
            return batch;
        }

        private void EndProjectionBatch(IQueueProjectionBatch? batch)
        {
            if (batch == null)
                return;

            _currentProjectionBatch.Value = null;
        }

        private Task UpsertProjectedFileAsync(FileMetadata metadata, CancellationToken ct)
        {
            var currentBatch = _currentProjectionBatch.Value;
            if (currentBatch != null)
                return currentBatch.UpsertProjectedFileAsync(metadata, ct);

            return _projectionStore.UpsertProjectedFileAsync(metadata, ct);
        }

        private Task<bool> RemoveProjectedFileAsync(string tenantId, string fileKey, CancellationToken ct)
        {
            var currentBatch = _currentProjectionBatch.Value;
            if (currentBatch != null)
            {
                if (!string.Equals(currentBatch.TenantId, tenantId, StringComparison.Ordinal))
                    throw new InvalidOperationException("Projection batch tenant does not match the projected mutation.");

                return currentBatch.RemoveProjectedFileAsync(fileKey, ct);
            }

            return _projectionStore.RemoveProjectedFileAsync(tenantId, fileKey, ct);
        }

        private async Task ApplySequenceTrackingAsync(
            string tenantId,
            ProjectionCursor cursor,
            QueueEventRecord record,
            CancellationToken ct)
        {
            if (!record.SequenceNumber.HasValue || record.SequenceNumber.Value <= 0)
                return;

            var sequenceNumber = record.SequenceNumber.Value;
            if (cursor.LastSequenceNumber > 0 && sequenceNumber > cursor.LastSequenceNumber + 1)
            {
                var expectedSequence = cursor.LastSequenceNumber + 1;
                cursor.GapDetected = true;
                cursor.LastGapDetectedAtUtc = DateTime.UtcNow;
                cursor.GapExpectedSequenceNumber = expectedSequence;
                cursor.GapObservedSequenceNumber = sequenceNumber;
                QueueJournalMetrics.RecordSequenceGapDetected();

                _logger.LogWarning(
                    SequenceGapDetectedEventId,
                    "Queue journal sequence gap detected for tenant {TenantId}: expected {ExpectedSequenceNumber} but observed {ObservedSequenceNumber}. Triggering orphan recovery.",
                    tenantId,
                    expectedSequence,
                    sequenceNumber);

                await TriggerGapRecoveryAsync(tenantId, expectedSequence, sequenceNumber, ct).ConfigureAwait(false);
            }

            if (sequenceNumber > cursor.LastSequenceNumber)
                cursor.LastSequenceNumber = sequenceNumber;
        }

        private async Task TriggerGapRecoveryAsync(
            string tenantId,
            long expectedSequence,
            long observedSequence,
            CancellationToken ct)
        {
            if (_storageCleanupService == null)
                return;

            try
            {
                QueueJournalMetrics.RecordSequenceGapRecoveryAttempted();
                await _storageCleanupService
                    .RecoverOrphanedFilesAsync(new ProjectionRecoveryTenantContext(tenantId), ct)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                QueueJournalMetrics.RecordSequenceGapRecoveryFailed();
                _logger.LogWarning(
                    SequenceGapRecoveryFailedEventId,
                    ex,
                    "Automatic orphan recovery after queue sequence gap failed for tenant {TenantId} (expected {ExpectedSequenceNumber}, observed {ObservedSequenceNumber})",
                    tenantId,
                    expectedSequence,
                    observedSequence);
            }
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
                case QueueEventType.ProcessingTimedOut:
                    await ProjectProcessingTimedOutAsync(record, ct).ConfigureAwait(false);
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
                case QueueEventType.DeadLettered:
                    await ProjectDeadLetteredAsync(record, ct).ConfigureAwait(false);
                    return;
                default:
                    return;
            }
        }

        private async Task ProjectAcceptedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await GetExistingMetadataAsync(record, ct).ConfigureAwait(false);
            if (existing != null)
            {
                if (QueueProjectionMetadataState.IsAcceptedProjectionApplied(existing))
                    return;

                var tenantReservationConsumed = false;
                var directoryReservationConsumed = false;
                try
                {
                    tenantReservationConsumed = await ApplyAcceptedTenantProjectionAsync(record.TenantId, ct).ConfigureAwait(false);
                    directoryReservationConsumed = await ApplyAcceptedDirectoryProjectionAsync(record.TenantId, existing.DirectoryPath, ct).ConfigureAwait(false);

                    var updated = existing.Clone();
                    QueueProjectionMetadataState.MarkAcceptedProjectionApplied(updated);
                    await UpsertProjectedFileAsync(updated, ct).ConfigureAwait(false);
                    return;
                }
                catch
                {
                    await RollbackAcceptedProjectionAsync(
                        record.TenantId,
                        existing.DirectoryPath,
                        tenantReservationConsumed,
                        directoryReservationConsumed).ConfigureAwait(false);
                    throw;
                }
            }

            await EnsureMetadataExistsAsync(record, record.Status ?? FileProcessingStatus.Pending, ct).ConfigureAwait(false);
        }

        private async Task ProjectProcessingStartedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var eventProcessingStart = record.ProcessingStartTimeUtc ?? record.OccurredAtUtc;
            var existing = await GetExistingMetadataAsync(record, ct).ConfigureAwait(false);
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
            updated.DeadLetteredAt = null;
            updated.AvailableForProcessingAt = null;
            updated.LastError = record.ErrorMessage ?? updated.LastError;
            QueueProjectionMetadataState.ClearDeadLetterProjection(updated);

            await UpsertProjectedFileAsync(updated, ct).ConfigureAwait(false);
        }

        private async Task ProjectProcessingFailedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var status = record.Status ?? FileProcessingStatus.Pending;
            var existing = await GetExistingMetadataAsync(record, ct).ConfigureAwait(false);
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
            updated.DeadLetteredAt = null;
            updated.RetryCount = record.RetryCount ?? updated.RetryCount;
            updated.LastError = record.ErrorMessage;
            updated.LastFailedAt = record.OccurredAtUtc;
            updated.AvailableForProcessingAt = status == FileProcessingStatus.Pending
                ? record.AvailableForProcessingAtUtc
                : null;
            QueueProjectionMetadataState.ClearDeadLetterProjection(updated);

            await UpsertProjectedFileAsync(updated, ct).ConfigureAwait(false);
        }

        private async Task ProjectProcessingTimedOutAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await GetExistingMetadataAsync(record, ct).ConfigureAwait(false);
            if (existing == null)
            {
                existing = await EnsureMetadataExistsAsync(record, FileProcessingStatus.Pending, ct).ConfigureAwait(false);
                if (existing == null)
                    return;
            }

            if (ShouldSkipTimedOutProjection(existing, record))
                return;

            var updated = existing.Clone();
            updated.Status = FileProcessingStatus.Pending;
            updated.ProcessingStartTime = null;
            updated.CompletedAt = null;
            updated.DeleteSucceededAt = null;
            updated.DeadLetteredAt = null;
            updated.AvailableForProcessingAt = record.AvailableForProcessingAtUtc ?? record.OccurredAtUtc;
            QueueProjectionMetadataState.ClearDeadLetterProjection(updated);

            await UpsertProjectedFileAsync(updated, ct).ConfigureAwait(false);
        }

        private async Task ProjectProcessingCompletedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await GetExistingMetadataAsync(record, ct).ConfigureAwait(false);
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
            updated.DeadLetteredAt = null;
            updated.AvailableForProcessingAt = null;
            updated.LastError = null;
            QueueProjectionMetadataState.ClearDeadLetterProjection(updated);

            await UpsertProjectedFileAsync(updated, ct).ConfigureAwait(false);
        }

        private async Task ProjectDeleteRequestedAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await GetExistingMetadataAsync(record, ct).ConfigureAwait(false);
            if (existing == null)
            {
                await EnsureMetadataExistsAsync(record, FileProcessingStatus.DeleteRequested, ct).ConfigureAwait(false);
                return;
            }

            if (existing.Status == FileProcessingStatus.DeleteRequested
                && existing.CompletedAt.HasValue
                && !existing.ProcessingStartTime.HasValue
                && !existing.AvailableForProcessingAt.HasValue
                && string.IsNullOrEmpty(existing.LastError))
            {
                return;
            }

            var updated = existing.Clone();
            updated.Status = FileProcessingStatus.DeleteRequested;
            updated.ProcessingStartTime = null;
            updated.CompletedAt = existing.CompletedAt ?? record.OccurredAtUtc;
            updated.DeleteSucceededAt = null;
            updated.DeadLetteredAt = null;
            updated.AvailableForProcessingAt = null;
            updated.LastError = null;
            QueueProjectionMetadataState.ClearDeadLetterProjection(updated);

            await UpsertProjectedFileAsync(updated, ct).ConfigureAwait(false);
        }

        private async Task ProjectDeleteSucceededAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await GetExistingMetadataAsync(record, ct).ConfigureAwait(false);
            if (existing == null)
                return;

            var directoryPath = DirectoryPathNormalizer.Normalize(existing.DirectoryPath);
            var tenantQuotaDecremented = false;
            var directoryQuotaDecremented = false;

            try
            {
                await ApplyDeleteSucceededDirectoryProjectionAsync(existing.TenantId, directoryPath, ct).ConfigureAwait(false);
                directoryQuotaDecremented = true;

                await ApplyDeleteSucceededTenantProjectionAsync(existing.TenantId, ct).ConfigureAwait(false);
                tenantQuotaDecremented = true;

                var metadataRemoved = await RemoveProjectedFileAsync(existing.TenantId, existing.FileKey, ct).ConfigureAwait(false);
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
                    await RollbackDeleteSucceededTenantProjectionAsync(tenantId).ConfigureAwait(false);
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
                    await RollbackDeleteSucceededDirectoryProjectionAsync(tenantId, directoryPath).ConfigureAwait(false);
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
            var physicalPath = record.PhysicalPath;
            if (string.IsNullOrWhiteSpace(physicalPath))
                return null;

            var confirmedPhysicalPath = physicalPath!;

            if (!TryConfirmPhysicalFileExists(confirmedPhysicalPath, out var physicalFileExists))
            {
                throw new DeferredProjectionException(
                    record.TenantId,
                    record.FileKey,
                    confirmedPhysicalPath,
                    "Physical file existence could not be verified.");
            }

            if (!physicalFileExists)
                return null;

            var metadata = BuildMetadata(record, fallbackStatus);
            var tenantReservationConsumed = false;
            var directoryReservationConsumed = false;
            try
            {
                tenantReservationConsumed = await ApplyAcceptedTenantProjectionAsync(record.TenantId, ct).ConfigureAwait(false);
                directoryReservationConsumed = await ApplyAcceptedDirectoryProjectionAsync(record.TenantId, metadata.DirectoryPath, ct).ConfigureAwait(false);

                QueueProjectionMetadataState.MarkAcceptedProjectionApplied(metadata);
                await UpsertProjectedFileAsync(metadata, ct).ConfigureAwait(false);
                return metadata;
            }
            catch
            {
                await RollbackAcceptedProjectionAsync(
                    record.TenantId,
                    metadata.DirectoryPath,
                    tenantReservationConsumed,
                    directoryReservationConsumed).ConfigureAwait(false);
                throw;
            }
        }

        private async Task ProjectDeadLetteredAsync(QueueEventRecord record, CancellationToken ct)
        {
            var existing = await GetExistingMetadataAsync(record, ct).ConfigureAwait(false);
            if (existing == null)
            {
                existing = await EnsureMetadataExistsAsync(record, FileProcessingStatus.DeadLettered, ct).ConfigureAwait(false);
                if (existing == null)
                    return;
            }

            if (QueueProjectionMetadataState.IsDeadLetterProjectionApplied(existing)
                && existing.Status == FileProcessingStatus.DeadLettered
                && existing.DeadLetteredAt.HasValue
                && existing.DeadLetteredAt.Value >= record.OccurredAtUtc)
            {
                return;
            }

            var directoryPath = DirectoryPathNormalizer.Normalize(existing.DirectoryPath);
            var tenantQuotaDecremented = false;
            var directoryQuotaDecremented = false;

            try
            {
                await ApplyDeleteSucceededDirectoryProjectionAsync(existing.TenantId, directoryPath, ct).ConfigureAwait(false);
                directoryQuotaDecremented = true;

                await ApplyDeleteSucceededTenantProjectionAsync(existing.TenantId, ct).ConfigureAwait(false);
                tenantQuotaDecremented = true;

                var updated = existing.Clone();
                updated.Status = FileProcessingStatus.DeadLettered;
                updated.ProcessingStartTime = null;
                updated.CompletedAt = null;
                updated.DeleteSucceededAt = null;
                updated.DeadLetteredAt = record.OccurredAtUtc;
                updated.AvailableForProcessingAt = null;
                updated.PhysicalPath = record.PhysicalPath ?? existing.PhysicalPath;
                updated.VolumeId = record.VolumeId ?? existing.VolumeId;
                QueueProjectionMetadataState.MarkDeadLetterProjectionApplied(updated);

                await UpsertProjectedFileAsync(updated, ct).ConfigureAwait(false);
            }
            catch
            {
                await RollbackDeleteSucceededProjectionAsync(existing.TenantId, directoryPath, tenantQuotaDecremented, directoryQuotaDecremented).ConfigureAwait(false);
                throw;
            }
        }

        private bool TryConfirmPhysicalFileExists(string physicalPath, out bool physicalFileExists)
        {
            physicalFileExists = false;

            if (_fileSystem.File.Exists(physicalPath))
            {
                try
                {
                    using (var stream = _fileSystem.File.Open(
                        physicalPath,
                        FileMode.Open,
                        FileAccess.Read,
                        FileShare.ReadWrite | FileShare.Delete))
                    {
                    }

                    physicalFileExists = true;
                    return true;
                }
                catch (FileNotFoundException)
                {
                    return true;
                }
                catch (DirectoryNotFoundException)
                {
                    return true;
                }
                catch (UnauthorizedAccessException)
                {
                    return false;
                }
                catch (IOException)
                {
                    return false;
                }
            }

            try
            {
                using (var stream = _fileSystem.File.Open(
                    physicalPath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete))
                {
                }

                physicalFileExists = true;
                return true;
            }
            catch (FileNotFoundException)
            {
                return true;
            }
            catch (DirectoryNotFoundException)
            {
                return true;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }
            catch (IOException)
            {
                return false;
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
                DeleteSucceededAt = record.EventType == QueueEventType.DeleteSucceeded
                    || record.Status == FileProcessingStatus.DeleteSucceeded
                    ? record.OccurredAtUtc
                    : null,
                DeadLetteredAt = record.EventType == QueueEventType.DeadLettered
                    || record.Status == FileProcessingStatus.DeadLettered
                    ? record.OccurredAtUtc
                    : null,
                AvailableForProcessingAt = record.AvailableForProcessingAtUtc,
                LastError = record.ErrorMessage,
                LastFailedAt = record.EventType == QueueEventType.ProcessingFailed ? record.OccurredAtUtc : null,
                OriginalFileName = record.OriginalFileName,
                FileExtension = record.FileExtension,
            };
        }

        private Task<FileMetadata?> GetExistingMetadataAsync(QueueEventRecord record, CancellationToken ct)
        {
            return _projectionStore.GetProjectedFileAsync(record.TenantId, record.FileKey, ct);
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

        private bool ShouldSkipTimedOutProjection(FileMetadata existing, QueueEventRecord record)
        {
            var eventProcessingStart = record.ProcessingStartTimeUtc ?? record.OccurredAtUtc;
            if (existing.ProcessingStartTime.HasValue && existing.ProcessingStartTime.Value > eventProcessingStart)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingTimedOut projection for file {FileKey}: existing lease {ExistingLease} > event lease {EventLease}",
                    existing.FileKey,
                    existing.ProcessingStartTime.Value,
                    eventProcessingStart);
                return true;
            }

            if (existing.LastFailedAt.HasValue && existing.LastFailedAt.Value >= record.OccurredAtUtc)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingTimedOut projection for file {FileKey}: existing failure {LastFailedAt} >= event timeout {EventTimeout}",
                    existing.FileKey,
                    existing.LastFailedAt.Value,
                    record.OccurredAtUtc);
                return true;
            }

            if (existing.CompletedAt.HasValue && existing.CompletedAt.Value >= record.OccurredAtUtc)
            {
                _logger.LogDebug(
                    "Skipping stale ProcessingTimedOut projection for file {FileKey}: existing completion {CompletedAt} >= event timeout {EventTimeout}",
                    existing.FileKey,
                    existing.CompletedAt.Value,
                    record.OccurredAtUtc);
                return true;
            }

            var eventAvailableForProcessingAt = record.AvailableForProcessingAtUtc ?? record.OccurredAtUtc;
            if (existing.Status == FileProcessingStatus.Pending
                && !existing.ProcessingStartTime.HasValue
                && existing.AvailableForProcessingAt.HasValue
                && existing.AvailableForProcessingAt.Value >= eventAvailableForProcessingAt)
            {
                _logger.LogDebug(
                    "Skipping duplicate/stale ProcessingTimedOut projection for file {FileKey}: existing pending-at {ExistingPendingAt} >= event pending-at {EventPendingAt}",
                    existing.FileKey,
                    existing.AvailableForProcessingAt.Value,
                    eventAvailableForProcessingAt);
                return true;
            }

            return false;
        }

        private static bool IsCompletedEvent(QueueEventRecord record)
        {
            return record.EventType == QueueEventType.ProcessingCompleted
                || record.EventType == QueueEventType.DeleteRequested
                || record.EventType == QueueEventType.DeleteSucceeded
                || record.Status == FileProcessingStatus.Completed
                || record.Status == FileProcessingStatus.DeleteRequested
                || record.Status == FileProcessingStatus.DeleteSucceeded;
        }

        private async Task<ProjectionCursor> LoadCursorStateAsync(string tenantId, CancellationToken ct)
        {
            if (_cursorCache.TryGetValue(tenantId, out var cached))
                return CloneCursor(cached.Cursor);

            var path = GetCursorPath(tenantId);
            ProjectionCursor cursor;
            if (!_fileSystem.File.Exists(path))
            {
                cursor = new ProjectionCursor();
            }
            else
            {
                using (var stream = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    cursor = await JsonSerializer.DeserializeAsync<ProjectionCursor>(stream, JsonOptions, ct).ConfigureAwait(false)
                        ?? new ProjectionCursor();
                }
            }

            var cachedCursor = new CachedProjectionCursor(CloneCursor(cursor), DateTime.UtcNow, dirty: false);
            _cursorCache.TryAdd(tenantId, cachedCursor);
            return CloneCursor(cachedCursor.Cursor);
        }

        private async Task<long> LoadCursorAsync(string tenantId, CancellationToken ct)
        {
            var cursor = await LoadCursorStateAsync(tenantId, ct).ConfigureAwait(false);
            return cursor.Offset;
        }

        private Task SaveCursorAsync(string tenantId, long offset, CancellationToken ct)
        {
            return PersistCursorStateAsync(tenantId, new ProjectionCursor { Offset = offset }, ct);
        }

        private async Task QueueCursorStateAsync(string tenantId, ProjectionCursor cursor, CancellationToken ct)
        {
            CacheCursorState(tenantId, cursor, markPersisted: false);
            await FlushDirtyCursorStateIfNeededAsync(tenantId, force: false, ct).ConfigureAwait(false);
        }

        private async Task FlushDirtyCursorStatesAsync(bool force, CancellationToken ct)
        {
            foreach (var tenantId in _cursorCache.Keys)
            {
                ct.ThrowIfCancellationRequested();
                await FlushDirtyCursorStateIfNeededAsync(tenantId, force, ct).ConfigureAwait(false);
            }
        }

        private async Task FlushDirtyCursorStateIfNeededAsync(string tenantId, bool force, CancellationToken ct)
        {
            if (!_cursorCache.TryGetValue(tenantId, out var cached) || !cached.Dirty)
                return;

            if (!force && DateTime.UtcNow - cached.LastPersistedAtUtc < CursorPersistenceDebounce)
                return;

            await PersistCursorStateAsync(tenantId, cached.Cursor, ct).ConfigureAwait(false);
        }

        private async Task PersistCursorStateAsync(string tenantId, ProjectionCursor cursor, CancellationToken ct)
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
                await JsonSerializer.SerializeAsync(stream, cursor, JsonOptions, ct).ConfigureAwait(false);
                await DurableFileWrite.FlushToDiskAsync(stream, ct).ConfigureAwait(false);
            }

            ReplaceFileAtomically(tempPath, path);
            CacheCursorState(tenantId, cursor, markPersisted: true);
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

        private string GetJournalStatePath(string tenantId)
        {
            return _fileSystem.Path.Combine(_options.QueueDirectory, tenantId, "queue.state.json");
        }

        private string GetSnapshotPath(string tenantId)
        {
            return _fileSystem.Path.Combine(_options.QueueDirectory, tenantId, "projection.snapshot.json");
        }

        private async Task<JournalStateView?> LoadJournalStateViewAsync(string tenantId, CancellationToken ct)
        {
            var path = GetJournalStatePath(tenantId);
            if (!_fileSystem.File.Exists(path))
                return null;

            try
            {
                using (var stream = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
                {
                    return await JsonSerializer.DeserializeAsync<JournalStateView>(stream, JsonOptions, ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is IOException || ex is JsonException || ex is UnauthorizedAccessException)
            {
                _logger.LogWarning(ex, "Failed to load queue journal state view for tenant {TenantId}", tenantId);
                return null;
            }
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

        private async Task<bool> ShouldSaveAutomaticSnapshotAsync(string tenantId, long cursorOffset, CancellationToken ct)
        {
            if (!_options.EnableAutomaticSnapshots)
                return false;

            cursorOffset = await NormalizeCursorOffsetAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
            var snapshot = await LoadSnapshotAsync(tenantId, ct).ConfigureAwait(false);
            if (snapshot == null)
                return true;

            if (cursorOffset < snapshot.CursorOffset)
                return true;

            if (cursorOffset == snapshot.CursorOffset)
                return false;

            var advancedBytes = cursorOffset - snapshot.CursorOffset;
            if (advancedBytes >= _options.MinBytesBeforeAutomaticSnapshot)
                return true;

            if (_options.AutomaticSnapshotInterval == TimeSpan.Zero)
                return true;

            return DateTime.UtcNow - snapshot.CreatedAtUtc >= _options.AutomaticSnapshotInterval;
        }

        private async Task<QueueProjectionTenantState> BuildTenantStateAsync(string tenantId, CancellationToken ct)
        {
            var cursor = await GetEffectiveCursorStateAsync(tenantId, ct).ConfigureAwait(false);
            return await BuildTenantStateAsync(tenantId, cursor, ct).ConfigureAwait(false);
        }

        private async Task<QueueProjectionTenantState> BuildTenantStateAsync(string tenantId, long cursorOffset, CancellationToken ct)
        {
            var cursor = await GetEffectiveCursorStateAsync(tenantId, ct).ConfigureAwait(false);
            cursor.Offset = await NormalizeCursorOffsetAsync(tenantId, cursorOffset, ct).ConfigureAwait(false);
            return await BuildTenantStateAsync(tenantId, cursor, ct).ConfigureAwait(false);
        }

        private async Task<QueueProjectionTenantState> BuildTenantStateAsync(string tenantId, ProjectionCursor cursor, CancellationToken ct)
        {
            cursor.Offset = await NormalizeCursorOffsetAsync(tenantId, cursor.Offset, ct).ConfigureAwait(false);
            var journalSizeBytes = await _journal.GetTailOffsetAsync(tenantId, ct).ConfigureAwait(false);
            var snapshot = await LoadSnapshotAsync(tenantId, ct).ConfigureAwait(false);
            var journalState = await LoadJournalStateViewAsync(tenantId, ct).ConfigureAwait(false);
            return new QueueProjectionTenantState
            {
                TenantId = tenantId,
                CursorOffset = cursor.Offset,
                JournalSizeBytes = journalSizeBytes,
                LagBytes = Math.Max(0, journalSizeBytes - cursor.Offset),
                HasSnapshot = snapshot != null,
                SnapshotCreatedAtUtc = snapshot?.CreatedAtUtc,
                SnapshotCursorOffset = snapshot?.CursorOffset ?? 0,
                SnapshotFileCount = snapshot?.Files.Count ?? 0,
                LastProjectedSequenceNumber = cursor.LastSequenceNumber,
                GapDetected = cursor.GapDetected,
                LastGapDetectedAtUtc = cursor.LastGapDetectedAtUtc,
                GapExpectedSequenceNumber = cursor.GapExpectedSequenceNumber,
                GapObservedSequenceNumber = cursor.GapObservedSequenceNumber,
                JournalFormat = journalState?.Format?.ToString(),
                JournalCorruptTailDetected = journalState?.CorruptTailDetected ?? false,
                LastJournalCorruptTailDetectedAtUtc = journalState?.LastCorruptTailDetectedAtUtc,
                LastJournalCorruptTailOffset = journalState?.LastCorruptTailOffset,
                JournalAutoRepairCount = journalState?.AutoRepairCount ?? 0,
            };
        }

        private async Task<ProjectionCursor> GetEffectiveCursorStateAsync(string tenantId, CancellationToken ct)
        {
            var cursor = await LoadCursorStateAsync(tenantId, ct).ConfigureAwait(false);
            cursor.Offset = await NormalizeCursorOffsetAsync(tenantId, cursor.Offset, ct).ConfigureAwait(false);
            return cursor;
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

        private async Task<ProjectionSnapshotCapture> CaptureProjectionSnapshotAsync(string tenantId, long cursorOffset, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            IReadOnlyList<FileMetadata> metadata = _projectionStore is IProjectionSnapshotSource snapshotSource
                ? snapshotSource.CaptureProjectedFilesSnapshot(tenantId)
                : (await _projectionStore.GetProjectedFilesAsync(tenantId, ct).ConfigureAwait(false)).ToArray();

            return new ProjectionSnapshotCapture(tenantId, cursorOffset, metadata);
        }

        private async Task ExecuteProjectionMaintenanceAsync(ProjectionMaintenanceWork maintenanceWork, CancellationToken ct)
        {
            await PersistSnapshotAsync(maintenanceWork.SnapshotCapture, ct).ConfigureAwait(false);

            if (maintenanceWork.CompactAfterSnapshot)
            {
                await _journal
                    .CompactAsync(maintenanceWork.SnapshotCapture.TenantId, maintenanceWork.SnapshotCapture.CursorOffset, ct)
                    .ConfigureAwait(false);
            }
        }

        private async Task PersistSnapshotAsync(ProjectionSnapshotCapture snapshotCapture, CancellationToken ct)
        {
            var snapshotPath = GetSnapshotPath(snapshotCapture.TenantId);
            var snapshotDirectory = _fileSystem.Path.GetDirectoryName(snapshotPath);
            if (snapshotDirectory is string directory && directory.Length > 0 && !_fileSystem.Directory.Exists(directory))
                _fileSystem.Directory.CreateDirectory(directory);

            var snapshot = new ProjectionSnapshot
            {
                TenantId = snapshotCapture.TenantId,
                CreatedAtUtc = DateTime.UtcNow,
                CursorOffset = snapshotCapture.CursorOffset,
                Files = snapshotCapture.Metadata
                    .OrderBy(item => item.CreatedAt)
                    .ThenBy(item => item.FileKey, StringComparer.Ordinal)
                    .Select(item => item.Clone())
                    .ToList()
            };

            var tempPath = snapshotPath + ".tmp";
            using (var stream = _fileSystem.File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await JsonSerializer.SerializeAsync(stream, snapshot, JsonOptions, ct).ConfigureAwait(false);
                await DurableFileWrite.FlushToDiskAsync(stream, ct).ConfigureAwait(false);
            }

            ReplaceFileAtomically(tempPath, snapshotPath);
        }

        private async Task<long?> TryRestoreSnapshotAsync(string tenantId, CancellationToken ct)
        {
            var snapshot = await LoadSnapshotAsync(tenantId, ct).ConfigureAwait(false);
            if (snapshot == null)
                return null;

            var projectionBatch = BeginProjectionBatch(tenantId);
            try
            {
                foreach (var file in snapshot.Files)
                {
                    ct.ThrowIfCancellationRequested();
                    await UpsertProjectedFileAsync(file, ct).ConfigureAwait(false);
                }

                if (projectionBatch != null)
                    await projectionBatch.FlushAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                EndProjectionBatch(projectionBatch);
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
            var metadataEntries = await _projectionStore.GetProjectedFilesAsync(tenantId, ct).ConfigureAwait(false);
            var projectionBatch = BeginProjectionBatch(tenantId);
            try
            {
                foreach (var metadata in metadataEntries)
                {
                    ct.ThrowIfCancellationRequested();
                    await RemoveProjectedFileAsync(tenantId, metadata.FileKey, ct).ConfigureAwait(false);
                }

                if (projectionBatch != null)
                    await projectionBatch.FlushAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                EndProjectionBatch(projectionBatch);
            }
        }

        private static void ValidateTenantId(string tenantId)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));
        }

        private async Task ResetQuotaProjectionAsync(string tenantId, CancellationToken ct)
        {
            if (_quotaMaintenanceStore == null)
                return;

            using (var rebuildHandle = await _quotaMaintenanceStore.BeginDatabaseRebuildAsync(tenantId, ct).ConfigureAwait(false))
            {
                var quotaRows = await _quotaMaintenanceStore.GetQuotaRowsAsync(tenantId, ct).ConfigureAwait(false);
                foreach (var quota in quotaRows)
                {
                    ct.ThrowIfCancellationRequested();

                    if (string.IsNullOrWhiteSpace(quota.DirectoryPath))
                        continue;

                    if (HasExplicitQuotaLimit(quota) || string.Equals(quota.DirectoryPath, tenantId, StringComparison.Ordinal))
                    {
                        await _quotaMaintenanceStore
                            .SetProjectedCountForRebuildAsync(tenantId, quota.DirectoryPath, 0, ct)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await _quotaMaintenanceStore.RemoveQuotaAsync(tenantId, quota.DirectoryPath, ct).ConfigureAwait(false);
                    }
                }

                await _quotaMaintenanceStore
                    .SetProjectedCountForRebuildAsync(tenantId, tenantId, 0, ct)
                    .ConfigureAwait(false);
            }
        }

        private async Task ReconcileQuotaCountsAfterRebuildAsync(string tenantId, CancellationToken ct)
        {
            if (_quotaMaintenanceStore != null)
            {
                await ReconcileQuotaCountsCoreAsync(tenantId, ct).ConfigureAwait(false);
                return;
            }

            if (_storageCleanupService != null)
            {
                await _storageCleanupService.ReconcileQuotaCountsAsync(tenantId, ct).ConfigureAwait(false);
            }
        }

        private async Task TryReconcileQuotaCountsAfterFailedRebuildAsync(string tenantId)
        {
            try
            {
                await ReconcileQuotaCountsAfterRebuildAsync(tenantId, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception reconcileEx)
            {
                _logger.LogWarning(
                    reconcileEx,
                    "Projection rebuild left tenant {TenantId} in a partial state and quota reconciliation fallback also failed",
                    tenantId);
            }
        }

        private async Task ReconcileQuotaCountsCoreAsync(string tenantId, CancellationToken ct)
        {
            if (_quotaMaintenanceStore == null)
                return;

            var metadataEntries = (await _projectionStore.GetProjectedFilesAsync(tenantId, ct).ConfigureAwait(false))
                .Where(ActiveQuotaMetadata.CountsTowardActiveQuota)
                .ToArray();
            var expectedTenantCount = metadataEntries.Length;
            var expectedDirectoryCounts = metadataEntries
                .GroupBy(metadata => NormalizeDirectoryPath(metadata.DirectoryPath), StringComparer.Ordinal)
                .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

            using (var rebuildHandle = await _quotaMaintenanceStore.BeginDatabaseRebuildAsync(tenantId, ct).ConfigureAwait(false))
            {
                var existingDirectoryQuotas = (await _quotaMaintenanceStore.GetQuotaRowsAsync(tenantId, ct).ConfigureAwait(false))
                    .Where(quota => !string.IsNullOrWhiteSpace(quota.DirectoryPath)
                        && !string.Equals(quota.DirectoryPath, tenantId, StringComparison.Ordinal))
                    .ToDictionary(quota => quota.DirectoryPath, quota => quota, StringComparer.Ordinal);

                var knownDirectoryPaths = new HashSet<string>(existingDirectoryQuotas.Keys, StringComparer.Ordinal);
                foreach (var directoryPath in expectedDirectoryCounts.Keys)
                    knownDirectoryPaths.Add(directoryPath);

                foreach (var directoryPath in knownDirectoryPaths)
                {
                    ct.ThrowIfCancellationRequested();

                    var expectedDirectoryCount = expectedDirectoryCounts.TryGetValue(directoryPath, out var count)
                        ? count
                        : 0;

                    if (expectedDirectoryCount == 0
                        && existingDirectoryQuotas.TryGetValue(directoryPath, out var existingQuota)
                        && !HasExplicitQuotaLimit(existingQuota))
                    {
                        await _quotaMaintenanceStore.RemoveQuotaAsync(tenantId, directoryPath, ct).ConfigureAwait(false);
                        continue;
                    }

                    await _quotaMaintenanceStore.SetProjectedCountForRebuildAsync(
                        tenantId,
                        directoryPath,
                        expectedDirectoryCount,
                        ct).ConfigureAwait(false);
                }

                await _quotaMaintenanceStore
                    .SetProjectedCountForRebuildAsync(tenantId, tenantId, expectedTenantCount, ct)
                    .ConfigureAwait(false);
            }
        }

        private static bool HasExplicitQuotaLimit(DirectoryQuota quota)
        {
            if (quota == null)
                throw new ArgumentNullException(nameof(quota));

            return quota.MaxCount > 0;
        }

        private void ReplaceFileAtomically(string tempPath, string destinationPath)
        {
            if (_fileSystem.File.Exists(destinationPath))
            {
                _fileSystem.File.Replace(tempPath, destinationPath, null);
                return;
            }

            _fileSystem.File.Move(tempPath, destinationPath);
        }

        private async Task<bool> ApplyAcceptedTenantProjectionAsync(string tenantId, CancellationToken ct)
        {
            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
            {
                return await tenantQuotaProjectionManager.ApplyAcceptedProjectionAsync(tenantId, ct).ConfigureAwait(false);
            }

            if (_tenantQuotaManager is ITenantQuotaCompensationManager tenantQuotaCompensationManager)
            {
                await tenantQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, ct).ConfigureAwait(false);
                return false;
            }

            await _tenantQuotaManager.IncrementFileCountAsync(tenantId, ct).ConfigureAwait(false);
            return false;
        }

        private void CacheCursorState(string tenantId, ProjectionCursor cursor, bool markPersisted)
        {
            var timestampUtc = DateTime.UtcNow;
            _cursorCache.AddOrUpdate(
                tenantId,
                _ => new CachedProjectionCursor(
                    CloneCursor(cursor),
                    markPersisted ? timestampUtc : DateTime.MinValue,
                    dirty: !markPersisted),
                (_, existing) => new CachedProjectionCursor(
                    CloneCursor(cursor),
                    markPersisted ? timestampUtc : existing.LastPersistedAtUtc,
                    dirty: !markPersisted));
        }

        private static ProjectionCursor CloneCursor(ProjectionCursor cursor)
        {
            return new ProjectionCursor
            {
                Offset = cursor.Offset,
                LastSequenceNumber = cursor.LastSequenceNumber,
                GapDetected = cursor.GapDetected,
                LastGapDetectedAtUtc = cursor.LastGapDetectedAtUtc,
                GapExpectedSequenceNumber = cursor.GapExpectedSequenceNumber,
                GapObservedSequenceNumber = cursor.GapObservedSequenceNumber,
            };
        }

        private async Task<bool> ApplyAcceptedDirectoryProjectionAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                return await directoryQuotaProjectionManager
                    .ApplyAcceptedProjectionAsync(tenantId, directoryPath, ct)
                    .ConfigureAwait(false);
            }

            if (_directoryQuotaManager is IDirectoryQuotaCompensationManager directoryQuotaCompensationManager)
            {
                await directoryQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, directoryPath, ct).ConfigureAwait(false);
                return false;
            }

            await _directoryQuotaManager.IncrementFileCountAsync(tenantId, directoryPath, ct).ConfigureAwait(false);
            return false;
        }

        private async Task ApplyDeleteSucceededTenantProjectionAsync(string tenantId, CancellationToken ct)
        {
            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
            {
                await tenantQuotaProjectionManager.ApplyDeleteSucceededProjectionAsync(tenantId, ct).ConfigureAwait(false);
                return;
            }

            await _tenantQuotaManager.DecrementFileCountAsync(tenantId, ct).ConfigureAwait(false);
        }

        private async Task ApplyDeleteSucceededDirectoryProjectionAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                await directoryQuotaProjectionManager
                    .ApplyDeleteSucceededProjectionAsync(tenantId, directoryPath, ct)
                    .ConfigureAwait(false);
                return;
            }

            await _directoryQuotaManager.DecrementFileCountAsync(tenantId, directoryPath, ct).ConfigureAwait(false);
        }

        private async Task RollbackDeleteSucceededTenantProjectionAsync(string tenantId)
        {
            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
            {
                await tenantQuotaProjectionManager.RollbackDeleteSucceededProjectionAsync(tenantId, default).ConfigureAwait(false);
                return;
            }

            if (_tenantQuotaManager is ITenantQuotaCompensationManager tenantQuotaCompensationManager)
            {
                await tenantQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, default).ConfigureAwait(false);
                return;
            }

            await _tenantQuotaManager.IncrementFileCountAsync(tenantId, default).ConfigureAwait(false);
        }

        private async Task RollbackDeleteSucceededDirectoryProjectionAsync(string tenantId, string directoryPath)
        {
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                await directoryQuotaProjectionManager
                    .RollbackDeleteSucceededProjectionAsync(tenantId, directoryPath, default)
                    .ConfigureAwait(false);
                return;
            }

            if (_directoryQuotaManager is IDirectoryQuotaCompensationManager directoryQuotaCompensationManager)
            {
                await directoryQuotaCompensationManager.CompensateIncrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
                return;
            }

            await _directoryQuotaManager.IncrementFileCountAsync(tenantId, directoryPath, default).ConfigureAwait(false);
        }

        private async Task RollbackAcceptedProjectionAsync(
            string tenantId,
            string directoryPath,
            bool tenantRestoreReservation,
            bool directoryRestoreReservation)
        {
            if (_directoryQuotaManager is IDirectoryQuotaProjectionManager directoryQuotaProjectionManager)
            {
                try
                {
                    await directoryQuotaProjectionManager
                        .RollbackAcceptedProjectionAsync(tenantId, directoryPath, directoryRestoreReservation, default)
                        .ConfigureAwait(false);
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
            else if (directoryRestoreReservation)
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

            if (_tenantQuotaManager is ITenantQuotaProjectionManager tenantQuotaProjectionManager)
            {
                try
                {
                    await tenantQuotaProjectionManager
                        .RollbackAcceptedProjectionAsync(tenantId, tenantRestoreReservation, default)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed to rollback projected tenant quota increment for tenant {TenantId}",
                        tenantId);
                }
            }
            else if (tenantRestoreReservation)
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

            public long LastSequenceNumber { get; set; }

            public bool GapDetected { get; set; }

            public DateTime? LastGapDetectedAtUtc { get; set; }

            public long? GapExpectedSequenceNumber { get; set; }

            public long? GapObservedSequenceNumber { get; set; }
        }

        private sealed class CachedProjectionCursor
        {
            public CachedProjectionCursor(ProjectionCursor cursor, DateTime lastPersistedAtUtc, bool dirty)
            {
                Cursor = cursor ?? throw new ArgumentNullException(nameof(cursor));
                LastPersistedAtUtc = lastPersistedAtUtc;
                Dirty = dirty;
            }

            public ProjectionCursor Cursor { get; }

            public DateTime LastPersistedAtUtc { get; }

            public bool Dirty { get; }
        }

        private sealed class ProjectionSnapshot
        {
            public string TenantId { get; set; } = string.Empty;

            public DateTime CreatedAtUtc { get; set; }

            public long CursorOffset { get; set; }

            public System.Collections.Generic.List<FileMetadata> Files { get; set; } = new System.Collections.Generic.List<FileMetadata>();
        }

        private sealed class JournalStateView
        {
            [JsonConverter(typeof(JsonStringEnumConverter))]
            public JournalFormat? Format { get; set; }

            public bool CorruptTailDetected { get; set; }

            public DateTime? LastCorruptTailDetectedAtUtc { get; set; }

            public long? LastCorruptTailOffset { get; set; }

            public int AutoRepairCount { get; set; }
        }

        private sealed class ProjectionCycleResult
        {
            public ProjectionCycleResult(long? advancedCursorOffset, ProjectionMaintenanceWork? maintenanceWork)
            {
                AdvancedCursorOffset = advancedCursorOffset;
                MaintenanceWork = maintenanceWork;
            }

            public long? AdvancedCursorOffset { get; }

            public ProjectionMaintenanceWork? MaintenanceWork { get; }
        }

        private sealed class ProjectionMaintenanceWork
        {
            public ProjectionMaintenanceWork(ProjectionSnapshotCapture snapshotCapture, bool compactAfterSnapshot)
            {
                SnapshotCapture = snapshotCapture ?? throw new ArgumentNullException(nameof(snapshotCapture));
                CompactAfterSnapshot = compactAfterSnapshot;
            }

            public ProjectionSnapshotCapture SnapshotCapture { get; }

            public bool CompactAfterSnapshot { get; }
        }

        private sealed class ProjectionSnapshotCapture
        {
            public ProjectionSnapshotCapture(string tenantId, long cursorOffset, IReadOnlyList<FileMetadata> metadata)
            {
                TenantId = tenantId ?? throw new ArgumentNullException(nameof(tenantId));
                CursorOffset = cursorOffset;
                Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
            }

            public string TenantId { get; }

            public long CursorOffset { get; }

            public IReadOnlyList<FileMetadata> Metadata { get; }
        }

        private sealed class DeferredProjectionException : IOException
        {
            public DeferredProjectionException(string tenantId, string fileKey, string physicalPath, string message)
                : base(message)
            {
                TenantId = tenantId;
                FileKey = fileKey;
                PhysicalPath = physicalPath;
            }

            public string TenantId { get; }

            public string FileKey { get; }

            public string PhysicalPath { get; }
        }

        private sealed class ProjectionRecoveryTenantContext : ITenantContext
        {
            public ProjectionRecoveryTenantContext(string tenantId)
            {
                TenantId = tenantId ?? throw new ArgumentNullException(nameof(tenantId));
            }

            public string TenantId { get; }

            public TenantStatus Status => TenantStatus.Enabled;
        }
    }
}
