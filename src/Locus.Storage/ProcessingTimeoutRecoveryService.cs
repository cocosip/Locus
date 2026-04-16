using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Shared timed-out Processing recovery implementation used by scheduler and cleanup.
    /// </summary>
    public sealed class ProcessingTimeoutRecoveryService : IProcessingTimeoutRecoveryService
    {
        private readonly IQueueProjectionCleanupStore _projectionCleanupStore;
        private readonly IQueueEventJournal? _queueEventJournal;
        private readonly IProcessingTimeoutRecoveryCoordinator _recoveryCoordinator;
        private readonly ILogger<ProcessingTimeoutRecoveryService> _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProcessingTimeoutRecoveryService"/> class.
        /// </summary>
        public ProcessingTimeoutRecoveryService(
            IQueueProjectionCleanupStore projectionCleanupStore,
            IProcessingTimeoutRecoveryCoordinator recoveryCoordinator,
            ILogger<ProcessingTimeoutRecoveryService> logger,
            IQueueEventJournal? queueEventJournal = null)
        {
            _projectionCleanupStore = projectionCleanupStore ?? throw new ArgumentNullException(nameof(projectionCleanupStore));
            _recoveryCoordinator = recoveryCoordinator ?? throw new ArgumentNullException(nameof(recoveryCoordinator));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _queueEventJournal = queueEventJournal;
        }

        /// <inheritdoc/>
        public async Task<ProcessingTimeoutRecoveryResult> RecoverTimedOutFilesAsync(
            string tenantId,
            int batchSize,
            DateTime nowUtc,
            TimeSpan timeout,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (batchSize <= 0 || timeout <= TimeSpan.Zero)
                return default;

            if (!_recoveryCoordinator.TryEnter(tenantId, out var releaser))
            {
                _logger.LogDebug(
                    "Skipped timed-out Processing recovery because another worker already owns tenant {TenantId}",
                    tenantId);
                return default;
            }

            using (releaser)
            {
                var cutoffUtc = nowUtc - timeout;
                var batch = await _projectionCleanupStore.GetTimedOutProcessingFilesAsync(
                    tenantId,
                    cutoffUtc,
                    batchSize,
                    ct).ConfigureAwait(false);
                if (batch.Count == 0)
                    return new ProcessingTimeoutRecoveryResult(true, 0, 0);

                var recoveredCount = 0;
                foreach (var metadata in batch)
                {
                    if (!metadata.ProcessingStartTime.HasValue)
                        continue;

                    try
                    {
                        if (_queueEventJournal != null)
                        {
                            await _queueEventJournal.AppendAsync(
                                CreateProcessingTimedOutEvent(metadata, nowUtc),
                                default).ConfigureAwait(false);
                        }

                        var reset = await _projectionCleanupStore.TryResetTimedOutFileAsync(
                            tenantId,
                            metadata.FileKey,
                            metadata.ProcessingStartTime.Value,
                            nowUtc,
                            ct).ConfigureAwait(false);
                        if (!reset)
                        {
                            _logger.LogDebug(
                                "Skipped timed-out reset for file {FileKey} because the processing lease changed concurrently",
                                metadata.FileKey);
                            continue;
                        }

                        recoveredCount++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(
                            ex,
                            "Failed to reset timed-out file. Tenant={TenantId}, FileKey={FileKey}",
                            tenantId,
                            metadata.FileKey);
                    }
                }

                return new ProcessingTimeoutRecoveryResult(true, batch.Count, recoveredCount);
            }
        }

        private static QueueEventRecord CreateProcessingTimedOutEvent(FileMetadata metadata, DateTime availableForProcessingAtUtc)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.ProcessingTimedOut,
                OccurredAtUtc = availableForProcessingAtUtc,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.Pending,
                ProcessingStartTimeUtc = metadata.ProcessingStartTime,
                RetryCount = metadata.RetryCount,
                AvailableForProcessingAtUtc = availableForProcessingAtUtc,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension
            };
        }
    }
}
