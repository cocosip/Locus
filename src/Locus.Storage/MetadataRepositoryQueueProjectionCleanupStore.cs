using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Adapts <see cref="MetadataRepository"/> to cleanup-oriented projection operations.
    /// </summary>
    public sealed class MetadataRepositoryQueueProjectionCleanupStore : IQueueProjectionCleanupStore
    {
        private readonly MetadataRepository _repository;

        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataRepositoryQueueProjectionCleanupStore"/> class.
        /// </summary>
        public MetadataRepositoryQueueProjectionCleanupStore(MetadataRepository repository)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        }

        /// <inheritdoc/>
        public Task<IReadOnlyList<FileMetadata>> GetTimedOutProcessingFilesAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            CancellationToken ct = default)
        {
            return _repository.GetProcessingTimedOutAsync(tenantId, cutoffUtc, limit, ct);
        }

        /// <inheritdoc/>
        public Task<bool> TryResetTimedOutFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            DateTime availableForProcessingAtUtc,
            CancellationToken ct = default)
        {
            return _repository.TryResetTimedOutFileAsync(
                tenantId,
                fileKey,
                expectedProcessingStartTimeUtc,
                availableForProcessingAtUtc,
                ct);
        }

        /// <inheritdoc/>
        public Task<IReadOnlyList<FileMetadata>> GetCompletedFilesOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            ISet<string>? excludedFileKeys = null,
            CancellationToken ct = default)
        {
            return _repository.GetCompletedOlderThanAsync(tenantId, cutoffUtc, limit, excludedFileKeys, ct);
        }

        /// <inheritdoc/>
        public Task<IReadOnlyList<FileMetadata>> GetDeleteRequestedFilesOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            ISet<string>? excludedFileKeys = null,
            CancellationToken ct = default)
        {
            return _repository.GetDeleteRequestedOlderThanAsync(tenantId, cutoffUtc, limit, excludedFileKeys, ct);
        }

        /// <inheritdoc/>
        public Task<bool> TryMarkDeleteSucceededAsync(
            string tenantId,
            string fileKey,
            DateTime expectedCompletedAtUtc,
            DateTime deleteSucceededAtUtc,
            CancellationToken ct = default)
        {
            return _repository.TryMarkDeleteSucceededAsync(
                tenantId,
                fileKey,
                expectedCompletedAtUtc,
                deleteSucceededAtUtc,
                ct);
        }

        /// <inheritdoc/>
        public Task<IReadOnlyList<FileMetadata>> GetPermanentlyFailedFilesOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            ISet<string>? excludedFileKeys = null,
            CancellationToken ct = default)
        {
            return _repository.GetPermanentlyFailedOlderThanAsync(tenantId, cutoffUtc, limit, excludedFileKeys, ct);
        }

        /// <inheritdoc/>
        public Task<bool> TryRemovePermanentlyFailedFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedLastFailedAtUtc,
            CancellationToken ct = default)
        {
            return _repository.TryRemovePermanentlyFailedFileAsync(tenantId, fileKey, expectedLastFailedAtUtc, ct);
        }

        /// <inheritdoc/>
        public Task<bool> TryMarkPermanentlyFailedDeleteSucceededAsync(
            string tenantId,
            string fileKey,
            DateTime expectedLastFailedAtUtc,
            DateTime deleteSucceededAtUtc,
            CancellationToken ct = default)
        {
            return _repository.TryMarkPermanentlyFailedDeleteSucceededAsync(
                tenantId,
                fileKey,
                expectedLastFailedAtUtc,
                deleteSucceededAtUtc,
                ct);
        }

        /// <inheritdoc/>
        public Task<bool> TryMarkPermanentlyFailedDeadLetteredAsync(
            string tenantId,
            string fileKey,
            DateTime expectedLastFailedAtUtc,
            DateTime deadLetteredAtUtc,
            string deadLetterPhysicalPath,
            string? volumeId,
            bool projectionApplied,
            CancellationToken ct = default)
        {
            return _repository.TryMarkPermanentlyFailedDeadLetteredAsync(
                tenantId,
                fileKey,
                expectedLastFailedAtUtc,
                deadLetteredAtUtc,
                deadLetterPhysicalPath,
                volumeId,
                projectionApplied,
                ct);
        }

        /// <inheritdoc/>
        public Task<bool> TryRemoveCompletedFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedCompletedAtUtc,
            CancellationToken ct = default)
        {
            return _repository.TryRemoveCompletedFileAsync(tenantId, fileKey, expectedCompletedAtUtc, ct);
        }
    }
}
