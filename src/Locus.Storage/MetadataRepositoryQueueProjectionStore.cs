using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Adapts <see cref="MetadataRepository"/> to the scheduler-facing projection store contract.
    /// </summary>
    public sealed class MetadataRepositoryQueueProjectionStore : IQueueProjectionStore, IProjectionSnapshotSource, IQueueProjectionBatchStore
    {
        private readonly MetadataRepository _repository;

        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataRepositoryQueueProjectionStore"/> class.
        /// </summary>
        public MetadataRepositoryQueueProjectionStore(MetadataRepository repository)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> GetProjectedTenantIdsAsync(CancellationToken ct = default)
        {
            return (await _repository.GetAllTenantIdsAsync(ct).ConfigureAwait(false)).ToList();
        }

        /// <inheritdoc/>
        public Task<FileMetadata?> LeaseNextPendingFileAsync(string tenantId, CancellationToken ct = default)
        {
            return _repository.GetNextPendingFileAsync(tenantId, ct);
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<FileMetadata>> LeaseNextPendingBatchAsync(
            string tenantId,
            int batchSize,
            CancellationToken ct = default)
        {
            return (await _repository.GetNextPendingBatchAsync(tenantId, batchSize, ct).ConfigureAwait(false)).ToList();
        }

        /// <inheritdoc/>
        public Task<FileMetadata?> GetProjectedFileAsync(string tenantId, string fileKey, CancellationToken ct = default)
        {
            return _repository.GetAsync(tenantId, fileKey, ct);
        }

        /// <inheritdoc/>
        public Task<FileMetadata?> TryUpdateProjectedProcessingFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            Func<FileMetadata, FileMetadata> updateFactory,
            CancellationToken ct = default)
        {
            return _repository.TryUpdateProcessingFileAsync(
                tenantId,
                fileKey,
                expectedProcessingStartTimeUtc,
                updateFactory,
                ct);
        }

        /// <inheritdoc/>
        public Task<FileMetadata?> TryMarkProjectedPendingFileAsProcessingAsync(
            string tenantId,
            string fileKey,
            DateTime processingStartTimeUtc,
            CancellationToken ct = default)
        {
            return _repository.TryMarkPendingFileAsProcessingAsync(tenantId, fileKey, processingStartTimeUtc, ct);
        }

        /// <inheritdoc/>
        public Task<FileMetadata?> TryResetProjectedFileToPendingAsync(
            string tenantId,
            string fileKey,
            CancellationToken ct = default)
        {
            return _repository.TryResetFileToPendingAsync(tenantId, fileKey, ct);
        }

        /// <inheritdoc/>
        public Task UpsertProjectedFileAsync(FileMetadata metadata, CancellationToken ct = default)
        {
            return _repository.AddOrUpdateDirectAsync(metadata, ct);
        }

        /// <inheritdoc/>
        public Task<bool> RemoveProjectedFileAsync(string tenantId, string fileKey, CancellationToken ct = default)
        {
            return _repository.RemoveDirectAsync(tenantId, fileKey, ct);
        }

        /// <inheritdoc/>
        IQueueProjectionBatch IQueueProjectionBatchStore.BeginBatch(string tenantId)
        {
            return _repository.BeginProjectionBatch(tenantId);
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<FileMetadata>> GetProjectedFilesAsync(string tenantId, CancellationToken ct = default)
        {
            return (await _repository.GetByTenantAsync(tenantId, ct).ConfigureAwait(false)).ToList();
        }

        IReadOnlyList<FileMetadata> IProjectionSnapshotSource.CaptureProjectedFilesSnapshot(string tenantId)
        {
            return _repository.SnapshotTenantMetadataRaw(tenantId);
        }
    }
}
