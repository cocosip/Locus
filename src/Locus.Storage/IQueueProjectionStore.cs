using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Represents the projected queue state used by scheduling and replay.
    /// This keeps scheduler-facing code on the projection surface instead of
    /// coupling it to the underlying metadata repository implementation.
    /// </summary>
    public interface IQueueProjectionStore
    {
        /// <summary>
        /// Lists projected tenant identifiers.
        /// </summary>
        Task<IReadOnlyList<string>> GetProjectedTenantIdsAsync(CancellationToken ct = default);

        /// <summary>
        /// Leases the next projected pending file for a tenant.
        /// </summary>
        Task<FileMetadata?> LeaseNextPendingFileAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Leases the next projected batch of pending files for a tenant.
        /// </summary>
        Task<IReadOnlyList<FileMetadata>> LeaseNextPendingBatchAsync(string tenantId, int batchSize, CancellationToken ct = default);

        /// <summary>
        /// Gets the projected metadata for a file.
        /// </summary>
        Task<FileMetadata?> GetProjectedFileAsync(string tenantId, string fileKey, CancellationToken ct = default);

        /// <summary>
        /// Updates a projected processing file if the lease still matches.
        /// </summary>
        Task<FileMetadata?> TryUpdateProjectedProcessingFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            Func<FileMetadata, FileMetadata> updateFactory,
            CancellationToken ct = default);

        /// <summary>
        /// Marks a projected pending file as processing if it is still pending.
        /// </summary>
        Task<FileMetadata?> TryMarkProjectedPendingFileAsProcessingAsync(
            string tenantId,
            string fileKey,
            DateTime processingStartTimeUtc,
            CancellationToken ct = default);

        /// <summary>
        /// Resets a projected file back to pending.
        /// </summary>
        Task<FileMetadata?> TryResetProjectedFileToPendingAsync(string tenantId, string fileKey, CancellationToken ct = default);

        /// <summary>
        /// Upserts projected metadata synchronously.
        /// </summary>
        Task UpsertProjectedFileAsync(FileMetadata metadata, CancellationToken ct = default);

        /// <summary>
        /// Removes projected metadata synchronously.
        /// </summary>
        Task<bool> RemoveProjectedFileAsync(string tenantId, string fileKey, CancellationToken ct = default);

        /// <summary>
        /// Lists projected metadata for a tenant.
        /// </summary>
        Task<IReadOnlyList<FileMetadata>> GetProjectedFilesAsync(string tenantId, CancellationToken ct = default);
    }
}
