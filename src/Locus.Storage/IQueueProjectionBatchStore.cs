using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Provides tenant-scoped projection batching so a replay cycle can persist
    /// multiple metadata mutations inside one durable transaction.
    /// </summary>
    internal interface IQueueProjectionBatchStore
    {
        /// <summary>
        /// Begins a tenant-scoped projection batch.
        /// </summary>
        IQueueProjectionBatch BeginBatch(string tenantId);
    }

    /// <summary>
    /// Represents an in-flight tenant-scoped projection batch.
    /// </summary>
    internal interface IQueueProjectionBatch
    {
        /// <summary>
        /// Gets the tenant identifier associated with the batch.
        /// </summary>
        string TenantId { get; }

        /// <summary>
        /// Applies an upsert to the in-memory projection and stages it for durable flush.
        /// </summary>
        Task UpsertProjectedFileAsync(FileMetadata metadata, CancellationToken ct = default);

        /// <summary>
        /// Applies a delete to the in-memory projection and stages it for durable flush.
        /// </summary>
        Task<bool> RemoveProjectedFileAsync(string fileKey, CancellationToken ct = default);

        /// <summary>
        /// Flushes the staged mutations durably.
        /// </summary>
        Task FlushAsync(CancellationToken ct = default);
    }
}
