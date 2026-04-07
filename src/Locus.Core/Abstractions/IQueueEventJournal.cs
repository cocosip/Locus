using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Provides durable append-only queue event storage per tenant.
    /// </summary>
    public interface IQueueEventJournal
    {
        /// <summary>
        /// Appends a queue event record to the tenant journal.
        /// </summary>
        Task AppendAsync(QueueEventRecord record, CancellationToken ct = default);

        /// <summary>
        /// Reads a bounded batch of queue event records from a tenant journal, starting at a byte offset.
        /// </summary>
        Task<QueueEventReadBatch> ReadBatchAsync(
            string tenantId,
            long offset,
            int maxRecords,
            CancellationToken ct = default);

        /// <summary>
        /// Compacts a tenant journal by discarding bytes before the processed offset.
        /// Returns the next cursor offset for the compacted file.
        /// </summary>
        Task<long> CompactAsync(string tenantId, long processedOffset, CancellationToken ct = default);

        /// <summary>
        /// Gets the current logical tail offset for a tenant journal.
        /// This value includes any compacted base offset and is suitable for lag calculation.
        /// </summary>
        Task<long> GetTailOffsetAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Gets the current logical base offset for a tenant journal after compaction.
        /// </summary>
        Task<long> GetBaseOffsetAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Returns all tenant IDs that currently have a journal directory on disk.
        /// </summary>
        Task<IReadOnlyList<string>> GetTenantIdsAsync(CancellationToken ct = default);
    }
}
