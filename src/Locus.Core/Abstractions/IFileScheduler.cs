using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Schedules and manages file processing operations with concurrency control.
    /// </summary>
    public interface IFileScheduler
    {
        /// <summary>
        /// Gets the next available file for processing in a thread-safe manner.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The file location, or null if no files are available.</returns>
        Task<FileLocation?> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct = default);

        /// <summary>
        /// Gets a batch of files for processing in a thread-safe manner.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="batchSize">The maximum number of files to retrieve.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A collection of file locations. May contain fewer files than requested or be empty if no files are available.</returns>
        Task<IEnumerable<FileLocation>> GetNextBatchForProcessingAsync(ITenantContext tenant, int batchSize, CancellationToken ct = default);

        /// <summary>
        /// Marks a file as currently being processed.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="ct">Cancellation token.</param>
        Task MarkAsProcessingAsync(string tenantId, string fileKey, CancellationToken ct = default);

        /// <summary>
        /// Marks a file as completed and leaves physical deletion to background cleanup.
        /// </summary>
        /// <param name="lease">The tenant-scoped processing lease returned when the file was allocated.</param>
        /// <param name="ct">Cancellation token.</param>
        Task MarkAsCompletedAsync(FileProcessingLease lease, CancellationToken ct = default);

        /// <summary>
        /// Marks a file as failed and returns it to the pool for retry or permanent failure.
        /// </summary>
        /// <param name="lease">The tenant-scoped processing lease returned when the file was allocated.</param>
        /// <param name="errorMessage">The error message describing the failure.</param>
        /// <param name="ct">Cancellation token.</param>
        Task MarkAsFailedAsync(FileProcessingLease lease, string errorMessage, CancellationToken ct = default);

        /// <summary>
        /// Resets the processing status of a file to pending.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="ct">Cancellation token.</param>
        Task ResetProcessingStatusAsync(string tenantId, string fileKey, CancellationToken ct = default);

        /// <summary>
        /// Gets the current processing status of a file.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The file processing status.</returns>
        Task<FileProcessingStatus> GetFileStatusAsync(string tenantId, string fileKey, CancellationToken ct = default);

        /// <summary>
        /// Cleans up orphaned metadata where physical files no longer exist.
        /// Returns the number of orphaned metadata records removed.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The number of orphaned metadata records removed.</returns>
        Task<int> CleanupOrphanedMetadataAsync(CancellationToken ct = default);
    }
}
