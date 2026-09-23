using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Storage
{
    /// <summary>
    /// Persists active source-file cleanup work independently from imported-file history.
    /// </summary>
    public interface ISourceCleanupStore
    {
        /// <summary>Gets the active job for a source path, if one exists.</summary>
        /// <param name="sourcePath">The source path to look up.</param>
        /// <param name="fingerprint">The current source fingerprint.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The active job for the path, or <see langword="null"/>.</returns>
        Task<SourceCleanupJob?> GetActiveAsync(string sourcePath, string fingerprint, CancellationToken ct = default);

        /// <summary>Creates or replaces the active job for a watcher and source path.</summary>
        /// <param name="job">The cleanup job to persist.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The persisted job identifier.</returns>
        Task<long> UpsertAsync(SourceCleanupJob job, CancellationToken ct = default);

        /// <summary>Gets due non-keep jobs up to the requested limit.</summary>
        /// <param name="nowUtc">The current UTC time.</param>
        /// <param name="limit">The maximum number of jobs to return.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Due cleanup jobs.</returns>
        Task<IReadOnlyList<SourceCleanupJob>> GetDueAsync(DateTime nowUtc, int limit, CancellationToken ct = default);

        /// <summary>Attempts to acquire a lease for a cleanup job.</summary>
        /// <param name="id">The cleanup job identifier.</param>
        /// <param name="nowUtc">The current UTC time.</param>
        /// <param name="leaseUntilUtc">The requested lease expiration time.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns><see langword="true"/> when the lease was acquired.</returns>
        Task<bool> TryClaimAsync(long id, DateTime nowUtc, DateTime leaseUntilUtc, CancellationToken ct = default);

        /// <summary>Updates an active cleanup job and releases its lease.</summary>
        /// <param name="job">The job to update.</param>
        /// <param name="ct">Cancellation token.</param>
        Task UpdateAsync(SourceCleanupJob job, CancellationToken ct = default);

        /// <summary>Removes a completed cleanup job.</summary>
        /// <param name="id">The cleanup job identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RemoveAsync(long id, CancellationToken ct = default);
    }
}
