using System;
using System.Threading;
using System.Threading.Tasks;

namespace Locus.Storage
{
    /// <summary>
    /// Coordinates timed-out Processing recovery for a single tenant.
    /// </summary>
    public interface IProcessingTimeoutRecoveryService
    {
        /// <summary>
        /// Recovers a bounded batch of timed-out Processing files for a tenant.
        /// </summary>
        Task<ProcessingTimeoutRecoveryResult> RecoverTimedOutFilesAsync(
            string tenantId,
            int batchSize,
            DateTime nowUtc,
            TimeSpan timeout,
            CancellationToken ct = default);
    }
}
