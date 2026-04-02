using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Optional maintenance capability for directory quota managers.
    /// Allows reconciliation code to compensate counts for files that already exist physically
    /// without re-applying normal admission checks.
    /// </summary>
    public interface IDirectoryQuotaCompensationManager
    {
        /// <summary>
        /// Compensates the directory file count by incrementing it without enforcing normal write-time quota checks.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The normalized directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CompensateIncrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct = default);
    }
}
