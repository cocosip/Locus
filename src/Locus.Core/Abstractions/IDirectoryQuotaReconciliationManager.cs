using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Optional maintenance capability for directory quota managers.
    /// Allows recovery code to set an exact file count derived from authoritative metadata.
    /// </summary>
    public interface IDirectoryQuotaReconciliationManager
    {
        /// <summary>
        /// Sets the exact file count for a logical directory.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The normalized logical directory path.</param>
        /// <param name="count">The exact file count.</param>
        /// <param name="ct">Cancellation token.</param>
        Task SetFileCountAsync(string tenantId, string directoryPath, int count, CancellationToken ct = default);
    }
}
