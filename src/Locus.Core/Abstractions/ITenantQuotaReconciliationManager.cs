using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Optional maintenance capability for tenant quota managers.
    /// Allows recovery code to set an exact file count derived from authoritative metadata.
    /// </summary>
    public interface ITenantQuotaReconciliationManager
    {
        /// <summary>
        /// Sets the exact file count for a tenant.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="count">The exact file count.</param>
        /// <param name="ct">Cancellation token.</param>
        Task SetFileCountAsync(string tenantId, int count, CancellationToken ct = default);
    }
}
