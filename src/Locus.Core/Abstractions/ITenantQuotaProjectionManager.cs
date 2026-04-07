using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Applies tenant quota changes from durable queue projection events.
    /// </summary>
    public interface ITenantQuotaProjectionManager
    {
        /// <summary>
        /// Applies an Accepted projection for a tenant.
        /// Returns true when an existing reservation was consumed.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task<bool> ApplyAcceptedProjectionAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Rolls back a previously applied Accepted projection.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="restoreReservation">True when the accepted projection had consumed a reservation.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RollbackAcceptedProjectionAsync(string tenantId, bool restoreReservation, CancellationToken ct = default);

        /// <summary>
        /// Applies a DeleteSucceeded projection for a tenant.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task ApplyDeleteSucceededProjectionAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Rolls back a previously applied DeleteSucceeded projection.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RollbackDeleteSucceededProjectionAsync(string tenantId, CancellationToken ct = default);
    }
}
