using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Applies directory quota changes from durable queue projection events.
    /// </summary>
    public interface IDirectoryQuotaProjectionManager
    {
        /// <summary>
        /// Applies an Accepted projection for a logical directory.
        /// Returns true when an existing reservation was consumed.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The normalized logical directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        Task<bool> ApplyAcceptedProjectionAsync(string tenantId, string directoryPath, CancellationToken ct = default);

        /// <summary>
        /// Rolls back a previously applied Accepted projection.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The normalized logical directory path.</param>
        /// <param name="restoreReservation">True when the accepted projection had consumed a reservation.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RollbackAcceptedProjectionAsync(string tenantId, string directoryPath, bool restoreReservation, CancellationToken ct = default);

        /// <summary>
        /// Applies a DeleteSucceeded projection for a logical directory.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The normalized logical directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        Task ApplyDeleteSucceededProjectionAsync(string tenantId, string directoryPath, CancellationToken ct = default);

        /// <summary>
        /// Rolls back a previously applied DeleteSucceeded projection.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The normalized logical directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RollbackDeleteSucceededProjectionAsync(string tenantId, string directoryPath, CancellationToken ct = default);
    }
}
