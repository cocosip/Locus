using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Automatically manages file watchers based on root configuration.
    /// </summary>
    public interface IFileWatcherAutoManager
    {
        /// <summary>
        /// Applies a root configuration and automatically creates/updates watchers.
        /// </summary>
        /// <param name="rootConfig">The root configuration.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of watchers created or updated.</returns>
        Task<int> ApplyRootConfigurationAsync(FileWatcherRootConfiguration rootConfig, CancellationToken ct);

        /// <summary>
        /// Discovers new tenant directories and creates watchers for them.
        /// Only applicable in multi-tenant mode.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of new watchers created.</returns>
        Task<int> DiscoverAndCreateWatchersAsync(CancellationToken ct);

        /// <summary>
        /// Removes all watchers managed by this auto-manager.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        Task RemoveAllWatchersAsync(CancellationToken ct);

        /// <summary>
        /// Gets the current root configuration.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The root configuration, or null if not set.</returns>
        Task<FileWatcherRootConfiguration?> GetRootConfigurationAsync(CancellationToken ct);
    }
}
