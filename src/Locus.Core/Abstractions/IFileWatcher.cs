using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Monitors directories for new files and automatically imports them into the storage pool.
    /// Supports multi-tenant directory mapping with recursive subdirectory scanning.
    /// </summary>
    public interface IFileWatcher
    {
        /// <summary>
        /// Registers a new directory watcher for a tenant.
        /// </summary>
        /// <param name="configuration">The watcher configuration.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RegisterWatcherAsync(FileWatcherConfiguration configuration, CancellationToken ct);

        /// <summary>
        /// Updates an existing watcher configuration.
        /// </summary>
        /// <param name="configuration">The updated configuration.</param>
        /// <param name="ct">Cancellation token.</param>
        Task UpdateWatcherAsync(FileWatcherConfiguration configuration, CancellationToken ct);

        /// <summary>
        /// Removes a watcher configuration.
        /// </summary>
        /// <param name="watcherId">The unique watcher identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RemoveWatcherAsync(string watcherId, CancellationToken ct);

        /// <summary>
        /// Enables a watcher for a specific tenant.
        /// </summary>
        /// <param name="watcherId">The unique watcher identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task EnableWatcherAsync(string watcherId, CancellationToken ct);

        /// <summary>
        /// Disables a watcher for a specific tenant.
        /// </summary>
        /// <param name="watcherId">The unique watcher identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task DisableWatcherAsync(string watcherId, CancellationToken ct);

        /// <summary>
        /// Gets all watcher configurations for a specific tenant.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A collection of watcher configurations.</returns>
        Task<IEnumerable<FileWatcherConfiguration>> GetWatchersForTenantAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Gets a specific watcher configuration.
        /// </summary>
        /// <param name="watcherId">The unique watcher identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The watcher configuration, or null if not found.</returns>
        Task<FileWatcherConfiguration?> GetWatcherAsync(string watcherId, CancellationToken ct);

        /// <summary>
        /// Manually triggers an immediate scan of a watcher's directory.
        /// </summary>
        /// <param name="watcherId">The unique watcher identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Statistics about the scan operation.</returns>
        Task<FileWatcherScanResult> ScanNowAsync(string watcherId, CancellationToken ct);

        /// <summary>
        /// Gets all registered watcher configurations.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A collection of all watcher configurations.</returns>
        Task<IEnumerable<FileWatcherConfiguration>> GetAllWatchersAsync(CancellationToken ct);
    }

    /// <summary>
    /// Result of a file watcher scan operation.
    /// </summary>
    public class FileWatcherScanResult
    {
        /// <summary>
        /// Gets or sets the number of files discovered.
        /// </summary>
        public int FilesDiscovered { get; set; }

        /// <summary>
        /// Gets or sets the number of files successfully imported.
        /// </summary>
        public int FilesImported { get; set; }

        /// <summary>
        /// Gets or sets the number of files skipped (already imported, too large, etc.).
        /// </summary>
        public int FilesSkipped { get; set; }

        /// <summary>
        /// Gets or sets the number of files that failed to import.
        /// </summary>
        public int FilesFailed { get; set; }

        /// <summary>
        /// Gets or sets the total bytes imported.
        /// </summary>
        public long BytesImported { get; set; }

        /// <summary>
        /// Gets or sets any error messages encountered during scanning.
        /// </summary>
        public List<string> Errors { get; set; } = new List<string>();
    }
}
