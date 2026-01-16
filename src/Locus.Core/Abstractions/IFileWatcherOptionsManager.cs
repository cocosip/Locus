using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Manages global file watcher service configuration.
    /// </summary>
    public interface IFileWatcherOptionsManager
    {
        /// <summary>
        /// Gets the current file watcher options.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The current options.</returns>
        Task<FileWatcherOptions> GetOptionsAsync(CancellationToken ct);

        /// <summary>
        /// Updates the file watcher options.
        /// </summary>
        /// <param name="options">The new options.</param>
        /// <param name="ct">Cancellation token.</param>
        Task UpdateOptionsAsync(FileWatcherOptions options, CancellationToken ct);

        /// <summary>
        /// Enables the file watcher service globally.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        Task EnableServiceAsync(CancellationToken ct);

        /// <summary>
        /// Disables the file watcher service globally.
        /// When disabled, all file monitoring will be stopped.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        Task DisableServiceAsync(CancellationToken ct);

        /// <summary>
        /// Checks if the file watcher service is globally enabled.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>True if enabled, false otherwise.</returns>
        Task<bool> IsServiceEnabledAsync(CancellationToken ct);
    }
}
