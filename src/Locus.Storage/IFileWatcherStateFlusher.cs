using System.Threading;
using System.Threading.Tasks;

namespace Locus.Storage
{
    /// <summary>
    /// Optional lifecycle capability for flushing durable file watcher state.
    /// </summary>
    public interface IFileWatcherStateFlusher
    {
        /// <summary>
        /// Flushes pending watcher state to durable storage.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        Task FlushStateAsync(CancellationToken ct = default);
    }
}
