using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.IO
{
    /// <summary>
    /// Provides best-effort durable flush helpers for state files.
    /// Atomic replace protects file integrity; these helpers additionally request
    /// that the OS flush file contents to the storage device when the underlying
    /// stream supports it.
    /// </summary>
    public static class DurableFileWrite
    {
        /// <summary>
        /// Asynchronously flushes the stream to disk with best-effort durability guarantees.
        /// </summary>
        /// <param name="stream">The stream to flush to disk.</param>
        /// <param name="ct">Cancellation token for the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <remarks>
        /// This method performs two levels of flushing:
        /// 1. Calls FlushAsync to flush buffered data to the OS file system
        /// 2. If the stream is a FileStream, calls Flush(true) to request the OS
        ///    to write data to the physical storage device
        /// </remarks>
        public static async Task FlushToDiskAsync(Stream stream, CancellationToken ct = default)
        {
            await stream.FlushAsync(ct).ConfigureAwait(false);

            if (stream is FileStream fileStream)
                fileStream.Flush(true);
        }

        /// <summary>
        /// Synchronously flushes the stream to disk with best-effort durability guarantees.
        /// </summary>
        /// <param name="stream">The stream to flush to disk.</param>
        /// <remarks>
        /// This method performs two levels of flushing:
        /// 1. Calls Flush to flush buffered data to the OS file system
        /// 2. If the stream is a FileStream, calls Flush(true) to request the OS
        ///    to write data to the physical storage device
        /// </remarks>
        public static void FlushToDisk(Stream stream)
        {
            stream.Flush();

            if (stream is FileStream fileStream)
                fileStream.Flush(true);
        }
    }
}