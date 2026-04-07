using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Represents the write-behind entry point for projected queue metadata.
    /// This keeps write-side callers on a projection surface instead of
    /// coupling them to the underlying metadata repository implementation.
    /// </summary>
    public interface IQueueProjectionWriteStore
    {
        /// <summary>
        /// Queues projected metadata for persistence.
        /// </summary>
        Task QueueProjectedFileAsync(FileMetadata metadata, CancellationToken ct = default);
    }
}
