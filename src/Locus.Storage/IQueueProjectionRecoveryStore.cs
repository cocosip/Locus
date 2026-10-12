using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Optional atomic creation capability for recovering physical files without metadata.
    /// </summary>
    public interface IQueueProjectionRecoveryStore
    {
        /// <summary>
        /// Creates recovered metadata only when no current record or pending mutation exists.
        /// Returns false without changing an existing record.
        /// </summary>
        Task<bool> TryAddRecoveredFileAsync(FileMetadata metadata, CancellationToken ct = default);
    }
}
