using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Allows a storage volume to warm write-path caches during startup.
    /// </summary>
    public interface IStorageVolumeWritePathWarmup
    {
        /// <summary>
        /// Warms any internal caches used by the write path.
        /// </summary>
        Task WarmWritePathCacheAsync(CancellationToken ct = default);
    }
}
