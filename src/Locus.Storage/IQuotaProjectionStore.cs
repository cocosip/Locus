using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Represents quota projection persistence used by quota managers.
    /// This keeps quota hot-state management on a storage-facing surface
    /// instead of coupling it directly to DirectoryQuotaRepository.
    /// </summary>
    public interface IQuotaProjectionStore
    {
        /// <summary>
        /// Gets an existing quota row or creates a default row when missing.
        /// </summary>
        Task<DirectoryQuota> GetOrCreateQuotaAsync(string tenantId, string directoryPath, CancellationToken ct = default);

        /// <summary>
        /// Gets a quota row when it already exists.
        /// </summary>
        Task<DirectoryQuota?> GetQuotaAsync(string tenantId, string directoryPath, CancellationToken ct = default);

        /// <summary>
        /// Persists quota configuration.
        /// </summary>
        Task UpdateQuotaAsync(string tenantId, DirectoryQuota quota, CancellationToken ct = default);

        /// <summary>
        /// Persists the projected live count.
        /// </summary>
        Task SetProjectedCountAsync(string tenantId, string directoryPath, int count, CancellationToken ct = default);
    }
}
