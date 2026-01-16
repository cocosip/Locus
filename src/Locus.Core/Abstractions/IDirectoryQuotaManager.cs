using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Manages directory-level file count quotas.
    /// </summary>
    public interface IDirectoryQuotaManager
    {
        /// <summary>
        /// Checks whether a file can be added to the specified directory without exceeding the quota.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>True if a file can be added; otherwise, false.</returns>
        Task<bool> CanAddFileAsync(string tenantId, string directoryPath, CancellationToken ct);

        /// <summary>
        /// Increments the file count for the specified directory atomically.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        Task IncrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct);

        /// <summary>
        /// Decrements the file count for the specified directory atomically.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        Task DecrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct);

        /// <summary>
        /// Gets the current file count for the specified directory.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The current file count.</returns>
        Task<int> GetFileCountAsync(string tenantId, string directoryPath, CancellationToken ct);

        /// <summary>
        /// Gets the quota limit for the specified directory.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The maximum file count allowed.</returns>
        Task<int> GetLimitAsync(string tenantId, string directoryPath, CancellationToken ct);

        /// <summary>
        /// Sets the quota limit for the specified directory.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="maxFiles">The maximum number of files allowed.</param>
        /// <param name="ct">Cancellation token.</param>
        Task SetLimitAsync(string tenantId, string directoryPath, int maxFiles, CancellationToken ct);
    }
}
