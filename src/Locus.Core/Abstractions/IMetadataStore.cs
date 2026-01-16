using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Provides high-performance in-memory metadata storage with persistent backing.
    /// Combines concurrent in-memory operations with file-based durability.
    /// </summary>
    public interface IMetadataStore
    {
        /// <summary>
        /// Adds a new file to the metadata store.
        /// </summary>
        /// <param name="fileLocation">The file location metadata.</param>
        /// <param name="ct">Cancellation token.</param>
        Task AddFileAsync(FileLocation fileLocation, CancellationToken ct);

        /// <summary>
        /// Gets a file's metadata by its key.
        /// </summary>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The file location, or null if not found.</returns>
        Task<FileLocation?> GetFileAsync(string fileKey, CancellationToken ct);

        /// <summary>
        /// Atomically dequeues the next pending file and marks it as processing.
        /// This operation is thread-safe and ensures no two threads get the same file.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The next pending file, or null if none available.</returns>
        Task<FileLocation?> DequeueNextPendingFileAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Updates a file's status.
        /// </summary>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="status">The new status.</param>
        /// <param name="ct">Cancellation token.</param>
        Task UpdateFileStatusAsync(string fileKey, FileProcessingStatus status, CancellationToken ct);

        /// <summary>
        /// Updates a file's retry information after a failure.
        /// </summary>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="retryCount">The new retry count.</param>
        /// <param name="lastError">The error message.</param>
        /// <param name="ct">Cancellation token.</param>
        Task UpdateFileRetryInfoAsync(string fileKey, int retryCount, string lastError, CancellationToken ct);

        /// <summary>
        /// Removes a file from the metadata store.
        /// </summary>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RemoveFileAsync(string fileKey, CancellationToken ct);

        /// <summary>
        /// Gets all files with a specific status.
        /// </summary>
        /// <param name="status">The file status to filter by.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A collection of file locations.</returns>
        Task<IEnumerable<FileLocation>> GetFilesByStatusAsync(FileProcessingStatus status, CancellationToken ct);

        /// <summary>
        /// Gets the current file count for a directory.
        /// </summary>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The current file count.</returns>
        Task<int> GetDirectoryFileCountAsync(string directoryPath, CancellationToken ct);

        /// <summary>
        /// Atomically increments the file count for a directory if under quota.
        /// </summary>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="maxFiles">The maximum allowed files.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>True if incremented successfully; false if quota exceeded.</returns>
        Task<bool> TryIncrementDirectoryFileCountAsync(string directoryPath, int maxFiles, CancellationToken ct);

        /// <summary>
        /// Atomically decrements the file count for a directory.
        /// </summary>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="ct">Cancellation token.</param>
        Task DecrementDirectoryFileCountAsync(string directoryPath, CancellationToken ct);

        /// <summary>
        /// Forces a snapshot of the current in-memory state to disk.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        Task FlushAsync(CancellationToken ct);
    }
}
