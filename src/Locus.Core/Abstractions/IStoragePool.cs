using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Provides file storage operations with multi-tenant isolation and automatic volume management.
    /// Storage volumes are configured at startup and managed internally.
    /// </summary>
    public interface IStoragePool
    {
        /// <summary>
        /// Writes a file to the storage pool asynchronously.
        /// The system automatically:
        /// - Generates a unique file key
        /// - Selects an appropriate storage volume based on available space
        /// - Validates tenant status and tenant quotas
        /// - Creates file metadata for tracking
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="content">A stream containing the file contents.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The system-generated unique file key for retrieving the file later.</returns>
        /// <exception cref="Core.Exceptions.TenantDisabledException">Thrown when the tenant is disabled.</exception>
        /// <exception cref="Core.Exceptions.TenantQuotaExceededException">Thrown when tenant file count limit is exceeded.</exception>
        /// <exception cref="Core.Exceptions.InsufficientStorageException">Thrown when no volume has sufficient space.</exception>
        Task<string> WriteFileAsync(ITenantContext tenant, Stream content, CancellationToken ct);

        /// <summary>
        /// Reads a file from the storage pool asynchronously using its file key.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="fileKey">The unique file key returned from WriteFileAsync.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A stream containing the file contents.</returns>
        /// <exception cref="Core.Exceptions.TenantDisabledException">Thrown when the tenant is disabled.</exception>
        /// <exception cref="System.IO.FileNotFoundException">Thrown when the file is not found.</exception>
        Task<Stream> ReadFileAsync(ITenantContext tenant, string fileKey, CancellationToken ct);

        /// <summary>
        /// Gets the file information (metadata) for a given file key.
        /// Returns basic file information without physical location details.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The file information, or null if not found.</returns>
        Task<Models.FileInfo?> GetFileInfoAsync(ITenantContext tenant, string fileKey, CancellationToken ct);

        /// <summary>
        /// Gets the file location and metadata for a given file key.
        /// Useful for diagnostics and monitoring.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The file location information, or null if not found.</returns>
        Task<Models.FileLocation?> GetFileLocationAsync(ITenantContext tenant, string fileKey, CancellationToken ct);

        /// <summary>
        /// Gets the next file for processing in the queue.
        /// This operation is thread-safe - each call returns a unique file not being processed by other threads.
        /// The file status is automatically changed from Pending to Processing.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The next file available for processing, or null if no files are available.</returns>
        /// <exception cref="Core.Exceptions.TenantDisabledException">Thrown when the tenant is disabled.</exception>
        Task<Models.FileLocation?> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct);

        /// <summary>
        /// Gets a batch of files for processing in the queue.
        /// This operation is thread-safe - each call returns unique files not being processed by other threads.
        /// The file status is automatically changed from Pending to Processing for all returned files.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="batchSize">The maximum number of files to retrieve.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A collection of files available for processing (may be less than batchSize).</returns>
        /// <exception cref="Core.Exceptions.TenantDisabledException">Thrown when the tenant is disabled.</exception>
        Task<System.Collections.Generic.IEnumerable<Models.FileLocation>> GetNextBatchForProcessingAsync(
            ITenantContext tenant,
            int batchSize,
            CancellationToken ct);

        /// <summary>
        /// Marks a file as successfully processed and deletes it from storage.
        /// This removes both the physical file and metadata, and decrements the tenant quota count.
        /// Use this method after successfully processing a file obtained from GetNextFileForProcessingAsync.
        /// </summary>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="ct">Cancellation token.</param>
        Task MarkAsCompletedAsync(string fileKey, CancellationToken ct);

        /// <summary>
        /// Marks a file as failed processing.
        /// The file will be automatically retried based on the configured retry policy.
        /// If retry count exceeds the maximum, the file is marked as PermanentlyFailed.
        /// </summary>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="errorMessage">The error message describing the failure.</param>
        /// <param name="ct">Cancellation token.</param>
        Task MarkAsFailedAsync(string fileKey, string errorMessage, CancellationToken ct);

        /// <summary>
        /// Gets the current processing status of a file.
        /// </summary>
        /// <param name="fileKey">The unique file key.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The current status of the file.</returns>
        Task<Models.FileProcessingStatus> GetFileStatusAsync(string fileKey, CancellationToken ct);

        /// <summary>
        /// Gets the total capacity across all configured storage volumes in bytes.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The total capacity in bytes.</returns>
        Task<long> GetTotalCapacityAsync(CancellationToken ct);

        /// <summary>
        /// Gets the total available space across all configured storage volumes in bytes.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The total available space in bytes.</returns>
        Task<long> GetAvailableSpaceAsync(CancellationToken ct);
    }
}
