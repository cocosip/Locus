using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Represents a storage volume in the storage pool system.
    /// </summary>
    public interface IStorageVolume
    {
        /// <summary>
        /// Gets the unique identifier for this storage volume.
        /// </summary>
        string VolumeId { get; }

        /// <summary>
        /// Gets the mount path of this storage volume.
        /// </summary>
        string MountPath { get; }

        /// <summary>
        /// Gets the total capacity of this storage volume in bytes.
        /// </summary>
        long TotalCapacity { get; }

        /// <summary>
        /// Gets the available space on this storage volume in bytes.
        /// </summary>
        long AvailableSpace { get; }

        /// <summary>
        /// Gets whether this storage volume is healthy and available for operations.
        /// </summary>
        bool IsHealthy { get; }

        /// <summary>
        /// Reads a file from the storage volume asynchronously.
        /// </summary>
        /// <param name="path">The relative path of the file to read.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A stream containing the file contents.</returns>
        Task<Stream> ReadAsync(string path, CancellationToken ct);

        /// <summary>
        /// Writes a file to the storage volume asynchronously.
        /// </summary>
        /// <param name="path">The relative path where the file should be written.</param>
        /// <param name="content">A stream containing the file contents.</param>
        /// <param name="ct">Cancellation token.</param>
        Task WriteAsync(string path, Stream content, CancellationToken ct);

        /// <summary>
        /// Deletes a file from the storage volume asynchronously.
        /// </summary>
        /// <param name="path">The relative path of the file to delete.</param>
        /// <param name="ct">Cancellation token.</param>
        Task DeleteAsync(string path, CancellationToken ct);
    }
}
