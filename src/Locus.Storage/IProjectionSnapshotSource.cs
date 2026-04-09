using System.Collections.Generic;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Provides a point-in-time metadata snapshot that callers can serialize after leaving a hot lock.
    /// Returned metadata objects must be treated as read-only.
    /// </summary>
    internal interface IProjectionSnapshotSource
    {
        IReadOnlyList<FileMetadata> CaptureProjectedFilesSnapshot(string tenantId);
    }
}
