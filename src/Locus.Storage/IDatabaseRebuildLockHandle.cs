using System;

namespace Locus.Storage
{
    /// <summary>
    /// Represents an acquired database rebuild lock that must be disposed to release it.
    /// </summary>
    public interface IDatabaseRebuildLockHandle : IDisposable
    {
        /// <summary>
        /// Gets the backup path created for the corrupted database, if any.
        /// </summary>
        string? BackupPath { get; }
    }
}
