using System;
using System.Collections.Generic;

namespace Locus.Core.Models
{
    /// <summary>
    /// Configuration options for SQLite databases used by MetadataRepository and DirectoryQuotaRepository.
    /// </summary>
    public class SqliteOptions
    {
        /// <summary>
        /// Gets or sets the SQLite journal mode.
        /// WAL (Write-Ahead Logging) is strongly recommended: supports concurrent reads
        /// during writes, provides crash recovery, and avoids database-level locking.
        /// Options: WAL (default), DELETE, TRUNCATE, PERSIST, MEMORY, OFF
        /// Default: "WAL"
        /// </summary>
        public string JournalMode { get; set; } = "WAL";

        /// <summary>
        /// Gets or sets the SQLite synchronous mode.
        /// NORMAL: WAL checkpoint may be deferred one OS buffer flush cycle. Safe against
        ///   process crashes; a power failure could lose up to one second of commits.
        /// FULL:   Every write is fsynced immediately. Safest, but ~30% slower.
        /// Recommended: NORMAL for most deployments; FULL for maximum durability.
        /// Default: "NORMAL"
        /// </summary>
        public string SynchronousMode { get; set; } = "NORMAL";

        /// <summary>
        /// Gets or sets the page cache size.
        /// Negative values = kilobytes; positive values = number of pages (4 KB each).
        /// Default: -4000 (4 MB cache per connection).
        /// </summary>
        public int CacheSizeKb { get; set; } = -4000;

        /// <summary>
        /// Gets or sets the busy timeout in milliseconds.
        /// How long to wait when the database is locked by another writer before throwing.
        /// Default: 5000 ms.
        /// </summary>
        public int BusyTimeoutMs { get; set; } = 5000;

        /// <summary>
        /// Gets or sets whether to run a WAL checkpoint after every batch commit.
        /// When true, each write cycle calls PRAGMA wal_checkpoint(PASSIVE) after COMMIT,
        /// which merges committed WAL pages into the main database file. This keeps the WAL
        /// small and reduces the risk of corruption if the process is killed during a later
        /// automatic checkpoint. The slight extra I/O is outweighed by reliability gains for
        /// small metadata files.
        /// Default: true
        /// </summary>
        public bool CheckpointAfterBatch { get; set; } = true;

        /// <summary>
        /// Builds the SQLite connection string for the given database file path.
        /// </summary>
        /// <param name="databasePath">Absolute or relative path to the .db file.</param>
        /// <returns>A connection string suitable for <c>SqliteConnection</c>.</returns>
        public string BuildConnectionString(string databasePath)
        {
            if (string.IsNullOrWhiteSpace(databasePath))
                throw new ArgumentException("Database path cannot be empty", nameof(databasePath));

            // Cache=Shared enables connection-level shared cache within the same process,
            // allowing concurrent reads from different SqliteConnection instances on the same file.
            // Mode=ReadWriteCreate creates the file if it does not yet exist.
            return $"Data Source={databasePath};Cache=Shared;Mode=ReadWriteCreate";
        }

        // Whitelists for PRAGMA values that are user-configurable strings.
        // CacheSizeKb and BusyTimeoutMs are integers so no whitelist is needed there.
        private static readonly HashSet<string> ValidJournalModes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF" };

        private static readonly HashSet<string> ValidSynchronousModes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "OFF", "NORMAL", "FULL", "EXTRA", "0", "1", "2", "3" };

        /// <summary>
        /// Builds the PRAGMA initialization SQL to run immediately after opening a connection.
        /// </summary>
        /// <returns>A SQL string containing all PRAGMA statements separated by semicolons.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when <see cref="JournalMode"/> or <see cref="SynchronousMode"/> contain values
        /// outside the known-safe whitelist, preventing PRAGMA SQL injection.
        /// </exception>
        public string BuildPragmaSql()
        {
            if (!ValidJournalModes.Contains(JournalMode))
                throw new InvalidOperationException(
                    $"Invalid SQLite journal mode '{JournalMode}'. Allowed values: {string.Join(", ", ValidJournalModes)}");

            if (!ValidSynchronousModes.Contains(SynchronousMode))
                throw new InvalidOperationException(
                    $"Invalid SQLite synchronous mode '{SynchronousMode}'. Allowed values: {string.Join(", ", ValidSynchronousModes)}");

            return
                $"PRAGMA journal_mode={JournalMode};" +
                $"PRAGMA synchronous={SynchronousMode};" +
                $"PRAGMA cache_size={CacheSizeKb};" +
                $"PRAGMA busy_timeout={BusyTimeoutMs};" +
                "PRAGMA foreign_keys=OFF;" +
                "PRAGMA temp_store=MEMORY;";
        }
    }
}
