using System;
using System.Collections.Generic;

namespace Locus.Core.Models
{
    /// <summary>
    /// Configuration options for LiteDB databases.
    /// Controls Write-Ahead Logging (WAL), checkpoints, and timeouts for optimal performance
    /// in network storage and high-concurrency scenarios.
    /// </summary>
    public class LiteDBOptions
    {
        /// <summary>
        /// Gets or sets whether to enable Write-Ahead Logging (WAL) journal mode.
        /// WAL mode significantly improves reliability in network storage (NFS/Ceph/cloud PVC)
        /// and prevents database corruption from concurrent access or Pod crashes.
        /// STRONGLY RECOMMENDED for Kubernetes/Docker environments.
        /// Default: true
        /// </summary>
        public bool EnableJournal { get; set; } = true;

        /// <summary>
        /// Gets or sets the number of transactions before automatic WAL checkpoint.
        /// A checkpoint merges the WAL journal into the main database file and clears the journal.
        /// Lower values = smaller journal files, more frequent checkpoints (may impact performance).
        /// Higher values = larger journal files, fewer checkpoints (better performance).
        /// Recommended values:
        /// - Low concurrency (&lt; 100 writes/sec): 500
        /// - Medium concurrency (100-500 writes/sec): 1000 (default)
        /// - High concurrency (&gt; 500 writes/sec): 2000
        /// Default: 1000
        /// </summary>
        public int CheckpointInterval { get; set; } = 1000;

        /// <summary>
        /// Gets or sets the database lock timeout in seconds.
        /// How long to wait for a database lock before throwing a timeout exception.
        /// Network storage environments (NFS, cloud storage) may need higher timeouts
        /// to account for network latency and I/O delays.
        /// Recommended values:
        /// - Local storage: 30 seconds
        /// - Network storage: 60 seconds (default)
        /// - Unstable network: 120 seconds
        /// Default: 60
        /// </summary>
        public int TimeoutSeconds { get; set; } = 60;

        /// <summary>
        /// Gets or sets the connection mode.
        /// "shared" = Multiple processes can access the database (recommended for Kubernetes).
        /// "direct" = Single process only, better performance but no multi-process support.
        /// Default: "shared"
        /// </summary>
        public string ConnectionMode { get; set; } = "shared";

        /// <summary>
        /// Builds the LiteDB connection string from the configured options.
        /// </summary>
        /// <param name="databasePath">The path to the database file.</param>
        /// <returns>A LiteDB connection string with all configured options.</returns>
        public string BuildConnectionString(string databasePath)
        {
            if (string.IsNullOrWhiteSpace(databasePath))
                throw new ArgumentException("Database path cannot be empty", nameof(databasePath));

            var parts = new List<string>
            {
                $"Filename={databasePath}",
                $"Connection={ConnectionMode}"
            };

            if (EnableJournal)
            {
                parts.Add("Journal=true");
                parts.Add($"Checkpoint={CheckpointInterval}");
            }

            parts.Add($"Timeout={TimeoutSeconds}");

            return string.Join(";", parts);
        }
    }
}
