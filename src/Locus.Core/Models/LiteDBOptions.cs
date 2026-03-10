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
        /// Gets or sets the number of WAL pages before automatic LiteDB checkpoint.
        /// A checkpoint merges the WAL journal into the main database file and clears the journal.
        /// Each WAL page is 4 KB, so the default of 200 caps the journal at ~800 KB before
        /// LiteDB automatically checkpoints. This limits the amount of unflushed data that can
        /// be lost if the process is killed during a checkpoint operation.
        /// Lower values = smaller journal files at risk, more frequent checkpoints (slightly more I/O).
        /// Higher values = larger journal files, fewer checkpoints (higher corruption risk on crash).
        /// Recommended values:
        /// - Stable environment (local SSD): 500
        /// - Default (balanced stability / performance): 200
        /// - High-reliability (network storage, frequent crashes): 50
        /// Default: 200
        /// </summary>
        public int CheckpointInterval { get; set; } = 200;

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
