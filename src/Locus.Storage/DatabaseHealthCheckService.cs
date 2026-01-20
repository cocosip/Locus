using System;
using System.Collections.Generic;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Background service that checks database health on startup.
    /// </summary>
    public class DatabaseHealthCheckService : IHostedService
    {
        private readonly IDatabaseRecoveryService _recoveryService;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<DatabaseHealthCheckService> _logger;
        private readonly string _metadataDirectory;
        private readonly IEnumerable<string> _volumePaths;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseHealthCheckService"/> class.
        /// </summary>
        public DatabaseHealthCheckService(
            IDatabaseRecoveryService recoveryService,
            IFileSystem fileSystem,
            ILogger<DatabaseHealthCheckService> logger,
            string metadataDirectory,
            IEnumerable<string> volumePaths)
        {
            _recoveryService = recoveryService ?? throw new ArgumentNullException(nameof(recoveryService));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _metadataDirectory = metadataDirectory ?? throw new ArgumentNullException(nameof(metadataDirectory));
            _volumePaths = volumePaths ?? throw new ArgumentNullException(nameof(volumePaths));
        }

        /// <inheritdoc/>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Starting database health check...");

            // Add delay to avoid startup timing conflicts
            // Other services (MetadataRepository, etc.) need time to initialize
            _logger.LogDebug("Waiting 2 seconds for other services to initialize...");
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

            try
            {
                var report = await CheckWithRetryAsync(cancellationToken);

                // Check for orphaned files if no databases exist
                if (report.HealthyDatabases == 0 && report.CorruptedDatabases.Count == 0)
                {
                    await CheckForOrphanedFilesAsync(cancellationToken);
                }
                else if (report.AllHealthy)
                {
                    _logger.LogInformation("Database health check completed. All {Count} databases are healthy.",
                        report.HealthyDatabases);
                }
                else
                {
                    _logger.LogWarning(
                        "Database health check completed. Healthy: {Healthy}, Corrupted: {Corrupted}",
                        report.HealthyDatabases,
                        report.CorruptedDatabases.Count);

                    foreach (var corruptedDb in report.CorruptedDatabases)
                    {
                        _logger.LogError(
                            "CORRUPTED DATABASE DETECTED: Type={Type}, Tenant={TenantId}, Path={Path}. " +
                            "Use DatabaseRecoveryService.Rebuild{Type}DatabaseAsync() to recover.",
                            corruptedDb.DatabaseType,
                            corruptedDb.TenantId,
                            corruptedDb.DatabasePath,
                            corruptedDb.DatabaseType);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during database health check");
            }
        }

        /// <summary>
        /// Checks all databases with retry mechanism to handle startup timing conflicts.
        /// </summary>
        private async Task<DatabaseHealthReport> CheckWithRetryAsync(CancellationToken cancellationToken)
        {
            const int maxRetries = 3;
            const int retryDelayMs = 1000;

            DatabaseHealthReport? lastReport = null;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                lastReport = await _recoveryService.CheckAllDatabasesAsync(cancellationToken);

                // If all healthy, no need to retry
                if (lastReport.AllHealthy)
                {
                    if (attempt > 1)
                    {
                        _logger.LogInformation(
                            "Database health check succeeded on attempt {Attempt}/{MaxRetries}",
                            attempt, maxRetries);
                    }
                    return lastReport;
                }

                // If this is not the last attempt, retry
                if (attempt < maxRetries)
                {
                    _logger.LogDebug(
                        "Database health check found {CorruptedCount} potentially locked databases on attempt {Attempt}/{MaxRetries}. " +
                        "Retrying in {DelayMs}ms to rule out startup timing conflicts...",
                        lastReport.CorruptedDatabases.Count, attempt, maxRetries, retryDelayMs);

                    await Task.Delay(retryDelayMs, cancellationToken);
                }
            }

            // After all retries, return the last report (guaranteed to be non-null after loop)
            _logger.LogWarning(
                "Database health check completed after {MaxRetries} attempts. " +
                "Still found {CorruptedCount} corrupted database(s).",
                maxRetries, lastReport!.CorruptedDatabases.Count);

            return lastReport;
        }

        /// <summary>
        /// Checks for orphaned files (physical files without metadata) in storage volumes.
        /// Uses fast detection: only checks if files exist, does not count all files.
        /// </summary>
        private Task CheckForOrphanedFilesAsync(CancellationToken ct)
        {
            if (!_volumePaths.Any())
            {
                _logger.LogInformation("No database files found. No storage volumes configured yet.");
                return Task.CompletedTask;
            }

            // Get existing database tenant IDs
            var existingTenantIds = new HashSet<string>();
            if (_fileSystem.Directory.Exists(_metadataDirectory))
            {
                var dbFiles = _fileSystem.Directory.GetFiles(_metadataDirectory, "*.db");
                foreach (var dbFile in dbFiles)
                {
                    var tenantId = _fileSystem.Path.GetFileNameWithoutExtension(dbFile);
                    existingTenantIds.Add(tenantId);
                }
            }

            // Scan storage volumes for tenant directories with files
            var orphanedTenants = new List<string>();
            var totalTenantsWithFiles = 0;

            foreach (var volumePath in _volumePaths)
            {
                if (!_fileSystem.Directory.Exists(volumePath))
                    continue;

                // Get all tenant directories (subdirectories in volume)
                var tenantDirs = _fileSystem.Directory.GetDirectories(volumePath);

                foreach (var tenantDir in tenantDirs)
                {
                    ct.ThrowIfCancellationRequested();

                    var tenantId = _fileSystem.Path.GetFileName(tenantDir);

                    // Fast check: does this tenant directory have any files?
                    // Use EnumerateFiles with FirstOrDefault for best performance
                    if (HasAnyFiles(tenantDir))
                    {
                        totalTenantsWithFiles++;

                        // Check if metadata database exists for this tenant
                        if (!existingTenantIds.Contains(tenantId))
                        {
                            orphanedTenants.Add(tenantId);
                        }
                    }
                }
            }

            // Report results
            if (totalTenantsWithFiles == 0)
            {
                _logger.LogInformation(
                    "No database files found. No physical files in storage volumes. This is normal for first startup.");
            }
            else if (orphanedTenants.Count > 0)
            {
                _logger.LogWarning(
                    "METADATA LOSS DETECTED: Found {OrphanedCount} tenant(s) with physical files but no metadata database. " +
                    "Total tenants with files: {TotalTenants}. " +
                    "Orphaned tenants: {TenantList}. " +
                    "Use IDatabaseRecoveryService.RebuildMetadataDatabaseAsync() to recover metadata.",
                    orphanedTenants.Count,
                    totalTenantsWithFiles,
                    string.Join(", ", orphanedTenants));
            }
            else
            {
                _logger.LogInformation(
                    "No database files found, but all {TotalTenants} tenant(s) with physical files have valid metadata databases.",
                    totalTenantsWithFiles);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Fast check if a directory has any files (recursive).
        /// Stops at first file found for maximum performance.
        /// </summary>
        private bool HasAnyFiles(string directoryPath)
        {
            try
            {
                // Use EnumerateFiles with FirstOrDefault for lazy evaluation
                // This stops as soon as one file is found
                return _fileSystem.Directory
                    .EnumerateFiles(directoryPath, "*", System.IO.SearchOption.AllDirectories)
                    .Any();
            }
            catch
            {
                // Ignore access denied or other errors
                return false;
            }
        }

        /// <inheritdoc/>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
