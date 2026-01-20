using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Sample.StressTest;

/// <summary>
/// Multi-threaded stress test application for Locus storage pool.
/// Simulates concurrent read/write operations across multiple tenants and storage volumes.
/// </summary>
class Program
{
    // Configuration constants
    private const int TENANT_COUNT = 2;
    private const int VOLUME_COUNT = 2;
    private const int WRITER_THREAD_COUNT = 10;
    private const int READER_THREAD_COUNT = 10;
    private const int FILES_PER_WRITER_BATCH = 50;
    private const int STATS_REFRESH_INTERVAL_MS = 2000;

    // Simulate failures to test retry mechanism (0-100, 0 = no failures, 10 = 10% failure rate)
    private const int SIMULATED_FAILURE_PERCENTAGE = 0;

    private static readonly string TestDirectory = Path.Combine(Path.GetTempPath(), "locus-stress-test", DateTime.Now.ToString("yyyyMMdd-HH"));

    // Thread-safe statistics
    private static long _totalFilesWritten = 0;
    private static long _totalFilesProcessed = 0;
    private static long _totalFilesFailed = 0;
    private static long _totalBytesWritten = 0;
    private static long _totalBytesRead = 0;
    private static long _totalWriteErrors = 0;
    private static long _totalReadErrors = 0;

    private static readonly ConcurrentDictionary<string, long> _filesPerTenant = new();
    private static readonly ConcurrentDictionary<string, long> _filesPerVolume = new();
    private static readonly ConcurrentBag<Exception> _errors = new();

    private static bool _shouldStop = false;
    private static readonly Stopwatch _totalRuntime = new();

    static async Task Main(string[] args)
    {
        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║         Locus Storage Pool - Stress Test Application          ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"Test Directory: {TestDirectory}");
        Console.WriteLine($"Configuration:");
        Console.WriteLine($"  - Tenants: {TENANT_COUNT}");
        Console.WriteLine($"  - Storage Volumes: {VOLUME_COUNT}");
        Console.WriteLine($"  - Writer Threads: {WRITER_THREAD_COUNT}");
        Console.WriteLine($"  - Reader/Processor Threads: {READER_THREAD_COUNT}");
        Console.WriteLine($"  - Files per Writer Batch: {FILES_PER_WRITER_BATCH}");
        Console.WriteLine($"  - File Watchers: {TENANT_COUNT} (one per tenant)");
        Console.WriteLine();
        Console.WriteLine("File Watcher Directories:");
        for (int i = 1; i <= TENANT_COUNT; i++)
        {
            var watchPath = Path.Combine(TestDirectory, "watch", $"tenant-{i:D3}");
            Console.WriteLine($"  - tenant-{i:D3}: {watchPath}");
        }
        Console.WriteLine();
        Console.WriteLine("TIP: Drop files into the watch directories above to test automatic import!");
        Console.WriteLine("Press Ctrl+C to stop the test gracefully...");
        Console.WriteLine();

        // Setup graceful shutdown
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            Console.WriteLine("\n\n[SHUTDOWN] Stopping stress test gracefully...");
            _shouldStop = true;
        };

        // Create test directory
        Directory.CreateDirectory(TestDirectory);

        try
        {
            await RunStressTestAsync();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"\n[FATAL ERROR] {ex.Message}");
            Console.WriteLine(ex.StackTrace);
            Console.ResetColor();
        }
        finally
        {
            PrintFinalReport();
        }

        Console.WriteLine("\nPress any key to exit...");
        Console.ReadKey();
    }

    static async Task RunStressTestAsync()
    {
        // Build service provider
        var services = new ServiceCollection();

        // Configure logging
        services.AddLogging(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Error); // Only show errors, hide retry warnings
        });

        // Configure Locus storage pool
        services.AddLocus(builder =>
        {
            // Add multiple storage volumes
            for (int i = 1; i <= VOLUME_COUNT; i++)
            {
                var volumePath = Path.Combine(TestDirectory, $"volume{i:D2}");
                builder.AddLocalVolume($"vol-{i:D3}", volumePath);
                Console.WriteLine($"[SETUP] Added volume: vol-{i:D3} at {volumePath}");
            }

            builder
                .WithMetadataDirectory(Path.Combine(TestDirectory, "metadata"))
                .WithQuotaDirectory(Path.Combine(TestDirectory, "quota"))
                .WithRetryPolicy(policy =>
                {
                    policy.MaxRetryCount = 3;
                    policy.InitialRetryDelay = TimeSpan.FromMilliseconds(200);
                    policy.UseExponentialBackoff = true;
                    policy.MaxRetryDelay = TimeSpan.FromSeconds(10);
                })
                .WithCleanupOptions(options =>
                {
                    options.CleanupInterval = TimeSpan.FromMinutes(5);
                    options.ProcessingTimeout = TimeSpan.FromMinutes(30);
                    options.FailedFileRetentionPeriod = TimeSpan.FromDays(7);
                })
                .EnableDatabaseHealthCheck() 
                .DisableBackgroundCleanup() // We'll manage cleanup manually for this test
                .AddFileWatcher(watcher =>
                {
                    // Create watch root directory
                    var watchRoot = Path.Combine(TestDirectory, "watch");
                    Directory.CreateDirectory(watchRoot);

                    watcher.WatcherId = "multi-tenant-watcher";
                    watcher.WatchPath = watchRoot;
                    watcher.MultiTenantMode = true; // Enable multi-tenant mode
                    watcher.AutoCreateTenantDirectories = true; // Auto-create tenant subdirectories
                    watcher.TenantId = string.Empty; // Must be empty in multi-tenant mode
                    watcher.Enabled = true;
                    watcher.IncludeSubdirectories = true;
                    watcher.FilePatterns = new List<string> { "*.*" };
                    watcher.PostImportAction = Core.Models.PostImportAction.Delete;
                    watcher.PollingInterval = TimeSpan.FromSeconds(5);
                    watcher.MinFileAge = TimeSpan.FromSeconds(2);
                    watcher.MaxConcurrentImports = 8; // Increased for multi-tenant

                    Console.WriteLine($"[SETUP] Added multi-tenant file watcher monitoring: {watchRoot}");
                    Console.WriteLine($"[SETUP] Auto-create tenant directories: Enabled");
                    Console.WriteLine($"[SETUP] Tenant subdirectories will be created automatically on first scan");
                });
        });

        var serviceProvider = services.BuildServiceProvider();

        // Initialize tenants
        var tenantManager = serviceProvider.GetRequiredService<ITenantManager>();
        var tenants = new List<ITenantContext>();

        Console.WriteLine();
        for (int i = 1; i <= TENANT_COUNT; i++)
        {
            var tenantId = $"tenant-{i:D3}";

            // Try to create tenant, ignore if already exists
            try
            {
                await tenantManager.CreateTenantAsync(tenantId, default);
                Console.WriteLine($"[SETUP] Created tenant: {tenantId}");
            }
            catch (Exception ex) when (ex.Message.Contains("already exists"))
            {
                Console.WriteLine($"[SETUP] Tenant already exists, using existing: {tenantId}");
            }

            var tenant = await tenantManager.GetTenantAsync(tenantId, default);
            tenants.Add(tenant!);
            _filesPerTenant[tenantId] = 0;
        }

        // Manually trigger FileWatcher registration and initial scan
        // (Since we're not using IHost, FileWatcherInitializationService won't run automatically)
        var fileWatcher = serviceProvider.GetRequiredService<IFileWatcher>();
        var locusOptions = serviceProvider.GetRequiredService<LocusOptions>();

        Console.WriteLine();
        Console.WriteLine("[SETUP] Registering FileWatcher configurations...");
        foreach (var watcherConfig in locusOptions.FileWatchers)
        {
            await fileWatcher.RegisterWatcherAsync(watcherConfig, default);
            Console.WriteLine($"[SETUP] Registered FileWatcher: {watcherConfig.WatcherId}");
        }

        Console.WriteLine("[SETUP] Triggering FileWatcher initial scan to create tenant directories...");
        var importedFiles = await fileWatcher.ScanNowAsync("multi-tenant-watcher", default);
        Console.WriteLine($"[SETUP] FileWatcher scan completed. Imported {importedFiles} files. Tenant directories created.");

        Console.WriteLine();
        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                    Starting Stress Test                        ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        _totalRuntime.Start();

        var storagePool = serviceProvider.GetRequiredService<IStoragePool>();

        // Start all worker threads
        var tasks = new List<Task>();

        // Start writer threads
        for (int i = 0; i < WRITER_THREAD_COUNT; i++)
        {
            int writerId = i;
            tasks.Add(Task.Run(() => WriterThreadAsync(writerId, tenants, storagePool)));
        }

        // Start reader/processor threads
        for (int i = 0; i < READER_THREAD_COUNT; i++)
        {
            int readerId = i;
            tasks.Add(Task.Run(() => ReaderThreadAsync(readerId, tenants, storagePool)));
        }

        // Start statistics monitor thread
        tasks.Add(Task.Run(StatisticsMonitorAsync));

        // Wait for all threads to complete (or user cancellation)
        await Task.WhenAll(tasks);

        _totalRuntime.Stop();
    }

    static async Task WriterThreadAsync(int writerId, List<ITenantContext> tenants, IStoragePool storagePool)
    {
        var random = new Random(writerId);
        var batchNumber = 0;

        Console.WriteLine($"[WRITER-{writerId:D2}] Started");

        try
        {
            while (!_shouldStop)
            {
                batchNumber++;

                for (int fileId = 0; fileId < FILES_PER_WRITER_BATCH; fileId++)
                {
                    if (_shouldStop) break;

                    try
                    {
                        // Select tenant in round-robin fashion
                        var tenant = tenants[fileId % TENANT_COUNT];

                        // Generate file content
                        var content = $"Writer-{writerId:D2}_Batch-{batchNumber:D4}_File-{fileId:D4}_Tenant-{tenant.TenantId}_Time-{DateTime.UtcNow:O}";
                        var contentBytes = Encoding.UTF8.GetBytes(content);

                        // Write file
                        using var stream = new MemoryStream(contentBytes);
                        var fileKey = await storagePool.WriteFileAsync(tenant, stream, null, CancellationToken.None);

                        // Update statistics
                        Interlocked.Increment(ref _totalFilesWritten);
                        Interlocked.Add(ref _totalBytesWritten, contentBytes.Length);
                        _filesPerTenant.AddOrUpdate(tenant.TenantId, 1, (_, count) => count + 1);

                        // Get volume distribution
                        var location = await storagePool.GetFileLocationAsync(tenant, fileKey, CancellationToken.None);
                        if (location != null)
                        {
                            _filesPerVolume.AddOrUpdate(location.VolumeId, 1, (_, count) => count + 1);
                        }

                        // Simulate variable write speed
                        await Task.Delay(random.Next(10, 50));
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref _totalWriteErrors);
                        _errors.Add(ex);
                    }
                }

                // Small delay between batches
                await Task.Delay(random.Next(100, 500));
            }
        }
        finally
        {
            Console.WriteLine($"[WRITER-{writerId:D2}] Stopped (Batches: {batchNumber})");
        }
    }

    static async Task ReaderThreadAsync(int readerId, List<ITenantContext> tenants, IStoragePool storagePool)
    {
        var random = new Random(readerId + 1000);
        var processedCount = 0;

        Console.WriteLine($"[READER-{readerId:D2}] Started");

        try
        {
            // Give writers a head start
            await Task.Delay(2000);

            while (!_shouldStop)
            {
                try
                {
                    // Try to get a file from each tenant in round-robin
                    var tenant = tenants[processedCount % TENANT_COUNT];

                    var fileLocation = await storagePool.GetNextFileForProcessingAsync(tenant, CancellationToken.None);

                    if (fileLocation == null)
                    {
                        // No files available for this tenant, wait a bit and try again
                        await Task.Delay(100);
                        continue;
                    }

                    // Simulate processing
                    string content;
                    // CRITICAL: Use explicit scope to ensure stream is disposed before deletion
                    {
                        using var stream = await storagePool.ReadFileAsync(tenant, fileLocation.FileKey, CancellationToken.None);
                        using var reader = new StreamReader(stream);
                        content = await reader.ReadToEndAsync();
                        // Stream will be disposed here when exiting this block
                    }

                    Interlocked.Add(ref _totalBytesRead, content.Length);

                    // Simulate variable processing time (faster processing)
                    await Task.Delay(random.Next(5, 20));

                    // Randomly simulate failures based on configured percentage
                    if (SIMULATED_FAILURE_PERCENTAGE > 0 && random.Next(0, 100) < SIMULATED_FAILURE_PERCENTAGE)
                    {
                        // Mark as failed (will retry automatically)
                        await storagePool.MarkAsFailedAsync(
                            fileLocation.FileKey,
                            $"Simulated failure by reader-{readerId}",
                            CancellationToken.None);

                        Interlocked.Increment(ref _totalFilesFailed);
                    }
                    else
                    {
                        // Mark as completed (deletes the file)
                        // Stream is now safely closed, file can be deleted
                        await storagePool.MarkAsCompletedAsync(fileLocation.FileKey, CancellationToken.None);

                        Interlocked.Increment(ref _totalFilesProcessed);
                        processedCount++;
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref _totalReadErrors);
                    _errors.Add(ex);

                    // Log first few errors to understand what's happening
                    if (_totalReadErrors <= 10)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"[READER-{readerId:D2}] ERROR: {ex.GetType().Name}: {ex.Message}");
                        Console.ResetColor();
                    }

                    await Task.Delay(100); // Back off on error
                }
            }
        }
        finally
        {
            Console.WriteLine($"[READER-{readerId:D2}] Stopped (Processed: {processedCount})");
        }
    }

    static async Task StatisticsMonitorAsync()
    {
        var lastWritten = 0L;
        var lastProcessed = 0L;
        var lastBytesWritten = 0L;
        var lastBytesRead = 0L;

        while (!_shouldStop)
        {
            await Task.Delay(STATS_REFRESH_INTERVAL_MS);

            var currentWritten = Interlocked.Read(ref _totalFilesWritten);
            var currentProcessed = Interlocked.Read(ref _totalFilesProcessed);
            var currentBytesWritten = Interlocked.Read(ref _totalBytesWritten);
            var currentBytesRead = Interlocked.Read(ref _totalBytesRead);
            var currentFailed = Interlocked.Read(ref _totalFilesFailed);
            var writeErrors = Interlocked.Read(ref _totalWriteErrors);
            var readErrors = Interlocked.Read(ref _totalReadErrors);

            // Calculate rates
            var writeRate = (currentWritten - lastWritten) / (STATS_REFRESH_INTERVAL_MS / 1000.0);
            var processRate = (currentProcessed - lastProcessed) / (STATS_REFRESH_INTERVAL_MS / 1000.0);
            var writeThroughput = (currentBytesWritten - lastBytesWritten) / (STATS_REFRESH_INTERVAL_MS / 1000.0);
            var readThroughput = (currentBytesRead - lastBytesRead) / (STATS_REFRESH_INTERVAL_MS / 1000.0);

            lastWritten = currentWritten;
            lastProcessed = currentProcessed;
            lastBytesWritten = currentBytesWritten;
            lastBytesRead = currentBytesRead;

            // Clear previous stats (move cursor up)
            Console.SetCursorPosition(0, Console.CursorTop);

            // Print current statistics
            Console.WriteLine($"═══════════════════════════════════════════════════════════════════════════");
            Console.WriteLine($"Runtime: {_totalRuntime.Elapsed:hh\\:mm\\:ss}");
            Console.WriteLine($"─────────────────────────────────────────────────────────────────────────── ");
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"  Files Written:    {currentWritten,10:N0}  ({writeRate,8:F2} files/sec, {FormatBytes((long)writeThroughput)}/sec)");
            Console.ResetColor();
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"  Files Processed:  {currentProcessed,10:N0}  ({processRate,8:F2} files/sec, {FormatBytes((long)readThroughput)}/sec)");
            Console.ResetColor();
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"  Files Failed:     {currentFailed,10:N0}");
            Console.ResetColor();
            Console.WriteLine($"  Pending:          {currentWritten - currentProcessed - currentFailed,10:N0}");
            Console.WriteLine($"─────────────────────────────────────────────────────────────────────────── ");

            if (writeErrors > 0 || readErrors > 0)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"  Write Errors:     {writeErrors,10:N0}");
                Console.WriteLine($"  Read Errors:      {readErrors,10:N0}");
                Console.ResetColor();
                Console.WriteLine($"─────────────────────────────────────────────────────────────────────────── ");
            }

            // Per-tenant distribution
            Console.WriteLine("  Per-Tenant Distribution:");
            foreach (var (tenantId, count) in _filesPerTenant.OrderBy(x => x.Key))
            {
                var percentage = currentWritten > 0 ? (count * 100.0 / currentWritten) : 0;
                Console.WriteLine($"    {tenantId}: {count,8:N0} ({percentage,5:F1}%)");
            }

            // Per-volume distribution
            Console.WriteLine($"─────────────────────────────────────────────────────────────────────────── ");
            Console.WriteLine("  Per-Volume Distribution:");
            foreach (var (volumeId, count) in _filesPerVolume.OrderBy(x => x.Key))
            {
                var percentage = currentWritten > 0 ? (count * 100.0 / currentWritten) : 0;
                Console.WriteLine($"    {volumeId}: {count,8:N0} ({percentage,5:F1}%)");
            }

            Console.WriteLine($"═══════════════════════════════════════════════════════════════════════════");
            Console.WriteLine();
        }
    }

    static void PrintFinalReport()
    {
        Console.WriteLine();
        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                        Final Report                            ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"Total Runtime:        {_totalRuntime.Elapsed:hh\\:mm\\:ss}");
        Console.WriteLine($"Files Written:        {_totalFilesWritten:N0}");
        Console.WriteLine($"Files Processed:      {_totalFilesProcessed:N0}");
        Console.WriteLine($"Files Failed:         {_totalFilesFailed:N0}");
        Console.WriteLine($"Files Pending:        {_totalFilesWritten - _totalFilesProcessed - _totalFilesFailed:N0}");
        Console.WriteLine($"Total Bytes Written:  {FormatBytes(_totalBytesWritten)}");
        Console.WriteLine($"Total Bytes Read:     {FormatBytes(_totalBytesRead)}");
        Console.WriteLine($"Write Errors:         {_totalWriteErrors:N0}");
        Console.WriteLine($"Read Errors:          {_totalReadErrors:N0}");
        Console.WriteLine();

        if (_totalRuntime.Elapsed.TotalSeconds > 0)
        {
            var avgWriteRate = _totalFilesWritten / _totalRuntime.Elapsed.TotalSeconds;
            var avgProcessRate = _totalFilesProcessed / _totalRuntime.Elapsed.TotalSeconds;
            Console.WriteLine($"Average Write Rate:   {avgWriteRate:F2} files/sec");
            Console.WriteLine($"Average Process Rate: {avgProcessRate:F2} files/sec");
            Console.WriteLine();
        }

        Console.WriteLine("Per-Tenant Summary:");
        foreach (var (tenantId, count) in _filesPerTenant.OrderBy(x => x.Key))
        {
            var percentage = _totalFilesWritten > 0 ? (count * 100.0 / _totalFilesWritten) : 0;
            Console.WriteLine($"  {tenantId}: {count,10:N0} files ({percentage,5:F1}%)");
        }
        Console.WriteLine();

        Console.WriteLine("Per-Volume Summary:");
        foreach (var (volumeId, count) in _filesPerVolume.OrderBy(x => x.Key))
        {
            var percentage = _totalFilesWritten > 0 ? (count * 100.0 / _totalFilesWritten) : 0;
            Console.WriteLine($"  {volumeId}: {count,10:N0} files ({percentage,5:F1}%)");
        }
        Console.WriteLine();

        if (_errors.Count > 0)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Errors encountered: {_errors.Count}");
            Console.WriteLine("Sample errors:");
            foreach (var error in _errors.Take(10))
            {
                Console.WriteLine($"  - {error.GetType().Name}: {error.Message}");
            }
            Console.ResetColor();
            Console.WriteLine();
        }

        Console.WriteLine($"Test Directory: {TestDirectory}");
        Console.WriteLine("(You can inspect the physical files and metadata)");
    }

    static string FormatBytes(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1)
        {
            order++;
            len = len / 1024;
        }
        return $"{len:0.##} {sizes[order]}";
    }
}
