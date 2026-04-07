using System.Text;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus.Sample.Console;

/// <summary>
/// Interactive local test console for Locus storage pool.
/// Reads configuration from appsettings.json and provides a menu for testing all major features.
/// </summary>
class Program
{
    static async Task<int> Main(string[] args)
    {
        System.Console.OutputEncoding = Encoding.UTF8;
        System.Console.Title = "Locus Sample Console";

        PrintBanner();

        var host = CreateHost(args);

        System.Console.WriteLine("[HOST] Starting Locus services...");
        await host.StartAsync();
        System.Console.WriteLine("[HOST] All services started.\n");

        try
        {
            await RunMenuAsync(host.Services);
        }
        finally
        {
            System.Console.WriteLine("\n[HOST] Stopping Locus services...");
            await host.StopAsync();
            System.Console.WriteLine("[HOST] Stopped. Goodbye.");
        }

        return 0;
    }

    // ────────────────────────────────────────────────────────────────────────
    // Host setup
    // ────────────────────────────────────────────────────────────────────────

    static IHost CreateHost(string[] args)
    {
        return Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((ctx, cfg) =>
            {
                cfg.SetBasePath(AppContext.BaseDirectory);
                cfg.AddJsonFile("appsettings.json", optional: false, reloadOnChange: false);
                cfg.AddCommandLine(args);
            })
            .ConfigureLogging((ctx, logging) =>
            {
                logging.ClearProviders();
                logging.AddSimpleConsole(o =>
                {
                    o.SingleLine = true;
                    o.TimestampFormat = "HH:mm:ss ";
                });
                logging.SetMinimumLevel(LogLevel.Warning);
                logging.AddFilter("Locus", LogLevel.Information);
            })
            .ConfigureServices((ctx, services) =>
            {
                services.AddLocus(ctx.Configuration);
            })
            .UseConsoleLifetime(o => o.SuppressStatusMessages = true)
            .Build();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Main menu
    // ────────────────────────────────────────────────────────────────────────

    static async Task RunMenuAsync(IServiceProvider services)
    {
        while (true)
        {
            PrintMenu();
            var key = System.Console.ReadKey(intercept: true).Key;
            System.Console.WriteLine();

            switch (key)
            {
                case ConsoleKey.D1: await TestBasicWriteReadAsync(services); break;
                case ConsoleKey.D2: await TestMultiTenantIsolationAsync(services); break;
                case ConsoleKey.D3: await TestQuotaLimitAsync(services); break;
                case ConsoleKey.D4: await TestDisabledTenantAsync(services); break;
                case ConsoleKey.D5: await TestFileSchedulerAsync(services); break;
                case ConsoleKey.D6: await TestRetryMechanismAsync(services); break;
                case ConsoleKey.D7: await TestFileWatcherAsync(services); break;
                case ConsoleKey.D8: await ShowStorageStatsAsync(services); break;
                case ConsoleKey.D9: await ManageTenantsAsync(services); break;
                case ConsoleKey.Q:
                case ConsoleKey.Escape:
                    return;
                default:
                    WriteWarning("Unknown option. Press Q to quit.");
                    break;
            }
        }
    }

    // ────────────────────────────────────────────────────────────────────────
    // Test 1: Basic write / read / delete
    // ────────────────────────────────────────────────────────────────────────

    static async Task TestBasicWriteReadAsync(IServiceProvider services)
    {
        PrintSection("Test 1 — Basic Write / Read / Delete");

        var pool = services.GetRequiredService<IStoragePool>();
        var tenantManager = services.GetRequiredService<ITenantManager>();

        var tenant = await tenantManager.GetTenantAsync("tenant-001", default);
        if (tenant == null) { WriteError("tenant-001 not found. Ensure AutoCreateTenants = true."); Pause(); return; }

        // Write
        var content = $"Hello from Locus! Time={DateTime.UtcNow:O}";
        var bytes = Encoding.UTF8.GetBytes(content);

        WriteInfo($"Writing {bytes.Length} bytes as 'hello.txt' ...");
        string fileKey;
        using (var ms = new MemoryStream(bytes))
        {
            fileKey = await pool.WriteFileAsync(tenant, ms, "hello.txt", default);
        }
        WriteOk($"Written. FileKey = {fileKey}");

        // Read back
        WriteInfo("Reading file back ...");
        using (var readStream = await pool.ReadFileAsync(tenant, fileKey, default))
        using (var sr = new StreamReader(readStream))
        {
            var readBack = await sr.ReadToEndAsync();
            WriteOk($"Read back: \"{readBack}\"");
            System.Console.WriteLine($"  Match: {(readBack == content ? "YES ✓" : "NO ✗")}");
        }

        // File info
        var info = await pool.GetFileInfoAsync(tenant, fileKey, default);
        if (info != null)
            WriteInfo($"FileInfo: Size={FormatBytes(info.FileSize)}, Created={info.CreatedAt:HH:mm:ss}");

        // Write a few more files
        WriteInfo("Writing 5 more files ...");
        var keys = new List<string> { fileKey };
        for (int i = 1; i <= 5; i++)
        {
            var data = Encoding.UTF8.GetBytes($"File #{i} — {Guid.NewGuid()}");
            using var ms = new MemoryStream(data);
            var k = await pool.WriteFileAsync(tenant, ms, $"file-{i}.txt", default);
            keys.Add(k);
        }
        WriteOk($"Total files written: {keys.Count}");

        // Clean up via MarkAsCompleted (also deletes physical file + metadata)
        WriteInfo("Deleting all test files via MarkAsCompletedAsync ...");
        int deleted = 0;
        while (true)
        {
            var next = await pool.GetNextFileForProcessingAsync(tenant, default);
            if (next == null)
                break;

            try
            {
                await pool.MarkAsCompletedAsync(GetRequiredLease(next), default);
                deleted++;
            }
            catch (Exception ex)
            {
                WriteWarning($"  Could not delete {next.FileKey[..16]}...: {ex.Message}");
            }
        }
        WriteOk($"Deleted {deleted}/{keys.Count} files.");
        Pause();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Test 2: Multi-tenant isolation
    // ────────────────────────────────────────────────────────────────────────

    static async Task TestMultiTenantIsolationAsync(IServiceProvider services)
    {
        PrintSection("Test 2 — Multi-Tenant Isolation");

        var pool = services.GetRequiredService<IStoragePool>();
        var tenantManager = services.GetRequiredService<ITenantManager>();

        var t1 = await tenantManager.GetTenantAsync("tenant-001", default);
        var t2 = await tenantManager.GetTenantAsync("tenant-002", default);

        if (t1 == null || t2 == null)
        {
            WriteError("tenant-001 or tenant-002 not found.");
            Pause();
            return;
        }

        // Write one file per tenant
        var secret1 = $"Secret of tenant-001: {Guid.NewGuid()}";
        var secret2 = $"Secret of tenant-002: {Guid.NewGuid()}";

        string key1, key2;
        using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(secret1)))
            key1 = await pool.WriteFileAsync(t1, ms, "secret.txt", default);
        using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(secret2)))
            key2 = await pool.WriteFileAsync(t2, ms, "secret.txt", default);

        WriteOk($"tenant-001 file key: {key1}");
        WriteOk($"tenant-002 file key: {key2}");

        // Read back from correct tenant
        WriteInfo("Reading each file from its own tenant ...");
        using (var s = await pool.ReadFileAsync(t1, key1, default))
        using (var sr = new StreamReader(s))
        {
            var val = await sr.ReadToEndAsync();
            WriteOk($"tenant-001 read OK: {val[..Math.Min(50, val.Length)]}...");
        }

        // Cross-tenant read should fail
        WriteInfo("Attempting cross-tenant read (tenant-002 reads tenant-001 file) ...");
        try
        {
            using var s = await pool.ReadFileAsync(t2, key1, default);
            WriteWarning("UNEXPECTED: cross-tenant read succeeded — check isolation logic.");
        }
        catch (Exception ex)
        {
            WriteOk($"Correctly rejected: {ex.GetType().Name} — {ex.Message}");
        }

        // Cleanup
        var item1 = await pool.GetNextFileForProcessingAsync(t1, default);
        if (item1 != null)
            await pool.MarkAsCompletedAsync(GetRequiredLease(item1), default);

        var item2 = await pool.GetNextFileForProcessingAsync(t2, default);
        if (item2 != null)
            await pool.MarkAsCompletedAsync(GetRequiredLease(item2), default);
        WriteOk("Test files cleaned up.");
        Pause();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Test 3: Directory quota limit
    // ────────────────────────────────────────────────────────────────────────

    static async Task TestQuotaLimitAsync(IServiceProvider services)
    {
        PrintSection("Test 3 — Directory Quota");

        var pool = services.GetRequiredService<IStoragePool>();
        var tenantManager = services.GetRequiredService<ITenantManager>();
        var quotaManager = services.GetRequiredService<IDirectoryQuotaManager>();

        var tenant = await tenantManager.GetTenantAsync("tenant-001", default);
        if (tenant == null) { WriteError("tenant-001 not found."); Pause(); return; }

        // Display current global tenant quota (via ITenantQuotaManager)
        var tenantQuotaManager = services.GetRequiredService<ITenantQuotaManager>();
        var globalQuota = await tenantQuotaManager.GetGlobalLimitAsync(default);
        WriteInfo($"Global quota limit (all tenants): {(globalQuota <= 0 ? "unlimited" : globalQuota.ToString())}");

        // Set a custom directory-level quota
        const string tenantId = "tenant-001";
        const string testDirPath = "quota-test-dir";
        const int limit = 5;

        WriteInfo($"Setting directory quota: tenantId={tenantId}, dir='{testDirPath}', limit={limit} ...");
        await quotaManager.SetLimitAsync(tenantId, testDirPath, limit, default);

        var currentLimit = await quotaManager.GetLimitAsync(tenantId, testDirPath, default);
        var currentCount = await quotaManager.GetFileCountAsync(tenantId, testDirPath, default);
        WriteOk($"Quota set. Limit={currentLimit}, CurrentCount={currentCount}");

        // Write test files to the storage pool (quota enforcement on specific dir requires
        // files to be routed there; global pool writes go through tenant quota, not dir quota)
        WriteInfo("Writing 4 files to tenant-001 (within global quota) ...");
        var keys = new List<string>();
        for (int i = 1; i <= 4; i++)
        {
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes($"quota-file-{i}"));
            var k = await pool.WriteFileAsync(tenant, ms, $"qfile-{i}.txt", default);
            keys.Add(k);
            WriteOk($"  [{i}] {k[..16]}...");
        }

        // Demonstrate global tenant quota enforcement by lowering the default quota
        WriteInfo("Tip: To test quota exceeded, set DefaultTenantQuota=3 in appsettings.json and restart.");

        // Cleanup
        while (true)
        {
            var next = await pool.GetNextFileForProcessingAsync(tenant, default);
            if (next == null)
                break;

            await pool.MarkAsCompletedAsync(GetRequiredLease(next), default);
        }
        WriteOk($"Cleaned up {keys.Count} files.");
        Pause();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Test 4: Disabled tenant
    // ────────────────────────────────────────────────────────────────────────

    static async Task TestDisabledTenantAsync(IServiceProvider services)
    {
        PrintSection("Test 4 — Disabled Tenant Rejection");

        var pool = services.GetRequiredService<IStoragePool>();
        var tenantManager = services.GetRequiredService<ITenantManager>();

        // Ensure the disabled tenant exists
        var tenant = await tenantManager.GetTenantAsync("tenant-disabled", default);
        if (tenant == null)
        {
            WriteWarning("tenant-disabled not found, creating it ...");
            await tenantManager.CreateTenantAsync("tenant-disabled", default);
            await tenantManager.DisableTenantAsync("tenant-disabled", default);
            tenant = await tenantManager.GetTenantAsync("tenant-disabled", default);
        }

        WriteInfo($"Tenant status: {tenant!.Status}");
        WriteInfo("Attempting to write to the disabled tenant ...");

        try
        {
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes("should be rejected"));
            var k = await pool.WriteFileAsync(tenant, ms, "test.txt", default);
            WriteWarning($"UNEXPECTED: write succeeded with key {k}");
        }
        catch (Exception ex)
        {
            WriteOk($"Correctly rejected: {ex.GetType().Name}");
            WriteInfo($"  Message: {ex.Message}");
        }

        // Re-enable and retry
        WriteInfo("Re-enabling tenant ...");
        await tenantManager.EnableTenantAsync("tenant-disabled", default);
        tenant = await tenantManager.GetTenantAsync("tenant-disabled", default);
        WriteInfo($"Tenant status after enable: {tenant!.Status}");

        try
        {
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes("now enabled"));
            var k = await pool.WriteFileAsync(tenant, ms, "test.txt", default);
            WriteOk($"Write succeeded after re-enable. Key = {k[..16]}...");
            var allocated = await pool.GetNextFileForProcessingAsync(tenant, default);
            if (allocated != null)
                await pool.MarkAsCompletedAsync(GetRequiredLease(allocated), default);
        }
        catch (Exception ex)
        {
            WriteError($"Write failed even after re-enable: {ex.Message}");
        }

        // Disable again to match config
        await tenantManager.DisableTenantAsync("tenant-disabled", default);
        WriteInfo("Tenant disabled again (restored to original state).");
        Pause();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Test 5: File scheduler — concurrent processing simulation
    // ────────────────────────────────────────────────────────────────────────

    static async Task TestFileSchedulerAsync(IServiceProvider services)
    {
        PrintSection("Test 5 — File Scheduler (Concurrent Processing)");

        var pool = services.GetRequiredService<IStoragePool>();
        var tenantManager = services.GetRequiredService<ITenantManager>();

        var tenant = await tenantManager.GetTenantAsync("tenant-001", default);
        if (tenant == null) { WriteError("tenant-001 not found."); Pause(); return; }

        const int fileCount = 12;
        const int workerCount = 4;

        // Write files
        WriteInfo($"Writing {fileCount} files to tenant-001 (Pending state) ...");
        for (int i = 0; i < fileCount; i++)
        {
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes($"job-{i:D3}-{Guid.NewGuid()}"));
            await pool.WriteFileAsync(tenant, ms, $"job-{i:D3}.dat", default);
        }
        WriteOk($"{fileCount} files written.");

        // Concurrent workers
        WriteInfo($"Starting {workerCount} concurrent workers ...");
        var processedCount = 0;
        var workerTasks = Enumerable.Range(0, workerCount).Select(workerId => Task.Run(async () =>
        {
            while (true)
            {
                FileLocation? file;
                try
                {
                    file = await pool.GetNextFileForProcessingAsync(tenant, default);
                }
                catch (Exception)
                {
                    break;
                }

                if (file == null) break;

                // Simulate work
                await Task.Delay(15);

                await pool.MarkAsCompletedAsync(GetRequiredLease(file), default);
                var n = Interlocked.Increment(ref processedCount);
                System.Console.Write($"\r  Worker {workerId} — processed {n}/{fileCount}    ");
            }
        })).ToArray();

        await Task.WhenAll(workerTasks);
        System.Console.WriteLine();
        WriteOk($"All workers finished. Total processed: {processedCount}");
        Pause();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Test 6: Retry mechanism
    // ────────────────────────────────────────────────────────────────────────

    static async Task TestRetryMechanismAsync(IServiceProvider services)
    {
        PrintSection("Test 6 — Retry Mechanism (MarkAsFailed → Pending → PermanentlyFailed)");

        var pool = services.GetRequiredService<IStoragePool>();
        var tenantManager = services.GetRequiredService<ITenantManager>();

        var tenant = await tenantManager.GetTenantAsync("tenant-001", default);
        if (tenant == null) { WriteError("tenant-001 not found."); Pause(); return; }

        // Write one file
        string fileKey;
        using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("retry-test-payload")))
            fileKey = await pool.WriteFileAsync(tenant, ms, "retry.dat", default);

        WriteOk($"File written: {fileKey[..16]}...");

        var prevStatus = FileProcessingStatus.Pending;

        for (int attempt = 1; attempt <= 6; attempt++)
        {
            FileLocation? loc = null;
            try { loc = await pool.GetNextFileForProcessingAsync(tenant, default); }
            catch { /* no files yet due to retry delay */ }

            if (loc == null || loc.FileKey != fileKey)
            {
                var st = await pool.GetFileStatusAsync(tenant, fileKey, default);
                WriteInfo($"  Attempt {attempt}: file not available yet (status={st}, retry delay active).");
                await Task.Delay(300);
                continue;
            }

            WriteInfo($"  Attempt {attempt}: file dequeued — RetryCount={loc.RetryCount}, Status={loc.Status}");
            await pool.MarkAsFailedAsync(GetRequiredLease(loc), $"Simulated failure #{attempt}", default);

            var afterStatus = await pool.GetFileStatusAsync(tenant, fileKey, default);
            WriteInfo($"  After MarkAsFailed: status={afterStatus}");

            if (afterStatus == FileProcessingStatus.PermanentlyFailed)
            {
                WriteOk("File reached PermanentlyFailed — retry limit exceeded as expected.");
                break;
            }

            prevStatus = afterStatus;
            await Task.Delay(200); // give retry delay a moment
        }

        var finalStatus = await pool.GetFileStatusAsync(tenant, fileKey, default);
        WriteInfo($"Final file status: {finalStatus}");

        if (finalStatus != FileProcessingStatus.PermanentlyFailed)
            WriteWarning("Note: file may need more attempts to exhaust retries (MaxRetryCount in appsettings.json).");

        // Let cleanup service remove permanently failed files, or clean up manually
        WriteInfo("(Permanently failed files are removed by BackgroundCleanupService automatically)");
        Pause();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Test 7: File watcher — manual scan
    // ────────────────────────────────────────────────────────────────────────

    static async Task TestFileWatcherAsync(IServiceProvider services)
    {
        PrintSection("Test 7 — File Watcher (Manual Scan)");

        var fileWatcher = services.GetRequiredService<IFileWatcher>();
        var locusOptions = services.GetRequiredService<LocusOptions>();

        var watchers = locusOptions.FileWatchers;
        if (watchers.Count == 0)
        {
            WriteWarning("No FileWatcher configs found in appsettings.json.");
            Pause();
            return;
        }

        WriteInfo($"Found {watchers.Count} watcher(s):");
        foreach (var w in watchers)
            System.Console.WriteLine($"  [{w.WatcherId}]  Path={w.WatchPath}  MultiTenant={w.MultiTenantMode}  Enabled={w.Enabled}");

        System.Console.WriteLine();

        // Register watchers (idempotent — already registered by IHost startup services, but harmless to call again)
        foreach (var w in watchers)
        {
            try
            {
                await fileWatcher.RegisterWatcherAsync(w, default);
                WriteInfo($"Registered: {w.WatcherId}");
            }
            catch (Exception ex)
            {
                WriteWarning($"Register {w.WatcherId}: {ex.Message}");
            }
        }

        // Drop test files into the VIP watcher directory and trigger a scan
        var vipWatcher = watchers.FirstOrDefault(w => !w.MultiTenantMode && w.Enabled);
        if (vipWatcher != null)
        {
            var watchPath = Path.GetFullPath(vipWatcher.WatchPath);
            Directory.CreateDirectory(watchPath);

            WriteInfo($"Dropping 3 test files into: {watchPath}");
            for (int i = 1; i <= 3; i++)
            {
                var filePath = Path.Combine(watchPath, $"test-drop-{i}.pdf");
                await File.WriteAllTextAsync(filePath, $"Test file {i} — {DateTime.UtcNow:O}");
            }
            WriteOk("3 files created.");

            var minAge = vipWatcher.MinFileAge.TotalMilliseconds + 500;
            WriteInfo($"Waiting {minAge / 1000:F1}s for MinFileAge ({vipWatcher.MinFileAge.TotalSeconds}s) ...");
            await Task.Delay((int)minAge);

            WriteInfo($"Triggering manual scan: '{vipWatcher.WatcherId}' ...");
            var imported = await fileWatcher.ScanNowAsync(vipWatcher.WatcherId, default);
            WriteOk($"Scan done. Files imported: {imported}");
        }

        // Multi-tenant watcher — creates tenant subdirectories and imports files
        var multiWatcher = watchers.FirstOrDefault(w => w.MultiTenantMode && w.Enabled);
        if (multiWatcher != null)
        {
            WriteInfo($"Triggering multi-tenant watcher '{multiWatcher.WatcherId}' (auto-creates tenant dirs) ...");
            var imported = await fileWatcher.ScanNowAsync(multiWatcher.WatcherId, default);
            WriteOk($"Multi-tenant scan done. Files imported: {imported}");

            var sharedPath = Path.GetFullPath(multiWatcher.WatchPath);
            if (Directory.Exists(sharedPath))
            {
                var dirs = Directory.GetDirectories(sharedPath);
                WriteInfo($"Tenant directories under shared path ({dirs.Length} total):");
                foreach (var d in dirs.Take(10))
                    System.Console.WriteLine($"  {Path.GetFileName(d)}/");
                if (dirs.Length > 10) WriteInfo($"  ... and {dirs.Length - 10} more");
            }
        }

        Pause();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Test 8: Storage statistics
    // ────────────────────────────────────────────────────────────────────────

    static async Task ShowStorageStatsAsync(IServiceProvider services)
    {
        PrintSection("Test 8 — Storage Statistics");

        var pool = services.GetRequiredService<IStoragePool>();
        var tenantManager = services.GetRequiredService<ITenantManager>();
        var tenantQuotaMgr = services.GetRequiredService<ITenantQuotaManager>();

        var totalCap = await pool.GetTotalCapacityAsync(default);
        var available = await pool.GetAvailableSpaceAsync(default);
        var used = totalCap - available;

        System.Console.WriteLine();
        System.Console.WriteLine($"  Total capacity : {FormatBytes(totalCap)}");
        System.Console.WriteLine($"  Available space: {FormatBytes(available)}");
        System.Console.WriteLine($"  Used space     : {FormatBytes(used)}");
        System.Console.WriteLine();

        var tenants = (await tenantManager.GetAllTenantsAsync(default)).ToList();
        WriteInfo($"Tenants ({tenants.Count}):");
        foreach (var t in tenants)
        {
            var quota = await tenantQuotaMgr.GetFileCountAsync(t.TenantId, default);
            var limit = await tenantQuotaMgr.GetGlobalLimitAsync(default);
            System.Console.WriteLine($"  TenantId={t.TenantId,-20} Status={t.Status,-10} Files={quota}  GlobalLimit={( limit <= 0 ? "unlimited" : limit.ToString())}");
        }

        Pause();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Test 9: Tenant management
    // ────────────────────────────────────────────────────────────────────────

    static async Task ManageTenantsAsync(IServiceProvider services)
    {
        PrintSection("Test 9 — Tenant Management");

        var tenantManager = services.GetRequiredService<ITenantManager>();

        var tenants = (await tenantManager.GetAllTenantsAsync(default)).ToList();
        WriteInfo($"All tenants ({tenants.Count}):");
        for (int i = 0; i < tenants.Count; i++)
        {
            var t = tenants[i];
            var enabled = t.Status == TenantStatus.Enabled ? "(enabled)" : $"({t.Status})";
            System.Console.WriteLine($"  [{i + 1}] {t.TenantId,-25} {enabled}");
        }

        System.Console.WriteLine();
        System.Console.Write("  Enter new tenant ID to create (Enter to skip): ");
        var newId = System.Console.ReadLine()?.Trim();

        if (!string.IsNullOrEmpty(newId))
        {
            try
            {
                await tenantManager.CreateTenantAsync(newId, default);
                WriteOk($"Tenant '{newId}' created.");
            }
            catch (Exception ex)
            {
                WriteWarning($"Could not create '{newId}': {ex.Message}");
            }
        }

        System.Console.Write("  Enter tenant ID to toggle enable/disable (Enter to skip): ");
        var toggleId = System.Console.ReadLine()?.Trim();

        if (!string.IsNullOrEmpty(toggleId))
        {
            var t = await tenantManager.GetTenantAsync(toggleId, default);
            if (t == null)
            {
                WriteError($"Tenant '{toggleId}' not found.");
            }
            else if (t.Status == TenantStatus.Enabled)
            {
                await tenantManager.DisableTenantAsync(toggleId, default);
                WriteOk($"Tenant '{toggleId}' disabled.");
            }
            else
            {
                await tenantManager.EnableTenantAsync(toggleId, default);
                WriteOk($"Tenant '{toggleId}' enabled.");
            }
        }

        Pause();
    }

    // ────────────────────────────────────────────────────────────────────────
    // Helpers
    // ────────────────────────────────────────────────────────────────────────

    static void PrintBanner()
    {
        System.Console.ForegroundColor = ConsoleColor.Cyan;
        System.Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
        System.Console.WriteLine("║         Locus Storage Pool — Sample Console              ║");
        System.Console.WriteLine("║         Local Integration Test Tool                      ║");
        System.Console.WriteLine("╚══════════════════════════════════════════════════════════╝");
        System.Console.ResetColor();
        System.Console.WriteLine();
    }

    static void PrintMenu()
    {
        System.Console.WriteLine();
        System.Console.ForegroundColor = ConsoleColor.Yellow;
        System.Console.WriteLine("  ── Main Menu ───────────────────────────────────────────");
        System.Console.ResetColor();
        System.Console.WriteLine("  [1] Basic Write / Read / Delete");
        System.Console.WriteLine("  [2] Multi-Tenant Isolation");
        System.Console.WriteLine("  [3] Directory Quota");
        System.Console.WriteLine("  [4] Disabled Tenant Rejection");
        System.Console.WriteLine("  [5] File Scheduler (Concurrent Processing)");
        System.Console.WriteLine("  [6] Retry Mechanism (MarkAsFailed)");
        System.Console.WriteLine("  [7] File Watcher (Manual Scan)");
        System.Console.WriteLine("  [8] Storage Statistics");
        System.Console.WriteLine("  [9] Tenant Management");
        System.Console.WriteLine("  [Q] Quit");
        System.Console.WriteLine("  ────────────────────────────────────────────────────────");
        System.Console.Write("  Choice: ");
    }

    static void PrintSection(string title)
    {
        System.Console.WriteLine();
        System.Console.ForegroundColor = ConsoleColor.Cyan;
        System.Console.WriteLine($"  ┌─ {title}");
        System.Console.ResetColor();
    }

    static void WriteOk(string msg)
    {
        System.Console.ForegroundColor = ConsoleColor.Green;
        System.Console.WriteLine($"  + {msg}");
        System.Console.ResetColor();
    }

    static void WriteInfo(string msg)
    {
        System.Console.ForegroundColor = ConsoleColor.Gray;
        System.Console.WriteLine($"  . {msg}");
        System.Console.ResetColor();
    }

    static void WriteWarning(string msg)
    {
        System.Console.ForegroundColor = ConsoleColor.Yellow;
        System.Console.WriteLine($"  ! {msg}");
        System.Console.ResetColor();
    }

    static void WriteError(string msg)
    {
        System.Console.ForegroundColor = ConsoleColor.Red;
        System.Console.WriteLine($"  x {msg}");
        System.Console.ResetColor();
    }

    static void Pause()
    {
        System.Console.WriteLine();
        System.Console.ForegroundColor = ConsoleColor.DarkGray;
        System.Console.Write("  Press any key to return to menu...");
        System.Console.ResetColor();
        System.Console.ReadKey(intercept: true);
    }

    static FileProcessingLease GetRequiredLease(FileLocation file)
    {
        if (file.Lease != null)
            return file.Lease;

        if (!file.ProcessingStartTime.HasValue)
            throw new InvalidOperationException($"Missing processing lease for file {file.FileKey}.");

        return new FileProcessingLease
        {
            TenantId = file.TenantId,
            FileKey = file.FileKey,
            ProcessingStartTimeUtc = file.ProcessingStartTime.Value
        };
    }

    static string FormatBytes(long bytes)
    {
        if (bytes < 0) return "N/A";
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1024L * 1024 * 1024) return $"{bytes / 1024.0 / 1024:F1} MB";
        return $"{bytes / 1024.0 / 1024 / 1024:F2} GB";
    }
}
