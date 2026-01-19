# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Language Requirements

**IMPORTANT**: When working with this codebase:
- All responses, explanations, and discussions should be in **Chinese (中文)**
- All code comments, XML documentation, and code-related text should be in **English**
- Code identifiers (class names, method names, variables) must follow English naming conventions

## Project Overview

Locus is a file storage pool system targeting .NET netstandard2.0 that provides:
- Multi-tenant storage isolation (租户隔离存储)
- Dynamic storage volume mounting (动态挂载存储空间/硬盘)
- Concurrent read/write operations (并发读写)
- Unlimited storage expansion (无限空间扩展)
- Directory-level file count limits (目录级文件数量上限控制)

## Architecture Goals

### Multi-Tenant Storage
- Each tenant should have isolated storage space
- Tenant identification mechanism for routing file operations
- Tenant lifecycle management: enable/disable tenants (启用/禁用租户)
- Disabled tenants should reject all read/write operations
- Quota management per tenant (optional future feature)

### Storage Pool Management
- Abstract storage backend interface to support multiple storage providers
- Dynamic mounting/unmounting of storage volumes
- Load balancing across available storage volumes
- Health monitoring for storage volumes

### Concurrency
- Thread-safe file operations
- Support for multiple concurrent readers and writers
- Proper locking mechanisms to prevent data corruption
- Consider async/await patterns for I/O operations
- File allocation mechanism: ensure different threads read different files
- Return file location/path rather than directly reading file content
- Track file processing status to prevent duplicate reads

### Unlimited Storage Expansion
- Automatic volume expansion when storage capacity is low
- Intelligent file distribution across multiple volumes
- Support for adding new storage volumes at runtime
- No single volume size limitation - aggregate capacity from all volumes

### Directory-Level File Count Limits
- Configurable maximum file count per directory
- Pre-write validation to enforce limits
- Atomic counter management for concurrent writes
- Clear error handling when limits are reached

### Failure Retry Mechanism
- Automatic retry for failed file processing
- Configurable retry policy (max retries, delay, exponential backoff)
- Failed files automatically return to pool for retry
- Permanent failure status after exceeding max retries
- Track retry count and last error for diagnostics

### Automatic Cleanup
- Automatic cleanup of empty directories (空目录自动清理)
- Cleanup of orphaned files (physical file exists but no metadata)
- Cleanup of timed-out processing files (reset to pending status)
- Cleanup of permanently failed files
- Scheduled background cleanup tasks

## Key Design Considerations

### .NET Standard 2.0 Compatibility
- Must target netstandard2.0 for broad compatibility
- Use System.IO.Abstractions for testable file operations
- Avoid features only available in newer .NET versions

### Storage Volume Abstraction
- Define IStorageVolume interface for pluggable storage backends
- Support local file system, network drives, and extensible to cloud storage
- Volume metadata (capacity, available space, mount path)

### Tenant Context
- Thread-safe tenant context management
- Tenant identifier propagation through operation pipeline
- Separate storage paths or databases per tenant
- Tenant status management (Enabled/Disabled/Suspended)
- Pre-operation validation to check tenant status
- Tenant metadata storage (status, creation date, storage path, etc.)
- Consider caching tenant status for performance

### File Operations API
- Stream-based operations for memory efficiency
- Metadata management (file size, created date, modified date)
- Support for chunked uploads/downloads for large files
- File scheduler/allocator for concurrent read scenarios
- Return file metadata and location instead of direct content
- Status tracking: Pending → Processing → Completed/Failed

### Quota Management
- Directory-level file count tracking using atomic counters
- Configuration system for setting directory limits
- Pre-write validation to check against limits before accepting files
- Consider using separate metadata store (e.g., SQLite, LiteDB) for fast counter access
- Hierarchical quota inheritance (optional)

### Unlimited Storage Strategy
- Volume selection algorithm: prioritize volumes with most available space
- Automatic volume addition when aggregate free space falls below threshold
- File placement strategy: round-robin, least-used, or capacity-based
- Metadata tracking for file-to-volume mapping
- Handle volume failures gracefully with redundancy options (optional)

### Failure Retry Strategy
- When `MarkAsFailedAsync` is called, increment retry count
- If retry count < max retries: set status back to Pending with delay
- If retry count >= max retries: set status to PermanentlyFailed
- Use delay before retry to avoid immediate re-failure
- Exponential backoff formula: `InitialDelay * 2^(retryCount-1)`
- Store failure information for debugging and monitoring

### Automatic Cleanup Strategy
- Background service running on schedule (e.g., every hour, daily)
- Empty directory cleanup (空目录自动清理):
  - Recursively check directories for files
  - Remove directories with zero files
  - Respect whitelist of protected directories
  - Triggered after file deletion (immediate queue) or periodically (scheduled)
- Timeout detection:
  - Files in "Processing" status for longer than timeout threshold
  - Automatically reset to "Pending" status for retry
- Orphaned file cleanup:
  - Compare physical files against metadata store
  - Option to either delete or import orphaned files
- Permanently failed files:
  - Delete files that have exceeded max retry count
  - Configurable retention period before deletion
- Use configurable retention policies for each cleanup type

### Background Cleanup Service Implementation
```csharp
public class BackgroundCleanupService : BackgroundService
{
    private readonly IStorageCleanupService _cleanupService;
    private readonly ILogger<BackgroundCleanupService> _logger;
    private readonly TimeSpan _cleanupInterval = TimeSpan.FromHours(1);
    private readonly TimeSpan _processingTimeout = TimeSpan.FromMinutes(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation("Starting cleanup tasks...");

                // 1. 清理空目录
                await _cleanupService.CleanupAllEmptyDirectoriesAsync(stoppingToken);

                // 2. 清理处理超时的文件（重新放回池子）
                await _cleanupService.CleanupTimedOutProcessingFilesAsync(_processingTimeout, stoppingToken);

                // 3. 清理永久失败的文件（超过7天）
                await _cleanupService.CleanupPermanentlyFailedFilesAsync(TimeSpan.FromDays(7), stoppingToken);

                // 4. 清理孤立文件
                // await _cleanupService.CleanupOrphanedFilesAsync(...);

                var stats = await _cleanupService.GetCleanupStatisticsAsync(stoppingToken);
                _logger.LogInformation($"Cleanup completed: {stats.EmptyDirectoriesRemoved} dirs, " +
                    $"{stats.PermanentlyFailedFilesRemoved} failed files, " +
                    $"{stats.TimedOutFilesReset} timeout resets, " +
                    $"{stats.SpaceFreed / 1024 / 1024} MB freed");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during cleanup");
            }

            await Task.Delay(_cleanupInterval, stoppingToken);
        }
    }
}
```

## Build Commands

```bash
# Build the solution
dotnet build

# Build in Release mode
dotnet build -c Release

# Run tests
dotnet test

# Run specific test project
dotnet test path/to/test.csproj

# Pack NuGet package
dotnet pack -c Release
```

## Project Structure Recommendations

- `Locus.Core/` - Core abstractions and interfaces
- `Locus.FileSystem/` - Local file system storage implementation
- `Locus.Storage/` - Storage pool management
- `Locus.MultiTenant/` - Multi-tenant isolation logic
- `Locus.Tests/` - Unit and integration tests

## Implementation Notes

### Storage Volume Interface Example
```csharp
public interface IStorageVolume
{
    string VolumeId { get; }
    string MountPath { get; }
    long TotalCapacity { get; }
    long AvailableSpace { get; }
    bool IsHealthy { get; }
    Task<Stream> ReadAsync(string path, CancellationToken ct);
    Task WriteAsync(string path, Stream content, CancellationToken ct);
    Task DeleteAsync(string path, CancellationToken ct);
}
```

### Tenant Context Example
```csharp
public enum TenantStatus
{
    Enabled = 1,
    Disabled = 2,
    Suspended = 3
}

public interface ITenantContext
{
    string TenantId { get; }
    TenantStatus Status { get; }
}

public interface ITenantManager
{
    Task<ITenantContext> GetTenantAsync(string tenantId, CancellationToken ct);
    Task<bool> IsTenantEnabledAsync(string tenantId, CancellationToken ct);
    Task EnableTenantAsync(string tenantId, CancellationToken ct);
    Task DisableTenantAsync(string tenantId, CancellationToken ct);
    Task CreateTenantAsync(string tenantId, CancellationToken ct);
    Task<IEnumerable<ITenantContext>> GetAllTenantsAsync(CancellationToken ct);
}

public interface IStoragePool
{
    Task<Stream> ReadFileAsync(ITenantContext tenant, string fileKey, CancellationToken ct);
    Task WriteFileAsync(ITenantContext tenant, string fileKey, Stream content, CancellationToken ct);
    Task DeleteFileAsync(ITenantContext tenant, string fileKey, CancellationToken ct);
    Task MountVolumeAsync(IStorageVolume volume, CancellationToken ct);
    Task UnmountVolumeAsync(string volumeId, CancellationToken ct);
    Task<IEnumerable<IStorageVolume>> GetVolumesAsync(CancellationToken ct);
    Task<long> GetTotalCapacityAsync(CancellationToken ct);
    Task<long> GetAvailableSpaceAsync(CancellationToken ct);
}

public interface IDirectoryQuotaManager
{
    Task<bool> CanAddFileAsync(string directoryPath, CancellationToken ct);
    Task IncrementFileCountAsync(string directoryPath, CancellationToken ct);
    Task DecrementFileCountAsync(string directoryPath, CancellationToken ct);
    Task<int> GetFileCountAsync(string directoryPath, CancellationToken ct);
    Task<int> GetLimitAsync(string directoryPath, CancellationToken ct);
    Task SetLimitAsync(string directoryPath, int maxFiles, CancellationToken ct);
}

public class DirectoryQuotaConfig
{
    public string DirectoryPattern { get; set; } // e.g., "tenant-*/files/*"
    public int MaxFilesPerDirectory { get; set; }
    public bool Enabled { get; set; }
}

public enum FileProcessingStatus
{
    Pending = 0,
    Processing = 1,
    Completed = 2,
    Failed = 3,
    PermanentlyFailed = 4  // 超过最大重试次数，永久失败
}

public class FileLocation
{
    public string FileKey { get; set; }
    public string VolumeId { get; set; }
    public string PhysicalPath { get; set; }
    public string DirectoryPath { get; set; }
    public long FileSize { get; set; }
    public DateTime CreatedAt { get; set; }
    public FileProcessingStatus Status { get; set; }
    public int RetryCount { get; set; }  // 当前重试次数
    public DateTime? LastFailedAt { get; set; }  // 最后失败时间
    public string LastError { get; set; }  // 最后错误信息
}

public interface IFileScheduler
{
    // 获取下一个待处理的文件位置（并发安全，不会返回同一个文件给多个线程）
    Task<FileLocation> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct);

    // 获取批量待处理文件位置
    Task<IEnumerable<FileLocation>> GetNextBatchForProcessingAsync(ITenantContext tenant, int batchSize, CancellationToken ct);

    // 标记文件为处理中（防止其他线程重复获取）
    Task MarkAsProcessingAsync(string fileKey, CancellationToken ct);

    // 标记文件处理完成并删除（删除物理文件和元数据）
    Task MarkAsCompletedAndDeleteAsync(string fileKey, CancellationToken ct);

    // 标记文件处理失败（自动重新放回池子，支持重试）
    // 失败的文件状态变为 Pending，等待下一个线程处理
    Task MarkAsFailedAsync(string fileKey, string errorMessage, CancellationToken ct);

    // 重置处理状态（用于手动重试）
    Task ResetProcessingStatusAsync(string fileKey, CancellationToken ct);

    // 获取文件当前状态
    Task<FileProcessingStatus> GetFileStatusAsync(string fileKey, CancellationToken ct);
}

public class FileRetryPolicy
{
    public int MaxRetryCount { get; set; } = 3;  // 最大重试次数
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(5);  // 初始重试延迟
    public bool UseExponentialBackoff { get; set; } = true;  // 是否使用指数退避
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);  // 最大重试延迟
}

public interface IStorageCleanupService
{
    // 清理空目录
    Task CleanupEmptyDirectoriesAsync(ITenantContext tenant, CancellationToken ct);

    // 清理指定租户下的所有空目录
    Task CleanupEmptyDirectoriesAsync(string tenantId, CancellationToken ct);

    // 清理所有租户的空目录
    Task CleanupAllEmptyDirectoriesAsync(CancellationToken ct);

    // 清理已完成的文件记录（从元数据中删除，但保留物理文件）
    Task CleanupCompletedFileRecordsAsync(TimeSpan olderThan, CancellationToken ct);

    // 清理永久失败的文件（删除物理文件和元数据）
    Task CleanupPermanentlyFailedFilesAsync(TimeSpan olderThan, CancellationToken ct);

    // 清理孤立文件（物理文件存在但元数据不存在）
    Task CleanupOrphanedFilesAsync(ITenantContext tenant, CancellationToken ct);

    // 清理处理超时的文件（超时后重新放回池子）
    Task CleanupTimedOutProcessingFilesAsync(TimeSpan timeout, CancellationToken ct);

    // 获取清理统计信息
    Task<CleanupStatistics> GetCleanupStatisticsAsync(CancellationToken ct);
}

public class CleanupStatistics
{
    public int EmptyDirectoriesRemoved { get; set; }
    public int CompletedRecordsRemoved { get; set; }
    public int PermanentlyFailedFilesRemoved { get; set; }
    public int OrphanedFilesRemoved { get; set; }
    public int TimedOutFilesReset { get; set; }
    public long SpaceFreed { get; set; }  // 释放的空间（字节）
}
```

### Concurrency Considerations
- Use `SemaphoreSlim` for managing concurrent access limits
- Use `ReaderWriterLockSlim` for scenarios with frequent reads and occasional writes
- Consider using `ConcurrentDictionary` for tenant/volume mappings
- All public APIs should accept `CancellationToken`
- Atomic operations for file count increments using `Interlocked` or database transactions

### File Scheduler Implementation Strategy
**核心需求：确保多线程并发读取时，每个线程获取不同的文件**

实现方案选择：

1. **数据库事务方式** (推荐用于持久化场景)
   - 使用 `SELECT ... FOR UPDATE` 或等效的悲观锁
   - 原子性地获取并标记文件状态：Pending → Processing
   - 示例 SQL：`UPDATE files SET status=1, processing_by=@threadId WHERE status=0 LIMIT 1`

2. **内存队列方式** (适合临时任务)
   - 使用 `ConcurrentQueue<FileLocation>` 或 `BlockingCollection<FileLocation>`
   - 线程安全的 TryDequeue 操作自动保证不重复
   - 适合一次性批量处理场景

3. **分布式锁方式** (适合多进程场景)
   - 使用 Redis 的 SETNX 命令获取文件处理权
   - 设置锁超时避免死锁
   - 适合跨服务器的分布式处理

4. **混合方式** (推荐)
   - 使用轻量级数据库（SQLite/LiteDB）存储文件元数据和状态
   - 使用数据库事务保证原子性
   - 使用内存缓存提高性能
   - 示例流程：
     ```
     BEGIN TRANSACTION
     SELECT fileKey, physicalPath, volumeId
     FROM files
     WHERE tenantId=@tenant AND status=0
     LIMIT 1
     FOR UPDATE

     UPDATE files SET status=1, processingStartTime=@now
     WHERE fileKey=@selectedKey

     COMMIT
     ```

**关键点：**
- `GetNextFileForProcessingAsync` 必须是原子操作
- 使用数据库或分布式锁保证线程安全
- 考虑处理超时：如果文件标记为 Processing 但超过指定时间未完成，应允许重新分配
- 实现心跳机制：处理线程定期更新 lastHeartbeat 时间戳

**失败重试逻辑：**
```csharp
public async Task MarkAsFailedAsync(string fileKey, string errorMessage, CancellationToken ct)
{
    await using var transaction = await _db.BeginTransactionAsync(ct);

    var file = await _db.Files.FindAsync(fileKey);
    file.RetryCount++;
    file.LastError = errorMessage;
    file.LastFailedAt = DateTime.UtcNow;

    if (file.RetryCount >= _retryPolicy.MaxRetryCount)
    {
        // 超过最大重试次数，标记为永久失败
        file.Status = FileProcessingStatus.PermanentlyFailed;
    }
    else
    {
        // 重新放回池子，设置为 Pending 状态
        file.Status = FileProcessingStatus.Pending;

        // 计算下次重试延迟（指数退避）
        var delay = _retryPolicy.UseExponentialBackoff
            ? TimeSpan.FromMilliseconds(_retryPolicy.InitialRetryDelay.TotalMilliseconds * Math.Pow(2, file.RetryCount - 1))
            : _retryPolicy.InitialRetryDelay;

        // 限制最大延迟
        delay = delay > _retryPolicy.MaxRetryDelay ? _retryPolicy.MaxRetryDelay : delay;

        // 设置下次可以处理的时间（延迟重试）
        file.AvailableForProcessingAt = DateTime.UtcNow.Add(delay);
    }

    await _db.SaveChangesAsync(ct);
    await transaction.CommitAsync(ct);
}

// GetNextFileForProcessingAsync 需要考虑延迟时间
public async Task<FileLocation> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct)
{
    await using var transaction = await _db.BeginTransactionAsync(ct);

    var file = await _db.Files
        .Where(f => f.TenantId == tenant.TenantId
                 && f.Status == FileProcessingStatus.Pending
                 && (f.AvailableForProcessingAt == null || f.AvailableForProcessingAt <= DateTime.UtcNow))
        .OrderBy(f => f.CreatedAt)
        .FirstOrDefaultAsync(ct);

    if (file == null)
        return null;

    file.Status = FileProcessingStatus.Processing;
    file.ProcessingStartTime = DateTime.UtcNow;

    await _db.SaveChangesAsync(ct);
    await transaction.CommitAsync(ct);

    return MapToFileLocation(file);
}
```

### Validation Pipeline
Before any file operation (read/write/delete), validate in order:
1. Tenant exists and is enabled (throw `TenantDisabledException` if not)
2. Directory quota limit not exceeded (throw `DirectoryQuotaExceededException` if exceeded)
3. Storage volume is healthy and has sufficient space
4. File path is valid and sanitized

### Path Management
- Sanitize file paths to prevent directory traversal attacks
- Use Path.Combine for cross-platform compatibility
- Generate deterministic paths: `{VolumeMount}/{TenantId}/{FileKey}`

### Error Handling
Define custom exceptions:
- `TenantDisabledException` - When operations attempted on disabled tenant
- `TenantNotFoundException` - When tenant does not exist
- `DirectoryQuotaExceededException` - When directory file count limit reached
- `StorageVolumeUnavailableException` - When no healthy volumes available
- `InsufficientStorageException` - When aggregate storage is full (rare with unlimited expansion)
- `NoFilesAvailableException` - When no pending files available for processing
- `FileAlreadyProcessingException` - When attempting to process a file already being processed

### File Processing Workflow
**完整流程说明：**
1. **获取文件位置**：调用 `GetNextFileForProcessingAsync` 获取待处理文件的位置信息（不是文件内容）
2. **处理文件**：根据位置信息读取和处理文件
3. **处理成功**：调用 `MarkAsCompletedAndDeleteAsync` **删除物理文件和元数据**
4. **处理失败**：调用 `MarkAsFailedAsync` **重新放回池子**，等待下一个线程处理（支持自动重试）

**文件状态转换图：**
```
                    ┌──────────────────────────────────────┐
                    │         新文件写入                    │
                    └─────────────┬────────────────────────┘
                                  ↓
                            ┌──────────┐
                            │ Pending  │ ←──────────────┐
                            └─────┬────┘                │
                                  │                     │
        GetNextFileForProcessingAsync()      MarkAsFailedAsync()
                                  │              (RetryCount < MaxRetries)
                                  ↓                     │
                           ┌────────────┐               │
                           │ Processing │───────────────┘
                           └──────┬─────┘
                                  │
                    ┌─────────────┴──────────────┐
                    │                            │
        MarkAsCompletedAndDeleteAsync()   MarkAsFailedAsync()
            (处理成功)                  (RetryCount >= MaxRetries)
                    │                            │
                    ↓                            ↓
            ┌─────────────┐              ┌──────────────────┐
            │  文件被删除  │              │PermanentlyFailed │
            └─────────────┘              └──────────────────┘
                                                 │
                                    CleanupPermanentlyFailedFilesAsync()
                                                 │
                                                 ↓
                                         ┌─────────────┐
                                         │  文件被删除  │
                                         └─────────────┘
```

**关键点：**
- **Pending → Processing**: 获取文件位置时自动转换（并发安全，原子操作）
- **Processing → 删除**: 处理成功，立即删除物理文件和元数据
- **Processing → Pending**: 处理失败但未超过重试次数，重新放回池子（带延迟）
- **Processing → PermanentlyFailed**: 处理失败且超过最大重试次数
- **PermanentlyFailed → 删除**: 自动清理服务定期删除

### Concurrent File Processing Usage Example
```csharp
// 多线程并发处理文件示例
public async Task ProcessFilesInParallelAsync(ITenantContext tenant, int threadCount)
{
    var tasks = new List<Task>();

    for (int i = 0; i < threadCount; i++)
    {
        int threadId = i;
        tasks.Add(Task.Run(async () =>
        {
            while (true)
            {
                try
                {
                    // 1. 获取下一个待处理文件的位置（不是读取文件内容）
                    // 并发安全：每个线程获取不同的文件
                    var fileLocation = await fileScheduler.GetNextFileForProcessingAsync(tenant, CancellationToken.None);

                    if (fileLocation == null)
                        break; // 没有更多待处理文件

                    Console.WriteLine($"[Thread {threadId}] Processing file: {fileLocation.FileKey}");

                    try
                    {
                        // 2. 根据文件位置信息，从存储卷读取实际内容
                        using var stream = await storagePool.ReadFileAsync(tenant, fileLocation.FileKey, CancellationToken.None);

                        // 3. 处理文件内容
                        await ProcessFileContentAsync(stream);

                        // 4. 处理成功：删除文件（物理文件 + 元数据）
                        await fileScheduler.MarkAsCompletedAndDeleteAsync(fileLocation.FileKey, CancellationToken.None);

                        Console.WriteLine($"[Thread {threadId}] Successfully processed and deleted: {fileLocation.FileKey}");
                    }
                    catch (Exception ex)
                    {
                        // 5. 处理失败：重新放回池子，等待下一个线程处理
                        // MarkAsFailedAsync 会自动将状态改为 Pending，并增加重试计数
                        await fileScheduler.MarkAsFailedAsync(fileLocation.FileKey, ex.Message, CancellationToken.None);

                        Console.WriteLine($"[Thread {threadId}] Failed to process (will retry): {fileLocation.FileKey}, Error: {ex.Message}");

                        // 注意：失败的文件会自动重新放回池子
                        // 如果重试次数 < 最大重试次数，其他线程会再次获取并处理
                        // 如果重试次数 >= 最大重试次数，文件状态变为 PermanentlyFailed
                    }
                }
                catch (NoFilesAvailableException)
                {
                    break; // 没有更多待处理文件
                }
            }

            Console.WriteLine($"[Thread {threadId}] Completed all work");
        }));
    }

    await Task.WhenAll(tasks);
    Console.WriteLine("All threads completed");
}
```

### MarkAsCompletedAndDeleteAsync Implementation
```csharp
public async Task MarkAsCompletedAndDeleteAsync(string fileKey, CancellationToken ct)
{
    await using var transaction = await _db.BeginTransactionAsync(ct);

    var file = await _db.Files.FindAsync(fileKey);
    if (file == null)
        throw new FileNotFoundException($"File not found: {fileKey}");

    // 删除物理文件
    var physicalPath = Path.Combine(file.VolumeMount, file.PhysicalPath);
    if (File.Exists(physicalPath))
    {
        File.Delete(physicalPath);

        // 如果删除后目录为空，标记为需要清理
        var directory = Path.GetDirectoryName(physicalPath);
        if (Directory.Exists(directory) && !Directory.EnumerateFileSystemEntries(directory).Any())
        {
            // 空目录会被自动清理服务定期清理
            await _cleanupQueue.EnqueueEmptyDirectoryAsync(directory, ct);
        }
    }

    // 删除元数据记录
    _db.Files.Remove(file);

    // 更新目录文件计数（减1）
    await _quotaManager.DecrementFileCountAsync(file.DirectoryPath, ct);

    await _db.SaveChangesAsync(ct);
    await transaction.CommitAsync(ct);
}
```

### File Watcher Configuration

FileWatcher 支持自动监控目录并导入文件到存储池。

**多租户模式示例（推荐）：**
```csharp
services.AddLocus(builder => builder
    .AddFileWatcher(watcher =>
    {
        watcher.WatchPath = "/path/to/watch";
        watcher.MultiTenantMode = true;              // 启用多租户模式
        watcher.AutoCreateTenantDirectories = true;  // 自动创建租户子目录
        watcher.PollingInterval = TimeSpan.FromSeconds(30);
        watcher.PostImportAction = PostImportAction.Delete;
    })
);
```

**关键配置说明：**
- `MultiTenantMode = true`: 启用多租户模式，子目录名称作为租户ID
- `AutoCreateTenantDirectories = true`: 首次扫描时自动为所有已创建的租户创建子目录
  - 从 `ITenantManager.GetAllTenantsAsync()` 获取所有租户
  - 为每个租户在 WatchPath 下创建对应子目录（如果不存在）
  - 只在多租户模式下生效
- `PostImportAction`: 导入后的操作（Delete/Move/Keep）
- `PollingInterval`: 扫描间隔
- `MinFileAge`: 最小文件年龄（避免导入正在写入的文件）
- `MaxConcurrentImports`: 最大并发导入数

**目录结构示例：**
```
/path/to/watch/
  ├── tenant-001/     # 自动创建（如果 AutoCreateTenantDirectories = true）
  ├── tenant-002/     # 自动创建
  └── tenant-003/     # 自动创建
```

**工作流程：**
1. FileWatcher 扫描时检测到 `AutoCreateTenantDirectories = true`
2. 调用 `_tenantManager.GetAllTenantsAsync()` 获取所有租户
3. 为每个租户创建子目录（如果不存在）
4. 扫描所有子目录并导入文件到对应租户

**注意事项：**
- 租户必须先通过 `ITenantManager.CreateTenantAsync()` 创建
- 自动创建的目录名称与租户ID完全匹配
- 如果手动创建的子目录名称不是有效租户ID，文件将被跳过
