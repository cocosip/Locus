# Locus - Multi-Tenant File Storage Pool System

[![CI/CD](https://github.com/cocosip/Locus/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/cocosip/Locus/actions/workflows/ci-cd.yml)
[![NuGet](https://img.shields.io/nuget/v/Locus.svg)](https://www.nuget.org/packages/Locus/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A high-performance, multi-tenant file storage pool system for .NET targeting netstandard2.0 with LiteDB-based metadata management.

## Overview

Locus is designed as a **file queue system** that provides:
- **Multi-tenant isolation** - Each tenant has isolated storage space with enable/disable controls
- **Queue-based processing** - Files are processed as a queue with automatic retry on failure
- **Unlimited storage expansion** - Dynamically mount multiple storage volumes
- **High concurrency** - Thread-safe operations with per-tenant LiteDB databases and active-data caching
- **Automatic management** - System handles directory structure, file placement, and cleanup
- **Directory-level quota control** - Configurable file count limits per directory

## Key Concepts

### File Queue System

Locus is **not** a traditional file system where users specify file paths. Instead:

1. **Write** - User provides file content, system generates and returns a `fileKey`
2. **Process** - Workers fetch next pending file from queue for processing
3. **Complete/Retry** - Mark file as completed (deleted) or failed (retry)

Users **never** need to know:
- Which storage volume holds the file
- Which directory the file is in
- How files are distributed across volumes

### System-Generated File Keys

```csharp
// Get tenant context
var tenant = await tenantManager.GetTenantAsync("tenant-001", ct);

// Write a file - system generates unique fileKey
// Optional: provide original file name to preserve extension
string fileKey = await storagePool.WriteFileAsync(tenant, fileStream, "invoice.pdf", ct);
// Returns: "f7b3c9d2-4a1e-4f8b-9c3d-2e1a4b5c6d7e"
// Physical file: ./storage/vol-001/tenant-001/f7/b3/f7b3c9d2...pdf ✅

// Without original file name (backward compatible)
string fileKey2 = await storagePool.WriteFileAsync(tenant, fileStream, null, ct);
// Physical file: ./storage/vol-001/tenant-001/a1/b2/a1b2c3d4... (no extension)

// Read file content directly
using var stream = await storagePool.ReadFileAsync(tenant, fileKey, ct);

// Get file basic information
var fileInfo = await storagePool.GetFileInfoAsync(tenant, fileKey, ct);
// Returns: FileInfo { FileKey, FileSize, CreatedAt, Status }

// Get detailed location (for diagnostics)
var location = await storagePool.GetFileLocationAsync(tenant, fileKey, ct);
// Returns: FileLocation { FileKey, VolumeId, PhysicalPath, Status, RetryCount, ... }
```

## Architecture

```
┌─────────────────────────────────────────────┐
│   API Layer (IStoragePool)                 │
│   - Unified storage + queue interface       │
│   - File operations + processing control    │
├─────────────────────────────────────────────┤
│   Active-Data Cache (Per-Tenant)           │
│   - Only Pending/Processing/Failed files    │
│   - ConcurrentDictionary for fast lookups   │
│   - Automatic cache invalidation            │
├─────────────────────────────────────────────┤
│   Persistence Layer (Per-Tenant LiteDB)    │
│   - MetadataRepository: File metadata       │
│   - DirectoryQuotaRepository: Quota limits  │
│   - Atomic operations with transactions     │
├─────────────────────────────────────────────┤
│   Tenant Management (JSON + Cache)         │
│   - TenantMetadata: Status, creation date   │
│   - 5-minute in-memory cache                │
│   - Auto-create support                     │
├─────────────────────────────────────────────┤
│   Storage Volumes (Configured at startup)  │
│   - LocalFileSystemVolume (implemented)     │
│   - Network Drives (supported)              │
│   - Extensible to Cloud Storage             │
└─────────────────────────────────────────────┘
```

### Key Design Decisions

- **Unified API**: IStoragePool combines file storage and queue processing in one interface
- **Per-Tenant LiteDB**: Each tenant has isolated `.db` file for metadata
- **Active-Data Caching**: Only cache files in Pending/Processing/Failed states
- **Completed Files**: Automatically removed from cache and database after processing
- **Atomic Quota Operations**: SemaphoreSlim + LiteDB transactions ensure concurrency safety
- **Startup Volume Configuration**: Storage volumes are configured at startup and managed internally

## Core APIs

### IStoragePool - Unified Storage and Queue Management

**Note**: IStoragePool provides a unified interface that combines file storage operations with queue-based processing. Storage volumes are configured at startup and managed internally.

```csharp
public interface IStoragePool
{
    // ===== File Storage Operations =====

    // Write file → returns system-generated fileKey
    // Optional: provide originalFileName (e.g., "invoice.pdf") to preserve file extension
    Task<string> WriteFileAsync(ITenantContext tenant, Stream content, string? originalFileName, CancellationToken ct);

    // Read file by fileKey
    Task<Stream> ReadFileAsync(ITenantContext tenant, string fileKey, CancellationToken ct);

    // Get file basic information
    Task<FileInfo?> GetFileInfoAsync(ITenantContext tenant, string fileKey, CancellationToken ct);

    // Get file location (for diagnostics)
    Task<FileLocation?> GetFileLocationAsync(ITenantContext tenant, string fileKey, CancellationToken ct);

    // ===== Queue-Based Processing =====

    // Get next pending file (thread-safe, no duplicates)
    Task<FileLocation?> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct);

    // Get batch of pending files
    Task<IEnumerable<FileLocation>> GetNextBatchForProcessingAsync(
        ITenantContext tenant, int batchSize, CancellationToken ct);

    // Mark file as completed → deletes file and metadata
    Task MarkAsCompletedAsync(string fileKey, CancellationToken ct);

    // Mark file as failed → returns to queue for retry
    Task MarkAsFailedAsync(string fileKey, string errorMessage, CancellationToken ct);

    // Get current file status
    Task<FileProcessingStatus> GetFileStatusAsync(string fileKey, CancellationToken ct);

    // ===== Capacity Management =====

    // Get total capacity across all volumes
    Task<long> GetTotalCapacityAsync(CancellationToken ct);

    // Get available space across all volumes
    Task<long> GetAvailableSpaceAsync(CancellationToken ct);
}
```

### ITenantManager - Multi-Tenant Management

```csharp
public interface ITenantManager
{
    // Get tenant context (auto-create if enabled)
    Task<ITenantContext> GetTenantAsync(string tenantId, CancellationToken ct);

    // Check if tenant is enabled
    Task<bool> IsTenantEnabledAsync(string tenantId, CancellationToken ct);

    // Create new tenant
    Task CreateTenantAsync(string tenantId, CancellationToken ct);

    // Enable/Disable tenant
    Task EnableTenantAsync(string tenantId, CancellationToken ct);
    Task DisableTenantAsync(string tenantId, CancellationToken ct);

    // Get all tenants
    Task<IEnumerable<ITenantContext>> GetAllTenantsAsync(CancellationToken ct);
}
```

## Usage Example

### Basic File Queue Processing

```csharp
// Get tenant context
var tenant = await tenantManager.GetTenantAsync("tenant-001", ct);

// Producer: Write files to the queue
// Provide original file names to preserve extensions
var fileKey1 = await storagePool.WriteFileAsync(tenant, stream1, "document.pdf", ct);
var fileKey2 = await storagePool.WriteFileAsync(tenant, stream2, "invoice.xlsx", ct);
// Files are automatically queued as "Pending" status

// Consumer: Process files from the queue (10 concurrent workers)
var tasks = Enumerable.Range(0, 10).Select(async threadId =>
{
    while (true)
    {
        // Get next file (thread-safe, no duplicates across threads)
        var file = await storagePool.GetNextFileForProcessingAsync(tenant, ct);
        if (file == null) break; // No more files

        try
        {
            // Read and process file
            using var stream = await storagePool.ReadFileAsync(tenant, file.FileKey, ct);
            await ProcessFileAsync(stream);

            // Success: mark as completed (deletes file and metadata)
            await storagePool.MarkAsCompletedAsync(file.FileKey, ct);
        }
        catch (Exception ex)
        {
            // Failure: return to queue for retry
            await storagePool.MarkAsFailedAsync(file.FileKey, ex.Message, ct);
            // File will be retried based on retry policy
        }
    }
});

await Task.WhenAll(tasks);
```

### Batch Processing

```csharp
// Get tenant context
var tenant = await tenantManager.GetTenantAsync("tenant-001", ct);

// Process files in batches
while (true)
{
    // Get batch of 100 files
    var batch = await storagePool.GetNextBatchForProcessingAsync(tenant, 100, ct);
    if (!batch.Any()) break;

    // Process batch in parallel
    await Parallel.ForEachAsync(batch, ct, async (file, token) =>
    {
        try
        {
            using var stream = await storagePool.ReadFileAsync(tenant, file.FileKey, token);
            await ProcessFileAsync(stream);
            await storagePool.MarkAsCompletedAsync(file.FileKey, token);
        }
        catch (Exception ex)
        {
            await storagePool.MarkAsFailedAsync(file.FileKey, ex.Message, token);
        }
    });
}
```

## Performance

### Benchmark Results

Performance benchmarks run on Intel Core i5-9400 CPU 2.90GHz (Coffee Lake), 6 cores, .NET 10.0.0, Windows 11:

#### Write Throughput (Single-Threaded, 100 KB file)

| File Size | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| 100 KB | 1.074 ms | 7.34 KB | End-to-end: quota check + disk write + metadata |
| 1 MB | 1.296 ms | 7.34 KB | I/O dominated |
| 10 MB | 4.736 ms | 12.55 KB | I/O dominated |

#### Concurrent Write Scalability (1 MB per write)

| Concurrency | Mean Time | StdDev | Allocated | Notes |
|-------------|-----------|--------|-----------|-------|
| 1 writer (baseline) | 2.839 ms | 0.350 ms | 77.04 KB | Single-threaded baseline |
| 10 concurrent writers | 26.368 ms | 6.849 ms | 273.52 KB | All 10 writes in parallel |
| 50 concurrent writers | 113.463 ms | 10.608 ms | 701.02 KB | All 50 writes in parallel |
| 100 concurrent writers | 228.952 ms | 11.859 ms | 1163.76 KB | All 100 writes in parallel |

#### Metadata Operations (Write-Behind + `_pendingKeys` Index)

| Operation | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| AddOrUpdate single file | 1.878 μs | 2.4 KB | ⚡ Memory-first, async LiteDB persistence |
| Get file metadata (cache hit) | 40.91 ns | 72 B | ⚡ ConcurrentDictionary lookup |
| Get non-existent file (returns null) | 34.87 ns | 0 B | Cache miss → null, no LiteDB fallback |
| Batch insert 100 files | 237.9 μs | 63.4 KB | ~2.4 μs per file |
| Get next pending file (100-file pool) | 2.975 μs | 1.4 KB | ⚡ O(n_pending) scan via `_pendingKeys` |

#### Directory Quota Operations (Lock-Free CAS)

| Operation | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| Check can add (no limit) | 141.25 ns | 176 B | ⚡ AtomicQuotaState hot path |
| Check can add (with limit) | 139.07 ns | 176 B | ⚡ Lock-free CAS comparison |
| Increment file count | 94.80 ns | 72 B | ⚡ Lock-free CAS, near-zero contention |
| Decrement file count | 231.05 ns | 248 B | Increment + decrement pair |
| Set directory limit | 2.916 ms | 142.2 KB | LiteDB write (persisted immediately) |
| Get file count | 145.41 ns | 232 B | ⚡ AtomicQuotaState read |

#### Volume Health & Space (TTL Cached)

| Operation | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| IsHealthy | 17.88 ns | 0 B | ⚡ 30s TTL cache (no disk I/O) |
| AvailableSpace | 22.33 ns | 0 B | ⚡ 30s TTL cache |
| TotalCapacity | 22.32 ns | 0 B | ⚡ 30s TTL cache |

#### Tenant Management

| Operation | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| Create tenant | 2.589 ms | 7.0 KB | JSON write + directory creation |
| Get tenant (cache hit) | 81.95 ns | 104 B | ⚡ 5-minute in-memory cache |
| Get tenant (cache miss) | 24.23 μs | 1.28 KB | JSON file read + parse |
| Get tenant (auto-create) | 2.116 ms | 9.3 KB | Create + load |
| Check tenant enabled (cache hit) | 89.86 ns | 104 B | ⚡ Cache hit |
| Enable tenant | 3.060 ms | 21.1 KB | JSON update + cache invalidation |
| Disable tenant | 1.864 ms | 14.3 KB | JSON update + cache invalidation |

#### End-to-End Concurrent Operations

| Operation | threadCount | Mean Time | Allocated | Notes |
|-----------|-------------|-----------|-----------|-------|
| 10 concurrent reads | — | 14.413 ms | 1520 KB | Parallel file reads |
| Mixed read/write (20 ops) | — | 8.312 ms | 1157 KB | 10 writes + 10 reads concurrent |
| Concurrent writes | 10 | 3.076 ms | 380 KB | 10 simultaneous writes |
| Concurrent writes | 50 | 14.249 ms | 1644 KB | 50 simultaneous writes |
| Concurrent writes | 100 | 25.319 ms | 2781 KB | 100 simultaneous writes |

**Key Findings**:
- ⚡ **Directory quota CAS**: 94.80 ns per increment — lock-free atomic operations (vs. ~200 μs with SemaphoreSlim)
- ⚡ **Volume health/space**: 17–22 ns — 30-second TTL cache eliminates one disk I/O per write
- ⚡ **Metadata write-behind**: 1.878 μs per file — memory-first, LiteDB persistence is async
- ⚡ **Metadata cache hit**: 40.91 ns — pure ConcurrentDictionary lookup
- ⚡ **Tenant cache**: 82–90 ns — 5-minute cache keeps tenant lookups near-zero cost
- ✅ **100 KB write**: ~1.1 ms end-to-end (quota check + disk write + metadata)
- ✅ **100-concurrent writes (1 MB)**: 229 ms total for 100 simultaneous writes, StdDev 5.2%

### Running Benchmarks

```bash
cd tests/Locus.Benchmarks
dotnet run -c Release

# Run specific benchmark class
dotnet run -c Release --filter "Locus.Benchmarks.MetadataRepositoryBenchmarks*"
dotnet run -c Release --filter "Locus.Benchmarks.DirectoryQuotaBenchmarks*"
dotnet run -c Release --filter "Locus.Benchmarks.TenantManagerBenchmarks*"
dotnet run -c Release --filter "Locus.Benchmarks.StoragePoolWriteThroughputBenchmarks*"
dotnet run -c Release --filter "Locus.Benchmarks.StoragePoolConcurrencyBenchmarks*"
dotnet run -c Release --filter "Locus.Benchmarks.VolumeHealthCheckBenchmarks*"
```

See [Benchmark README](tests/Locus.Benchmarks/README.md) for detailed analysis.

## Project Structure

```
Locus/
├── src/
│   ├── Locus.Core/              # Core abstractions and interfaces
│   │   ├── Abstractions/
│   │   │   ├── IStoragePool.cs
│   │   │   ├── IFileScheduler.cs
│   │   │   ├── ITenantManager.cs
│   │   │   ├── IStorageVolume.cs
│   │   │   ├── IDirectoryQuotaManager.cs
│   │   │   ├── ITenantQuotaManager.cs
│   │   │   └── IStorageCleanupService.cs
│   │   ├── Models/
│   │   │   ├── FileLocation.cs
│   │   │   ├── FileProcessingStatus.cs
│   │   │   ├── TenantStatus.cs
│   │   │   ├── FileRetryPolicy.cs
│   │   │   ├── DirectoryQuotaConfig.cs
│   │   │   └── CleanupStatistics.cs
│   │   └── Exceptions/
│   │       ├── TenantDisabledException.cs
│   │       ├── TenantNotFoundException.cs
│   │       ├── DirectoryQuotaExceededException.cs
│   │       ├── NoFilesAvailableException.cs
│   │       └── InsufficientStorageException.cs
│   ├── Locus.FileSystem/        # Local file system implementation
│   │   ├── LocalFileSystemVolume.cs
│   │   └── FileSystemPathSanitizer.cs
│   ├── Locus.Storage/           # Storage pool and metadata management
│   │   ├── StoragePool.cs
│   │   ├── FileScheduler.cs
│   │   ├── DirectoryQuotaManager.cs
│   │   ├── TenantQuotaManager.cs
│   │   ├── StorageCleanupService.cs
│   │   └── Data/
│   │       ├── FileMetadata.cs
│   │       ├── DirectoryQuota.cs
│   │       ├── MetadataRepository.cs
│   │       └── DirectoryQuotaRepository.cs
│   ├── Locus.MultiTenant/       # Multi-tenant isolation
│   │   ├── TenantManager.cs
│   │   ├── TenantContext.cs
│   │   └── Data/
│   │       └── TenantMetadata.cs
│   └── Locus/                   # Main library (aggregates all components)
│       ├── LocusBuilder.cs
│       └── ServiceCollectionExtensions.cs
├── tests/
│   ├── Locus.FileSystem.Tests/  # 40 tests ✅
│   ├── Locus.Storage.Tests/     # 103 tests ✅
│   ├── Locus.MultiTenant.Tests/ # 11 tests ✅
│   ├── Locus.IntegrationTests/  # 6 tests ✅
│   └── Locus.Benchmarks/        # Performance benchmarks
│       ├── MetadataRepositoryBenchmarks.cs
│       ├── DirectoryQuotaBenchmarks.cs
│       ├── TenantManagerBenchmarks.cs
│       └── ConcurrentOperationsBenchmarks.cs
└── samples/
    └── Locus.Sample.Console/
```

## Test Coverage

**All tests passing: 194/194 ✅**

- ✅ FileSystem.Tests: 50 tests
- ✅ Storage.Tests: 127 tests
- ✅ MultiTenant.Tests: 11 tests
- ✅ IntegrationTests: 6 tests

## Implementation Status

### ✅ Completed (Phases 1-6)

**Core Infrastructure:**
- ✅ Solution and project structure
- ✅ All core interfaces (IStoragePool, IFileScheduler, ITenantManager, etc.)
- ✅ All models and exceptions
- ✅ Central package management (Directory.Packages.props)
- ✅ Zero build warnings or errors

**Multi-Tenant Management (Phase 2):**
- ✅ TenantManager with JSON-based metadata
- ✅ Per-tenant isolation with auto-creation support
- ✅ 5-minute cache with status checking
- ✅ Enable/Disable/Suspend tenant controls

**Storage Volumes (Phase 3):**
- ✅ LocalFileSystemVolume implementation
- ✅ Path sanitizer for security
- ✅ Health checks and capacity monitoring
- ✅ Cross-platform path handling

**Directory Quota Management (Phase 4):**
- ✅ DirectoryQuotaRepository with LiteDB
- ✅ Atomic increment/decrement with SemaphoreSlim
- ✅ Per-directory file count limits
- ✅ Concurrent-safe operations

**File Scheduler (Phase 5):**
- ✅ FileScheduler with queue-based processing
- ✅ Concurrent file allocation (no duplicates)
- ✅ Retry mechanism with exponential backoff
- ✅ Status tracking (Pending → Processing → Completed/Failed/PermanentlyFailed)

**Storage Pool (Phase 6):**
- ✅ StoragePool with volume management
- ✅ MetadataRepository with per-tenant LiteDB
- ✅ Active-data caching strategy
- ✅ Automatic volume selection
- ✅ TenantQuotaManager integration

**Testing:**
- ✅ 160 unit tests (100% passing)
- ✅ Integration tests
- ✅ Performance benchmarks

### 🚧 In Progress (Phases 7-8)

- ⏳ StorageCleanupService (background cleanup)
- ⏳ BackgroundCleanupService (scheduled tasks)
- ⏳ Configuration and DI setup (LocusBuilder)

### 📋 Planned (Phases 9-10)

- Sample applications
- ~~NuGet packaging~~ ✅ Completed - Single consolidated package
- Documentation and guides

## Build Commands

```bash
# Build the solution
dotnet build

# Build in Release mode
dotnet build -c Release

# Run tests
dotnet test

# Pack NuGet package (includes all components)
dotnet pack src/Locus/Locus.csproj -c Release
```

## Documentation

### Core Documentation
- **[CLAUDE.md](CLAUDE.md)** - Complete implementation guidelines, architecture decisions, API references, and FileWatcher usage guide

### Sample Projects
- **[Locus.Sample.Console](samples/Locus.Sample.Console/)** - Complete working example with appsettings.json configuration
- **[Locus.Sample.StressTest](samples/Locus.Sample.StressTest/)** - Multi-threaded stress test with FileWatcher integration

### Key Features

📦 **Multi-Tenant Storage**
- Tenant isolation with enable/disable controls
- Per-tenant LiteDB databases for metadata
- Active-data caching for high concurrency
- **File extension preservation** - Original file extensions are preserved in physical storage

🔄 **File Queue Processing**
- System-generated file keys
- Automatic retry on failure with exponential backoff
- Processing status tracking (Pending → Processing → Completed/Failed)

📁 **FileWatcher Auto-Import**
- Multi-tenant mode with automatic directory creation
- Configurable polling intervals and concurrency
- Post-import actions (Delete/Move/Keep)

🧹 **Automatic Cleanup**
- Empty directory cleanup
- Timeout detection and reset
- Orphaned file cleanup
- Failed file retention policies

🔧 **Storage Management**
- Dynamic volume mounting/unmounting
- Automatic volume expansion
- Load balancing across volumes
- Directory-level quota control

## License

MIT

## Contributing

See CLAUDE.md for detailed implementation guidelines and architecture decisions.
# CI/CD 快速配置指南

本文档帮助你快速配置 Locus 项目的 CI/CD 流程。

## 前置条件

- GitHub 账号和仓库
- NuGet.org 账号 (用于发布包)
- Git 已安装并配置

## 配置步骤

### 1. 更新项目元数据

编辑 `src/Directory.Build.props` 文件，替换以下占位符：

```xml
<Authors>Your Name or Organization</Authors>          <!-- 替换为你的名字或组织名 -->
<Company>Your Company</Company>                        <!-- 替换为你的公司名 -->
<PackageProjectUrl>https://github.com/yourusername/Locus</PackageProjectUrl>  <!-- 替换为你的仓库 URL -->
<RepositoryUrl>https://github.com/yourusername/Locus</RepositoryUrl>          <!-- 替换为你的仓库 URL -->
```

### 2. 更新 README 徽章

编辑 `README.md` 文件顶部的徽章 URL：

```markdown
[![CI/CD](https://github.com/yourusername/Locus/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/yourusername/Locus/actions/workflows/ci-cd.yml)
```

将 `yourusername` 替换为你的 GitHub 用户名或组织名。

### 3. 配置 NuGet API Key

#### 3.1 获取 NuGet API Key

1. 访问 https://www.nuget.org/
2. 登录你的账号
3. 点击右上角用户名 → **API Keys**
4. 点击 **Create**
5. 配置:
   - Key Name: `Locus GitHub Actions`
   - Select Scopes: 选择 **Push new packages and package versions**
   - Select Packages: 选择 **Glob Pattern**，输入 `Locus.*`
   - Expiration: 设置合理的过期时间 (建议 1 年)
6. 点击 **Create**
7. **立即复制生成的 API Key** (之后无法再查看)

#### 3.2 添加 GitHub Secret

1. 打开你的 GitHub 仓库
2. 进入 **Settings** → **Secrets and variables** → **Actions**
3. 点击 **New repository secret**
4. 配置:
   - Name: `NUGET_API_KEY`
   - Secret: 粘贴你复制的 NuGet API Key
5. 点击 **Add secret**

### 4. 推送代码到 GitHub

```bash
# 添加所有文件
git add .

# 提交更改
git commit -m "feat: Add CI/CD configuration"

# 推送到 GitHub (假设远程名为 origin)
git push origin main
# 或者
git push origin master
```

### 5. 验证 CI 构建

1. 打开 GitHub 仓库
2. 点击 **Actions** 标签
3. 你应该看到 "CI/CD Pipeline" workflow 正在运行
4. 点击查看详细日志
5. 确保 "build-and-test" job 成功完成

## 发布第一个版本

### 步骤 1: 确保代码稳定

```bash
# 本地运行测试
dotnet test

# 确保所有测试通过
```

### 步骤 2: 创建版本标签

```bash
# 创建 v1.0.0 标签
git tag v1.0.0

# 推送标签到远程
git push origin v1.0.0
```

### 步骤 3: 监控发布流程

1. 打开 **Actions** 标签
2. 你会看到两个 job:
   - **build-and-test**: 构建和测试
   - **pack-and-publish**: 打包和发布 (仅在 tag 推送时)
3. 等待两个 job 都完成 (大约 5-10 分钟)

### 步骤 4: 验证发布结果

#### 检查 GitHub Release
1. 打开 **Releases** 标签
2. 你应该看到 "Release v1.0.0"
3. 包含:
   - 完整的 Changelog
   - 所有 NuGet 包文件

#### 检查 NuGet 包
1. 访问 https://www.nuget.org/profiles/[你的用户名]
2. 确认 **Locus** 包已发布 (包含所有依赖组件)

## 后续版本发布

### 选择版本号

使用语义化版本 (Semantic Versioning):

- **Patch (修订)**: `v1.0.1`, `v1.0.2`, etc.
  - Bug 修复
  - 性能改进
  - 文档更新

- **Minor (次版本)**: `v1.1.0`, `v1.2.0`, etc.
  - 新功能添加
  - 向后兼容的 API 更改

- **Major (主版本)**: `v2.0.0`, `v3.0.0`, etc.
  - 破坏性更改
  - 不向后兼容的 API 更改

### 发布流程

```bash
# 1. 确保在最新的 main/master 分支
git checkout main
git pull

# 2. 确保所有测试通过
dotnet test

# 3. 创建并推送新标签
git tag v1.1.0
git push origin v1.1.0

# 4. 等待 CI/CD 自动完成
```

## 常见问题

### Q: CI 构建失败了怎么办?

**A**: 检查错误日志:
1. 进入 Actions 标签
2. 点击失败的 workflow
3. 展开失败的步骤查看详细日志
4. 根据错误信息修复问题
5. 推送修复代码，CI 会自动重新运行

### Q: NuGet 发布失败?

**A**: 检查以下几点:
- NUGET_API_KEY secret 是否正确配置
- API Key 是否有 Push 权限
- 包名是否与现有包冲突
- 版本号是否已存在

### Q: 如何跳过 NuGet 发布只创建 Release?

**A**: 两种方法:
1. 删除 NUGET_API_KEY secret (workflow 会跳过发布步骤)
2. 修改 workflow 文件，注释掉发布步骤

### Q: 如何修改已发布的版本?

**A**: NuGet 包一旦发布无法修改，只能:
1. 取消列出 (unlist) 旧版本
2. 发布新版本 (增加版本号)

### Q: Changelog 不准确怎么办?

**A**: Changelog 从 Git 提交历史自动生成:
1. 确保提交信息清晰明确
2. 使用约定的格式 (feat:, fix:, docs:, etc.)
3. 如需自定义，可编辑 Release 描述

## 高级配置

### 配置分支保护规则

保护 main/master 分支，防止直接推送:

1. 进入 **Settings** → **Branches**
2. 点击 **Add rule**
3. 配置:
   - Branch name pattern: `main` (或 `master`)
   - ✅ Require status checks to pass before merging
   - ✅ Require branches to be up to date before merging
   - 选择 **build-and-test** 作为必需检查
   - ✅ Require pull request reviews before merging (推荐)
4. 点击 **Create**

### 自动生成更详细的 Changelog

可以使用第三方工具如 `conventional-changelog` 或 `release-drafter`:

```yaml
# .github/workflows/release-drafter.yml
name: Release Drafter

on:
  push:
    branches:
      - main
      - master

jobs:
  update_release_draft:
    runs-on: ubuntu-latest
    steps:
      - uses: release-drafter/release-drafter@v5
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

### 添加代码覆盖率报告

在 workflow 中添加:

```yaml
- name: Generate coverage report
  run: dotnet test --collect:"XPlat Code Coverage"

- name: Upload coverage to Codecov
  uses: codecov/codecov-action@v3
```

## 总结

完成以上配置后，你的 CI/CD 流程已经就绪:

✅ 每次推送到 main/master 都会自动构建和测试
✅ 每个 Pull Request 都会自动运行测试
✅ 推送 tag 会自动打包并发布到 NuGet
✅ 自动创建 GitHub Release 包含完整 Changelog

开始享受自动化带来的便利吧！🚀
