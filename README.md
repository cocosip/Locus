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
string fileKey = await storagePool.WriteFileAsync(tenant, fileStream, ct);
// Returns: "f7b3c9d2-4a1e-4f8b-9c3d-2e1a4b5c6d7e"

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
    Task<string> WriteFileAsync(ITenantContext tenant, Stream content, CancellationToken ct);

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
var fileKey1 = await storagePool.WriteFileAsync(tenant, stream1, ct);
var fileKey2 = await storagePool.WriteFileAsync(tenant, stream2, ct);
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

Performance benchmarks run on Intel Core Ultra 9 185H, .NET 10.0.1:

#### Metadata Operations
| Operation | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| AddOrUpdate single file | 205.6 μs | 90 KB | LiteDB write + cache update |
| Get file metadata (cache hit) | 33.6 μs | 19.6 KB | ⚡ Pure memory read |
| Get file metadata (cache miss) | 14.5 ms | 4.15 MB | LiteDB read + cache load |
| Batch insert 100 files | 24.2 ms | 9.46 MB | ~242 μs per file |
| Get pending files (10) | 3.6 ms | 1.15 MB | Queue retrieval |

#### Directory Quota Operations
| Operation | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| Check can add (no limit) | 131 μs | 68.3 KB | Fast path |
| Check can add (with limit) | 169 μs | 93.7 KB | Includes limit check |
| Increment file count | 194 μs | 94.6 KB | SemaphoreSlim + LiteDB |
| Decrement file count | 270 μs | 115 KB | Atomic decrement |
| Get file count | 35.8 μs | 13.1 KB | ⚡ Cache read |

#### Tenant Management
| Operation | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| Create tenant | 583 μs | 7.2 KB | JSON write + directory creation |
| Get tenant (cache hit) | 52 ns | 104 B | ⚡ Extremely fast |
| Get tenant (cache miss) | 34.4 μs | 1.3 KB | JSON read + parse |
| Get tenant (auto-create) | 952 μs | 9.6 KB | Create + load |
| Check tenant enabled | 54.5 ns | 104 B | ⚡ Cache hit |
| Enable/Disable tenant | ~1.3-1.4 ms | ~15-22 KB | JSON update |

#### Concurrent Operations
| Operation | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| 10 concurrent reads | 10.4 ms | 1.08 MB | Parallel file reads |
| Mixed 10W + 10R | 12.8 ms | 1.58 MB | Concurrent read/write |

**Key Findings**:
- ⚡ **Cache hit rates** are critical: 33.6 μs vs 14.5 ms (432x faster)
- ⚡ **Tenant lookups** are extremely fast with cache: 52 ns
- ✅ **Write operations** are performant: ~200 μs per file
- ✅ **Concurrent operations** scale well with thread pool

### Running Benchmarks

```bash
cd tests/Locus.Benchmarks
dotnet run -c Release

# Run specific benchmarks
dotnet run -c Release --filter "*MetadataRepository*"
dotnet run -c Release --filter "*DirectoryQuota*"
dotnet run -c Release --filter "*TenantManager*"
dotnet run -c Release --filter "*ConcurrentOperations*"
```

**Note**: Concurrent write benchmarks may fail in BenchmarkDotNet due to LiteDB file locking when separate processes try to access the same database. This is a limitation of the benchmark environment, not the actual system which handles concurrent writes correctly in production.

See [Benchmark README](tests/Locus.Benchmarks/README.md) for detailed analysis and optimization tips.

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

**All tests passing: 160/160 ✅**

- ✅ FileSystem.Tests: 40 tests
- ✅ Storage.Tests: 103 tests
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

## License

MIT

## Contributing

See CLAUDE.md for detailed implementation guidelines and architecture decisions.
