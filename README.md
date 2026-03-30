# Locus - Multi-Tenant File Storage Pool System

[![CI/CD](https://github.com/cocosip/Locus/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/cocosip/Locus/actions/workflows/ci-cd.yml)
[![NuGet](https://img.shields.io/nuget/v/Locus.svg)](https://www.nuget.org/packages/Locus/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A high-performance, multi-tenant file storage pool system for .NET targeting netstandard2.0 with SQLite-based metadata management.

## Overview

Locus is designed as a **file queue system** that provides:
- **Multi-tenant isolation** - Each tenant has isolated storage space with enable/disable controls
- **Queue-based processing** - Files are processed as a queue with automatic retry on failure
- **Unlimited storage expansion** - Dynamically mount multiple storage volumes
- **High concurrency** - Thread-safe operations with per-tenant SQLite databases and active-data caching
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
│   Persistence Layer (Per-Tenant SQLite)    │
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
- **Per-Tenant SQLite**: Each tenant has an isolated subdirectory with `metadata.db` and `quotas.db`
- **Active-Data Caching**: Only cache files in Pending/Processing/Failed states
- **Completed Files**: Automatically removed from cache and database after processing
- **Atomic Quota Operations**: Lock-free CAS counters + Write-Behind timer ensure concurrency safety
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

    // Mark file as completed → deletes file and metadata (requires lease token)
    Task MarkAsCompletedAsync(string fileKey, DateTime expectedProcessingStartTimeUtc, CancellationToken ct);

    // Mark file as failed → returns to queue for retry (requires lease token)
    Task MarkAsFailedAsync(string fileKey, DateTime expectedProcessingStartTimeUtc, string errorMessage, CancellationToken ct);

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
            var expectedProcessingStartTimeUtc = file.ProcessingStartTime
                ?? throw new InvalidOperationException("Missing processing start time.");
            await storagePool.MarkAsCompletedAsync(file.FileKey, expectedProcessingStartTimeUtc, ct);
        }
        catch (Exception ex)
        {
            // Failure: return to queue for retry
            var expectedProcessingStartTimeUtc = file.ProcessingStartTime
                ?? throw new InvalidOperationException("Missing processing start time.");
            await storagePool.MarkAsFailedAsync(file.FileKey, expectedProcessingStartTimeUtc, ex.Message, ct);
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
            var expectedProcessingStartTimeUtc = file.ProcessingStartTime
                ?? throw new InvalidOperationException("Missing processing start time.");
            await storagePool.MarkAsCompletedAsync(file.FileKey, expectedProcessingStartTimeUtc, token);
        }
        catch (Exception ex)
        {
            var expectedProcessingStartTimeUtc = file.ProcessingStartTime
                ?? throw new InvalidOperationException("Missing processing start time.");
            await storagePool.MarkAsFailedAsync(file.FileKey, expectedProcessingStartTimeUtc, ex.Message, token);
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
| AddOrUpdate single file | 1.878 μs | 2.4 KB | ⚡ Memory-first, async SQLite persistence |
| Get file metadata (cache hit) | 40.91 ns | 72 B | ⚡ ConcurrentDictionary lookup |
| Get non-existent file (returns null) | 34.87 ns | 0 B | Cache miss → null, no SQLite fallback |
| Batch insert 100 files | 237.9 μs | 63.4 KB | ~2.4 μs per file |
| Get next pending file (100-file pool) | 2.975 μs | 1.4 KB | ⚡ O(n_pending) scan via `_pendingKeys` |

#### Directory Quota Operations (Lock-Free CAS)

| Operation | Mean Time | Allocated | Notes |
|-----------|-----------|-----------|-------|
| Check can add (no limit) | 141.25 ns | 176 B | ⚡ AtomicQuotaState hot path |
| Check can add (with limit) | 139.07 ns | 176 B | ⚡ Lock-free CAS comparison |
| Increment file count | 94.80 ns | 72 B | ⚡ Lock-free CAS, near-zero contention |
| Decrement file count | 231.05 ns | 248 B | Increment + decrement pair |
| Set directory limit | 2.916 ms | 142.2 KB | SQLite write (persisted immediately) |
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

Last updated: 2026-02-28 (BenchmarkDotNet v0.15.8, .NET 10.0.0, IterationCount=10, WarmupCount=3)

| Operation | threadCount | Mean Time | Allocated | Notes |
|-----------|-------------|-----------|-----------|-------|
| Core path: 100 concurrent writes (no Task.Run) | — | 77.599 ms | 26243.21 KB | Core path, no Task.Run |
| 10 concurrent reads (pure read) | — | 0.653 ms | 104.10 KB | Read-only, no setup |
| Core path: 10 concurrent reads (no Task.Run) | — | 2.128 ms | 104.43 KB | Core path, no Task.Run |
| 10 concurrent reads (write+read) | — | 8.802 ms | 3132.49 KB | Includes pre-write setup + parallel reads |
| Mixed read/write (20 ops) | — | 7.840 ms | 4417.77 KB | 10 writes + 10 reads concurrent |
| Concurrent writes | 10 | 3.149 ms | 2158.13 KB | 10 simultaneous writes |
| Concurrent writes | 50 | 13.551 ms | 10720.19 KB | 50 simultaneous writes |
| Concurrent writes | 100 | 26.945 ms | 22256.45 KB | 100 simultaneous writes |

`ConcurrentReads_PureRead` benchmark is now included to report read-only throughput separately from setup cost.

**Key Findings**:
- ⚡ **Directory quota CAS**: 94.80 ns per increment — lock-free atomic operations (vs. ~200 μs with SemaphoreSlim)
- ⚡ **Volume health/space**: 17–22 ns — 30-second TTL cache eliminates one disk I/O per write
- ⚡ **Metadata write-behind**: 1.878 μs per file — memory-first, SQLite persistence is async
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
│   ├── Locus.FileSystem.Tests/  # 51 tests ✅
│   ├── Locus.Storage.Tests/     # 179 tests ✅
│   ├── Locus.MultiTenant.Tests/ # 12 tests ✅
│   ├── Locus.IntegrationTests/  # 10 tests ✅
│   └── Locus.Benchmarks/        # Performance benchmarks
│       ├── MetadataRepositoryBenchmarks.cs
│       ├── DirectoryQuotaBenchmarks.cs
│       ├── TenantManagerBenchmarks.cs
│       └── ConcurrentOperationsBenchmarks.cs
└── samples/
    └── Locus.Sample.Console/
```

## Test Coverage

**All tests passing: 252/252 ✅**

- ✅ FileSystem.Tests: 51 tests
- ✅ Storage.Tests: 179 tests
- ✅ MultiTenant.Tests: 12 tests
- ✅ IntegrationTests: 10 tests

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
- ✅ DirectoryQuotaRepository with SQLite
- ✅ Lock-free CAS atomic increment/decrement with Write-Behind persistence
- ✅ Per-directory file count limits
- ✅ Concurrent-safe operations

**File Scheduler (Phase 5):**
- ✅ FileScheduler with queue-based processing
- ✅ Concurrent file allocation (no duplicates)
- ✅ Retry mechanism with exponential backoff
- ✅ Status tracking (Pending → Processing → Completed/Failed/PermanentlyFailed)

**Storage Pool (Phase 6):**
- ✅ StoragePool with volume management
- ✅ MetadataRepository with per-tenant SQLite (WAL mode, per-tenant subdirectory)
- ✅ Active-data caching strategy
- ✅ Automatic volume selection
- ✅ TenantQuotaManager integration

**Testing:**
- ✅ 252 unit/integration tests (100% passing)
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
- Per-tenant SQLite databases in isolated subdirectories
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
