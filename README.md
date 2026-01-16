# Locus - Multi-Tenant File Storage Pool System

A high-performance, queue-based file storage pool system for .NET targeting netstandard2.0.

## Overview

Locus is designed as a **file queue system** that provides:
- **Multi-tenant isolation** - Each tenant has isolated storage space with enable/disable controls
- **Queue-based processing** - Files are processed as a queue with automatic retry on failure
- **Unlimited storage expansion** - Dynamically mount multiple storage volumes
- **High concurrency** - Thread-safe operations with in-memory caching and file-based persistence
- **Automatic management** - System handles directory structure, file placement, and cleanup

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
// User writes a file
string fileKey = await storagePool.WriteFileAsync(tenant, fileStream, ct);
// Returns: "f7b3c9d2-4a1e-4f8b-9c3d-2e1a4b5c6d7e"

// Later: retrieve file metadata
var location = await storagePool.GetFileLocationAsync(tenant, fileKey, ct);

// Or: read file content
using var stream = await storagePool.ReadFileAsync(tenant, fileKey, ct);
```

## Architecture

```
┌─────────────────────────────────────────────┐
│   API Layer (IStoragePool, IFileScheduler)  │
├─────────────────────────────────────────────┤
│   In-Memory Metadata Store                  │
│   - ConcurrentDictionary for fast lookups   │
│   - ConcurrentQueue for pending files       │
├─────────────────────────────────────────────┤
│   Persistence Layer                         │
│   - WAL (Write-Ahead Log)                   │
│   - Periodic Snapshots                      │
├─────────────────────────────────────────────┤
│   Storage Volumes                           │
│   - Local File System                       │
│   - Network Drives                          │
│   - Extensible to Cloud Storage             │
└─────────────────────────────────────────────┘
```

## Core APIs

### IStoragePool - File Storage Operations

```csharp
public interface IStoragePool
{
    // Write file → returns system-generated fileKey
    Task<string> WriteFileAsync(ITenantContext tenant, Stream content, CancellationToken ct);

    // Read file by fileKey
    Task<Stream> ReadFileAsync(ITenantContext tenant, string fileKey, CancellationToken ct);

    // Get file metadata
    Task<FileLocation?> GetFileLocationAsync(ITenantContext tenant, string fileKey, CancellationToken ct);

    // Volume management
    Task MountVolumeAsync(IStorageVolume volume, CancellationToken ct);
    Task UnmountVolumeAsync(string volumeId, CancellationToken ct);
    Task<IEnumerable<IStorageVolume>> GetVolumesAsync(CancellationToken ct);
}
```

### IFileScheduler - Queue-Based Processing

```csharp
public interface IFileScheduler
{
    // Get next pending file (thread-safe, no duplicates)
    Task<FileLocation> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct);

    // Mark file as completed → deletes file
    Task MarkAsCompletedAndDeleteAsync(string fileKey, CancellationToken ct);

    // Mark file as failed → returns to queue for retry
    Task MarkAsFailedAsync(string fileKey, string errorMessage, CancellationToken ct);

    // Get current file status
    Task<FileProcessingStatus> GetFileStatusAsync(string fileKey, CancellationToken ct);
}
```

### ITenantManager - Multi-Tenant Management

```csharp
public interface ITenantManager
{
    Task CreateTenantAsync(string tenantId, CancellationToken ct);
    Task EnableTenantAsync(string tenantId, CancellationToken ct);
    Task DisableTenantAsync(string tenantId, CancellationToken ct);
    Task<ITenantContext> GetTenantAsync(string tenantId, CancellationToken ct);
    Task<bool> IsTenantEnabledAsync(string tenantId, CancellationToken ct);
}
```

## Usage Example

### Basic File Queue Processing

```csharp
// Producer: Write files to the queue
var fileKey1 = await storagePool.WriteFileAsync(tenant, stream1, ct);
var fileKey2 = await storagePool.WriteFileAsync(tenant, stream2, ct);

// Consumer: Process files from the queue
var tasks = Enumerable.Range(0, 10).Select(async threadId =>
{
    while (true)
    {
        // Get next file (thread-safe)
        var file = await fileScheduler.GetNextFileForProcessingAsync(tenant, ct);
        if (file == null) break;

        try
        {
            // Read and process file
            using var stream = await storagePool.ReadFileAsync(tenant, file.FileKey, ct);
            await ProcessFileAsync(stream);

            // Success: delete file
            await fileScheduler.MarkAsCompletedAndDeleteAsync(file.FileKey, ct);
        }
        catch (Exception ex)
        {
            // Failure: return to queue for retry
            await fileScheduler.MarkAsFailedAsync(file.FileKey, ex.Message, ct);
        }
    }
});

await Task.WhenAll(tasks);
```

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
│   │   │   └── IMetadataStore.cs
│   │   ├── Models/
│   │   │   ├── FileLocation.cs
│   │   │   ├── FileProcessingStatus.cs
│   │   │   ├── TenantStatus.cs
│   │   │   ├── FileRetryPolicy.cs
│   │   │   └── CleanupStatistics.cs
│   │   └── Exceptions/
│   │       ├── TenantDisabledException.cs
│   │       ├── DirectoryQuotaExceededException.cs
│   │       └── NoFilesAvailableException.cs
│   ├── Locus.FileSystem/        # Local file system implementation
│   ├── Locus.Storage/           # Storage pool and metadata management
│   ├── Locus.MultiTenant/       # Multi-tenant isolation
│   └── Locus/                   # Main library (aggregates all components)
├── tests/
│   ├── Locus.Core.Tests/
│   ├── Locus.Storage.Tests/
│   ├── Locus.MultiTenant.Tests/
│   └── Locus.IntegrationTests/
└── samples/
    └── Locus.Sample.Console/
```

## Phase 1 Completion Status ✅

**Completed:**
- ✅ Solution and project structure created
- ✅ All core interfaces defined (IStoragePool, IFileScheduler, ITenantManager, etc.)
- ✅ All models and enums defined (FileLocation, FileProcessingStatus, etc.)
- ✅ All custom exceptions defined
- ✅ Central package management configured (Directory.Packages.props)
- ✅ Common build properties configured (Directory.Build.props)
- ✅ Zero build warnings or errors
- ✅ Metadata store abstraction defined (IMetadataStore)

**Design Decisions:**
- ✅ Removed LiteDB dependency - using in-memory + file-based persistence instead
- ✅ WriteFileAsync returns system-generated fileKey (not user-provided)
- ✅ Directory structure is completely internal (users never specify paths)
- ✅ Queue-based processing model confirmed

## Next Steps (Phase 2+)

- Implement TenantManager with file-based tenant metadata
- Implement LocalFileSystemVolume
- Implement InMemoryMetadataStore with WAL and snapshot support
- Implement StoragePool with automatic volume selection
- Implement FileScheduler with concurrent queue processing
- Implement DirectoryQuotaManager
- Implement StorageCleanupService
- Add comprehensive unit and integration tests

## Build Commands

```bash
# Build the solution
dotnet build

# Build in Release mode
dotnet build -c Release

# Run tests
dotnet test

# Pack NuGet packages
dotnet pack -c Release
```

## License

MIT

## Contributing

See CLAUDE.md for detailed implementation guidelines and architecture decisions.
