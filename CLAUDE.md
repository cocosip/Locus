# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build
dotnet build

# Run all tests
dotnet test

# Run a single test project
dotnet test tests/Locus.Storage.Tests

# Run with specific filter
dotnet test --filter "FullyQualifiedName~ClassName"

# Run benchmarks
cd tests/Locus.Benchmarks && dotnet run -c Release
dotnet run -c Release --filter "Locus.Benchmarks.MetadataRepositoryBenchmarks*"
```

## Architecture

The solution targets **netstandard2.0** for maximum compatibility, with central package versioning (`Directory.Packages.props`). All projects share `common.props` for metadata/versioning and use `Nullable=enable`, `LangVersion=latest`.

### Project Layers (dependency direction: top → down)

```
Locus (meta-package, aggregates all)
├── Locus.Storage ──────────────┐
│   (StoragePool, FileScheduler,│
│    MetadataRepository,        │
│    QueueEventProjection,      │
│    Cleanup/Recovery services) │
├── Locus.MultiTenant ──────────┤
│   (TenantManager, JSON-based  │
│    tenant metadata)           │
├── Locus.FileSystem ───────────┤
│   (LocalFileSystemVolume,     │
│    path sanitization)         │
└── Locus.Core
    (All interfaces, models, exceptions — no implementation)
```

- **`Locus.Core`**: Zero dependencies. Defines `IStoragePool`, `IFileScheduler`, `ITenantManager`, `IStorageVolume`, `IDirectoryQuotaManager`, `IStorageCleanupService`, `IQueueEventJournal`, projection interfaces, plus all models and exception types.
- **`Locus.FileSystem`**: Depends only on Core. Implements `IStorageVolume` for local disk storage with path sharding.
- **`Locus.Storage`**: The heavy layer. Contains `StoragePool`, `FileScheduler`, `MetadataRepository` (SQLite via Dapper), `DirectoryQuotaRepository`, `FileQueueEventJournal`, `QueueEventProjectionService`, timeout recovery, cleanup, orphan recovery, FileWatcher. Depends on Core + FileSystem.
- **`Locus.MultiTenant`**: Depends only on Core. JSON-file tenant metadata with 5-minute in-memory cache.
- **`Locus`**: Meta-package. Contains `LocusBuilder` (fluent config), `ServiceCollectionExtensions` (DI wiring), and `LocusOptions`. Embeds all sub-project DLLs into the published NuGet package.

### DI Wiring

`ServiceCollectionExtensions.AddLocus(LocusOptions)` in `src/Locus/ServiceCollectionExtensions.cs` registers everything. Most services are singletons. Async startup work (`StorageVolumeInitializationService`, `QueueEventProjectionService`, `BackgroundCleanupService`, etc.) uses `IHostedService` / `BackgroundService`.

Configuration can come from code (`LocusBuilder` fluent API), `LocusOptions` directly, or `IConfiguration` binding from the `"Locus"` section.

### Queue Journal & Projection Model

This is the most important architectural pattern to understand:

1. **Physical files** live on `IStorageVolume` implementations (mounted file system paths)
2. **`queue.log`** (per-tenant, binary or JSON) is the durable source of truth for all queue state transitions (`Accepted`, `ProcessingStarted`, `ProcessingCompleted`, `ProcessingFailed`, etc.)
3. **SQLite databases** (`metadata.db`, `quotas.db` per tenant) store **projections** — queryable current state rebuilt from the journal. They are rebuildable, not the primary durability layer.
4. **In-memory caches** (`ConcurrentDictionary`-based) hold active-file metadata for hot-path reads. On first tenant access or restart, the cache warms from SQLite projections.

Key classes:
- `FileQueueEventJournal` — append-only binary journal (`IQueueEventJournal`)
- `QueueEventProjectionService` — reads journal entries, updates SQLite projections + in-memory caches (implements `IHostedService`)
- `MetadataRepository` — SQLite access via Dapper, write-behind persistence with configurable batch draining
- `MetadataRepositoryQueueProjectionStore` / `MetadataRepositoryQueueProjectionWriteStore` / `MetadataRepositoryQueueProjectionCleanupStore` — facade adapters so the rest of the system doesn't depend on MetadataRepository internals
- `FileScheduler` — thread-safe queue allocation, lease-based completion/failure, retry with exponential backoff

### Lease Model

`GetNextFileForProcessingAsync` returns a `FileProcessingLease` containing the file key and `ProcessingStartTime`. `MarkAsCompletedAsync` / `MarkAsFailedAsync` require the lease — this acts as an optimistic concurrency check. If the `ProcessingStartTime` doesn't match, the operation is rejected (another worker claimed it, or it timed out and was reset).

### Cleanup & Recovery

- `StorageCleanupService` — main cleanup orchestrator (completed file deletion, permanently failed handling, orphan detection, timeout reset)
- `BackgroundCleanupService` — `IHostedService` wrapper that periodically invokes `StorageCleanupService`
- `OrphanFileRecoveryService` — scans volumes for physical files with no matching metadata
- `ProcessingTimeoutRecoveryService` — resets files stuck in `Processing` beyond the configured timeout
- `DatabaseRecoveryService` — integrity check + automatic rebuild of corrupted SQLite databases on startup
- `DatabaseHealthCheckService` — `IHostedService` that runs recovery at startup (opt-in)

### Directory & Tenant Quotas

Lock-free CAS (compare-and-swap) atomic counters track file counts per directory, backed by `DirectoryQuotaRepository` with write-behind SQLite persistence. `DirectoryQuotaManager` handles directory-level limits; `TenantQuotaManager` handles tenant-level file counts. Both use the projection model — quota state is maintained in memory and persisted asynchronously.

### Key Conventions

- Exceptions defined in `Locus.Core.Exceptions` namespace are the public API error surface
- `IFileSystem` from `System.IO.Abstractions` is used everywhere (never `File`/`Directory` statics directly)
- All async methods accept `CancellationToken`
- Central package versioning with `CentralPackageTransitivePinningEnabled=true`
- Test projects use xUnit + Moq; `Directory.Build.props` in tests disables MSCoverage reference path maps
