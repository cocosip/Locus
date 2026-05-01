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
- Database optimization (LiteDB shrinking to reclaim space from deleted records)

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
- **File extension preservation**: Original file names can be provided to preserve extensions in physical storage
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
- Database optimization (LiteDB space reclamation):
  - LiteDB databases grow continuously and don't shrink automatically when records are deleted
  - Deleted records leave "dead space" that's marked as reusable but doesn't reduce file size
  - Use LiteDB's Rebuild() method to compact databases and reclaim space
  - Run periodically (e.g., weekly) during low-activity periods
  - Independent optimization interval from regular cleanup tasks
  - Track space reclaimed and optimization statistics
- Use configurable retention policies for each cleanup type

### 配置调整
- 如果 LocusOptions 或者他引用的任何子 Options 发生变更后，项目中的配置  *.appsettings.json 都要跟着调整，同时需要更新 appsettings-sample-reference.md 配置说明文档