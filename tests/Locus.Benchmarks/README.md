# Locus 性能基准测试

本项目使用 [BenchmarkDotNet](https://benchmarkdotnet.org/) 进行性能基准测试。

## 运行基准测试

### 运行所有基准测试

```bash
cd tests/Locus.Benchmarks
dotnet run -c Release
```

### 运行特定的基准测试类

```bash
# 元数据仓库基准测试
dotnet run -c Release --filter "Locus.Benchmarks.MetadataRepositoryBenchmarks*"

# 并发操作基准测试
dotnet run -c Release --filter "Locus.Benchmarks.ConcurrentOperationsBenchmarks*"

# 目录配额基准测试
dotnet run -c Release --filter "Locus.Benchmarks.DirectoryQuotaBenchmarks*"

# 租户管理基准测试
dotnet run -c Release --filter "Locus.Benchmarks.TenantManagerBenchmarks*"

# StoragePool 写入吞吐量基准测试
dotnet run -c Release --filter "Locus.Benchmarks.StoragePoolWriteThroughputBenchmarks*"

# StoragePool 并发写入基准测试
dotnet run -c Release --filter "Locus.Benchmarks.StoragePoolConcurrencyBenchmarks*"

# Volume 健康检查基准测试
dotnet run -c Release --filter "Locus.Benchmarks.VolumeHealthCheckBenchmarks*"

# 待处理文件分配锁竞争（第7点）基准测试
dotnet run -c Release --filter "Locus.Benchmarks.PendingAllocationContentionBenchmarks*"

# 第5/6点基准测试（脏键刷盘 + known-dir 裁剪）
dotnet run -c Release --filter "Locus.Benchmarks.DirectoryQuotaDirtyFlushBenchmarks*"
dotnet run -c Release --filter "Locus.Benchmarks.KnownDirectoryTrimBenchmarks*"

# 第1~4点基准测试（FileWatcher 主路径）
dotnet run -c Release --filter "Locus.Benchmarks.FileWatcherLargeDirectoryBenchmarks*"
dotnet run -c Release --filter "Locus.Benchmarks.FileWatcherConfigurationDispatchBenchmarks*"
dotnet run -c Release --filter "Locus.Benchmarks.FileWatcherMultiTenantStatusBenchmarks*"
dotnet run -c Release --filter "Locus.Benchmarks.FileWatcherPrunePressureBenchmarks*"

# 生成第5/6点参数矩阵对比面板（会自动跑基准并输出 Markdown）
pwsh ./tests/Locus.Benchmarks/build-phase56-matrix-panel.ps1

# 生成第1~4点基准对比面板（会自动跑基准并输出 Markdown）
pwsh ./tests/Locus.Benchmarks/build-phase14-benchmark-panel.ps1
```

> **注意**: BenchmarkDotNet 过滤器使用全限定名 `namespace.typeName.methodName`，不支持 `|` OR 语法。每次只能过滤一个类。

### 运行特定的基准测试方法

```bash
# 只运行并发写入测试
dotnet run -c Release --filter "*ConcurrentWrites*"

# 只运行元数据添加测试
dotnet run -c Release --filter "*AddOrUpdateAsync_Single*"
```

## 基准测试类说明

### 1. MetadataRepositoryBenchmarks
测试文件元数据仓库的性能（Write-Behind + `_pendingKeys` 索引架构）:
- **AddOrUpdate single file metadata**: 单个文件元数据的添加/更新性能（内存优先，O(1)）
- **Get file metadata (cache hit)**: 缓存命中时的元数据查询性能（纯 ConcurrentDictionary 查找）
- **Get non-existent file (returns null)**: 查询不存在的 key 的性能（无 LiteDB fallback，直接返回 null）
- **Batch insert 100 files**: 批量插入100个文件的性能
- **Get next pending file (100-file pool, stable state)**: 从固定100文件池中获取下一个待处理文件（`_pendingKeys` O(n_pending) 扫描）

### 2. DirectoryQuotaBenchmarks
测试目录配额管理的性能:
- **Check can add file (no limit)**: 无配额限制时检查是否可添加文件
- **Check can add file (with limit)**: 有配额限制时检查是否可添加文件
- **Increment file count**: 原子递增文件计数
- **Decrement file count**: 原子递减文件计数
- **Set directory limit**: 设置目录限制
- **Get file count**: 获取文件计数

### 3. TenantManagerBenchmarks
测试租户管理的性能:
- **Create tenant**: 创建租户的性能
- **Get tenant (cache hit)**: 缓存命中时获取租户
- **Get tenant (cache miss)**: 缓存未命中时获取租户
- **Get tenant with auto-create**: 自动创建租户的性能
- **Check tenant enabled**: 检查租户是否启用
- **Enable tenant**: 启用租户
- **Disable tenant**: 禁用租户

### 4. ConcurrentOperationsBenchmarks
测试通过 `StoragePool` 的并发端到端场景:
- **10/50/100 concurrent writes**: 10/50/100个并发写入操作
- **10 concurrent reads (pure read)**: 仅并发读取预置文件（不含预写成本）
- **10 concurrent reads (write+read)**: 每次基准先写入再并发读取（端到端准备+读取）
- **Mixed read/write operations**: 混合读写操作 (10写+10读)

### 5. VolumeHealthCheckBenchmarks
测试 Volume 健康检查与磁盘空间查询（TTL 缓存路径）:
- **IsHealthy**: 缓存命中时的健康检查延迟
- **AvailableSpace**: 缓存命中时的可用空间查询延迟
- **TotalCapacity**: 缓存命中时的总容量查询延迟

### 6. StoragePoolWriteThroughputBenchmarks
测试 `StoragePool.WriteFileAsync` 端到端单线程写入吞吐量:
- **Single-threaded write (100 KB)**: 100 KB 文件顺序写入
- **Single-threaded write (1 MB)**: 1 MB 文件顺序写入
- **Single-threaded write (10 MB)**: 10 MB 文件顺序写入

### 7. StoragePoolConcurrencyBenchmarks
测试 `StoragePool.WriteFileAsync` 并发写入扩展性:
- **1 writer (baseline)**: 单线程 baseline
- **10 concurrent writers**: 10 线程并发写入
- **50 concurrent writers**: 50 线程并发写入
- **100 concurrent writers**: 100 线程并发写入

### 8. PendingAllocationContentionBenchmarks
测试 `MetadataRepository` 在单租户高并发下的待处理文件分配竞争:
- **pending allocation contention (single-file API)**: 并发 `GetNextPendingFileAsync` + 回填 Pending
- **pending allocation contention (batch API)**: 并发 `GetNextPendingBatchAsync` + 回填 Pending

### 9. DirectoryQuotaDirtyFlushBenchmarks
测试 `DirectoryQuotaRepository` 在大目录总量下的“脏键索引”刷盘路径:
- **directory quota flush (indexed sparse dirty set)**: 仅刷脏目录的后台 flush
- 参数矩阵：`TotalDirectories = [20000, 100000]` × `DirtyDirectories = [32, 128, 512]`

### 10. KnownDirectoryTrimBenchmarks
测试 `LocalFileSystemVolume` known-directory 缓存在高 churn 下的裁剪路径:
- **known-directory cache trim under churn**: 高频新增目录触发缓存裁剪
- 参数矩阵：`CacheMaxEntries = [256, 512, 2048]` × `DirectoryAddsPerOperation = [2048, 4096, 8192]`

### 11. Phase56 Matrix Panel（对比面板）
用于把第5/6点矩阵结果汇总成一个可对比的 Markdown 面板：
- 输出文件：`BenchmarkDotNet.Artifacts/results/Locus.Benchmarks.Phase56-Matrix-Panel.md`
- 源数据：`Locus.Benchmarks.DirectoryQuotaDirtyFlushBenchmarks-report.csv` + `Locus.Benchmarks.KnownDirectoryTrimBenchmarks-report.csv`
- 明细表包含 `RatioVsBaseline`（相对基线倍率，x）

### 12. FileWatcherLargeDirectoryBenchmarks
测试第1点（流式扫描 + 小队列）在大目录下的扫描性能:
- **point1 large-directory streamed scan**: 单租户大目录扫描与导入
- 参数矩阵：`FileCount = [2000, 10000]` × `MaxConcurrentImports = [1, 8]`

### 13. FileWatcherConfigurationDispatchBenchmarks
测试第2点（按配置对象扫描）调度路径:
- **point2 scan dispatch by watcher id (forced config load)**: 强制按 watcherId 重载配置
- **point2 scan dispatch by configuration snapshot**: 直接按配置对象扫描

### 14. FileWatcherMultiTenantStatusBenchmarks
测试第3点（复用 GetTenantAsync 状态）在多租户扫描下的表现:
- **point3 multi-tenant scan with status reuse**
- 参数矩阵：`TenantDirectoryCount = [40, 160]` × `DisabledRatioPercent = [0, 50]`

### 15. FileWatcherPrunePressureBenchmarks
测试第4点（prune 脱离热路径）在历史压力下的扫描表现:
- **point4 scan under imported-history prune pressure**
- 参数矩阵：`ExistingImportedEntries = [60000, 120000]` × `NewFiles = [256, 1024]`

### 16. Phase14 Benchmark Panel（对比面板）
用于把第1~4点基准结果汇总成一个可对比的 Markdown 面板：
- 输出文件：`BenchmarkDotNet.Artifacts/results/Locus.Benchmarks.Phase14-Benchmark-Panel.md`
- 明细表包含 `RatioVsBaseline`（相对基线倍率，x）

## 性能指标说明

BenchmarkDotNet 会输出以下关键指标:

- **Mean**: 平均执行时间
- **Error**: 误差范围 (99.9% 置信区间的一半)
- **StdDev**: 标准差
- **Median**: 中位数执行时间
- **Gen0/Gen1/Gen2**: GC 回收次数 (每1000次操作)
- **Allocated**: 每次操作分配的内存

## 实际基准测试结果

> 测试环境: Intel Core i5-9400 CPU 2.90GHz (Coffee Lake), 6 cores, .NET 10.0.0, Windows 11 24H2

### StoragePool 写入吞吐量（单线程）

| FileSize | Mean | StdDev | Allocated |
|----------|------|--------|-----------|
| 100 KB | 1.074 ms | 0.086 ms | 7.34 KB |
| 1 MB | 1.296 ms | 0.023 ms | 7.34 KB |
| 10 MB | 4.736 ms | 0.400 ms | 12.55 KB |

### StoragePool 并发写入扩展性（1 MB/文件，iterationCount=10）

| Concurrency | Mean | StdDev | Allocated |
|-------------|------|--------|-----------|
| 1 writer (baseline) | 2.839 ms | 0.350 ms | 77.04 KB |
| 10 concurrent writers | 26.368 ms | 6.849 ms | 273.52 KB |
| 50 concurrent writers | 113.463 ms | 10.608 ms | 701.02 KB |
| 100 concurrent writers | 228.952 ms | 11.859 ms | 1163.76 KB |

### 端到端并发场景（iterationCount=10）

| Method | threadCount | Mean | StdDev | Allocated |
|--------|-------------|------|--------|-----------|
| 10 concurrent reads (write+read) | — | 14.413 ms | 4.913 ms | 1520.69 KB |
| Mixed read/write (20 ops) | — | 8.312 ms | 0.469 ms | 1157.40 KB |
| Concurrent writes | 10 | 3.076 ms | 0.174 ms | 380.72 KB |
| Concurrent writes | 50 | 14.249 ms | 1.270 ms | 1644.43 KB |
| Concurrent writes | 100 | 25.319 ms | 1.970 ms | 2781.31 KB |

> `ConcurrentReads_PureRead` 已新增，用于单独测量纯读取延迟；请运行最新基准获取独立数值。

### 目录配额操作（Lock-Free CAS）

| Method | Mean | StdDev | Allocated |
|--------|------|--------|-----------|
| Check can add file (no limit) | 141.25 ns | 2.966 ns | 176 B |
| Check can add file (with limit) | 139.07 ns | 1.272 ns | 176 B |
| **Increment file count** | **94.80 ns** | **0.906 ns** | **72 B** |
| Decrement file count | 231.05 ns | 0.968 ns | 248 B |
| Set directory limit | 2.916 ms | 11.215 μs | 142.2 KB |
| Get file count | 145.41 ns | 0.194 ns | 232 B |

### Volume 健康检查（30s TTL 缓存）

| Method | Mean | StdDev | Allocated |
|--------|------|--------|-----------|
| IsHealthy (cached) | 17.88 ns | 0.065 ns | 0 B |
| AvailableSpace (cached) | 22.33 ns | 0.029 ns | 0 B |
| TotalCapacity (cached) | 22.32 ns | 0.022 ns | 0 B |

### 元数据操作（Write-Behind + `_pendingKeys` 索引）

| Method | Mean | StdDev | Allocated |
|--------|------|--------|-----------|
| AddOrUpdate single file | 1.878 μs | 121.9 ns | 2.4 KB |
| Get file (cache hit) | 40.91 ns | 0.062 ns | 72 B |
| Get non-existent file (returns null) | 34.87 ns | 0.017 ns | 0 B |
| Batch insert 100 files | 237.9 μs | 17.73 μs | 63.4 KB |
| Get next pending file (100-file pool) | 2.975 μs | 316.1 ns | 1.4 KB |

### 租户管理（5分钟缓存）

| Method | Mean | StdDev | Allocated |
|--------|------|--------|-----------|
| Create tenant | 2.589 ms | 1.854 ms | 7.0 KB |
| Get tenant (cache hit) | 81.95 ns | 0.230 ns | 104 B |
| Get tenant (cache miss) | 24.23 μs | 105.8 ns | 1.28 KB |
| Get tenant (auto-create) | 2.116 ms | 1.177 ms | 9.3 KB |
| Check tenant enabled (cache hit) | 89.86 ns | 2.417 ns | 104 B |
| Enable tenant | 3.060 ms | 1.267 ms | 21.1 KB |
| Disable tenant | 1.864 ms | 274.2 μs | 14.3 KB |

### 关键结论

- ⚡ **目录配额 CAS 无锁**: 94.80 ns 每次计数递增（vs 旧版 SemaphoreSlim + LiteDB 同步写入 ~200 μs，提升约 **2000x**）
- ⚡ **Volume 健康/空间缓存**: 17–22 ns，消除每次写入的额外磁盘 I/O（旧版每次写入触发临时文件创建+删除）
- ⚡ **元数据缓存命中**: 40.91 ns（纯 ConcurrentDictionary 查找，旧测量值 317.7 ns 因错误地把 AddOrUpdateAsync 计入测量范围而虚高）
- ⚡ **元数据 Write-Behind**: 1.878 μs 每文件（内存优先，LiteDB 后台异步落盘）
- ⚡ **`_pendingKeys` 索引**: GetNextPendingFileAsync 从 O(n_active) 全量扫描降为 O(n_pending) 线性扫描，旧测量值 5.886 ms 因状态累积严重失真，实际为 2.975 μs（100 文件池）
- ⚡ **租户缓存**: 82–90 ns（5分钟缓存，远低于旧版 JSON 文件读取 ~24 μs）
- ✅ **100 KB 文件写入**: ~1.1 ms 端到端（配额检查 + 磁盘写入 + 元数据）
- ✅ **100 并发写入（1 MB/文件）**: 229 ms，StdDev 5.2%（iterationCount=10，统计更可靠）

> **注意**: 并发写入 StdDev 受系统调度抖动影响。实际性能取决于硬件配置（CPU、磁盘类型: HDD vs SSD）。
> `ConcurrentOperationsBenchmarks` 的 "10 concurrent reads" StdDev 较大（4.9 ms），
> 原因是读取场景涉及磁盘 I/O，受操作系统缓存和调度影响较大；绝对误差仍在可接受范围。

## 优化建议

根据基准测试结果，如遇到性能问题可考虑以下方向:

1. **如果元数据查询慢**（cache hit > 100 ns，或 GetNextPendingFileAsync > 10 μs）:
   - 检查 `_pendingKeys` 索引是否正常工作（应只扫描 Pending 文件，而非全量 active 文件）
   - 如果 active 文件数量巨大（>10 万），考虑加快清理服务运行频率，及时清理 PermanentlyFailed 文件
   - 检查是否有大量 Completed 文件未及时清理，导致 LiteDB 数据库过大

2. **如果并发写入慢**（> 250 ms / 100 写入）:
   - 首要检查磁盘 I/O 吞吐量（100 KB 文件顺序写入 < 1.1 ms 是正常水平）
   - 考虑挂载多个存储 Volume 以分散 I/O 压力
   - 考虑使用 SSD 存储

3. **如果 Volume 健康/空间查询慢**（> 100 ns）:
   - 检查 `LocalFileSystemVolume` 的 TTL 缓存是否生效（默认 30 秒）
   - 如果每次都触发文件写删测试，说明缓存失效过快

4. **如果配额检查慢**（> 200 ns）:
   - 检查 `AtomicQuotaState` 是否被正确初始化（GlobalSetup 应预热热路径）
   - 如果每次都需要创建新路径，会触发 Write-Behind flush

5. **如果租户操作慢**（缓存命中 > 200 ns）:
   - 增加租户缓存时间（默认 5 分钟）
   - 检查是否频繁调用 `EnableTenantAsync`/`DisableTenantAsync` 导致缓存失效

## 持续集成

可以将基准测试集成到 CI 流程中，跟踪性能变化:

```bash
# 生成 Markdown 格式的性能报告
dotnet run -c Release --exporters markdown

# 生成 JSON + Markdown 格式报告
dotnet run -c Release --exporters json markdown

# 导出结果到指定目录
dotnet run -c Release --exporters markdown --artifacts ./benchmark-results
```

结果文件保存在 `BenchmarkDotNet.Artifacts/results/` 目录，文件名格式:
- `Locus.Benchmarks.<ClassName>-report-github.md` — GitHub Flavored Markdown 表格
- `Locus.Benchmarks.<ClassName>-report.csv` — CSV 格式（适合自动化比较）
- `Locus.Benchmarks.<ClassName>-report.html` — HTML 可视化报告

## 相关文档

- [BenchmarkDotNet 文档](https://benchmarkdotnet.org/)
- [LiteDB 性能文档](https://www.litedb.org/docs/performance/)

## P0-2 Peak Memory Validation (100k+ files)

To validate the **peak managed memory** target for orphan-rebuild (not only BDN `Allocated`), run:

```powershell
pwsh ./tests/Locus.Benchmarks/measure-orphan-peak-memory.ps1 `
  -Mode both `
  -FileCount 120000 `
  -MetadataRatio 0.90 `
  -AttachDelaySec 15 `
  -PostRunDelaySec 5
```

Or run directly:

```powershell
dotnet run -c Release --project tests/Locus.Benchmarks -- `
  --scenario orphan-rebuild-peak `
  --mode both `
  --file-count 120000 `
  --metadata-ratio 0.90 `
  --attach-delay-sec 15 `
  --post-run-delay-sec 5
```

During attach delay, use `dotnet-counters` (if installed) for external EventPipe verification:

```powershell
dotnet-counters monitor -p <pid> --counters System.Runtime[gc-heap-size]
```

The scenario prints:
- `baseline`: legacy full-HashSet style path check
- `current`: current on-demand orphan rebuild path
- peak values and `Delta(current-vs-baseline)`

If `dotnet-counters` is unavailable, the command falls back to an in-process heap sampler and still reports a peak-bytes estimate for comparison.

## Phase D Cold-Start Benchmark

Run cold-start active-index loading benchmark:

```powershell
dotnet run -c Release --project tests/Locus.Benchmarks --filter "Locus.Benchmarks.MetadataRepositoryColdStartBenchmarks*"
```

## Architecture-Sensitive Benchmark Update (2026-04-09)

The benchmark suite was refreshed after the queue/cleanup architecture changes introduced dead-letter
disposition and made `DeadLettered` part of the active metadata lifecycle.

### Benchmark adjustments

- `CleanupLargeTenantBenchmarks` now seeds real permanently failed files on a mounted volume instead of
  benchmarking the already-missing-file fallback path.
- The cleanup benchmark now compares the current default `MoveToDeadLetter` path with the optional
  `Delete` disposition so we can track the extra cost of preserving dead-letter artifacts.
- `MetadataRepositoryColdStartBenchmarks` now includes `DeadLettered` rows in the 20,000-file startup
  dataset and probes a dead-lettered record after load to keep the new state in the measured path.

### Latest results

Environment:
- Ran on 2026-04-09 outside the sandbox.
- BenchmarkDotNet v0.15.8, .NET 10.0.5, Windows 11 25H2.
- Intel Core Ultra 9 185H, 22 logical cores / 16 physical cores.

#### Cleanup Status Path

| Scenario | Processing | PermanentlyFailed | Mean | StdDev | Allocated |
|----------|-----------:|------------------:|-----:|-------:|----------:|
| Move to dead letter | 1200 | 800 | 848.0 ms | 21.56 ms | 19.28 MB |
| Delete permanently failed files | 1200 | 800 | 559.8 ms | 76.37 ms | 13.97 MB |

Notes:
- The current default `MoveToDeadLetter` path is about 51.5% slower than `Delete` in this synthetic run.
- The additional cost comes from moving physical files and preserving metadata in `DeadLettered` state.

#### Metadata Cold Start (includes `DeadLettered`)

| Active files | Startup batch size | Mean | StdDev | Allocated |
|-------------:|-------------------:|-----:|-------:|----------:|
| 20000 | 512 | 114.6 ms | 22.38 ms | 29.94 MB |
| 20000 | 2000 | 111.9 ms | 14.62 ms | 29.55 MB |

Notes:
- Including `DeadLettered` rows did not materially change the cold-start profile between the two tested
  batch sizes.
- `StartupLoadBatchSize=2000` was slightly faster and allocated slightly less memory in this run.

### Raw reports

- `BenchmarkDotNet.Artifacts/results/Locus.Benchmarks.CleanupLargeTenantBenchmarks-report-github.md`
- `BenchmarkDotNet.Artifacts/results/Locus.Benchmarks.MetadataRepositoryColdStartBenchmarks-report-github.md`
