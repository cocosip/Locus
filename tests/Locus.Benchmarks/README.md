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
# 只运行元数据仓库基准测试
dotnet run -c Release --filter "*MetadataRepositoryBenchmarks*"

# 只运行并发操作基准测试
dotnet run -c Release --filter "*ConcurrentOperationsBenchmarks*"

# 只运行目录配额基准测试
dotnet run -c Release --filter "*DirectoryQuotaBenchmarks*"

# 只运行租户管理基准测试
dotnet run -c Release --filter "*TenantManagerBenchmarks*"
```

### 运行特定的基准测试方法

```bash
# 只运行并发写入测试
dotnet run -c Release --filter "*ConcurrentWrites*"

# 只运行元数据添加测试
dotnet run -c Release --filter "*AddOrUpdateAsync_Single*"
```

## 基准测试类说明

### 1. MetadataRepositoryBenchmarks
测试文件元数据仓库的性能:
- **AddOrUpdate single file metadata**: 单个文件元数据的添加/更新性能
- **Get file metadata (cache hit)**: 缓存命中时的元数据查询性能
- **Get file metadata (cache miss)**: 缓存未命中时的元数据查询性能
- **Batch insert 100 files**: 批量插入100个文件的性能
- **Get pending files**: 获取待处理文件的性能

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
测试并发场景下的性能:
- **10/50/100 concurrent writes**: 10/50/100个并发写入操作
- **10 concurrent reads**: 10个并发读取操作
- **Mixed read/write operations**: 混合读写操作 (10写+10读)

## 性能指标说明

BenchmarkDotNet 会输出以下关键指标:

- **Mean**: 平均执行时间
- **Error**: 误差范围 (99.9% 置信区间的一半)
- **StdDev**: 标准差
- **Median**: 中位数执行时间
- **Gen0/Gen1/Gen2**: GC 回收次数 (每1000次操作)
- **Allocated**: 每次操作分配的内存

## 结果解读

### 预期性能范围

#### 元数据操作
- 单次 AddOrUpdate: < 1 ms (LiteDB 写入 + 内存缓存)
- 缓存命中查询: < 10 μs (纯内存读取)
- 缓存未命中查询: < 500 μs (LiteDB 读取 + 缓存更新)

#### 配额操作
- 配额检查 (无限制): < 50 μs (纯内存操作)
- 配额检查 (有限制): < 100 μs (内存读取 + 比较)
- 递增/递减计数: < 1 ms (LiteDB 事务 + 内存更新)

#### 租户操作
- 创建租户: < 5 ms (JSON 序列化 + 文件写入 + 目录创建)
- 获取租户 (缓存命中): < 10 μs
- 获取租户 (自动创建): < 5 ms

#### 并发操作
- 10 个并发写入: < 100 ms total (约每个 10 ms)
- 50 个并发写入: < 500 ms total (约每个 10 ms)
- 100 个并发写入: < 1000 ms total (约每个 10 ms)

> **注意**: 实际性能取决于硬件配置 (CPU, 磁盘类型: HDD vs SSD)

## 优化建议

根据基准测试结果,可以考虑以下优化:

1. **如果元数据查询慢**:
   - 增加缓存过期时间
   - 增加内存缓存大小限制

2. **如果并发写入慢**:
   - 检查磁盘 I/O 性能
   - 考虑批量写入优化
   - 考虑使用 SSD 存储

3. **如果配额检查慢**:
   - 优化配额缓存策略
   - 减少不必要的配额检查

4. **如果租户操作慢**:
   - 增加租户缓存时间
   - 优化文件系统访问

## 持续集成

可以将基准测试集成到 CI 流程中,跟踪性能变化:

```bash
# 生成性能报告并保存
dotnet run -c Release --exporters json markdown

# 比较两次运行的结果
dotnet run -c Release -- --filter "*" --join
```

## 相关文档

- [BenchmarkDotNet 文档](https://benchmarkdotnet.org/)
- [LiteDB 性能文档](https://www.litedb.org/docs/performance/)
