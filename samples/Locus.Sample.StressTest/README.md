# Locus Stress Test Application

这是一个专门用于测试 Locus 存储池系统的并发压力测试程序。

## 功能特性

### 多租户测试
- 同时测试 **5 个租户**
- 每个租户独立统计
- 验证租户隔离性

### 多存储卷测试
- 配置 **4 个独立存储卷**
- 自动负载均衡
- 统计文件在各卷的分布情况

### 多线程并发写入
- **10 个写入线程** 同时运行
- 每个线程批量写入文件（每批 50 个文件）
- 实时统计写入速率和吞吐量

### 多线程并发读取/处理
- **10 个读取/处理线程** 同时运行
- 每个线程获取独立的文件（无重复）
- 模拟文件处理失败和自动重试（10% 失败率）
- 成功处理后自动删除文件

### 实时监控
- 每 2 秒刷新一次统计数据
- 显示当前写入速率、处理速率
- 显示吞吐量（字节/秒）
- 按租户和存储卷显示文件分布
- 显示错误统计

## 运行方法

### 1. 构建项目

```bash
cd samples/Locus.Sample.StressTest
dotnet build
```

### 2. 运行压力测试

```bash
dotnet run
```

### 3. 停止测试

按 `Ctrl+C` 优雅停止测试，程序会显示最终统计报告。

## 输出示例

```
╔════════════════════════════════════════════════════════════════╗
║         Locus Storage Pool - Stress Test Application          ║
╚════════════════════════════════════════════════════════════════╝

Test Directory: C:\Users\...\locus-stress-test\20260119-143022
Configuration:
  - Tenants: 5
  - Storage Volumes: 4
  - Writer Threads: 10
  - Reader/Processor Threads: 10
  - Files per Writer Batch: 50

Press Ctrl+C to stop the test gracefully...

═══════════════════════════════════════════════════════════════════════════
Runtime: 00:05:32
───────────────────────────────────────────────────────────────────────────
  Files Written:         12,450  (   37.42 files/sec,  45.23 KB/sec)
  Files Processed:       11,823  (   35.54 files/sec,  42.89 KB/sec)
  Files Failed:           1,245
  Pending:                  382
───────────────────────────────────────────────────────────────────────────
  Per-Tenant Distribution:
    tenant-001:      2,490 ( 20.0%)
    tenant-002:      2,491 ( 20.0%)
    tenant-003:      2,489 ( 20.0%)
    tenant-004:      2,490 ( 20.0%)
    tenant-005:      2,490 ( 20.0%)
───────────────────────────────────────────────────────────────────────────
  Per-Volume Distribution:
    vol-001:         3,112 ( 25.0%)
    vol-002:         3,113 ( 25.0%)
    vol-003:         3,112 ( 25.0%)
    vol-004:         3,113 ( 25.0%)
═══════════════════════════════════════════════════════════════════════════
```

## 测试内容

### 1. 并发写入测试
- 验证多线程同时写入不会冲突
- 验证文件在多个存储卷之间均衡分布
- 验证文件在多个租户之间正确隔离

### 2. 并发读取/处理测试
- 验证多线程读取时每个线程获取不同的文件（无重复）
- 验证文件状态管理（Pending → Processing → Completed）
- 验证并发安全性

### 3. 失败重试测试
- 模拟 10% 的文件处理失败
- 验证失败文件自动重新放回队列
- 验证重试机制正常工作

### 4. 负载均衡测试
- 验证文件在多个存储卷之间的分布
- 验证存储卷选择算法

## 配置参数

在 `Program.cs` 中可以调整以下参数：

```csharp
private const int TENANT_COUNT = 5;              // 租户数量
private const int VOLUME_COUNT = 4;              // 存储卷数量
private const int WRITER_THREAD_COUNT = 10;      // 写入线程数
private const int READER_THREAD_COUNT = 10;      // 读取线程数
private const int FILES_PER_WRITER_BATCH = 50;   // 每批写入文件数
private const int STATS_REFRESH_INTERVAL_MS = 2000; // 统计刷新间隔（毫秒）
```

## 测试数据位置

测试数据存储在临时目录中：
```
%TEMP%\locus-stress-test\{timestamp}\
  ├── volume01\          # 存储卷1
  ├── volume02\          # 存储卷2
  ├── volume03\          # 存储卷3
  ├── volume04\          # 存储卷4
  ├── metadata\          # 文件元数据
  └── quota\             # 配额数据
```

程序结束后，你可以检查这些目录来验证文件分布和元数据。

## 注意事项

1. **长时间运行**: 此程序设计为可以长时间运行，按 Ctrl+C 可随时停止
2. **资源占用**: 多线程会占用较多 CPU 和 I/O 资源
3. **磁盘空间**: 确保有足够的临时磁盘空间
4. **清理**: 程序会在临时目录创建测试数据，建议定期清理

### ⚠️ 重要：文件流管理

在使用 Locus 时，如果需要读取文件后再删除（通过 `MarkAsCompletedAsync`），**必须确保文件流在删除前已经被释放**。

**错误示例：**
```csharp
// ❌ 错误：流还在作用域内，文件被锁定，无法删除
using var stream = await storagePool.ReadFileAsync(tenant, fileKey, ct);
using var reader = new StreamReader(stream);
var content = await reader.ReadToEndAsync();

// 此时流还没有释放，会导致删除失败：
// "The process cannot access the file because it is being used by another process"
await storagePool.MarkAsCompletedAsync(fileKey, ct);
```

**正确示例：**
```csharp
// ✅ 正确：使用显式代码块确保流在删除前释放
string content;
{
    using var stream = await storagePool.ReadFileAsync(tenant, fileKey, ct);
    using var reader = new StreamReader(stream);
    content = await reader.ReadToEndAsync();
    // 流在此处（代码块结束时）被释放
}

// 现在可以安全删除文件
await storagePool.MarkAsCompletedAsync(fileKey, ct);
```

这个问题在 Windows 上尤其明显，因为 Windows 对文件锁的管理更严格。

### ⚠️ 重要：LiteDB 并发模式

Locus 使用 LiteDB 存储文件元数据和配额信息。为了支持多线程并发访问，所有 LiteDB 数据库都配置为 **共享模式（Shared）**。

**已配置的数据库：**
- 文件元数据数据库：`metadata/{tenant-id}.db`
- 配额数据库：`quota/{tenant-id}-quotas.db`

**连接字符串：**
```csharp
var connectionString = $"Filename={dbPath};Mode=Shared";
var db = new LiteDatabase(connectionString);
```

如果你在自己的代码中创建 LiteDB 实例，也需要使用 `Mode=Shared` 来支持并发访问。

## 预期结果

正常情况下应该看到：
- ✅ 写入速率和处理速率基本匹配（处理可能稍慢）
- ✅ 文件在各租户之间均匀分布（约 20% 每个）
- ✅ 文件在各存储卷之间均匀分布（约 25% 每个）
- ✅ 错误数为 0 或很少
- ✅ 失败的文件会自动重试并最终被处理

## 问题排查

### 如果出现大量错误
- 检查日志输出
- 降低线程数量
- 检查磁盘空间

### 如果文件分布不均
- 可能是存储卷容量差异导致
- 检查各存储卷的可用空间

### 如果性能不佳
- 调整线程数量
- 调整批次大小
- 检查磁盘 I/O 性能
