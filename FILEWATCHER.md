# FileWatcher 混合模式使用指南

## 混合模式架构

混合模式允许你为重要租户配置专属 Watcher，同时为普通租户共享一个统一的 Watcher。

### 配置策略

```json
{
  "FileWatchers": [
    {
      "WatcherId": "watcher-vip-tenant-001",
      "TenantId": "tenant-001",           // 指定租户ID
      "MultiTenantMode": false,           // 单租户模式
      "WatchPath": "./watch/vip/tenant-001",
      "PollingInterval": "00:00:10",      // VIP: 10秒扫描
      "MaxConcurrentImports": 16          // VIP: 16个并发
    },
    {
      "WatcherId": "watcher-all-regular-tenants",
      "TenantId": "",                      // 留空
      "MultiTenantMode": true,             // 多租户模式
      "WatchPath": "./watch/shared",       // 共享目录
      "PollingInterval": "00:00:30",       // 普通: 30秒扫描
      "MaxConcurrentImports": 8            // 普通: 8个并发
    }
  ]
}
```

## 目录结构

### 完整示例

```
项目根目录/
│
├── watch/                          # 文件监控根目录
│   ├── vip/                        # VIP租户专属目录
│   │   └── tenant-001/             # tenant-001 的专属监控目录
│   │       ├── file1.pdf
│   │       ├── file2.docx
│   │       └── invoices/
│   │           └── invoice.xlsx
│   │
│   └── shared/                     # 共享监控目录（多租户）
│       ├── tenant-002/             # tenant-002 的文件
│       │   ├── data1.csv
│       │   └── report.pdf
│       ├── tenant-003/             # tenant-003 的文件
│       │   └── image.png
│       ├── tenant-004/             # tenant-004 的文件
│       │   └── document.txt
│       └── tenant-999/             # 任意租户都可以
│           └── file.zip
│
├── storage/                        # 实际存储位置（自动分片）
│   ├── volume-1/
│   │   ├── tenant-001/
│   │   │   ├── a/1/a1b2c3...      # 文件自动分片存储
│   │   │   └── b/2/b2c3d4...
│   │   ├── tenant-002/
│   │   └── tenant-003/
│   └── volume-2/
│
└── locus-metadata/                 # 元数据存储
    ├── tenant-001.db
    ├── tenant-002.db
    └── tenant-003.db
```

## 工作流程

### 1. VIP租户（tenant-001）

**放入文件**：
```bash
# 将文件放入 VIP 专属目录
cp invoice.pdf ./watch/vip/tenant-001/
```

**处理流程**：
1. Watcher `watcher-vip-tenant-001` 每10秒扫描一次
2. 检测到 `invoice.pdf`
3. 等待 3 秒（MinFileAge）确保文件写入完成
4. 导入到 `tenant-001` 的存储池
5. 删除原文件（PostImportAction = Delete）
6. 文件进入队列，状态为 `Pending`

### 2. 普通租户（tenant-002 ~ tenant-999）

**放入文件**：
```bash
# 将文件放入共享目录，子目录名即租户ID
mkdir -p ./watch/shared/tenant-002
cp data.csv ./watch/shared/tenant-002/

mkdir -p ./watch/shared/tenant-999
cp report.pdf ./watch/shared/tenant-999/
```

**处理流程**：
1. Watcher `watcher-all-regular-tenants` 每30秒扫描一次
2. 检测到 `./watch/shared/tenant-002/data.csv`
3. **自动识别租户ID为 `tenant-002`**（从目录名提取）
4. 等待 5 秒确保文件写入完成
5. 导入到 `tenant-002` 的存储池
6. 删除原文件

## 配置参数说明

| 参数 | 说明 | VIP推荐值 | 普通推荐值 |
|------|------|-----------|-----------|
| `PollingInterval` | 扫描间隔 | `00:00:10` (10秒) | `00:00:30` (30秒) |
| `MaxConcurrentImports` | 并发导入数 | 16 | 8 |
| `MinFileAge` | 最小文件年龄 | `00:00:03` (3秒) | `00:00:05` (5秒) |
| `MaxFileSizeBytes` | 最大文件大小 | 104857600 (100MB) | 0 (无限制) |
| `FilePatterns` | 文件过滤 | `["*.pdf","*.docx"]` | `["*.*"]` (全部) |
| `PostImportAction` | 导入后操作 | `Delete` | `Delete` |

## 使用场景

### 场景1：VIP客户需要实时处理

```json
{
  "WatcherId": "watcher-vip-realtime",
  "TenantId": "vip-customer-001",
  "MultiTenantMode": false,
  "WatchPath": "./watch/vip/vip-customer-001",
  "PollingInterval": "00:00:05",      // 5秒实时扫描
  "MaxConcurrentImports": 32,         // 高并发
  "FilePatterns": ["*.xml", "*.json"] // 只处理特定格式
}
```

### 场景2：普通租户统一管理

```json
{
  "WatcherId": "watcher-standard",
  "TenantId": "",
  "MultiTenantMode": true,
  "WatchPath": "./watch/standard",
  "PollingInterval": "00:01:00",      // 1分钟扫描
  "MaxConcurrentImports": 4,          // 低并发
  "FilePatterns": ["*.*"]             // 所有文件
}
```

### 场景3：特殊处理（移动而非删除）

```json
{
  "WatcherId": "watcher-archive",
  "TenantId": "archive-tenant",
  "MultiTenantMode": false,
  "WatchPath": "./watch/archive",
  "PostImportAction": "Move",
  "MoveToDirectory": "./watch/processed", // 移动到已处理目录
  "PollingInterval": "00:05:00"
}
```

## 代码使用示例

### 启动时配置

```csharp
services.AddLocus(options =>
{
    // 基础配置
    options.MetadataDirectory = "./locus-metadata";
    options.AutoCreateTenants = true;

    // 存储卷配置
    options.Volumes.Add(new VolumeConfiguration
    {
        VolumeId = "vol-001",
        MountPath = "./storage/volume-1",
        ShardingDepth = 2
    });

    // VIP租户专属 Watcher
    options.FileWatchers.Add(new FileWatcherConfiguration
    {
        WatcherId = "watcher-vip-001",
        TenantId = "tenant-001",
        MultiTenantMode = false,
        WatchPath = "./watch/vip/tenant-001",
        PollingInterval = TimeSpan.FromSeconds(10),
        MaxConcurrentImports = 16,
        PostImportAction = PostImportAction.Delete
    });

    // 普通租户共享 Watcher
    options.FileWatchers.Add(new FileWatcherConfiguration
    {
        WatcherId = "watcher-shared",
        TenantId = "",
        MultiTenantMode = true,
        WatchPath = "./watch/shared",
        PollingInterval = TimeSpan.FromSeconds(30),
        MaxConcurrentImports = 8,
        PostImportAction = PostImportAction.Delete
    });
});
```

### 运行时手动扫描

```csharp
// 注入 IFileWatcher
public class MyService
{
    private readonly IFileWatcher _fileWatcher;

    public MyService(IFileWatcher fileWatcher)
    {
        _fileWatcher = fileWatcher;
    }

    public async Task ManualScanAsync()
    {
        // 手动触发扫描
        var count = await _fileWatcher.ScanNowAsync("watcher-vip-001", CancellationToken.None);
        Console.WriteLine($"Imported {count} files");
    }

    public async Task EnableWatcherAsync()
    {
        // 启用/禁用 Watcher
        await _fileWatcher.EnableWatcherAsync("watcher-shared", CancellationToken.None);
        await _fileWatcher.DisableWatcherAsync("watcher-vip-001", CancellationToken.None);
    }
}
```

## 最佳实践

### 1. 目录权限
确保应用程序有读写权限：
```bash
chmod -R 755 ./watch
chmod -R 755 ./storage
```

### 2. 监控健康状态
定期检查 Watcher 状态：
```csharp
var watchers = await _fileWatcher.GetAllWatchersAsync(ct);
foreach (var watcher in watchers)
{
    _logger.LogInformation("Watcher {Id}: {Status}",
        watcher.WatcherId,
        watcher.Enabled ? "Enabled" : "Disabled");
}
```

### 3. 错误处理
文件导入失败会自动重试（根据 RetryPolicy 配置）：
- 第1次失败：等待 5 秒后重试
- 第2次失败：等待 10 秒后重试（指数退避）
- 第3次失败：标记为 `PermanentlyFailed`

### 4. 清理策略
配置自动清理，避免磁盘占满：
```json
{
  "EnableBackgroundCleanup": true,
  "CleanupOptions": {
    "CleanupInterval": "01:00:00",
    "ProcessingTimeout": "00:30:00",
    "FailedFileRetentionPeriod": "7.00:00:00"
  }
}
```

## 常见问题

### Q1: 文件被导入后，原文件还在怎么办？
A: 检查 `PostImportAction` 配置是否为 `Delete`。如果设置为 `Keep` 则会保留原文件。

### Q2: 共享目录下新增租户需要重启吗？
A: **不需要**。多租户模式会自动识别新的子目录，无需重启。

### Q3: 同一个租户可以有多个 Watcher 吗？
A: 可以，但不推荐。一个租户应该只有一个专属 Watcher 或使用共享 Watcher。

### Q4: 文件太大导入失败怎么办？
A: 设置 `MaxFileSizeBytes` 限制，超过限制的文件会被跳过并记录日志。

### Q5: 如何知道文件导入成功？
A: 查看日志或通过 `IStoragePool.GetFileStatusAsync(fileKey)` 检查状态。

## 性能调优

### 大量文件场景
```json
{
  "MaxConcurrentImports": 32,
  "PollingInterval": "00:00:05"
}
```

### 大文件场景
```json
{
  "MaxConcurrentImports": 2,
  "MaxFileSizeBytes": 1073741824,
  "MinFileAge": "00:00:30"
}
```

### 低资源场景
```json
{
  "MaxConcurrentImports": 1,
  "PollingInterval": "00:05:00"
}
```
