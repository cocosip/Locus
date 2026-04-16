# appsettings.sample.json 配置说明

本文档对应 [appsettings.sample.json](../src/Locus/appsettings.sample.json)，用于解释样例配置文件中各个字段的含义、作用，以及它们对系统行为的影响。

## 使用说明

- 配置根节点是 `Locus`
- 时间类型使用 .NET `TimeSpan` 字符串格式，例如：
  - `00:05:00` 表示 5 分钟
  - `7.00:00:00` 表示 7 天
- 布尔值使用 `true` / `false`
- 当前样例偏向“生产环境图像写入基线”：
  - 保留 journal durability、文件写入强制 flush、数据库健康检查与启动恢复
  - 同时通过更大的 buffer、更积极的后台批处理和更高的 watcher 并发提升图像写入吞吐
- 当 `LocusOptions` 或它引用的任意子 `Options` 结构发生变更时，必须同步更新：
  - [appsettings.sample.json](../src/Locus/appsettings.sample.json)
  - [appsettings.json](../samples/Locus.Sample.Console/appsettings.json)
  - [appsettings-sample-reference.md](./appsettings-sample-reference.md)

## 生产环境推荐基线（图像写入）

这份样例的核心目标是：

- 对图片批量写入和目录导入场景保持较高吞吐
- 不牺牲 `QueueEventJournal` 的 durable 语义
- 不关闭 `ForceFlushAfterWrite`
- 避免生产环境误建租户、无限制占满容量

这次样例的关键取舍如下：

- `AutoCreateTenants = false`
  - 生产环境建议显式建租户，避免错误租户 ID 被自动创建
- `DefaultTenantQuota = 1000000`
  - 不再默认无限制，避免异常流量直接打满系统
- `QueueEventJournal.AckMode = Durable`
  - 继续保证“成功返回时 journal 已落盘”
- `Sqlite.SynchronousMode = NORMAL`
  - 在 `WAL + Durable Journal + Startup Recovery` 前提下，换取更好的元数据写入吞吐
  - 如果你更看重 SQLite 自身在掉电场景下的最强 durability，可以改回 `FULL`
- `Volumes[*].ForceFlushAfterWrite = true`
  - 保留文件内容落盘安全性

## QueueEventJournal 调参建议

结合当前本地 benchmark 结果，以及“图像写入吞吐 + 生产安全”这个目标，`QueueEventJournal` 建议按下面三档理解：

- 稳妥版：
  - `JournalFormat = BinaryV1`
  - `AckMode = Durable`
  - `Linger = 5ms`
  - `MaxBatchRecords = 32`
  - `MaxBatchBytes = 1MB`
  - 适合作为默认生产配置，保持“`WriteFileAsync` 返回成功时 journal 已持久化”的语义不变
- 吞吐版：
  - `JournalFormat = BinaryV1`
  - `AckMode = Balanced`
  - `Linger = 5ms`
  - `BalancedFlushWindow = 8ms ~ 15ms`
  - 适合热点租户 burst 写入明显、但仍希望风险窗口可控的场景
- 极限版：
  - `JournalFormat = BinaryV1`
  - `AckMode = Async`
  - 不建议作为默认生产配置，更适合压测或低一致性要求场景

这轮本地 benchmark 的结论可以概括为：

- Binary 在“单租户热点 burst”下优势非常明显，吞吐提升远大于仅靠微批参数细调
- Binary 在“多租户 fan-out”下仍有稳定收益，同时显著降低分配
- `BalancedFlushWindow` 只对 `AckMode = Balanced` 生效，不会改变 `Durable` 的确认语义
- 如果目标是并发多文件写入吞吐，应优先做 binary codec、增大 batch 与投影吞吐；如果目标是单条极低延迟，不能只靠格式切换做判断

## QueueEventJournal 观测信号

当前 `QueueEventJournal` 已经具备基础的 metrics、状态字段和日志事件，用来覆盖：

- sequence gap detection
- corrupt tail detection
- auto-repair / auto-recovery

建议把它们理解成三层：

- metrics：适合做告警和趋势统计
- `QueueProjectionTenantState`：适合做维护态查询
- `queue.state.json` 与日志：适合做单租户排障

当前核心 metrics 包括：

- `locus.queue_journal.sequence_gap.detected`
- `locus.queue_journal.sequence_gap.recovery_attempted`
- `locus.queue_journal.sequence_gap.recovery_failed`
- `locus.queue_journal.corrupt_tail.detected`
- `locus.queue_journal.corrupt_tail.auto_repaired`

当前维护态会额外暴露这些字段：

- `GapExpectedSequenceNumber`
- `GapObservedSequenceNumber`
- `JournalFormat`
- `JournalCorruptTailDetected`
- `LastJournalCorruptTailDetectedAtUtc`
- `LastJournalCorruptTailOffset`
- `JournalAutoRepairCount`

其中 `queue.state.json` 里的 `Format` 现在按字符串持久化，例如 `BinaryV1`，方便人工排障。

更完整的说明见：

- [queue-journal-observability.md](./queue-journal-observability.md)

## 完整配置示例（JSONC 注释版）

下面这份配置使用 `jsonc` 风格写注释，目的是帮助理解字段含义。

- 这段内容适合阅读和讲解，不建议原样复制到运行中的 `appsettings.json`
- 实际可运行样例请以 [appsettings.sample.json](../src/Locus/appsettings.sample.json) 为准

```jsonc
{
  "Locus": {
    // 元数据目录：存放 metadata.db、租户元数据等
    "MetadataDirectory": "./locus-metadata",

    // 配额目录：存放 quota 数据库
    "QuotaDirectory": "./locus-quota",

    // FileWatcher 配置持久化目录
    "FileWatcherConfigurationDirectory": "./locus-watchers",

    // 是否允许首次访问未知租户时自动创建租户
    // 生产环境建议 false，避免错误租户 ID 被自动放大成真实数据
    "AutoCreateTenants": false,

    // 默认租户文件数配额
    // 生产环境不建议默认无限制
    "DefaultTenantQuota": 1000000,

    "MetadataRepository": {
      // 是否启用元数据后台持久化
      // true：前台请求先进入内存/队列，再后台批量刷入 SQLite
      "EnableBackgroundPersistence": true,

      // 元数据后台队列最大容量
      "MaxQueueSize": 500000,

      // 每轮后台落盘最多处理多少条
      "DrainBatchSize": 8000,

      // 队列达到该百分比后，更积极地合并重复写
      "SoftMergeThresholdPercent": 80,

      // 启动恢复期间，每批从数据库加载多少条
      "StartupLoadBatchSize": 8000,

      // 停机时等待后台持久化排空的最长时间（秒）
      "ShutdownDrainTimeoutSeconds": 180,

      // 空闲时后台持久化线程的轮询间隔（秒）
      "PersistenceIntervalSeconds": 1
    },

    "StoragePool": {
      // 完成态防重与并发保护的分片数
      "CompletionGuardStripeCount": 4096,

      // 当取件时发现没有 Pending 文件可用，允许同步回收多少个已超时的 Processing
      "EmptyQueueTimedOutReclaimBatchSize": 32,

      // 正常取件仍然成功时，低频后台渐进回收每次最多处理多少个已超时的 Processing
      "BackgroundTimedOutReclaimBatchSize": 8,

      // 同一租户两次渐进回收之间的最小冷却时间
      "TimedOutReclaimCooldown": "00:00:30",

      // 是否启用低频渐进式超时回收
      "EnableBackgroundTimedOutReclaim": true
    },

    "QueueEventJournal": {
      // durable queue 根目录
      // 每个租户会在这里拥有自己的 queue.log / state / cursor / snapshot
      "QueueDirectory": "./locus-queue",

      // 是否启用 durable queue
      "Enabled": true,

      // 是否允许旧版 non-journal 路径
      // 生产环境建议始终为 false
      "AllowLegacyNonJournalMode": false,

      // 是否启用后台投影服务
      // true：queue.log 会持续投影到 metadata / quota
      "EnableProjection": true,

      // 新建或空 journal 默认使用的编码格式
      // BinaryV1：推荐默认值，热点租户 burst 吞吐更好，分配更低
      // JsonLines：保留兼容格式，适合老 journal 持续读取/追加
      // 注意：已有非空 queue.log 会自动探测格式并继续沿用，不会在单文件内混写 JSON/binary
      "JournalFormat": "BinaryV1",

      // append 的确认策略
      // Durable：默认值。批次写入并 flush 后才返回
      // Balanced：先写入，再在极短窗口内合并 flush，吞吐更高但风险略增
      // Async：仅入队就返回，最快，但不建议默认生产使用
      "AckMode": "Durable",

      // append 成功后，queue.state.json 的防抖落盘间隔
      // 注意：这不会改变 Durable 模式下“journal 已 flush 才返回”的默认语义
      "StateFlushDebounce": "00:00:01",

      // 单租户 writer 在发现有积压时，用于收敛微批的等待窗口
      // 5ms 对图片 burst 写入更友好，同时保持 Durable 语义不变
      "Linger": "00:00:00.005",

      // 单个 journal append 批次最多合并多少条记录
      "MaxBatchRecords": 32,

      // 单个 journal append 批次最多合并多少字节
      "MaxBatchBytes": 1048576,

      // 单租户 writer 空闲多久后关闭持久打开的 FileStream
      "WriterIdleTimeout": "00:02:00",

      // 仅对 AckMode=Balanced 生效
      // 写入后允许延后 flush 的最大窗口
      // 8ms 是保守起点，只有切到 Balanced 时才会生效
      "BalancedFlushWindow": "00:00:00.008",

      // projector 每轮每个租户最多投影多少条记录
      "MaxRecordsPerTenantPerCycle": 1024,

      // projector 每轮最多处理多少个租户
      "MaxTenantsPerCycle": 32,

      // 上一轮仍有工作时，下一轮等待多久
      "BusyCycleDelay": "00:00:00.050",

      // 当前没有工作时，多久再检查一次
      "IdleCycleDelay": "00:00:01",

      // 单轮投影允许持续工作的最长时间
      "MaxProjectionTimePerCycle": "00:00:05",

      // 是否自动生成快照
      "EnableAutomaticSnapshots": true,

      // 同一租户自动快照的最小刷新间隔
      "AutomaticSnapshotInterval": "00:05:00",

      // 至少新增多少已投影字节后，才值得刷新快照
      "MinBytesBeforeAutomaticSnapshot": 8388608,

      // 是否启用 queue.log 压缩
      "EnableCompaction": true,

      // 至少累计多少已处理字节后才考虑压缩
      "MinBytesBeforeCompaction": 33554432
    },

    // 启动时是否检查 metadata/quota 数据库健康状态
    "EnableDatabaseHealthCheck": true,

    // 发现数据库损坏时是否自动尝试恢复
    "AutoRecoverCorruptedDatabasesOnStartup": true,

    // 如果自动恢复失败，是否让应用启动失败
    // 生产环境通常建议 true，避免系统带病运行
    "FailFastOnStartupRecoveryFailure": true,

    "Sqlite": {
      // SQLite journal mode
      // 推荐 WAL
      "JournalMode": "WAL",

      // SQLite 同步级别
      // 生产环境图像写入基线选择 NORMAL：
      // journal 仍然是 Durable，SQLite 则以更高吞吐为目标
      "SynchronousMode": "NORMAL",

      // SQLite cache size
      // 负数表示单位为 KB
      "CacheSizeKb": -32768,

      // 数据库被锁住时，最长等待多久再报错
      "BusyTimeoutMs": 30000,

      // 每次批处理后是否主动做 checkpoint
      "CheckpointAfterBatch": false
    },

    "RetryPolicy": {
      // 最大重试次数
      "MaxRetryCount": 5,

      // 首次失败后的延迟
      "InitialRetryDelay": "00:00:03",

      // 是否启用指数退避
      "UseExponentialBackoff": true,

      // 指数退避的最大上限
      "MaxRetryDelay": "00:10:00"
    },

    "Volumes": [
      {
        // 存储卷唯一标识
        "VolumeId": "vol-001",

        // 挂载目录
        "MountPath": "./storage/volume-1",

        // 卷类型
        "VolumeType": "LocalFileSystem",

        // 目录分片深度
        // 2 通常是比较均衡的生产值
        "ShardingDepth": 2,

        // 启动后首次健康检查前等待时间
        "InitialDelayMs": 1000,

        // 卷未就绪时的健康检查重试间隔
        "HealthCheckDelayMs": 1000,

        // 写入 buffer 大小
        "WriteBufferSize": 524288,

        // 复制 buffer 大小
        "CopyBufferSize": 524288,

        // 每次写入后是否强制 flush 到磁盘
        "ForceFlushAfterWrite": true
      },
      {
        "VolumeId": "vol-002",
        "MountPath": "./storage/volume-2",
        "VolumeType": "LocalFileSystem",
        "ShardingDepth": 2,
        "InitialDelayMs": 1000,
        "HealthCheckDelayMs": 1000,
        "WriteBufferSize": 524288,
        "CopyBufferSize": 524288,
        "ForceFlushAfterWrite": true
      }
    ],

    "Tenants": [
      {
        // 预置租户 ID
        "TenantId": "tenant-001",

        // 是否启用
        "Enabled": true,

        // 租户配额
        "Quota": 2000000
      },
      {
        "TenantId": "tenant-002",
        "Enabled": true,

        "Quota": 1000000
      },
      {
        "TenantId": "tenant-disabled",
        "Enabled": false,
        "Quota": null
      }
    ],

    "FileWatchers": [
      {
        // watcher 唯一标识
        "WatcherId": "watcher-vip-tenant-001",

        // 绑定租户
        "TenantId": "tenant-001",

        // false 表示单租户模式
        "MultiTenantMode": false,

        // 是否自动创建租户目录
        "AutoCreateTenantDirectories": false,

        // 自动创建/发现租户目录相关缓存时间
        "AutoCreateTenantDirectoriesCacheTtl": "00:01:00",

        // 监听路径
        "WatchPath": "./watch/vip/tenant-001",

        // 是否启用
        "Enabled": true,

        // 是否递归扫描
        "IncludeSubdirectories": true,

        // 允许导入的文件模式
        "FilePatterns": [
          "*.jpg",
          "*.jpeg",
          "*.png",
          "*.webp",
          "*.tif",
          "*.tiff",
          "*.bmp"
        ],

        // 导入后的动作
        "PostImportAction": "Delete",

        // 轮询周期
        "PollingInterval": "00:00:10",

        // 最大允许导入文件大小
        "MaxFileSizeBytes": 536870912,

        // 文件至少存在多久后才允许导入
        "MinFileAge": "00:00:03",

        // 稳定性检查延迟
        "FileStabilityCheckDelay": "00:00:00.200",

        // 文件太老时可跳过稳定性检查
        "SkipStabilityCheckAfterAge": "00:02:00",

        // 最大并发导入数
        "MaxConcurrentImports": 16,

        // imported history 清理节流
        "EnableImportedFilesPruneThrottle": true,

        // imported history 清理间隔
        "ImportedFilesPruneInterval": "00:05:00",

        // imported history 落盘防抖
        "EnableImportedFilesHistoryFlushDebounce": true,

        // imported history 落盘间隔
        "ImportedFilesHistoryFlushInterval": "00:00:02"
      },
      {
        "WatcherId": "watcher-all-regular-tenants",

        // 多租户模式下可留空，由扫描逻辑决定目标租户
        "TenantId": "",

        "MultiTenantMode": true,
        "AutoCreateTenantDirectories": true,
        "AutoCreateTenantDirectoriesCacheTtl": "00:01:00",
        "WatchPath": "./watch/shared",
        "Enabled": true,
        "IncludeSubdirectories": true,
        "FilePatterns": [
          "*.jpg",
          "*.jpeg",
          "*.png",
          "*.webp",
          "*.tif",
          "*.tiff",
          "*.bmp"
        ],
        "PostImportAction": "Delete",
        "PollingInterval": "00:00:15",

        "MaxFileSizeBytes": 536870912,

        "MinFileAge": "00:00:05",
        "FileStabilityCheckDelay": "00:00:00.200",
        "SkipStabilityCheckAfterAge": "00:02:00",
        "MaxConcurrentImports": 12,
        "EnableImportedFilesPruneThrottle": true,
        "ImportedFilesPruneInterval": "00:05:00",
        "EnableImportedFilesHistoryFlushDebounce": true,
        "ImportedFilesHistoryFlushInterval": "00:00:02"
      }
    ],

    "OrphanRecoveryOptions": {
      // 是否启用孤儿文件恢复
      "Enabled": true,

      // 启动后是否先执行一次恢复
      "RunOnStartup": true,

      // 周期性恢复间隔
      "RecoveryInterval": "04:00:00",

      // 启动后多久再执行第一轮恢复
      "InitialDelay": "00:02:00"
    },

    "CleanupOptions": {
      // 是否启用后台清理服务
      "Enabled": true,

      // 后台清理轮询间隔
      "CleanupInterval": "00:15:00",

      // 启动后首次清理前延迟
      "InitialDelay": "00:05:00",

      // 是否清理处理超时文件
      "CleanupTimedOutFiles": true,

      // 处理超时阈值
      "ProcessingTimeout": "00:20:00",

      // 永久失败文件处置方式
      "PermanentlyFailedDisposition": "MoveToDeadLetter",

      // 是否清理数据库损坏备份文件
      "CleanupInvalidDatabaseBackups": true,

      // 永久失败文件保留期
      "FailedFileRetentionPeriod": "14.00:00:00",

      "DeadLetter": {
        // dead letter 根目录
        "RootPath": ".deadletter",

        // 是否把 tenant 放入路径
        "IncludeTenantInPath": true,

        // 是否按日期分区
        "IncludeDatePartition": true,

        // dead letter 目录分片深度
        "ShardingDepth": 2
      },

      // 是否清理已完成文件
      "CleanupCompletedFiles": true,

      // 已完成文件保留期
      "CompletedFileRetentionPeriod": "06:00:00",

      // 是否周期性优化数据库
      "OptimizeDatabases": true,

      // 数据库优化周期
      "DatabaseOptimizationInterval": "7.00:00:00",

      // 单租户单轮清理批大小
      "CleanupBatchSizePerTenant": 2000,

      // 单轮孤儿文件扫描上限
      "MaxOrphanFilesPerRun": 20000,

      // 孤儿恢复路径查找缓存大小
      "OrphanRebuildLookupCacheSize": 32768,

      // 数据库优化时每批处理的租户数
      "DatabaseOptimizationTenantBatchSize": 10,

      // 数据库优化批次之间的暂停
      "DatabaseOptimizationPauseBetweenBatches": "00:00:00.500"
    }
  }
}
```

## QueueEventJournal 新增字段说明

这次变更新增了下面几个 journal 配置项，和“单写者 + 微批 + ACK 分级”方案直接相关。

`AckMode`
- journal append 返回时机
- `Durable`：默认值。写入并 `flush` 后返回
- `Balanced`：写入后允许短窗口聚合 `flush`
- `Async`：只要成功入队就返回

`Linger`
- 单租户 writer 检测到积压后，最多等待多久收敛成更大的微批
- 值太小：合批效果有限
- 值太大：单条延迟会上升
- 当前样例取 `5ms`，更偏向图片批量导入的 burst 写入

`MaxBatchRecords`
- 一个微批最多合并多少条记录
- 当前样例取 `32`

`MaxBatchBytes`
- 一个微批最多合并多少字节
- 当前样例取 `1MB`

`WriterIdleTimeout`
- 空闲多久后关闭持久打开的 `FileStream`
- 目的是减少空闲租户长期占用文件句柄

`BalancedFlushWindow`
- 只对 `AckMode=Balanced` 生效
- 表示“写完但尚未 flush”允许停留的最大时间窗口
- 当前样例预留 `8ms`，是偏吞吐优先的保守起始值：
  - 足够给多个小批次一个极短的合并机会
  - 会比 `5ms` 更容易收敛出更大的批次
  - 不把宕机时的未 flush 风险窗口放大太多

## 推荐调参思路

如果目标是“更稳”：

- `AckMode = Durable`
- `Linger = 5ms`
- `MaxBatchRecords = 32`
- `MaxBatchBytes = 1048576`
- `Sqlite.SynchronousMode = FULL`

如果目标是“吞吐更高，但仍希望风险窗口很小”：

- `AckMode = Balanced`
- `BalancedFlushWindow = 8ms ~ 15ms`
- `Linger = 5ms`
- 再结合压测看是否要提高 `MaxBatchRecords`

如果目标是“极限吞吐”：

- 可以尝试 `AckMode = Async`
- 但必须接受 durability 语义变化
- 不建议作为默认生产配置
