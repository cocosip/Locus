# appsettings.sample.json 配置说明

本文档对应 [src/Locus/appsettings.sample.json](/D:/Code/dotnet/Locus/src/Locus/appsettings.sample.json)，用于解释样例配置文件中各个字段的含义、作用，以及它们对系统行为的影响。

## 使用说明

- 配置根节点是 `Locus`。
- 时间类型使用 .NET `TimeSpan` 字符串格式，例如：
  - `00:05:00` 表示 5 分钟
  - `7.00:00:00` 表示 7 天
- 布尔值使用 `true` / `false`。
- 样例文件当前偏向“生产环境安全优先”的基线配置：
  - 优先保证 durability、恢复能力、后台自收敛
  - 不是单机极限吞吐的激进参数
- 当前样例里已经移除了 `QuotaReconciliation` 的常规定时配置。
  - quota reconciliation 现在属于显式维护动作，不再作为默认运行时后台任务

## 完整配置示例（JSONC 注释版）

下面这份配置使用 `jsonc` 风格写注释，目的是帮助理解字段含义。

- 这段内容适合阅读和讲解，不建议直接原样复制到运行中的 `appsettings.json`
- 实际生产配置请以 [src/Locus/appsettings.sample.json](/D:/Code/dotnet/Locus/src/Locus/appsettings.sample.json) 为准，再按需要删掉注释后使用

```jsonc
{
  "Locus": {
    // 元数据目录：存放 metadata.db、租户信息等
    "MetadataDirectory": "./locus-metadata",

    // 配额目录：存放 quotas.db 等目录/租户配额数据
    "QuotaDirectory": "./locus-quota",

    // FileWatcher 持久化配置目录
    "FileWatcherConfigurationDirectory": "./locus-watchers",

    // 自动创建租户
    // true：访问不存在的租户时可自动创建
    // false：必须提前创建好租户
    "AutoCreateTenants": true,

    // 默认租户配额
    // 0 表示不限额
    // null 不适用于这里，因为这是全局默认值
    "DefaultTenantQuota": 0,

    "MetadataRepository": {
      // 是否启用后台持久化
      // true：先进入内存/队列，再后台批量落盘
      "EnableBackgroundPersistence": true,

      // 元数据后台持久化队列容量上限
      // 越大越能抗突发流量，但会增加内存占用
      "MaxQueueSize": 200000,

      // 每轮后台落盘最多处理多少条记录
      "DrainBatchSize": 4000,

      // 队列达到该百分比后更积极合并重复更新
      // 用于减少写放大
      "SoftMergeThresholdPercent": 85,

      // 启动恢复活动索引时，每批加载多少条记录
      "StartupLoadBatchSize": 4000,

      // 服务停止时，等待后台持久化排空的最长秒数
      "ShutdownDrainTimeoutSeconds": 120,

      // 后台持久化线程空闲时，多久主动醒来一次
      "PersistenceIntervalSeconds": 1
    },

    "StoragePool": {
      // 完成态防重分片数
      // 高并发下可降低完成/失败回调时的锁竞争
      "CompletionGuardStripeCount": 1024
    },

    "QueueEventJournal": {
      // durable queue 根目录
      // 每个租户会在这里拥有自己的 queue.log / snapshot / cursor
      "QueueDirectory": "./locus-queue",

      // 是否启用 durable queue
      // 生产环境建议始终为 true
      "Enabled": true,

      // 是否允许旧版 non-journal 路径
      // 生产环境建议始终为 false
      "AllowLegacyNonJournalMode": false,

      // 是否启用后台投影服务
      // true：queue.log 会被持续投影到 SQLite
      "EnableProjection": true,

      // append 成功后，延迟/防抖写入 queue.state.json 的间隔
      // queue.log 仍然保持同步 flush，成功语义不变
      // 设为 00:00:00 可回退到每次 append 立即写 state 文件的旧行为
      "StateFlushDebounce": "00:00:01",

      // 单个租户每轮最多处理多少条 queue 记录
      "MaxRecordsPerTenantPerCycle": 256,

      // 每轮最多处理多少个租户
      "MaxTenantsPerCycle": 16,

      // 当上一轮仍然有工作时，下一轮等待多久
      "BusyCycleDelay": "00:00:00.200",

      // 当当前没有工作时，多久再检查一次
      "IdleCycleDelay": "00:00:02",

      // 单轮投影允许持续工作的最长时间
      // 防止后台线程长时间占用资源
      "MaxProjectionTimePerCycle": "00:00:03",

      // 是否自动生成快照
      // 快照用于加快恢复和重建
      "EnableAutomaticSnapshots": true,

      // 同一租户自动快照最短刷新间隔
      "AutomaticSnapshotInterval": "00:10:00",

      // 至少新增多少已投影字节后才值得刷新快照
      "MinBytesBeforeAutomaticSnapshot": 4194304,

      // 是否启用 queue.log 压缩
      // 在已投影且可恢复的前提下，避免日志无限增长
      "EnableCompaction": true,

      // 已处理字节达到这个值后，才考虑压缩日志
      "MinBytesBeforeCompaction": 16777216
    },

    // 启动时是否检查 metadata/quota 数据库健康状态
    "EnableDatabaseHealthCheck": true,

    // 检测到数据库损坏时，是否自动尝试恢复
    "AutoRecoverCorruptedDatabasesOnStartup": true,

    // 如果自动恢复仍失败，是否直接让应用启动失败
    // 生产环境通常建议 true，避免系统带病运行
    "FailFastOnStartupRecoveryFailure": true,

    "Sqlite": {
      // SQLite journal 模式
      // 推荐 WAL，支持更好的并发读写和恢复能力
      "JournalMode": "WAL",

      // SQLite 同步级别
      // FULL 更安全，NORMAL 更快
      // 当前样例偏生产安全，使用 FULL
      "SynchronousMode": "FULL",

      // SQLite 页缓存大小
      // 负数表示单位是 KB
      // -16384 约等于 16 MB
      "CacheSizeKb": -16384,

      // 数据库被锁住时，最长等待多久再报错
      "BusyTimeoutMs": 15000,

      // 每次批量提交后是否做 WAL checkpoint
      // true：更积极回收 WAL，但 I/O 更重
      // false：减少额外 I/O，更适合常规线上运行
      "CheckpointAfterBatch": false
    },

    "RetryPolicy": {
      // 最大重试次数
      // 达到后文件进入 PermanentlyFailed
      "MaxRetryCount": 3,

      // 第一次失败后的重试延迟
      "InitialRetryDelay": "00:00:05",

      // 是否启用指数退避
      // true：失败次数越多，延迟越长
      "UseExponentialBackoff": true,

      // 指数退避的最大上限
      "MaxRetryDelay": "00:05:00"
    },

    "Volumes": [
      {
        // 存储卷唯一标识
        "VolumeId": "vol-001",

        // 实际挂载目录
        "MountPath": "./storage/volume-1",

        // 卷类型
        // 当前样例使用本地文件系统
        "VolumeType": "LocalFileSystem",

        // 目录分片深度
        // 2 通常是线上比较均衡的选择
        "ShardingDepth": 2,

        // 服务启动后，对卷做健康检查前先等待多久
        "InitialDelayMs": 2000,

        // 卷未就绪时，健康检查重试间隔
        "HealthCheckDelayMs": 1000,

        // 文件写入缓冲区大小
        "WriteBufferSize": 262144,

        // 流复制缓冲区大小
        "CopyBufferSize": 131072,

        // 每次写入后是否强制 flush 到磁盘
        // true 更安全，但 I/O 更重
        "ForceFlushAfterWrite": true
      },
      {
        "VolumeId": "vol-002",
        "MountPath": "./storage/volume-2",
        "VolumeType": "LocalFileSystem",
        "ShardingDepth": 2,
        "InitialDelayMs": 2000,
        "HealthCheckDelayMs": 1000,
        "WriteBufferSize": 262144,
        "CopyBufferSize": 131072,
        "ForceFlushAfterWrite": true
      }
    ],

    "Tenants": [
      {
        // 预置租户 ID
        "TenantId": "tenant-001",

        // 是否启用该租户
        "Enabled": true,

        // 租户级文件配额
        // 10000 表示最多允许 10000 个文件
        "Quota": 10000
      },
      {
        "TenantId": "tenant-002",
        "Enabled": true,

        // null 表示继承 DefaultTenantQuota
        "Quota": null
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

        // 绑定的租户
        "TenantId": "tenant-001",

        // false 表示单租户模式
        "MultiTenantMode": false,

        // 是否自动创建租户目录
        "AutoCreateTenantDirectories": false,

        // 自动创建/发现租户目录相关缓存时间
        "AutoCreateTenantDirectoriesCacheTtl": "00:01:00",

        // watcher 实际扫描的目录
        "WatchPath": "./watch/vip/tenant-001",

        // 是否启用该 watcher
        "Enabled": true,

        // 是否递归扫描子目录
        "IncludeSubdirectories": true,

        // 允许导入的文件模式
        "FilePatterns": [
          "*.pdf",
          "*.docx",
          "*.xlsx"
        ],

        // 导入后动作
        // Delete / Move / Keep
        "PostImportAction": "Delete",

        // 扫描周期
        "PollingInterval": "00:00:15",

        // 允许导入的最大文件大小
        // 104857600 = 100 MB
        "MaxFileSizeBytes": 104857600,

        // 文件至少存在多久后才允许导入
        // 避免文件还没写完就被读走
        "MinFileAge": "00:00:05",

        // 稳定性复查延迟
        "FileStabilityCheckDelay": "00:00:00.100",

        // 文件太“老”时可跳过稳定性复查
        "SkipStabilityCheckAfterAge": "00:01:00",

        // 最大并发导入数
        "MaxConcurrentImports": 8,

        // 是否启用 imported history 清理节流
        "EnableImportedFilesPruneThrottle": true,

        // imported history 清理间隔
        "ImportedFilesPruneInterval": "00:05:00",

        // 是否启用 imported history 落盘防抖
        "EnableImportedFilesHistoryFlushDebounce": true,

        // imported history 落盘间隔
        "ImportedFilesHistoryFlushInterval": "00:00:02"
      },
      {
        "WatcherId": "watcher-all-regular-tenants",

        // 多租户模式下可留空，由子目录或扫描逻辑决定目标租户
        "TenantId": "",

        "MultiTenantMode": true,
        "AutoCreateTenantDirectories": true,
        "AutoCreateTenantDirectoriesCacheTtl": "00:01:00",
        "WatchPath": "./watch/shared",
        "Enabled": true,
        "IncludeSubdirectories": true,
        "FilePatterns": [
          "*.*"
        ],
        "PostImportAction": "Delete",
        "PollingInterval": "00:01:00",

        // 0 表示不限文件大小
        "MaxFileSizeBytes": 0,

        "MinFileAge": "00:00:10",
        "FileStabilityCheckDelay": "00:00:00.100",
        "SkipStabilityCheckAfterAge": "00:01:00",
        "MaxConcurrentImports": 4,
        "EnableImportedFilesPruneThrottle": true,
        "ImportedFilesPruneInterval": "00:05:00",
        "EnableImportedFilesHistoryFlushDebounce": true,
        "ImportedFilesHistoryFlushInterval": "00:00:02"
      }
    ],

    "OrphanRecoveryOptions": {
      // 是否启用孤儿文件恢复
      // 孤儿文件：磁盘上还在，但 metadata 丢了
      "Enabled": true,

      // 启动后是否先执行一次孤儿恢复
      "RunOnStartup": true,

      // 周期恢复间隔
      "RecoveryInterval": "06:00:00",

      // 启动后多久再执行第一轮恢复
      "InitialDelay": "00:02:00"
    },

    "CleanupOptions": {
      // 是否启用后台清理服务
      "Enabled": true,

      // 后台清理轮询间隔
      "CleanupInterval": "00:30:00",

      // 启动后首次清理延迟
      "InitialDelay": "00:05:00",

      // 是否回收处理超时文件
      "CleanupTimedOutFiles": true,

      // 处理超时阈值
      // 超过后，Processing 会被重置回 Pending
      "ProcessingTimeout": "00:30:00",

      // 永久失败文件处置策略
      "PermanentlyFailedDisposition": "MoveToDeadLetter",

      // 是否删除数据库损坏恢复后留下的 .corrupted.* 备份文件
      "CleanupInvalidDatabaseBackups": true,

      // 永久失败文件保留期
      "FailedFileRetentionPeriod": "7.00:00:00",

      // dead letter 目录配置
      "DeadLetter": {
        "RootPath": ".deadletter",
        "IncludeTenantInPath": true,
        "IncludeDatePartition": true,
        "ShardingDepth": 2
      },

      // 是否回收已完成文件
      // true 时，已完成文件在保留期后会被后台物理删除
      "CleanupCompletedFiles": true,

      // 已完成文件保留期
      "CompletedFileRetentionPeriod": "00:30:00",

      // 是否定期优化 SQLite 数据库
      "OptimizeDatabases": true,

      // 数据库优化间隔
      "DatabaseOptimizationInterval": "7.00:00:00",

      // 每个租户单轮状态清理的最大批量
      "CleanupBatchSizePerTenant": 1000,

      // 孤儿恢复时的物理路径查找缓存大小
      "OrphanRebuildLookupCacheSize": 16384,

      // 数据库优化时，每批处理多少个租户数据库
      "DatabaseOptimizationTenantBatchSize": 5,

      // 每批数据库优化之间暂停多久
      "DatabaseOptimizationPauseBetweenBatches": "00:00:01"
    }
  }
}
```

## Locus 根节点

| 字段 | 含义 | 说明 |
|---|---|---|
| `MetadataDirectory` | 元数据目录 | 用于存放元数据数据库、租户元信息等。每个租户的 `metadata.db` 会落在这里。 |
| `QuotaDirectory` | 配额目录 | 用于存放目录配额和租户配额相关的 SQLite 数据。 |
| `FileWatcherConfigurationDirectory` | FileWatcher 配置目录 | 用于持久化 watcher 的配置。 |
| `AutoCreateTenants` | 自动创建租户 | 为 `true` 时，如果访问了一个尚不存在的租户，系统可以自动创建该租户。 |
| `DefaultTenantQuota` | 默认租户配额 | 所有未单独配置配额的租户默认使用这个值。`0` 表示不限额。 |
| `EnableDatabaseHealthCheck` | 启动数据库健康检查 | 启动时检查 metadata / quota 数据库是否损坏。 |
| `AutoRecoverCorruptedDatabasesOnStartup` | 启动自动恢复损坏数据库 | 如果启动健康检查发现数据库损坏，尝试自动重建。 |
| `FailFastOnStartupRecoveryFailure` | 启动恢复失败时快速失败 | 为 `true` 时，如果自动恢复仍失败，应用启动直接失败，避免带着损坏状态继续运行。 |
| `CleanupOptions.Enabled` | 启用后台清理服务 | 控制 `BackgroundCleanupService` 是否启动。 |

## MetadataRepository

`MetadataRepository` 负责元数据写入、内存活动索引和后台持久化。

| 字段 | 含义 | 说明 |
|---|---|---|
| `EnableBackgroundPersistence` | 启用后台持久化 | 为 `true` 时，元数据先进入内存/队列，再由后台批量落盘。 |
| `MaxQueueSize` | 持久化队列上限 | 限制待落盘操作的缓冲量。值越大，突发流量承受能力越强，但内存占用也会变高。 |
| `DrainBatchSize` | 单次落盘批量大小 | 每轮后台持久化最多处理多少条记录。 |
| `SoftMergeThresholdPercent` | 软合并阈值 | 队列使用率达到该百分比后，系统会更积极地合并重复更新，减少写放大。 |
| `StartupLoadBatchSize` | 启动预加载批量大小 | 启动恢复活动文件索引时，每批加载多少条。 |
| `ShutdownDrainTimeoutSeconds` | 关机排空等待时间 | 应用停止时，等待后台持久化队列排空的最长秒数。 |
| `PersistenceIntervalSeconds` | 后台持久化轮询间隔 | 没有新消息时，后台落盘线程多久主动醒来一次。 |

## StoragePool

`StoragePool` 负责写入、读取、删除、卷选择和处理完成防重。

| 字段 | 含义 | 说明 |
|---|---|---|
| `CompletionGuardStripeCount` | 完成态防重分片数 | 用于减少高并发完成/失败回调时的锁竞争。数值越大，热点冲突越少，但内存开销略增。 |

## QueueEventJournal

`QueueEventJournal` 是当前架构里的 durable queue 核心配置，决定 `queue.log` 的启用方式、投影速度、快照和压缩策略。

| 字段 | 含义 | 说明 |
|---|---|---|
| `QueueDirectory` | 队列日志目录 | 保存每个租户的 `queue.log`、cursor、snapshot 等状态文件。 |
| `Enabled` | 是否启用 durable queue | 生产环境建议始终为 `true`。 |
| `AllowLegacyNonJournalMode` | 允许旧版非 journal 模式 | 兼容开关。生产环境建议始终为 `false`。 |
| `EnableProjection` | 启用后台投影 | 为 `true` 时，由后台服务将 queue 事件投影到 SQLite 元数据与配额库。 |
| `StateFlushDebounce` | state 文件防抖落盘间隔 | 控制 append 成功后 `queue.state.json` 多久批量/防抖写入。`queue.log` 仍保持同步 flush；设为 `00:00:00` 表示回退到每次 append 都立即写 state 文件。 |
| `MaxRecordsPerTenantPerCycle` | 单租户单轮最大投影记录数 | 控制每轮每个租户最多处理多少条 journal 记录。 |
| `MaxTenantsPerCycle` | 单轮最大处理租户数 | 控制一轮投影最多扫描多少个租户的日志。 |
| `BusyCycleDelay` | 忙时轮询间隔 | 当上一轮仍有工作时，后台投影线程等待多久再继续。 |
| `IdleCycleDelay` | 空闲轮询间隔 | 当没有工作时，后台投影线程多久再检查一次。 |
| `MaxProjectionTimePerCycle` | 单轮最大投影时间预算 | 控制后台线程一次最多连续工作多久，避免长时间独占资源。 |
| `EnableAutomaticSnapshots` | 自动生成快照 | 租户投影追平 journal tail 后自动生成/刷新快照。 |
| `AutomaticSnapshotInterval` | 自动快照刷新间隔 | 同一租户在有新进度时，最短多久允许再生成一次快照。 |
| `MinBytesBeforeAutomaticSnapshot` | 自动快照最小增量字节数 | 至少新增多少已投影字节后才值得刷新快照。 |
| `EnableCompaction` | 启用日志压缩 | 在可重建前提下压缩 `queue.log`，避免无限增长。 |
| `MinBytesBeforeCompaction` | 压缩最小阈值 | 已处理字节达到这个值后才考虑压缩，避免太频繁。 |

## Sqlite

`Sqlite` 同时影响 metadata 和 quota 数据库的连接 PRAGMA。

| 字段 | 含义 | 说明 |
|---|---|---|
| `JournalMode` | SQLite 日志模式 | 建议使用 `WAL`，有利于并发读写和崩溃恢复。 |
| `SynchronousMode` | SQLite 同步级别 | `FULL` 更安全，`NORMAL` 更快。样例当前偏生产安全，使用 `FULL`。 |
| `CacheSizeKb` | SQLite 页缓存大小 | 负数表示按 KB 指定大小。`-16384` 表示约 16 MB。 |
| `BusyTimeoutMs` | 数据库锁等待时间 | 当数据库被占用时，最多等待多久再报错。 |
| `CheckpointAfterBatch` | 每批提交后做 WAL checkpoint | 为 `false` 时减少额外 I/O；为 `true` 时能更积极压缩 WAL 文件。 |

## RetryPolicy

`RetryPolicy` 决定文件处理失败后的重试行为。

| 字段 | 含义 | 说明 |
|---|---|---|
| `MaxRetryCount` | 最大重试次数 | 达到次数后，文件转为 `PermanentlyFailed`。 |
| `InitialRetryDelay` | 初始重试延迟 | 第一次失败后，多久重新进入可处理状态。 |
| `UseExponentialBackoff` | 启用指数退避 | 为 `true` 时，失败次数越多，重试延迟越长。 |
| `MaxRetryDelay` | 最大重试延迟 | 限制指数退避的上限。 |

## Volumes

`Volumes` 是存储卷列表。每个对象表示一个可挂载卷。

### Volumes[] 字段说明

| 字段 | 含义 | 说明 |
|---|---|---|
| `VolumeId` | 卷唯一标识 | 逻辑 ID，用于元数据映射和日志记录。 |
| `MountPath` | 卷挂载路径 | 实际文件存放的根目录。 |
| `VolumeType` | 卷类型 | 当前样例使用 `LocalFileSystem`。 |
| `ShardingDepth` | 分片层级 | 控制物理目录打散深度。`2` 通常是比较均衡的生产值。 |
| `InitialDelayMs` | 启动健康检查初始延迟 | 留给卷挂载、网络盘就绪的缓冲时间。 |
| `HealthCheckDelayMs` | 健康检查重试间隔 | 卷未就绪时，多久再探测一次。 |
| `WriteBufferSize` | 写入缓冲区大小 | 影响文件流写入性能。 |
| `CopyBufferSize` | 拷贝缓冲区大小 | 影响流复制性能。 |
| `ForceFlushAfterWrite` | 写入后强制 flush | 为 `true` 时更安全，但 I/O 更重。 |

## Tenants

`Tenants` 是预置租户列表。适合在启动时就初始化部分固定租户。

### Tenants[] 字段说明

| 字段 | 含义 | 说明 |
|---|---|---|
| `TenantId` | 租户 ID | 系统内唯一标识。 |
| `Enabled` | 是否启用 | 为 `false` 时，该租户拒绝正常读写与处理流转。 |
| `Quota` | 租户文件配额 | `null` 表示继承 `DefaultTenantQuota`，`0` 表示不限额。 |

## FileWatchers

`FileWatchers` 用于配置目录扫描和自动导入。

### FileWatchers[] 字段说明

| 字段 | 含义 | 说明 |
|---|---|---|
| `WatcherId` | watcher 唯一标识 | 用于管理和日志追踪。 |
| `TenantId` | 目标租户 ID | 单租户模式下必须指定；多租户模式下可留空。 |
| `MultiTenantMode` | 多租户扫描模式 | 为 `true` 时，watch 路径下通常按子目录区分租户。 |
| `AutoCreateTenantDirectories` | 自动创建租户目录 | 多租户导入场景下常用。 |
| `AutoCreateTenantDirectoriesCacheTtl` | 租户目录缓存时间 | 控制自动创建/发现相关缓存的刷新周期。 |
| `WatchPath` | 监听目录 | watcher 实际扫描的目录。 |
| `Enabled` | 是否启用 watcher | 为 `false` 时该 watcher 不参与后台扫描。 |
| `IncludeSubdirectories` | 是否包含子目录 | 决定扫描是否递归。 |
| `FilePatterns` | 文件匹配模式 | 例如 `*.pdf`、`*.xlsx`。 |
| `PostImportAction` | 导入后动作 | 常见值是 `Delete`、`Move`、`Keep`。 |
| `PollingInterval` | 轮询间隔 | watcher 多久扫描一次目录。 |
| `MaxFileSizeBytes` | 最大允许导入大小 | `0` 表示不限。 |
| `MinFileAge` | 最小文件年龄 | 文件至少放置多久后才允许导入，避免尚未写完就被处理。 |
| `FileStabilityCheckDelay` | 稳定性检查延迟 | 二次确认文件大小/状态是否稳定的等待时间。 |
| `SkipStabilityCheckAfterAge` | 跳过稳定性检查阈值 | 文件如果已经足够“老”，可以直接认为稳定。 |
| `MaxConcurrentImports` | 最大并发导入数 | 限制同一个 watcher 一次并发处理多少文件。 |
| `EnableImportedFilesPruneThrottle` | 启用导入历史清理节流 | 防止 imported-history 维护过于频繁。 |
| `ImportedFilesPruneInterval` | 导入历史清理间隔 | 控制历史去重数据多久整理一次。 |
| `EnableImportedFilesHistoryFlushDebounce` | 启用导入历史落盘防抖 | 避免每次导入都立即写配置/历史文件。 |
| `ImportedFilesHistoryFlushInterval` | 导入历史落盘间隔 | 防抖后的刷新周期。 |

## OrphanRecoveryOptions

`OrphanRecoveryOptions` 控制“孤儿文件恢复”后台任务。这里的孤儿文件是指磁盘上还在，但 SQLite 元数据丢失的文件。

| 字段 | 含义 | 说明 |
|---|---|---|
| `Enabled` | 是否启用孤儿文件恢复 | 为 `true` 时，启动 `OrphanFileRecoveryService`。 |
| `RunOnStartup` | 启动后是否先跑一次 | 适合服务重启后尽快收敛异常状态。 |
| `RecoveryInterval` | 周期扫描间隔 | 正常运行时，多长时间检查一次孤儿文件。 |
| `InitialDelay` | 启动后的首次延迟 | 给卷挂载、后台服务初始化预留时间。 |

## CleanupOptions

`CleanupOptions` 控制后台清理服务 `BackgroundCleanupService` 的行为。它主要负责脏数据收敛、状态回收、空间回收，而不是业务级重建。

推荐理解方式：
- `CleanupTimedOutFiles` / `ProcessingTimeout` 负责把卡死的处理中任务放回队列
- `PermanentlyFailedDisposition` / `FailedFileRetentionPeriod` 负责处理已经达到最大重试次数的文件
- `CleanupCompletedFiles` / `CompletedFileRetentionPeriod` 负责回收已成功处理完成的物理文件
- `OptimizeDatabases` 这一组负责定期压缩 SQLite，回收数据库文件中的空洞空间

| 字段 | 含义 | 说明 |
|---|---|---|
| `Enabled` | 启用后台清理服务 | 控制 `BackgroundCleanupService` 是否注册并运行。默认 `true`。如果你希望完全手动触发清理而不是后台周期执行，可以设为 `false`。 |
| `CleanupInterval` | 清理轮询间隔 | 后台清理每隔多久跑一轮。值越小，状态收敛越快，但后台 I/O 越频繁。 |
| `InitialDelay` | 启动首次清理延迟 | 服务启动后先等待多久再跑第一轮。适合给卷挂载、数据库恢复、投影追平留出缓冲时间。 |
| `CleanupTimedOutFiles` | 清理处理超时文件 | 为 `true` 时，会把长时间卡在 `Processing` 的文件重置回可处理状态。适合处理进程崩溃、节点重启、线程卡死这类场景。 |
| `ProcessingTimeout` | 处理超时阈值 | 超过这个时间仍未完成的 `Processing` 文件会被视为超时。这个值应该大于你的正常最大处理时长，否则可能把仍在执行的任务误判为超时。 |
| `PermanentlyFailedDisposition` | 永久失败文件处置策略 | 控制永久失败文件在保留期后如何处理。`Keep` 表示保留原地不动；`MoveToDeadLetter` 表示转入 dead letter 并从活跃配额中扣除；`Delete` 表示物理删除。默认是 `MoveToDeadLetter`，更适合生产环境排障。 |
| `CleanupInvalidDatabaseBackups` | 清理数据库损坏备份文件 | 删除数据库自动恢复过程中留下的 `.corrupted.*` 文件。一般建议保持开启，否则这些备份文件会长期堆积。 |
| `FailedFileRetentionPeriod` | 永久失败文件保留期 | 文件进入 `PermanentlyFailed` 后，至少保留多久才应用 `PermanentlyFailedDisposition`。这个窗口主要用于人工排障、重试分析、问题取证。 |
| `DeadLetter.RootPath` | dead letter 根目录 | 永久失败文件转移后的目标根目录。相对路径会解析到所属 volume 下，绝对路径则直接使用该路径。 |
| `DeadLetter.IncludeTenantInPath` | dead letter 路径包含租户 | 为 `true` 时，会在 dead letter 下按租户分层。多租户场景建议开启，便于隔离和人工查找。 |
| `DeadLetter.IncludeDatePartition` | dead letter 路径包含日期分区 | 为 `true` 时，会追加 `yyyyMMdd` 目录。适合按天归档、限流清理、人工排查某天的失败文件。 |
| `DeadLetter.ShardingDepth` | dead letter 分片深度 | 控制 dead letter 目录按 `FileKey` 分片的层数。目录中文件很多时建议保留 1 到 2 级，避免单目录过大。 |
| `CleanupCompletedFiles` | 清理已完成文件 | 为 `true` 时，已成功处理完成的文件在保留期后会被后台物理删除。适用于“处理成功后源文件不再保留”的消费型场景。 |
| `CompletedFileRetentionPeriod` | 已完成文件保留期 | 已完成后多久允许后台回收物理文件。设为 `00:00:00` 表示下一轮清理就可以删除；设得更长则可以给下游系统留出额外的读取或审计窗口。 |
| `OptimizeDatabases` | 优化数据库 | 定期压缩 SQLite，回收删除记录留下的空间。数据库较大时这是重操作，建议放在业务低峰时段。 |
| `DatabaseOptimizationInterval` | 数据库优化间隔 | 两次数据库压缩之间的最短间隔。删除、状态变更多的场景可以配得短一些；普通场景按天或按周即可。 |
| `CleanupBatchSizePerTenant` | 每租户单轮清理批量 | 限制每轮状态清理对单个租户最多处理多少条记录。值越大，单轮收敛越快；值越小，对其他租户更公平，也更平滑。 |
| `MaxOrphanFilesPerRun` | 每轮孤儿恢复最大扫描文件数 | 控制每个租户每个 volume 在一次孤儿恢复中最多扫描多少物理文件。目录特别大时可以降低这个值，避免单轮扫描过重。小于等于 0 表示不设上限。 |
| `OrphanRebuildLookupCacheSize` | 孤儿恢复查找缓存大小 | 用于孤儿文件恢复时缓存物理路径查找结果，减少重复访问文件系统。文件很多时建议保留默认值；设为 `0` 表示关闭缓存。 |
| `DatabaseOptimizationTenantBatchSize` | 数据库优化分批租户数 | 一次优化多少个租户数据库后暂停一下。租户很多时这个参数可以降低单次持续 I/O 时间。 |
| `DatabaseOptimizationPauseBetweenBatches` | 数据库优化批次间暂停 | 每一批租户数据库优化完成后，额外暂停多久。主要用于降低连续 `VACUUM` 带来的磁盘抖动。 |

### PermanentlyFailedDisposition 取值建议

| 取值 | 说明 | 适用场景 |
|---|---|---|
| `Keep` | 到期后也不自动动文件，只保留在 `PermanentlyFailed` | 完全人工介入、严禁系统自动挪动或删除失败文件 |
| `MoveToDeadLetter` | 到期后转入 dead letter，并从活跃配额中移除 | 推荐默认值，既保留排障证据，又不占用活跃处理池 |
| `Delete` | 到期后直接删除物理文件 | 失败文件没有保留价值，且你明确接受自动删除 |

## 当前样例的配置取向

当前样例更适合下面这类环境：

- 长时间常驻运行的服务
- 更关注恢复能力和一致性，而不是极限吞吐
- 允许后台持续做 journal 投影、孤儿恢复、状态清理和数据库整理

如果你的目标是：

- 极致写入吞吐
- 短生命周期任务进程
- 单机实验环境

那通常还需要再单独调整以下几类配置：

- `Sqlite.SynchronousMode`
- `MetadataRepository.*`
- `QueueEventJournal.*`
- `CleanupOptions.*`
- `FileWatchers[].PollingInterval`
- `FileWatchers[].MaxConcurrentImports`
