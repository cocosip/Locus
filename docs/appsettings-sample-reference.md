# appsettings.sample.json 配置说明

本文档对应 [appsettings.sample.json](../src/Locus/appsettings.sample.json)，用于用 `jsonc` 注释的方式说明配置文件里各个字段的作用、使用方式和主要取舍。

## 使用说明

- 配置根节点是 `Locus`。
- 时间类型使用 .NET `TimeSpan` 字符串格式，例如 `00:05:00` 表示 5 分钟，`1.00:00:00` 表示 1 天。
- 布尔值使用 `true` / `false`。
- 下面的 JSONC 适合阅读和维护说明；实际运行用 JSON 不支持注释，请以 [appsettings.sample.json](../src/Locus/appsettings.sample.json) 为准。
- 当前样例偏向高吞吐图片写入和目录导入场景，其中 `AckMode = Async`、部分卷 `ForceFlushAfterWrite = false` 都属于吞吐优先配置。生产环境如果更看重成功返回后的落盘语义，应按注释切回更稳妥的取值。

当 `LocusOptions` 或它引用的任意子 `Options` 结构发生变更时，需要同步更新：

- [appsettings.sample.json](../src/Locus/appsettings.sample.json)
- [appsettings.json](../samples/Locus.Sample.Console/appsettings.json)
- [appsettings-sample-reference.md](./appsettings-sample-reference.md)
- [statistics.md](./statistics.md)

## 完整配置示例（JSONC 注释版）

```jsonc
{
  "Locus": {
    // 元数据根目录。每个租户的 metadata.db 等投影数据会写到这里。
    // 可使用相对路径或绝对路径；相对路径按应用工作目录解析。
    "MetadataDirectory": "./locus-metadata",

    // 配额数据根目录。租户/目录配额相关 SQLite 数据会写到这里。
    "QuotaDirectory": "./locus-quota",

    // FileWatcher 运行时配置目录。FileWatcherOptionsManager 会在这里保存 watcher 全局选项。
    "FileWatcherConfigurationDirectory": "./locus-watchers",

    // 是否允许首次访问未知租户时自动创建租户。
    // false：必须预置或手动创建租户，生产环境更稳妥。
    // true：适合样例、测试或明确允许动态租户的场景。
    "AutoCreateTenants": false,

    // 全局默认租户文件数配额。租户未设置 Quota 时使用该值。
    // 0 表示不限制；大于 0 表示最多允许的文件数；不能为负数。
    "DefaultTenantQuota": 1000000,

    "MetadataRepository": {
      // 是否启用 metadata 写后异步持久化。
      // true：热路径先更新内存/队列，再由后台批量写入 SQLite，吞吐更好。
      // false：更接近同步持久化，通常只用于排障或特殊一致性验证。
      "EnableBackgroundPersistence": true,

      // 后台持久化队列最大容量。写入峰值越高，可以适当调大。
      // 队列过小会让前台写入更早感受到背压；过大则会占用更多内存。
      "MaxQueueSize": 500000,

      // 每轮后台落盘最多处理的 metadata 操作数量。
      // 调大可提升批量写入吞吐，但会增加单轮 SQLite 工作时间。
      "DrainBatchSize": 8000,

      // 队列达到该百分比后，开始更积极地合并同一文件的重复 metadata 写入。
      // 取值范围 1-100；越低越早合并，越高越接近只在高压力下合并。
      "SoftMergeThresholdPercent": 80,

      // 启动时从 SQLite 加载 active 文件索引的批大小。
      // 调大可缩短大量 active 数据的恢复时间，但会增加单批内存和数据库压力。
      "StartupLoadBatchSize": 8000,

      // 应用优雅停机时，等待后台持久化队列排空的最长秒数。
      // 调大可降低停机时未落盘 metadata 造成孤儿文件的概率。
      "ShutdownDrainTimeoutSeconds": 180,

      // 后台持久化循环在无新事件时的检查间隔，单位是秒。
      // 值越小，合并后的写入越快落盘；值越大，空闲时后台唤醒更少。
      "PersistenceIntervalSeconds": 1
    },

    "StoragePool": {
      // 完成态防重保护的分片数。高并发完成不同 fileKey 时，较大值可降低锁竞争。
      "CompletionGuardStripeCount": 4096,

      // 当取 pending 文件为空时，同步回收已超时 Processing 文件的批大小。
      // 0 表示这条空队列路径不做同步回收。
      "EmptyQueueTimedOutReclaimBatchSize": 32,

      // 正常仍能取到 pending 文件时，后台渐进回收超时 Processing 文件的批大小。
      // 0 表示关闭这类后台渐进回收。
      "BackgroundTimedOutReclaimBatchSize": 8,

      // 同一租户两次后台超时回收之间的最小间隔。
      "TimedOutReclaimCooldown": "00:00:30",

      // 是否启用低频后台超时回收。
      // true 可以让超时 Processing 文件在队列未空时也逐步回到 Pending。
      "EnableBackgroundTimedOutReclaim": true
    },

    "Statistics": {
      // 是否启用 Locus 进程内统计聚合。
      // false：注册 no-op recorder，热路径只保留极低成本调用；默认关闭。
      // true：在内存中按时间窗口聚合写入、读取、取走、SQLite 落盘、watcher 等统计。
      "Enabled": false,

      // 单个统计时间桶大小。
      // 用于把事件归入固定窗口，例如 00:05:00 表示按 5 分钟窗口聚合。
      // 文件写入、取走、watcher 导入等通常更适合分钟级趋势观察，而不是秒级抖动。
      "WindowSize": "00:05:00",

      // 内存统计桶保留时长。
      // 查询范围超过该时长时，旧桶可能已经被裁剪。
      "Retention": "01:00:00",

      // 进程内统计最多保留的时间桶和维度组合数量。
      // 达到上限后会丢弃新的组合，已有组合继续聚合，避免高基数维度造成内存无限增长。
      "MaxSeries": 16384,

      "Output": {
        // 是否由 Locus 自己周期性输出统计摘要。
        // false：宿主应用可通过 ILocusStatisticsReader 主动查询并自行输出。
        // true：注册后台输出服务，按 Interval 查询 QueryWindow 并写入配置的 sink。
        "Enabled": false,

        // 输出目标。当前内置支持 Logging。
        "Sink": "Logging",

        // Locus 自输出间隔。
        "Interval": "00:05:00",

        // 每次输出统计覆盖的时间范围。
        // 默认看最近 15 分钟，能更平滑地反映文件处理吞吐趋势。
        "QueryWindow": "00:15:00",

        // 是否输出全 0 的空统计摘要。
        "IncludeEmptySnapshots": false
      },

      "Dimensions": {
        // 是否保留 tenant_id 维度。
        // 默认 false，避免租户数量大时造成高基数和额外内存占用。
        "TenantId": false,

        // 是否保留 volume_id 维度。
        "VolumeId": true,

        // 是否保留 watcher_id 维度。
        "WatcherId": true,

        // 是否保留 operation 维度。
        "Operation": true
      }
    },

    "QueueEventJournal": {
      // durable queue 根目录。每个租户会在这里维护 queue.log、state、cursor、snapshot 等文件。
      "QueueDirectory": "./locus-queue",

      // 是否启用 durable queue journal。
      // 生产环境建议保持 true；关闭前必须同时显式允许 legacy non-journal 模式。
      "Enabled": true,

      // 是否允许旧版 non-journal 执行路径。
      // 仅用于兼容或测试；正常部署建议 false，避免绕过 journal durability/recovery 模型。
      "AllowLegacyNonJournalMode": false,

      // 是否启用后台投影服务。
      // true：queue.log 会持续投影到 metadata/quota SQLite 和内存状态。
      // false：通常只用于非常特殊的维护场景。
      "EnableProjection": true,

      // 新建或空 journal 使用的编码格式。
      // BinaryV1：推荐格式，长度前缀 + CRC，吞吐和分配表现更好。
      // JsonLines：文本格式，主要用于兼容老 journal 或人工排障。
      // 已存在且非空的 queue.log 会自动探测格式并继续沿用，不会在同一文件内混写。
      "JournalFormat": "BinaryV1",

      // journal append 的确认策略。
      // Durable：写入并 flush 后才返回，成功返回后的落盘语义最稳。
      // Balanced：写入后允许短窗口合并 flush，吞吐更高，存在很小未 flush 风险窗口。
      // Async：入队后即可返回，吞吐最高，但成功返回不代表 journal 已落盘。
      // 当前样例选择 Async，偏吞吐/压测；生产环境通常应优先评估 Durable 或 Balanced。
      "AckMode": "Async",

      // queue.state.json 的防抖落盘间隔。
      // 这只影响 state 文件刷新频率，不改变 Durable 模式下 journal flush 后才确认的语义。
      // 设为 00:00:00 可每次 append 后立即刷新 state 文件，但 I/O 更多。
      "StateFlushDebounce": "00:00:01",

      // 单租户 writer 发现有积压时，最多等待多久以合并微批。
      // 值越大越容易合批，单条低延迟会变差；图片 burst 写入可适当提高。
      "Linger": "00:00:00.005",

      // 单个 journal append 微批最多合并多少条记录。
      "MaxBatchRecords": 32,

      // 单个 journal append 微批最多合并多少序列化字节。
      "MaxBatchBytes": 1048576,

      // 单租户 writer 空闲多久后关闭长期打开的 FileStream。
      // 调大可减少频繁打开文件，调小可减少空闲租户占用句柄。
      "WriterIdleTimeout": "00:02:00",

      // 仅当 AckMode = Balanced 时生效，表示写入后最多延后多久再 flush。
      // 窗口越大越利于合并 flush，但宕机时未 flush 的风险窗口也越大。
      "BalancedFlushWindow": "00:00:00.008",

      // projector 单轮对单个租户最多投影多少条 journal 记录。
      "MaxRecordsPerTenantPerCycle": 1024,

      // projector 单轮最多处理多少个租户。
      "MaxTenantsPerCycle": 32,

      // 上一轮仍有工作时，下一轮投影前等待多久。
      // 值越小追赶越快，后台 CPU/IO 唤醒越频繁。
      "BusyCycleDelay": "00:00:00.100",

      // 当前没有可投影工作时，下一轮检查前等待多久。
      "IdleCycleDelay": "00:00:03",

      // 单轮投影允许持续工作的最长时间。
      // 用于避免一个忙租户长期占住投影循环。
      "MaxProjectionTimePerCycle": "00:00:05",

      // 是否在租户投影追上 journal tail 后自动刷新快照。
      // 快照可缩短启动或恢复时的 journal 重放成本。
      "EnableAutomaticSnapshots": true,

      // 同一租户两次自动快照之间的最小间隔。
      "AutomaticSnapshotInterval": "00:05:00",

      // 距离上次快照至少新增多少已投影字节后，才值得刷新自动快照。
      "MinBytesBeforeAutomaticSnapshot": 8388608,

      // 是否启用已投影 journal 前缀压缩。
      // true 可以避免 queue.log 长期无限增长。
      "EnableCompaction": true,

      // 至少累计多少已处理字节后，才考虑压缩 journal。
      "MinBytesBeforeCompaction": 33554432
    },

    // 启动时是否检查 metadata/quota 数据库健康状态。
    "EnableDatabaseHealthCheck": true,

    // 启动健康检查发现数据库损坏时，是否自动尝试重建/恢复。
    "AutoRecoverCorruptedDatabasesOnStartup": true,

    // 自动恢复失败后是否让应用启动失败。
    // true：避免系统带着损坏数据库继续运行；false：记录错误后继续启动。
    "FailFastOnStartupRecoveryFailure": true,

    "Sqlite": {
      // SQLite journal mode。推荐 WAL，支持写入期间并发读取，并提供较好的崩溃恢复表现。
      // 可选值包括 WAL、DELETE、TRUNCATE、PERSIST、MEMORY、OFF。
      "JournalMode": "WAL",

      // SQLite synchronous mode。
      // NORMAL：吞吐更好，适合大多数 WAL 场景。
      // FULL/EXTRA：更强同步语义，I/O 成本更高。
      // OFF：不建议生产使用。
      "SynchronousMode": "NORMAL",

      // SQLite page cache 大小。负数表示单位是 KB；正数表示页数。
      // -32768 表示每连接约 32MB cache。
      "CacheSizeKb": -32768,

      // 数据库被其他写入占用时，最长等待毫秒数。
      "BusyTimeoutMs": 30000,

      // 每次批量提交后是否主动执行 WAL checkpoint。
      // false：通常吞吐更好；true：可抑制 WAL 文件增长，但会增加每批 I/O。
      "CheckpointAfterBatch": false
    },

    "RetryPolicy": {
      // 文件处理失败后的最大重试次数。
      "MaxRetryCount": 5,

      // 第一次重试前的等待时间。
      "InitialRetryDelay": "00:00:03",

      // 是否使用指数退避。true 时，后续重试间隔会逐步增加。
      "UseExponentialBackoff": true,

      // 指数退避允许达到的最大重试间隔。
      "MaxRetryDelay": "00:10:00"
    },

    "Volumes": [
      {
        // 存储卷唯一 ID。metadata 会记录文件所在 VolumeId，修改或删除前要考虑历史数据。
        "VolumeId": "vol-001",

        // 存储卷挂载路径。LocalFileSystem 卷会把物理文件写到该目录下。
        "MountPath": "./storage/volume-1",

        // 卷类型。目前公共样例使用 LocalFileSystem。
        "VolumeType": "LocalFileSystem",

        // 文件目录分片深度。
        // 0：不分片；1：00-ff 一级；2：两级 00/00-ff/ff；3：三级。
        // 大量文件场景推荐 2，避免单目录文件过多。
        "ShardingDepth": 2,

        // 应用启动后，首次健康检查前等待的毫秒数。
        // 网络盘、K8s PVC 等可能需要更长挂载准备时间。
        "InitialDelayMs": 1000,

        // 卷未就绪时，健康检查重试间隔毫秒数。
        "HealthCheckDelayMs": 1000,

        // FileStream 写入缓冲区大小，单位字节。
        "WriteBufferSize": 131072,

        // 从输入流复制到卷时使用的池化缓冲区大小，单位字节。
        "CopyBufferSize": 262144,

        // 写完文件后是否强制 flush 到磁盘。
        // false：吞吐更好，但成功返回后的物理落盘语义较弱。
        // true：更稳妥，I/O 成本更高。生产可按卷的重要性分别配置。
        "ForceFlushAfterWrite": false
      },
      {
        "VolumeId": "vol-002",
        "MountPath": "./storage/volume-2",
        "VolumeType": "LocalFileSystem",
        "ShardingDepth": 2,
        "InitialDelayMs": 1000,
        "HealthCheckDelayMs": 1000,
        "WriteBufferSize": 131072,
        "CopyBufferSize": 262144,

        // 该卷示例选择 true，用来展示更强文件落盘语义的配置方式。
        "ForceFlushAfterWrite": true
      }
    ],

    "Tenants": [
      {
        // 启动时预置的租户 ID。
        "TenantId": "tenant-001",

        // 是否启用该租户。false 时，文件写入/读取等操作应被拒绝。
        "Enabled": true,

        // 该租户专属文件数配额。
        // null：使用 DefaultTenantQuota；0：该租户不限制；大于 0：该租户上限。
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
        // watcher 唯一 ID，用于注册、日志和运行时管理。
        "WatcherId": "watcher-vip-tenant-001",

        // 单租户模式下绑定的租户 ID。
        // MultiTenantMode=false 时必须指向实际租户；MultiTenantMode=true 时通常留空。
        "TenantId": "tenant-001",

        // false：整个 WatchPath 归属于 TenantId。
        // true：WatchPath 的一级子目录会被识别为租户 ID，例如 ./watch/shared/tenant-001。
        "MultiTenantMode": false,

        // 多租户模式下，是否为系统内已有租户自动创建 watcher 子目录。
        // 单租户模式下通常保持 false。
        "AutoCreateTenantDirectories": false,

        // 自动创建/发现租户目录时的租户列表缓存时间。
        "AutoCreateTenantDirectoriesCacheTtl": "00:01:00",

        // 监听根目录。
        "WatchPath": "./watch/vip/tenant-001",

        // 是否启用该 watcher。
        "Enabled": true,

        // 是否递归扫描子目录。
        "IncludeSubdirectories": true,

        // 允许导入的文件名模式。为空时表示全部文件；这里限制为常见图片格式。
        "FilePatterns": [
          "*.jpg",
          "*.jpeg",
          "*.png",
          "*.webp",
          "*.tif",
          "*.tiff",
          "*.bmp"
        ],

        // 导入成功后如何处理源文件。
        // Delete：删除源文件；Move：移动到 MoveToDirectory；Keep：保留源文件。
        // 使用 Move 时需要同时配置 MoveToDirectory。
        "PostImportAction": "Delete",

        // 目录扫描间隔。
        "PollingInterval": "00:00:20",

        // 单文件最大导入字节数。0 表示不限制。
        "MaxFileSizeBytes": 536870912,

        // 文件至少存在多久才允许导入，用于避开仍在写入中的文件。
        "MinFileAge": "00:00:03",

        // 稳定性检查的第二次探测延迟。小于等于 0 可关闭延迟探测。
        "FileStabilityCheckDelay": "00:00:00.200",

        // 文件年龄超过该阈值后，跳过延迟稳定性检查，只做可访问性检查。
        "SkipStabilityCheckAfterAge": "00:02:00",

        // 单个 watcher 最大并发导入数。调大可提升吞吐，也会增加磁盘和 CPU 压力。
        "MaxConcurrentImports": 16,

        // 是否节流 imported history 的过期清理。
        "EnableImportedFilesPruneThrottle": true,

        // imported history 两次过期清理之间的最小间隔。
        "ImportedFilesPruneInterval": "00:05:00",

        // 是否对 imported history 落盘做防抖。
        "EnableImportedFilesHistoryFlushDebounce": true,

        // imported history 两次落盘之间的最小间隔。
        "ImportedFilesHistoryFlushInterval": "00:00:02"
      },
      {
        "WatcherId": "watcher-all-regular-tenants",

        // 多租户 watcher 通常留空，由一级子目录名推断租户。
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
        "PollingInterval": "00:00:30",
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
      // 是否启用孤儿文件恢复服务。
      // 孤儿文件通常指物理文件存在，但 metadata/quota 投影缺失或不一致的情况。
      "Enabled": true,

      // 启动后是否先执行一轮恢复。
      "RunOnStartup": true,

      // 周期性孤儿恢复扫描间隔。
      "RecoveryInterval": "00:15:00",

      // 启动后首次恢复前等待时间，用于给存储卷挂载和服务初始化留出窗口。
      "InitialDelay": "00:03:00"
    },

    "CleanupOptions": {
      // 是否启用后台清理 hosted service。
      "Enabled": true,

      // 后台清理主循环间隔。
      "CleanupInterval": "00:10:00",

      // 启动后首次清理前等待时间。
      "InitialDelay": "00:03:00",

      // 是否清理处理超时的 Processing 文件。
      // true 时，超过 ProcessingTimeout 的文件会被重置回 Pending。
      "CleanupTimedOutFiles": true,

      // 是否递归清理 Thumbs.db、.DS_Store、desktop.ini 等常见垃圾文件。
      "CleanupJunkFiles": true,

      // 垃圾文件递归扫描最小间隔。null 或 0 表示每轮 cleanup 都扫描。
      "JunkFileCleanupInterval": "00:20:00",

      // 文件处于 Processing 状态超过该时间后，视为处理超时。
      "ProcessingTimeout": "00:20:00",

      // PermanentlyFailed 文件超过保留期后的处置方式。
      // Keep：保留原位，等待人工处理。
      // MoveToDeadLetter：移动到 DeadLetter 区域并从 active quota 中移除。
      // Delete：物理删除并清理 metadata。
      "PermanentlyFailedDisposition": "MoveToDeadLetter",

      // 是否清理 SQLite 损坏恢复时留下的 *.corrupted.* 备份文件。
      "CleanupInvalidDatabaseBackups": true,

      // PermanentlyFailed 文件在执行上面处置策略前保留多久。
      "FailedFileRetentionPeriod": "1.00:00:00",

      "DeadLetter": {
        // dead-letter 根目录。相对路径会解析到对应存储卷 MountPath 下。
        "RootPath": ".deadletter",

        // dead-letter 路径中是否包含租户 ID。
        "IncludeTenantInPath": true,

        // dead-letter 路径中是否包含 yyyyMMdd 日期分区。
        "IncludeDatePartition": true,

        // dead-letter 区域内的分片深度。
        "ShardingDepth": 2
      },

      "RetiredVolumes": [
        {
          // 明确声明已经退役的历史卷 ID。
          // 只从 Volumes 移除卷不会自动清理历史 metadata；未声明的未知卷会被保护性跳过。
          "VolumeId": "vol-retired-example",

          // 退役卷 metadata 处理方式。
          // Keep：保留 metadata，并继续跳过涉及该卷的物理清理。
          // PurgeMetadataOnly：只清理 metadata/quota，不访问历史物理卷。
          "Disposition": "Keep"
        }
      ],

      // 是否清理 Completed 文件。
      "CleanupCompletedFiles": true,

      // Completed 文件保留期。00:00:00 表示下一轮 cleanup 即可删除。
      "CompletedFileRetentionPeriod": "00:00:00",

      // 是否周期性优化 SQLite 数据库。
      // 优化会通过 VACUUM 等维护操作回收空间，属于较重 I/O。
      "OptimizeDatabases": true,

      // 数据库优化周期，独立于 CleanupInterval。
      "DatabaseOptimizationInterval": "1.00:00:00",

      // 单租户单轮清理批大小。
      // Completed / DeleteRequested / PermanentlyFailed 等冷状态清理会按该大小分页读取。
      "CleanupBatchSizePerTenant": 5000,

      // 单轮孤儿文件扫描上限。小于等于 0 表示不限制。
      "MaxOrphanFilesPerRun": 20000,

      // 孤儿恢复中物理路径查找缓存大小。0 表示禁用缓存。
      "OrphanRebuildLookupCacheSize": 32768,

      // 数据库优化时，每批处理多少个租户数据库后暂停。
      "DatabaseOptimizationTenantBatchSize": 10,

      // 数据库优化批次之间的暂停时间，用于降低持续 I/O 压力。
      "DatabaseOptimizationPauseBetweenBatches": "00:00:00.500"
    }
  }
}
```

## 快速调参提示

- 更稳妥：把 `QueueEventJournal.AckMode` 改为 `Durable`，并把关键卷的 `ForceFlushAfterWrite` 设为 `true`。
- 更高吞吐但保留较小 flush 风险窗口：使用 `AckMode = Balanced`，再根据压测调整 `BalancedFlushWindow`。
- 更偏压测/低一致性要求：可以使用 `AckMode = Async`，但要接受成功返回时 journal 可能尚未落盘。
- SQLite 更强落盘语义：把 `Sqlite.SynchronousMode` 从 `NORMAL` 改为 `FULL` 或 `EXTRA`，代价是写入吞吐下降。
- 统计默认关闭，只有确实需要按时间窗口观察写入 MB/s、文件取走数量、SQLite 落盘量或 watcher 导入量时再启用 `Statistics.Enabled`。
- 维度会影响内存占用和基数，租户数量较大时建议保持 `Statistics.Dimensions.TenantId = false`。
- 如果希望 Locus 自己周期性写统计摘要，启用 `Statistics.Output.Enabled`；如果宿主应用已有监控/日志出口，则优先通过 `ILocusStatisticsReader` 主动获取后自行输出。
- 默认统计窗口偏向文件处理趋势观察：5 分钟桶、15 分钟输出窗口、1 小时保留。排障时如果需要更实时的波动，可以临时缩短 `Statistics.WindowSize` 和 `Statistics.Output.QueryWindow`。
