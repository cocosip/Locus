本文档说明 `QueueEventJournal` 在当前实现中的观测信号，包括 metrics、状态字段和日志事件，方便接入监控平台或排障时统一参考。

## 目标

当前这套观测信号重点覆盖三类问题：

- journal sequence gap
- corrupt journal tail
- auto-repair / auto-recovery

它们对应的目标分别是：

- 可检测：出现 gap 或坏尾时，系统能留下明确状态和计数
- 可告警：日志平台或指标平台可以据此触发告警
- 可补偿：系统已经具备 orphan recovery 和坏尾自动截断能力

## Metrics

当前模块通过 `System.Diagnostics.Metrics` 暴露一个 meter：

- `Locus.Storage.QueueJournal`

当前可用指标如下。

### Sequence Gap

- `locus.queue_journal.sequence_gap.detected`
  - 含义：projection 流程发现 sequence 不连续的次数
  - 触发时机：`QueueEventProjectionService` 在回放 journal 时发现当前记录的 `SequenceNumber` 大于 `LastSequenceNumber + 1`
- `locus.queue_journal.sequence_gap.recovery_attempted`
  - 含义：gap 发生后，触发 orphan recovery 的次数
  - 触发时机：调用 `RecoverOrphanedFilesAsync` 之前
- `locus.queue_journal.sequence_gap.recovery_failed`
  - 含义：gap 发生后，自动 orphan recovery 失败的次数
  - 触发时机：`RecoverOrphanedFilesAsync` 抛异常时

### Corrupt Tail

- `locus.queue_journal.corrupt_tail.detected`
  - 含义：检测到 journal 坏尾的次数
  - 当前 tags：
    - `format`: `JsonLines` 或 `BinaryV1`
    - `stage`: 当前可能值为 `read`、`state_scan`
- `locus.queue_journal.corrupt_tail.auto_repaired`
  - 含义：坏尾被自动截断修复的次数
  - 当前 tags：
    - `format`: `JsonLines` 或 `BinaryV1`

## 维护态状态字段

除了 metrics，维护态查询 `QueueProjectionTenantState` 也会暴露最近一次 gap / 坏尾诊断结果。

### Gap 相关

- `GapDetected`
  - 是否检测到过 sequence gap
- `LastGapDetectedAtUtc`
  - 最近一次 gap 检测时间
- `GapExpectedSequenceNumber`
  - 最近一次 gap 中“期望看到的 sequence”
- `GapObservedSequenceNumber`
  - 最近一次 gap 中“实际读到的 sequence”

### Journal 健康相关

- `JournalFormat`
  - 当前租户 journal 实际使用的格式
  - 注意：这里反映的是“检测到并实际使用的格式”，不只是配置默认值
- `JournalCorruptTailDetected`
  - 是否检测到过坏尾
- `LastJournalCorruptTailDetectedAtUtc`
  - 最近一次坏尾检测时间
- `LastJournalCorruptTailOffset`
  - 最近一次坏尾起始的逻辑 offset
- `JournalAutoRepairCount`
  - 当前租户 journal 自动修复次数

## queue.state.json

每个租户的 `queue.state.json` 现在除了基础 offset / sequence 状态，还会保存 journal 健康信息。

关键点：

- `Format` 以字符串形式持久化，例如 `BinaryV1`
- `CorruptTailDetected`、`LastCorruptTailDetectedAtUtc`、`LastCorruptTailOffset`、`AutoRepairCount` 会在坏尾检测或自动修复时更新

这意味着：

- 不看日志，也能从状态文件恢复最近一次 journal 健康事件
- projection maintenance 查询可以直接把这些状态向上层暴露

## 日志事件

当前日志中已经为关键路径设置了固定 `EventId`，方便外部日志平台按事件类型做过滤和告警。

### FileQueueEventJournal

- `4101`: 读取时发现坏尾
- `4102`: 自动截断修复坏尾
- `4103`: state 扫描时发现坏尾

### QueueEventProjectionService

- `4201`: 检测到 sequence gap
- `4202`: gap 后自动 orphan recovery 失败

## 告警建议

如果要接 Prometheus / OpenTelemetry / 其他指标平台，建议先从下面几条规则开始：

- `sequence_gap.detected > 0`
  - 建议级别：warning
  - 表示 journal 条目连续性被破坏，需要确认是否存在丢条或外部手工修改
- `sequence_gap.recovery_failed > 0`
  - 建议级别：critical
  - 表示系统已经检测到 gap，但自动补偿失败，通常需要人工介入
- `corrupt_tail.detected > 0`
  - 建议级别：warning
  - 表示 journal 出现了坏尾，虽然系统可能已自动处理，但仍应关注底层磁盘/进程退出稳定性
- `corrupt_tail.auto_repaired` 持续增长
  - 建议级别：warning
  - 表示系统在不断依赖自动修复，通常意味着进程退出、文件系统或底层存储存在持续性问题

## 实际理解方式

推荐把三层信号一起看：

- metrics：适合做趋势统计和告警
- `QueueProjectionTenantState`：适合做运维查询和租户级排障
- `queue.state.json` / 日志：适合做单租户事件回溯

如果三者同时出现异常，优先级建议是：

1. 先看 `sequence_gap.recovery_failed`
2. 再看 `JournalCorruptTailDetected` 和 `LastJournalCorruptTailOffset`
3. 最后结合日志事件和 tenant 物理文件情况确认是否需要人工补救
