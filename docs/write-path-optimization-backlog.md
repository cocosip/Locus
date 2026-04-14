# Locus 写入链路后续优化清单

## 文档目的

这份文档用于沉淀当前写入链路在性能方面仍值得继续投入的方向，以及暂时不建议继续深挖的方向。

目标不是一次性列出所有可能性，而是给后续迭代提供一个统一的工程判断基线：

- 哪些点更可能带来真实收益
- 哪些点已经验证过收益有限或波动过大
- 后续优化前应该先看哪些监控和 benchmark

## 当前共识

截至目前，`LocalFileSystemVolume` 已经完成以下几类优化和观测补强：

- `MemoryStream` 公开缓冲直写 fast path
- `FileStream` 的同步 seekable file copy path
- `LocalFileSystemVolumeMetrics`
- `StoragePoolMetrics`
- `QueueJournalMetrics`
- `QueueJournalOperationMetrics`

当前更重要的事情，已经不再是继续微调单个 copy buffer，而是基于可观测数据决定下一步要优化哪一段主链路。

## 不建议继续优先投入的方向

### 1. 继续深挖 `LocalFileSystemVolume` 的 copy buffer 微调

原因：

- 多轮 benchmark 显示该方向结果容易受磁盘缓存、临时目录状态、冷热态切换影响
- 某些组合会改善大文件，但同时拖慢中等文件
- 当前已经进入明显的边际收益递减区间

结论：

- 除非新的真实业务数据表明 `volume_write` 仍绝对主导且瓶颈明确来自 copy loop
- 否则不建议把后续主要时间继续投入到 `256KiB / 512KiB / 1MiB` 这一类 buffer 调参

### 2. 重新尝试 `Accepted` 与物理写完全并发

原因：

- 之前针对真实 file source 的 benchmark 已经表明，这条路径多数情况下会退化
- journal append 和物理写很容易争用同一块磁盘/缓存

结论：

- 不建议直接把物理写与 `Accepted` journal append 做默认并发
- 如果未来要重试，必须先有新的 workload 假设或新的存储介质前提

## 建议优先推进的优化方向

### 1. `Queue journal` durable ack 路径

这是当前最值得继续投入的方向。

原因：

- 在 `1MiB` 左右的 durable 写入场景中，`Queue journal` 经常仍占请求总时延的明显比例
- 相比继续抠 `LocalFileSystemVolume` 的 copy 细节，这一段更可能提供稳定且可复用的收益

重点关注：

- `QueueEventJournalAckMode.Durable` 与 `Balanced` 的实际代价差异
- append batch 大小是否足够
- flush 触发频率是否过高
- 单 tenant 写入是否形成小 batch、频繁 flush

建议先看这些指标：

- `locus.queue_journal.append.batch.count`
- `locus.queue_journal.append.record.count`
- `locus.queue_journal.append.request_batch_size`
- `locus.queue_journal.append.bytes`
- `locus.queue_journal.append.duration`
- `locus.queue_journal.append.deferred_ack.count`
- `locus.queue_journal.flush.count`
- `locus.queue_journal.flush.duration`

如果这些指标显示：

- batch 很小
- flush 次数很高
- append duration 明显抬升

那么优先考虑：

- 调整 `BalancedFlushWindow`
- 调整 `MaxBatchRecords`
- 调整 `MaxBatchBytes`
- 评估特定场景下是否允许 `Balanced` 作为可选模式

### 2. `StoragePool.WriteFileAsync` 的 accepted path

这是第二优先级。

当前主链路大致分为：

1. tenant quota
2. directory quota
3. volume write
4. acceptance
5. projection enqueue

建议优先看这些指标：

- `locus.storage_pool.write.duration`
- `locus.storage_pool.write.tenant_quota.duration`
- `locus.storage_pool.write.directory_quota.duration`
- `locus.storage_pool.write.volume_write.duration`
- `locus.storage_pool.write.acceptance.duration`
- `locus.storage_pool.write.projection_enqueue.duration`

如果 `acceptance.duration` 长期偏高，应优先判断：

- 慢的是 `AppendAcceptedQueueEventAsync`
- 还是 fallback projection path

可探索方向：

- 是否能进一步降低 accepted path 上的同步阻塞
- 是否能让部分非关键步骤更晚完成
- 是否能把某些恢复性工作从请求热路径移出

注意：

- 这一类优化必须非常谨慎，不能破坏 durable accepted 语义
- 任何“提前返回”的改动都必须先明确恢复语义和回滚语义

### 3. quota 相关路径

如果后续监控发现：

- `tenant_quota.duration`
- `directory_quota.duration`

在主力 workload 中占比明显上升，那么 quota 也应成为独立优化方向。

可探索方向：

- 降低热路径上的持久化/锁竞争
- 优化投影更新与热缓存一致性成本
- 区分“预占配额”和“最终确认”阶段的热路径负担

### 4. 小文件场景下的固定开销

对于 `100KB ~ 1MB` 主力区间，如果后续发现 `volume_write` 已经不再是绝对主导，而总时延仍偏高，则应该关注固定开销：

- 路径校验
- 目录确保
- 指标打点成本
- 小对象分配
- 请求级状态构造

这类优化的特点是：

- 单项收益不大
- 但对高频中小文件 workload 更贴近真实收益

## 当前建议的推进顺序

建议按下面顺序继续：

1. 先用现有 metrics 观察真实测试或预发环境
2. 优先判断 `StoragePool` 的慢点到底在 `volume_write`、`acceptance` 还是 `projection_enqueue`
3. 如果 `acceptance` 明显偏高，优先做 `Queue journal` 路径优化
4. 只有在 `volume_write` 仍稳定占绝对主导时，才重新回到 `LocalFileSystemVolume` 微调

## 每轮优化前的要求

后续每一轮性能优化，建议都遵守以下约束：

- 先定义目标场景
- 先看现有 metrics
- 再做 benchmark
- 没有稳定收益证据，不进入默认生产路径

建议至少回答以下四个问题：

1. 优化针对的是哪类 workload？
2. 当前慢点是哪一段？
3. 改动后哪项指标发生了改善？
4. 改动是否引入了语义风险、恢复风险或跨 tenant 风险？

## 推荐的下一步

如果后续继续推进性能优化，建议下一轮先围绕以下主题展开：

- `Queue journal` durable 与 balanced 策略对比
- `StoragePool` accepted path 的阶段时延采样
- 请求级失败与重试分布分析

不建议下一轮直接继续做：

- 更激进的 `LocalFileSystemVolume` buffer 调参
- 默认并发化 `Accepted` journal append 与物理写
