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

建议优先使用下面这条 benchmark 命令做本地对比：

```powershell
dotnet run -c Release --project tests/Locus.Benchmarks -- write-path-breakdown --writes 60 --warmup 10 --sizes 262144,1048576,8388608 --ack-mode durable,balanced --include-no-flush false --source file
```

补充一轮 `LocalFileSystemVolume` 写入路径优化后的本地样本（2026-04-15，`BinaryV1`，`file source`，`forceFlushAfterWrite=true`）：

- `write-path-breakdown --writes 120 --warmup 20 --sizes 262144,1048576 --ack-mode durable --include-no-flush false --source file`
- `256KiB`：`Durable` 约 `1.329 ms`，其中 `volume_write` 约 `1.091 ms`，`queue journal` 约 `130.2 us`
- `1MiB`：`Durable` 约 `1.936 ms`，其中 `volume_write` 约 `1.667 ms`，`queue journal` 约 `151.2 us`
- 相比上一轮 queue journal hot path 优化后的样本，`256KiB` 再次明显下降，说明 `volume_write` 侧仍有可收割的固定成本
- `1MiB` 基本持平，说明当前主热点已更偏向真实 `FileStream` copy / 目录创建，而不是 accepted path 或 queue journal

这轮 `LocalFileSystemVolume` 的有效改动点：

- 可见 `MemoryStream` 的异步直写路径不再按 `_copyBufferSize` 分段循环，而是改成单次 `WriteAsync`
- 直写路径只在写入成功后推进 `MemoryStream.Position`，避免异常时源流位置提前漂移
- `FileStream` 只有在 `> 1MiB` 时才走 synchronous seekable file path，`= 1MiB` 会回到 exact-copy 小文件路径
- `MockFileSystem` 检测改为构造期缓存，减少每次创建写入流时的重复类型检查

建议配合下面这条命令单独观察异步直写内存流收益：

```powershell
dotnet run -c Release --project tests/Locus.Benchmarks -- direct-volume-breakdown --writes 120 --warmup 20 --sizes 1048897 --include-no-flush false --stream-modes visible-direct,visible-wrapped
```

当前本地样本显示：

- 在 `1048897 bytes`（略大于 `1MiB`）时，`visible-direct` 已稳定优于 `visible-wrapped`
- 例如 `depth2-random-precreated` 下，`visible-direct` 约 `761.1 us`，`visible-wrapped` 约 `1.989 ms`
- 这说明“大块连续内存被拆成多次 `WriteAsync`”原本确实是一个真实热点
- 但对主链路 `file source` 来说，后续若继续推进，应优先盯住 `FileStream` copy 与 cold directory 成本，而不是回到更激进的 buffer 调参

补充一轮 seekable copy 分界细化后的本地样本（2026-04-15，同样基于 `BinaryV1` + `file source` + `forceFlushAfterWrite=true`）：

- 非 direct-memory 的 seekable stream 在 `<= 2MiB` 时改走 exact-copy path，只有 direct-memory 继续保持 `<= 1MiB` 同步、`> 1MiB` 异步直写
- `write-path-breakdown --writes 120 --warmup 20 --sizes 262144,1048576,2097152 --ack-mode durable --include-no-flush false --source file`
- `256KiB`：约 `1.467 ms`，`volume_write` 约 `1.201 ms`
- `1MiB`：约 `1.864 ms`，`volume_write` 约 `1.561 ms`
- `2MiB`：约 `2.176 ms`，`volume_write` 约 `1.901 ms`
- 对比上一轮样本，`1MiB` 的 file-source 主链路继续下降，说明 `FileStream` / seekable copy 路径仍然存在可收割收益
- `direct-volume-breakdown --sizes 1048897,2097152 --stream-modes visible-direct,visible-wrapped` 也显示 `visible-wrapped` 在 `1MiB~2MiB` 区间明显改善，例如 `1048897 bytes` 的 `depth2-random-precreated` 从约 `1.989 ms` 降到约 `1.185 ms`
- 当前剩余的更稳定热点仍是 cold directory 创建成本，以及更大文件下的真实 `FileStream` copy 成本

完成单条 append hot path 优化后的本地刷新样本（2026-04-15，`BinaryV1`，`file source`，`forceFlushAfterWrite=true`）：

- `256KiB`：`Durable` 约 `1.513 ms`，`Balanced` 约 `1.626 ms`
- `1MiB`：`Durable` 约 `1.900 ms`，`Balanced` 约 `1.870 ms`
- `8MiB`：`Durable` 约 `5.567 ms`，`Balanced` 约 `5.928 ms`
- 相比优化前，这说明 queue journal 单条 append 的固定成本已经明显下降，`Durable` 与 `Balanced` 的差距被大幅缩小
- 在这组更新后的样本里，`volume_write` 又重新回到主导位置，`Queue journal` 已经不再是最突出的单段瓶颈
- 在当前这组更新后的本地样本里，`Balanced` 已经不再呈现稳定优势，因此仍然不建议仅凭本地 benchmark 就切换默认生产 `AckMode`

建议先看这些指标：

- `locus.queue_journal.append.batch.count`
- `locus.queue_journal.append.bytes`
- `locus.queue_journal.append.duration`
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

现有本地窗口扫描样本（2026-04-15，`Balanced`，`file source`，`forceFlushAfterWrite=true`）：

- `BalancedFlushWindow=5ms` 在已扫描的 `0ms / 1ms / 5ms / 10ms` 中更稳
- `0ms` 明显退化，尤其在 `256KiB` 下会把 `Queue journal` 开销抬得更高
- 这说明 `Balanced` 的收益并不是“窗口越小越好”，后续应继续按 workload 做窗口扫描
- 由于后面又做了单条 append hot path 优化，若要决定默认值，建议基于当前代码再补一轮窗口复测

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

如果需要继续拆开 `acceptance` 内部阶段，优先再看这些更细的子阶段指标：


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
