# Doris 外表 Metadata Planning 竞品实现分析（StarRocks / Trino）

## 1. 为什么要写这份文档

在 Doris 查询超大 Iceberg/Paimon 表时，`plan` 阶段需要读取和处理大量 metadata（manifest list、manifest、split 枚举、统计信息等）。当表足够大，planning 的 CPU 与 I/O 压力会集中到 FE，进而放大查询起始延迟。

Doris 目前已经存在两条技术路径：

1. 分布式 plan：把 metadata 任务切分并下放到 BE 并行执行。
2. FE 并行 plan：planning 主体仍在 FE，通过线程池、缓存、惰性 split 供给等方式加速。

这两条路径在竞品中都有对应实现。本调研的目标不是堆代码证据，而是解释竞品“设计是怎么想的、实现是怎么落的、收益和边界是什么”，并把这些结论转回 Doris 的技术选型。

---

## 2. 评估口径（简化但可落地）

为了避免“名词相似但机制不等价”，本文按以下维度判断：

1. 规划执行位点：metadata 主要由 coordinator 还是 worker 处理。
2. 触发机制：显式模式开关、自动阈值，还是纯参数调优。
3. 任务抽象：plan 任务如何建模、如何切分、如何在执行面消费。
4. 性能收益路径：是压 FE、提并行，还是减少重复读取。
5. 失败模型与回退：失败域在哪里、能否回退、排障复杂度多大。

---

## 3. StarRocks：显式分布式 plan（Iceberg）

### 3.1 设计思路

StarRocks 在 Iceberg 上把 metadata planning 做成 `local / distributed / auto` 三态。核心意图很明确：

1. 小 metadata 用 `local`，让 FE 利用缓存快速完成规划，避免分布式调度开销。
2. 大 metadata 用 `distributed`，把重 I/O 和过滤逻辑下放到 BE/CN。
3. `auto` 在两者之间做动态决策，避免用户长期手工配置。

它的价值不是“FE 并行”，而是“planning 执行位点迁移”。

### 3.2 FE 如何构建 plan 任务

StarRocks 并没有让 FE 直接跑一个“分布式 metadata RPC”。它采用“内部查询任务化”的方式：

1. FE 在 Iceberg scan 的 `doPlanFiles()` 决策到 remote 分支后，构建一个 `MetadataCollectJob`。
2. `MetadataCollectJob` 生成一条内部 SQL（查询 `logical_iceberg_metadata`），并用内部 `StatementPlanner` 编译成执行计划。
3. 该执行计划异步执行，结果被写入一个结果队列供后续消费。

直观理解：StarRocks 把 metadata planning 转化为“可调度的内部查询任务”，而不是在 FE 线程里一次性做完。

### 3.3 任务抽象如何建模

StarRocks 的任务抽象是分层的：

- `MetadataCollectJob`：控制面抽象，包含 catalog/db/table/snapshot/predicate/sinkType 与结果队列。
- `SerializedMetaSpec`：计划规格抽象，把“要扫什么”和“怎么分片”拆开建模。
- `RemoteMetaSplit`：分片抽象，描述每个 metadata split 的序列化内容、长度、路径。
- `TMetadataEntry/TIcebergMetadata`：结果抽象，BE 侧把每一行 metadata 结构化返回给 FE。
- `MetadataParser + AsyncIterable`：消费抽象，把结果流式还原为 `FileScanTask`，继续后续 scan pipeline。

这样的分层保证了 planning/执行解耦：FE 只负责编排与汇总，BE 负责读取与过滤。

### 3.4 BE 如何执行 metadata 任务

执行面在 BE/JNI：

1. FE 下发 `serialized_split / serialized_table / serialized_predicate / scanner_type`。
2. BE 通过 JNI scanner 工厂构建 Iceberg metadata scanner。
3. Scanner 读取 manifest，应用 predicate 过滤，将结果序列化为 `TIcebergMetadata` 写回 FE。

这一步的关键点在于：BE 在执行真正的 manifest 读取与过滤，而 FE 只是消费者与汇总者。

### 3.5 收益与边界

收益路径：

1. FE 从“执行者”变成“编排者”，显著降低 FE I/O/CPU 热点。
2. metadata I/O 从单点 FE 扩展到多 BE 并行，尾延迟更可控。
3. 对大表场景更可扩展，扩容直接提升 planning 吞吐。

边界：

1. 该机制在 Iceberg 上完整实现。
2. Paimon 侧主要是 manifest cache 等优化，并未体现同级 `plan_mode` 语义。

---

## 4. Trino：coordinator 并行 plan（Iceberg）

### 4.1 设计思路

Trino 的架构约束是：coordinator 负责 planning 和 split 组织，worker 执行 split。因此它的优化方向不是“把 planning 下放给 worker”，而是“让 coordinator 变快、更并行、更流水”。

这体现在三个设计点：

1. 规划并行度通过线程池参数化。
2. metadata 缓存在 coordinator 内存。
3. split 生成与调度采用拉式 pipeline（SplitSource），避免一次性全量生成。

### 4.2 并行 plan 的任务抽象

Trino 把 planning 抽象成 `ConnectorSplitSource` 拉模式：

1. `SplitManager` 调用 connector 的 `ConnectorSplitManager#getSplits()`。
2. Iceberg connector 返回 `IcebergSplitSource`，按批次 `getNextBatch()` 生成 split。
3. 内部用 `FileScanTaskWithDomain` 承载 file task 与统计域，结合 dynamic filter 做裁剪。

这个模式把“planning 产出”和“调度消费”变成流水线，而不是 FE 一次性产出全部 splits。

### 4.3 自实现 vs 调用 Iceberg/Paimon 接口

Trino 的并行 plan 有明确边界：

- 调用 Iceberg API 的部分：`newScan() / planWith(executor) / scan.planFiles() / FileScanTask.split()`。
  也就是说，metadata 枚举和任务切分核心逻辑来自 Iceberg 库。
- Trino 自实现的部分：split source 生命周期、dynamic filter 融合、统计剪枝、split 权重、批次调度与异常处理。
- Paimon：Trino 主线没有内置 connector，因此不存在“Trino 核心对 Paimon 的并行 plan 实现”。若使用外部插件，其能力不属于 Trino 主线机制。

### 4.4 收益与边界

收益路径：

1. 不改变职责边界，迭代风险低。
2. coordinator 并行与缓存对中大型表收益稳定。
3. split 生成流水化后，查询起跑阶段阻塞减少。

边界：

1. coordinator 仍是 metadata 压力中心，极端大 metadata 时上限受限。
2. Paimon 不在主线 connector 范围内，无法直接对标 Iceberg。

---

## 5. Doris 现状映射（两条路径并存）

### 5.1 分布式 plan 路径（MetadataScanNode）

Doris 在 `MetadataScanNode` 路径上已经具备“metadata split 下发到 BE 执行”的能力：

1. FE 生成 serialized splits。
2. FE 按 BE 并发切分并分配 scan range。
3. BE/JNI scanner 执行 metadata 读取与过滤。

这条路径的机制更接近 StarRocks 的 distributed 模式。

### 5.2 FE 并行 plan 路径（FileQueryScanNode）

在 `FileQueryScanNode + IcebergScanNode/PaimonScanNode` 路径上，planning 主体仍在 FE：

1. FE 调用外表 SDK `planFiles()` 或 `plan().splits()`。
2. 结合线程池、manifest cache、batch split 懒供给降低阻塞。
3. BE 主要执行数据 split，不承担 metadata planning 主体。

这条路径的机制更接近 Trino 的 coordinator 并行方案。

---

## 6. Doris 两种方案优缺点对比

| 维度 | 分布式 plan | FE 并行 plan |
| --- | --- | --- |
| 性能上限 | 高。可随 BE 数量扩展 metadata 并行度 | 中。最终受 FE 资源上限约束 |
| FE 压力 | 低。FE 主要编排与汇总 | 中高。FE 仍承担 planning 主体 |
| 实现复杂度 | 高。跨 FE/BE/JNI 协同 | 低到中。主要是 FE 内并行与缓存 |
| 失败域 | 分布式失败域更广 | 失败集中在 FE，排障更简单 |
| 迭代速度 | 较慢 | 较快 |
| 适用场景 | 超大 metadata / FE 瓶颈明显 | 中大规模 metadata / 追求低风险快收益 |

现实约束：

1. Doris 分布式 plan 已有基础，但覆盖路径尚未统一。
2. FE 并行 plan 更成熟，适合作为默认与兜底路径。

---

## 7. 设计建议（面向 Doris）

建议维持“双轨 + 自动切换”策略：

1. 模式语义：`local / distributed / auto`。
2. 默认策略：`auto`。
3. 自动判定信号：manifest 数、metadata 总字节、历史 planning P95、近期失败率。
4. 回退策略：distributed 失败自动回退 local（可配置是否强制失败）。

分阶段落地：

1. Phase-1：统一 Iceberg 两条路径到同一决策入口。
2. Phase-2：补齐 Paimon 在 metadata planning 上的统一语义。
3. Phase-3：抽象外表统一 metadata planning framework，减少 connector 重复造轮子。

---

## 8. 关键实现锚点（精简）

只保留少量关键定位，便于快速对齐实现语义：

1. StarRocks：`PlanMode`、`StarRocksIcebergTableScan#doPlanFiles/shouldPlanLocally`、`MetadataCollectJob`。  
2. Trino：`SplitManager#getSplits`、`IcebergSplitSource#getNextBatchInternal(scan.planFiles())`。  
3. Doris：`MetadataScanNode#createScanRangeLocations`、`IcebergScanNode/PaimonScanNode#getSplits`。

