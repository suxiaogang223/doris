# 外表回放异步清理：第一版实现说明

基线：`origin/master`，`b68d1d50522c9f08b53b376dd684a34270c4e608`。日期：2026-10-27。

这是供 review 的代码草稿。按要求未编译，未运行 Checkstyle、单测、回归测试或集群验证。

## 这一版改了什么

保留现有日志格式和传播方式。ALTER、DROP 和改名仍通过日志同步各 FE 的元数据；REFRESH 也仍走原来的日志路径。

| 操作 | 当前线程同步完成 | 后台完成 |
| --- | --- | --- |
| ALTER 属性 | 摘除旧 connector、context、线程池和权限控制器；注销旧缓存及额度；更新属性；失效 FE 对象缓存 | 释放本次注销缓存产生的旧 FileIO，然后关闭旧线程池、connector、context 和权限控制器 |
| DROP Catalog | 标记旧 Catalog 已移除，阻止旧对象再次初始化；摘除旧资源，注销缓存和额度，删除映射；清理原有约束、会话记录和查询统计 | 关闭已摘除的旧资源 |
| Catalog 改名 | 同一次 CatalogMgr 写锁内处理旧运行时摘除和名称切换，保留 Catalog ID；发布新名称前重置本地状态 | 关闭旧实例；任务不再读取 Catalog 的当前资源字段 |
| REFRESH Catalog/Database/Table，以及使用同一缓存的外表 DDL 回放 | 保留 connector 缓存版本切换、旧加载发布校验、FE 缓存失效、数据库回调及 row-count 失效 | Iceberg 表最后一个引用释放后，关闭该旧表拥有的 FileIO，并释放相应 catalog lease |

约束、会话记录和查询统计的清理仍同步执行，并保留在 CatalogMgr 写锁外。不会把 `cleanupRemovedCatalog()` 或数据库对象的删除回调整体提交到后台。

ALTER 原来按属性选择是否重建部分 FE 缓存；这一版在替换运行时时统一注销该 Catalog 的 FE 和 connector 缓存。原因是旧 connector 的后台关闭不能拖延旧额度注销，否则新 connector 使用不同额度时会发生配置冲突。REFRESH 没有改成这种整体注销。

## 调用链

```text
ALTER 回放 / Master 本地执行
  CatalogMgr.applyAlterCatalogProps
    [CatalogMgr 写锁 + Catalog 对象锁]
    ExternalCatalog.resetToUninitialized
      摘除旧资源，保存固定实例
      注销旧 FE/connector 缓存和额度
      更新属性
    [释放 Catalog 对象锁]
    同步失效 FE 对象缓存
  [释放 CatalogMgr 写锁]
  登记旧实例的清理任务
  返回，允许后续日志继续处理

后台回收线程
  清理本次缓存注销产生的旧 FileIO
  关闭旧线程池
  关闭旧 connector
  关闭旧 context
  关闭旧权限控制器
```

```text
DROP / 改名回放
  [CatalogMgr 写锁]
  标记旧对象不可初始化，摘除旧运行时并注销旧缓存
  删除旧映射；改名时重置并注册新名称
  [释放 CatalogMgr 写锁]
  同步清理约束、会话记录、查询统计
  返回，允许后续日志继续处理

后台回收线程
  只清理摘除时保存的旧实例
```

```text
REFRESH / 外表 DDL 的缓存失效
  [当前业务或回放线程]
  切换原有缓存版本，拒绝旧结果回填
  遍历旧条目并释放缓存引用
  保持 FE 数据库回调、派生缓存失效同步执行
  Iceberg TableOwner 最后一个引用释放
    向回收器登记该旧表的资源清理
  返回

后台回收线程
  关闭该旧表拥有的 FileIO
  释放其 catalog lease
```

## 回收器

新增 `ExternalResourceReclaimer`，由 FE 统一持有待回收记录，使用 4 个独立后台工作线程。

先登记资源归属，再尝试调度。工作线程满时保留记录，后台调度器重试；不阻塞入队，不使用 CallerRunsPolicy，不在回放线程等待 Future。工作线程完成一个任务后继续处理待回收记录，避免短任务都等待定时轮询。

同一批退休资源的依赖清理按顺序执行；独立任务可以并行。关闭失败会记录异常和失败数，并继续尝试该批剩余独立阶段；不会自动重试一个可能已执行一半的 close。

插件关闭时切换并恢复对应类加载器；永久后台线程使用 FE 类加载器，避免继承首次提交者的插件类加载器。

新增指标：`external_resource_cleanup_pending`、`running`、`failures`、`oldest_age_ms` 和 `last_duration_ms`，均使用 `external_resource_cleanup_` 前缀。待回收超过 1024 个或最老任务超过一分钟会记录积压告警；慢关闭也会记录耗时。

## 尚未解决的部分

- 缓存遍历、row-count 扫描、旧缓存额度注销仍同步执行。这一版把外部资源 close 搬走，并没有做到所有失效操作都是常数时间。
- `makeSureInitialized()` 仍在 Catalog 对象锁内初始化。查询如果在初始化期间变慢，回放获取同一个对象锁仍可能等待。初始化的配置快照、锁外创建、版本校验发布留待下一步。
- 待回收登记表没有硬容量上限。四个 worker 都卡在 close 时，后续资源仍会积压；告警和指标能够暴露积压，不能替代后续的容量控制和故障处理。
- 已有 Iceberg table/catalog lease 规则保留；这一版没有为所有 connector 和 engine context 新增统一的查询借用协议，不能声称已经解决所有正在执行的查询与 ALTER/DROP 之间的生命周期问题。
- 名称映射仍是两个并发 Map。写锁内完成切换缩短了缺失窗口，但没有把所有无锁读者改成读取同一份原子映射快照。
- 约束按名称清理与同名重建的并发语义沿用原实现，本版没有新增约束所有权标识。

## 测试代码与自查

测试只编写，尚未执行：

- 单个串行回放调用者在旧 close 未完成时继续应用另一 Catalog 的后续日志。
- ALTER 后新属性和新运行时可用，旧缓存额度已注销。
- DROP 后重建同名 Catalog、Catalog 改名后初始化，旧任务不关闭新 connector/context，不删除新映射。
- 子资源清理卡住时回放仍返回；旧 connector/context 的关闭等待子资源清理完成。
- 调度被拒绝时仍保留资源记录，不在提交者线程执行，不重复调度。
- 独立清理并行、异常计数、提交者的类加载器在任务中生效并在退出后恢复。
- REFRESH 在 FileIO 清理前已失效；旧清理不影响新缓存；最后一个借用者释放后才登记清理，并保留 catalog lease 到清理结束。
- connector 的 table cache 在 FE 已注销缓存 owner 后仍可重复关闭。

按仓库 code-review 指南逐项自查，以下是静态结论：

| 检查项 | 结论 |
| --- | --- |
| 目标和证据 | 已拆分资源摘除与关闭；测试代码覆盖关键顺序，尚无运行结果 |
| 改动范围 | FE Catalog 生命周期、资源回收器、Iceberg FileIO 释放及相应测试；无 BE 改动 |
| 并发和锁 | 旧字段在 Catalog monitor 内摘除；全局锁内不执行这些资源的物理 close；初始化和同步扫描仍是限制 |
| 生命周期和静态初始化 | 回收器按需单例；登记表持有旧实例直到关闭尝试结束；不把当前 Catalog 对象交给后台调用 onClose |
| 新配置 | 无新增配置，worker 数量暂为固定值 |
| 兼容性 | 不修改日志、Image、Thrift、Protobuf 或 connector SPI；进程重启不恢复本地回收队列 |
| 并行路径 | Master 本地执行和 FE 回放共用拆分；刷新沿用缓存统一的 FileIO 释放入口 |
| 条件和异常分支 | 保留冷资源无实例、调度拒绝、close 抛异常的处理；清理异常有日志 |
| 测试覆盖 | 新增并发和所有权测试，调整原先依赖同步关闭的断言；暂未执行 |
| 测试结果文件 | 没有新增或手写回归结果文件 |
| 可观测性 | 有积压、运行中任务、失败、等待年龄和关闭耗时指标及告警 |
| 持久化和主从切换 | 元数据日志顺序和格式保留；后台只负责当前进程的资源回收 |
| 数据写入与事务 | 不增加或重放远端业务写入；已有 connector 的事务/查询终止规则未扩展 |
| FE/BE 参数传递 | 无新增跨端参数 |
| 性能和资源开销 | close 已移出回放；同步扫描和待回收资源实际占用仍需后续测量与治理 |
| 其他风险 | 尚未运行编译和测试；上述剩余边界需继续 review，不能作为完整非阻塞保证 |
