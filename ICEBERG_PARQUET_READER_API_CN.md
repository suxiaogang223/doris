# Doris Iceberg + Parquet 组合式 Reader API 说明

本文档描述当前实验分支 `experiment/table-reader-composition` 下的接口分层。
目标是把 Iceberg 表语义和 Parquet 物理读取拆开，并验证 Doris 现有的延时物化、
schema change、delete file、row id 生成是否能在这个边界内成立。

## 1. 分层职责

### `TableReader`
表层读取器，只负责最终 Doris `Block` 的构造与收尾。

职责包括：
- Iceberg snapshot / schema 语义
- schema change 映射
- partition / missing / generated / synthesized 列
- equality delete / position delete / deletion vector
- `_row_id` 和 row lineage 列
- 最终投影与隐藏列清理

### `FileFormatReader`
文件格式读取器，只负责单个 split 对应的物理文件读取。

职责包括：
- 打开文件与 footer / schema
- row group / page 级过滤
- lazy materialization
- 物理列读取
- row position 传递
- 物理批次输出

### `ColumnReader`
列级解码接口，只保留 API，不落具体实现。

它的存在目的是把列解码、level 读取、skip/select/prefetch 这些能力单独抽象出来。

## 2. 关键接口

### `TableReadTask`
表层输入任务，代表 FE 切给 BE 的一个 split。

包含：
- `tuple_descriptor`
- `output_slots`
- `read_context`

### `FormatScanTask`
文件层输入任务，代表表层已整理好的单个 split 物理读取任务。

包含：
- `path`
- `split_start`
- `split_size`
- `schema_mapping_root`
- `required_fields`
- `predicates`
- `row_visibility`
- `virtual_columns`
- `need_row_positions`
- `physical_read_template`

### `PhysicalReadBatch`
文件层输出批次。

包含：
- `physical_block`
- `selection`
- `row_positions`
- `hidden_columns`
- `physical_rows`

### `FieldMappingNode`
递归 schema 映射节点。

用途：
- 表字段到文件字段映射
- Iceberg field id 优先，name fallback
- missing 字段表示
- cast plan
- 嵌套类型的 levels/reference 信息

### `RowVisibility`
描述行级可见性。

用途：
- position delete
- deletion vector
- split 范围裁剪

## 3. 当前执行流

### 3.1 表层
`IcebergParquetTableReader::open()` 做表语义准备：

1. 读取 Parquet schema
2. 构造 `FieldMappingNode`
3. 构造 `required_fields`
4. 解析 `RowVisibility`
5. 生成 `VirtualColumnPlan`
6. 组装 `FormatScanTask`
7. 调用内部 `FileFormatReader`

### 3.2 文件层
`ParquetReader::open(const FormatScanTask&)` 只接收物理任务：

1. 接收 schema mapping
2. 接收 required fields
3. 接收 row visibility
4. 接收 virtual column plan
5. 初始化 Parquet footer / row group 读取

### 3.3 返回批次
`ParquetReader::next_batch()` 返回 `PhysicalReadBatch`：

1. 先做物理过滤
2. 再做 predicate virtual columns
3. 再做 lazy payload materialization
4. 输出 row positions

### 3.4 表层收尾
`IcebergParquetTableReader::next_block()` 对 `PhysicalReadBatch` 做最终收尾：

1. equality delete 过滤
2. `_row_id` 生成
3. row lineage 生成
4. 隐藏列清理
5. 最终 `Block` 输出

## 4. 这版设计要证明的能力

### 延时物化
物理列和谓词列分离，谓词先读，剩余 payload 后读。

### schema change
递归 `FieldMappingNode` 支持：
- reorder
- rename
- missing 字段
- nested struct / array / map

### nested missing
`REFERENCE_LEVELS` / `LEVELS_ONLY` 允许读取层级信息而不是错误地按顶层行数填充。

### delete
表层先把 delete 语义压成 `RowVisibility` 或 hidden equality delete 字段，再交给文件层/表层处理。

### row id / lineage
依赖 `row_positions`，由表层最终生成。

## 5. 当前实验边界

这只是实验原型，不追求完整编译通过。

当前主要验证的是：
- 表层和文件层是否能清晰解耦
- 现有 Doris 优化是否能落进新接口
- split 驱动模型是否足够支撑 Iceberg + Parquet

