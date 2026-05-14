# Doris Iceberg + Parquet split 驱动组合式 Reader API

本文档描述当前实验分支 `experiment/table-reader-composition` 的彻底重写版本。

这版实验不考虑编译，目标是删除旧的 `ParquetReader` 和 `IcebergReader`
继承式实现，用伪代码级别的 API 重新表达 Doris BE 中 Iceberg + Parquet
查询应该如何分层。

## 1. Doris 的边界不是 MultiFile，而是 split

DuckDB 有 `MultiFileReader` / `MultiFileList`，因为它的 table function
框架需要在执行侧枚举文件。

Doris 不应该照搬这个概念。Doris 的 Iceberg snapshot、manifest、data file
枚举和 split 切分应该由 FE 完成。BE 只顺序消费 FE 下发的 `TFileRangeDesc`。

因此 BE 里的分层是：

```text
FileScanner
  IcebergReaderAdapter
    IcebergTableReader
      FileFormatReader
        ParquetReader
          ColumnReader API
```

`IcebergReaderAdapter` 只是旧 `FileScanner` 的 `GenericReader` 桥接层，不承载
Iceberg 语义。

## 2. 核心接口

### `TableReader`

表层 reader，输出最终 Doris `Block`。

职责：

- schema change
- partition fallback
- missing/default/generated/synthesized 列
- equality delete
- position delete / deletion vector
- `$row_id`
- row lineage
- residual predicate
- final projection

### `FileFormatReader`

文件格式 reader，输出物理批次 `PhysicalReadBatch`。

职责：

- 打开物理文件
- 读取 footer / schema
- row group / page pruning
- predicate column read
- lazy payload read
- levels-only read
- row position 输出
- hidden physical column 输出

### `ColumnReader`

列级 API。当前只定义接口，不实现具体解码。

职责边界：

- `open`
- `read`
- `filter`
- `select`
- `skip`
- `read_levels`
- `register_prefetch`

## 3. 关键数据结构

### `TableReadTask`

表层输入，代表一个 FE 切好的 split。

包含：

- `tuple_descriptor`
- `output_slots`
- `read_context`

### `FormatScanTask`

表层传给文件层的物理读取任务。

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

文件层输出。

包含：

- `physical_block`
- `selection`
- `row_positions`
- `hidden_columns`
- `physical_rows`

### `FieldMappingNode`

递归 schema mapping。

表达：

- table path
- file path
- Iceberg field id
- physical Parquet column id range
- missing / partition / generated / synthesized
- cast plan
- nested children

### `RequiredField`

描述文件层为什么需要读某个字段。

用途包括：

- `OUTPUT`
- `PREDICATE`
- `EQUALITY_DELETE_KEY`
- `ROW_ID`
- `ROW_LINEAGE`
- `LEVELS_ONLY`
- `REFERENCE_LEVELS`

### `RowVisibility`

描述行级可见性。

来源：

- position delete
- deletion vector
- split row range

`RowVisibility` 必须在 Parquet lazy payload read 前生效，避免被删除的行触发
payload 读取。

## 4. 执行流程

### 4.1 `IcebergTableReader::open`

伪代码流程：

1. 从 `TFileRangeDesc` 读取当前 split 的 Iceberg 元信息
2. 创建 `ParquetReader`
3. 调用 `ParquetReader::load_physical_schema`
4. 构造递归 `FieldMappingNode`
5. 构造 `IcebergDeletePlan`
6. 构造 `RequiredField`
7. 构造 `VirtualColumnPlan`
8. 组装 `FormatScanTask`
9. 调用 `ParquetReader::open`

### 4.2 `ParquetReader::next_batch`

伪代码流程：

1. 选择下一个 row group
2. 做 row group / page pruning
3. 应用 `RowVisibility`
4. 读取 predicate fields
5. 物化 predicate virtual columns
6. 计算 selection
7. 按 selection lazy 读取 payload fields
8. 读取 levels-only/reference-level fields
9. 输出 row positions
10. 输出 hidden equality-delete key columns

### 4.3 `IcebergTableReader::next_block`

伪代码流程：

1. 接收 `PhysicalReadBatch`
2. 应用 equality delete
3. 应用 residual predicate
4. 填充 missing / partition columns
5. 填充 generated columns
6. 生成 `$row_id` / row lineage
7. 删除 hidden columns
8. 输出最终 Doris `Block`

## 5. 对关键优化的证明方式

### 延时物化

`RequiredField` 把 predicate 和 payload 分开。`ParquetReader` 先读 predicate，
再按 selection 读 payload。

### schema change

`FieldMappingNode` 是递归树，不是平铺 map。它能表达 rename、reorder、missing、
cast 和 nested mapping。

### nested missing

nested missing 通过 `REFERENCE_LEVELS` 或 `LEVELS_ONLY` 读取物理 sibling 的
definition/repetition levels。表层填充时按 nested element cardinality，而不是按
顶层 row count。

### position delete / deletion vector

它们被压成 `RowVisibility`，在 payload lazy read 前参与 selection。

### equality delete

equality delete key 被作为 hidden `RequiredField` 读取。表层 finalize 时过滤并
删除 hidden columns。

### row id / lineage

`FormatScanTask::need_row_positions` 要求文件层返回 `row_positions`。表层使用
row position 和 split metadata 生成 `$row_id`、`_row_id`、
`_last_updated_sequence_number`。

## 6. 当前实验状态

当前版本有意删除旧实现并使用伪代码重写。

不保证：

- 编译通过
- 非 Iceberg Parquet reader 仍可使用
- ORC Iceberg reader 仍可使用

保证表达：

- Doris BE 的 split 驱动边界
- Iceberg table semantics 和 Parquet physical read 的组合关系
- 延时物化、schema change、delete、row id 能通过新 API 承载

