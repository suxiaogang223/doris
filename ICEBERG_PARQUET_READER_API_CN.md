# Doris Iceberg + Parquet DuckDB 风格 Reader API

本文档描述实验分支 `experiment/table-reader-composition` 当前的 reader API。

这版实验不考虑编译，目标是把 Doris BE 的 Iceberg + Parquet reader 改成接近
DuckDB 的分层方式，但命名和执行边界按 Doris 适配。

## 1. DuckDB 到 Doris 的角色映射

DuckDB 的核心结构是：

```text
MultiFileReader
  BaseFileReader
    ParquetReader
      ParquetReaderScanState
      ColumnReader tree
```

Doris 不直接照搬 `MultiFile` 命名，因为 BE 不负责 Iceberg 文件枚举。FE 已经完成
snapshot、manifest、data file 和 split 切分。`FileScanner` 仍然接收 thrift scan
range，但 thrift 只停留在 `IcebergReaderAdapter` 边界；`IcebergTableReader` 和
`ParquetReader` 只接收最小化的 reader options，便于单元测试直接构造。

Doris 中对应关系是：

```text
DuckDB MultiFileReader        -> Doris TableReader
DuckDB BaseFileReader         -> Doris BaseFileFormatReader
DuckDB ParquetReaderScanState -> Doris FormatReaderScanState / ParquetScanState
DuckDB ColumnReader           -> Doris ColumnReader API
```

运行栈是：

```text
FileScanner
  IcebergReaderAdapter
    TableReader
      IcebergTableReader
        FileFormatReader
          BaseFileFormatReader
            ParquetReader
            ParquetScanState
            ColumnReader tree
```

`IcebergReaderAdapter` 只是旧 `FileScanner` 的 `GenericReader` 桥接层，不承载
Iceberg 或 Parquet 语义。

## 2. 核心接口职责

### `TableReader`

`TableReader` 对应 DuckDB 的 `MultiFileReader` 角色，但 Doris 语义是
“table-format split reader”。

它负责：

- 创建具体 table-format scan state
- 选择并创建 `FileFormatReader`
- 把 table schema 映射到 file schema
- 规划 required fields、predicate fields、hidden fields
- 规划 position delete / deletion vector / equality delete
- 协调虚拟列、missing/default/generated/synthesized 列
- 接收文件层直接填好的 `Block` 并输出最终 Doris `Block`

接口：

```cpp
initialize_scan(TableReaderScanTask, TableReaderScanState*)
scan(TableReaderScanState*, Block*, size_t*, bool*)
finish_scan(TableReaderScanState*)
close()
```

### `IcebergTableReader`

`IcebergTableReader` 是 `TableReader` 的 Iceberg 实现。

它负责：

- Iceberg field id 优先、name fallback 的 schema mapping
- rename / reorder / missing / cast
- nested missing 的 reference levels 规划
- partition fallback
- equality delete matcher
- position delete / deletion vector 到 `RowVisibility`
- `$row_id`、`_row_id`、`_last_updated_sequence_number`
- residual predicate 和 final projection

### `FileFormatReader`

`FileFormatReader` 是文件格式 reader 的纯接口。

它只定义物理文件格式 reader 的生命周期，不放公共字段。

接口：

```cpp
open()
physical_schema()
initialize_scan(FormatReaderScanState*)
scan(FormatReaderScanState*, Block*, bool*)
close()
```

### `BaseFileFormatReader`

`BaseFileFormatReader` 对应 DuckDB 的 `BaseFileReader`。

它承载所有文件格式 reader 都需要的公共字段：

- `FileSplit`
- `ReaderRuntimeOptions`
- `scan_properties`

和 DuckDB `BaseFileReader` 一样，文件层读计划不是额外 task 对象，而是 base reader
上的成员配置。`scan_properties` 保存 schema mapping、required fields、row visibility、
virtual columns 等由 `TableReader` 下发给文件层的计划。

### `FileSplit` / `ReaderRuntimeOptions`

`FileSplit` 是 reader 层看到的最小物理 split：

- `path`
- `start_offset`
- `size`
- `file_size`
- `fs_name`
- `format`

`ReaderRuntimeOptions` 是 reader 层运行期依赖：

- `FileReadContext`
- `batch_size`
- `ctz`
- `io_ctx`
- `meta_cache`

`TFileScanRangeParams` 和 `TFileRangeDesc` 不进入 `TableReader` 或
`FileFormatReader` 构造函数。旧 scanner 适配层负责把 thrift 拆成这些小结构。

### `ParquetReader`

`ParquetReader` 是 `BaseFileFormatReader` 的 Parquet 实现。

它负责：

- 读取 footer / physical schema
- 创建递归 `ColumnReader` tree
- row group / page / dict / bloom pruning
- predicate fields 优先读取
- lazy payload read
- levels-only / reference-levels read
- row position hidden column 输出
- hidden physical columns 输出到同一个 `Block`
- prefetch 策略

它不负责：

- Iceberg schema change
- Iceberg delete file 语义
- partition/missing/generated 的最终填充
- final Doris projection

### `ColumnReader`

`ColumnReader` API 对齐 DuckDB 的能力边界。

核心方法：

- `initialize_read`
- `read`
- `filter`
- `select`
- `skip`
- `read_levels`
- `register_prefetch`

延时物化是否成立，关键就在 `filter/select/skip` 是否能下沉到列 reader。

## 3. Scan State

### `TableReaderScanState`

表层 scan 的可变状态，类似 DuckDB table function local scan state。

包含：

- `TableReaderScanTask`
- `FormatReaderScanState`
- scan 是否结束

### `IcebergTableReaderScanState`

Iceberg 的具体表层状态。

包含：

- 当前 split 的 Iceberg 元信息
- schema mapping
- delete plan
- required fields
- virtual column plan
- file reader scan properties
- file format reader

### `FormatReaderScanState`

文件层 scan 的抽象状态。

### `ParquetScanState`

Parquet 的具体文件层状态，对应 DuckDB `ParquetReaderScanState`。

包含：

- 当前 row group
- row group 内 offset
- row group first row
- selection（文件层内部状态，不跨层返回）
- lazy read plan
- output template
- root `ColumnReader` tree（伪代码）
- define/repeat level buffers（伪代码）
- prefetch 状态

## 4. 执行流程

### 4.1 `IcebergTableReader::initialize_scan`

1. 从 `TableReaderScanTask::split` 提取当前 split 的 Iceberg 元信息
2. 根据文件格式创建 `FileFormatReader`，当前实验只创建 `ParquetReader`
3. `ParquetReader::open()` 读取 footer 和 physical schema
4. `IcebergTableReader` 基于 physical schema 构造 `FieldMappingNode`
5. 构造 `IcebergDeletePlan`
6. 构造 `RequiredField`
7. 构造 `VirtualColumnPlan`
8. 配置 `BaseFileFormatReader::scan_properties`
9. 创建 `ParquetScanState`
10. 调用 `ParquetReader::initialize_scan`

### 4.2 `ParquetReader::scan`

1. 使用 `ParquetScanState` 推进 row group
2. 做 row group / page pruning
3. 注册 prefetch
4. 应用 `RowVisibility`
5. 读取 predicate fields
6. 物化 predicate virtual columns
7. 计算 selection
8. 对 payload fields 调用 `ColumnReader::select`
9. 对全过滤 payload 调用 `ColumnReader::skip`
10. 读取 levels-only/reference-level fields
11. 将 row positions、equality delete key、levels-only 数据作为 hidden columns 放入同一个 `Block`

### 4.3 `IcebergTableReader::scan`

1. 调用 `FileFormatReader::scan` 得到文件层填好的 `Block`
2. 应用 equality delete
3. 应用 residual predicate
4. 填充 missing / partition columns
5. 填充 generated columns
6. 生成 `$row_id` / row lineage
7. 删除 hidden columns
8. 输出最终 Doris `Block`

## 5. 对关键优化的证明方式

### 延时物化

`RequiredField` 把 predicate、payload、hidden、levels-only 字段分开。

`ParquetReader` 先对 predicate fields 执行 `ColumnReader::filter`，然后只对 surviving
rows 的 payload fields 执行 `ColumnReader::select`。如果 selection 为空，payload
列只执行 `skip`。

### schema change

`PhysicalFileSchema` 是纯 Parquet 物理 schema。

`FieldMappingNode` 是 Iceberg table schema 到 physical schema 的递归映射，支持：

- field id 匹配
- name fallback
- rename / reorder
- missing
- nested missing
- cast plan

### nested missing

nested missing 通过 `REFERENCE_LEVELS` 或 `LEVELS_ONLY` 请求物理 sibling 的
definition/repetition levels。表层填充时按 nested element cardinality，不按顶层
row count。

### position delete / deletion vector

它们被转换为 `RowVisibility`，在 payload lazy read 前参与 selection。

### equality delete

equality delete key 被加入 hidden `RequiredField`。`ParquetReader` 像读普通物理列
一样读 hidden key，`IcebergTableReader` 在 finalization 阶段过滤并删除 hidden 列。

### row id / lineage

`BaseFileFormatReader::scan_properties.need_row_positions` 要求文件层生成 hidden row-position
column。`IcebergTableReader` 结合 split metadata 生成 `$row_id`、`_row_id`、
`_last_updated_sequence_number`。

## 6. 当前实验状态

当前版本有意删除旧实现并使用伪代码级 API 重写。

不保证：

- 编译通过
- 非 Iceberg Parquet reader 仍可使用
- ORC Iceberg reader 仍可使用

保证表达：

- Doris 版 `TableReader` 对应 DuckDB `MultiFileReader` 的编排职责
- `IcebergTableReader` 是 `TableReader` 子类
- `ParquetReader` 是纯物理 `BaseFileFormatReader`
- scan cursor 和 reader 元信息分离
- 延时物化、schema change、delete、row id 可以通过新 API 承载
