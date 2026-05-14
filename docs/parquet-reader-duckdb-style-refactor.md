# DuckDB-style Parquet Reader Refactor for Doris

本文是 Doris Parquet reader 重构的 API 调研文档。当前阶段只定义边界和核心
API，不迁移具体解码实现。

## 背景

DuckDB 的 Parquet reader 迭代历史显示，它曾经让底层 reader 直接写入
global schema chunk。后续 DuckDB 将这个设计改为：

```text
ParquetReader
  读取 file-local schema
  输出 file-local block

MultiFileReader/TableReader
  负责 table/global schema mapping
  执行 cast/default/remap/generated column expressions
  输出最终 table block
```

Doris 当前的 Parquet reader 重构也面临同一个问题：底层 Parquet reader 应该
直接产出 table/global schema block，还是只产出文件本地 schema block，再由
上层 TableReader/ScanAdapter 做 schema mapping。

本次重构采用 DuckDB 的方向：底层 Parquet reader 不理解 global schema。

## 当前 Doris 问题

当前 Doris Parquet reader 将多层职责耦合在 `ParquetReader` /
`RowGroupReader` / `ParquetColumnReader` 中：

- Parquet footer/schema/page/level 解码；
- row group pruning；
- bloom filter、min-max、page index；
- lazy materialization；
- table schema change；
- Hive/Iceberg/Hudi/Paimon 继承定制；
- missing/partition/generated columns；
- delete file/position delete 处理；
- table-level filter 与 local file column filter 混合。

这导致底层 reader 必须同时理解：

```text
file schema
table schema
lakehouse table format
projection/filter expression
schema change mapping
```

重构目标是拆出明确边界。

## 目标分层

### TableReader 基类

通用 table reader 抽象，定义在：

```text
be/src/format/reader/table_reader.h
```

职责是理解 table/global schema，并把 Parquet/file-local block finalize 成
table block：

```cpp
class TableReader {
public:
    virtual Status init(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                        const TableReadOptions& options) = 0;
    virtual Status init_scan(const TableScanRequest& request) = 0;
    virtual Status next_block(Block* table_block, size_t* rows, bool* eof) = 0;
    virtual Status close() = 0;
};
```

### ParquetReader

`ParquetReader` 是本次重构中唯一的 Parquet 层 reader，只负责 Parquet 物理
文件读取。

职责：

- 打开文件；
- 解析 footer；
- 暴露 file-local schema；
- 根据 local column ids 初始化 scan；
- 读取 row group/page/column chunk；
- 使用 local filters 做 row group pruning、bloom filter、dictionary filter；
- 输出 file-local block。

不负责：

- table/global schema；
- Iceberg/Hive/Paimon/Hudi schema evolution；
- missing/default/partition columns；
- generated columns；
- table type cast；
- struct remap。

### IcebergTableReader

Iceberg table reader API 定义在：

```text
be/src/format/table/iceberg_reader.h
```

新设计中，`IcebergTableReader` 不再继承 `ParquetReader`，而是继承通用
`TableReader`，并组合底层 `ParquetReader`。

它对应 DuckDB 的两层职责：

```text
DuckDB IcebergMultiFileList
  -> Doris IcebergScanTask / manifest planning API

DuckDB IcebergMultiFileReader
  -> Doris IcebergTableReader
```

核心 API：

```cpp
class IcebergTableReader : public reader::TableReader {
public:
    virtual Status init_iceberg(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                                const IcebergReadOptions& options,
                                std::unique_ptr<parquet::ParquetReader> data_reader) = 0;

    virtual Status bind(const std::vector<reader::TableColumn>& iceberg_schema) = 0;
    virtual Status initialize_reader(const IcebergScanTask& task) = 0;
    virtual Status finalize_chunk(const parquet::ParquetScanResult& scan_result,
                                  Block* table_block) = 0;
    virtual Status apply_position_deletes(parquet::ParquetScanRequest* request) = 0;
    virtual Status apply_equality_deletes(Block* table_block) = 0;
    virtual Status materialize_virtual_columns(Block* table_block, size_t rows) = 0;
};
```

Iceberg-specific 职责：

- 绑定 Iceberg 当前表 schema；
- 通过 field id 做 schema mapping；
- 将 data file 规划成 scan task；
- 将 positional deletes 转成 file-local delete/filter request；
- 在 table block 上应用 equality deletes；
- 物化 `_row_id`、`_last_updated_sequence_number` 等虚拟列；
- 保持对底层 `ParquetReader` 的组合关系，而不是继承关系。

下面用伪代码说明这三个关键能力为什么能被这层拆分承接。

### 伪代码 1: 谓词下推

```cpp
// IcebergTableReader::bind
schema = iceberg_schema;
mappings = create_mapping(schema, parquet_columns);

// IcebergTableReader::localize_filters
for (filter : table_filters) {
    mapping = mappings[filter.table_column_id];
    if (mapping.file_column_id exists) {
        if (filter constant can safely cast to mapping.file_type) {
            local_predicate = cast(filter, mapping.file_type);
            request.local_filters.push_back(
                build_local_filter(mapping.file_column_id, local_predicate, mapping.predicates));
        } else {
            // 不能安全本地化，就让 ParquetReader 先算表达式，再由 table 层继续判断
            request.reader_expression_map.push_back(
                {mapping.file_column_id, mapping.reader_filter_expr});
        }
    } else {
        // 缺失列、partition 列、generated 列等由 table 层 finalize，不下推给 ParquetReader
        keep_filter_for_finalize(filter, mapping.finalize_expr);
    }
}

// ParquetReader::init_reader
load_metadata();
build_row_group_pruning(local_filters);
build_bloom_filter_lookup(local_filters);
prepare_reader_expressions(reader_expression_map);

// ParquetReader::scan
read_projected_file_columns();
evaluate_reader_expressions_if_needed();
apply_local_filters();
```

### 伪代码 2: schema change

```cpp
// IcebergTableReader::create_mapping
for (table_col : iceberg_schema) {
    file_col = find_file_column_by_field_id(table_col.field_id);
    if (!file_col.exists) {
        mapping.file_column_id = nullopt;
        mapping.finalize_expr = build_default_or_partition_expr(table_col);
        continue;
    }

    mapping.file_column_id = file_col.id;
    mapping.file_type = file_col.type;
    mapping.table_type = table_col.type;

    if (file_col.type == table_col.type) {
        mapping.finalize_expr = identity();
    } else if (can_cast_safely(file_col.type, table_col.type)) {
        mapping.finalize_expr = cast(file_col, table_col.type);
    } else {
        // 读时先保留文件类型，最终在 table 层做转换
        mapping.finalize_expr = cast(file_col, table_col.type);
        mapping.reader_filter_expr = cast(file_col, table_col.type);
    }
}

// IcebergTableReader::finalize_chunk
for (row in scan_result.local_block) {
    for (mapping : mappings) {
        if (mapping.file_column_id exists) {
            value = local_block[mapping.file_column_id];
            if (mapping.finalize_expr != identity()) {
                value = evaluate(mapping.finalize_expr, value);
            }
            table_block[mapping.table_column_id] = value;
        } else {
            table_block[mapping.table_column_id] = evaluate(mapping.finalize_expr);
        }
    }
}
```

### 伪代码 3: 延时物化

```cpp
// 场景: select a + b from t where a + b > 1

// 1. bind 阶段识别出谓词列和投影列是同一个表达式
predicate_expr = (a + b);
projection_expr = (a + b);
shared_output_col = allocate_reader_expression(predicate_expr);

// 2. localize 阶段把它放到 reader_expression_map
request.reader_expression_map.push_back({shared_output_col, predicate_expr});
request.projected_file_columns.push_back(a);
request.projected_file_columns.push_back(b);

// 3. ParquetReader 先物化这个表达式列
scan_result.local_block[shared_output_col] = eval(a + b);

// 4. 表层 finalize 时直接复用这列
// 谓词已经用过一次，projection 再用一次，不需要再次计算 a + b
output_block[0] = scan_result.local_block[shared_output_col];
if (predicate(scan_result.local_block[shared_output_col])) {
    emit_row();
}
```

## API 骨架

Parquet 文件层 API 定义在：

```text
be/src/format/parquet/parquet_reader.h
```

核心类型：

```cpp
class ParquetReader {
public:
    virtual Status open(io::FileReaderSPtr file, const ParquetReadOptions& options) = 0;
    virtual Status parse_metadata() = 0;
    virtual Status get_columns(std::vector<ParquetFileColumn>* columns) const = 0;
    virtual Status get_file_schema(const FieldDescriptor** schema) const = 0;
    virtual Status init_reader(const ParquetScanRequest& request) = 0;
    virtual Status scan(ParquetScanResult* result) = 0;
    virtual Status close() = 0;
};
```

Parquet 命名空间只保留文件物理读取 API。Iceberg/global schema、table column
mapping、finalize expression 都归属于 `IcebergTableReader`。

## Filter 处理策略

Table-level filters 先进入 `IcebergTableReader` 内部 mapping/localization 流程。
`IcebergTableReader` 基于 Iceberg field id 找到对应的 file-local column，再把
可以安全下推的谓词交给 `ParquetReader`。

路径一：trivial mapping。

```text
table col INTEGER
file col  INTEGER
WHERE col > 10

直接转换成 local filter:
file_col > 10
```

路径二：safe cast。

```text
table col BIGINT
file col  INTEGER
WHERE col = 42

将 42::BIGINT 转成 42::INTEGER
reader 使用 local INTEGER filter
finalize 再 CAST(file_col AS BIGINT)
```

路径三：不能 localize。

```text
table col BIGINT
file col  INTEGER
WHERE col = 3000000000

不能安全转成本地 INTEGER filter
安装 reader_expression_map:
  CAST(file_col AS BIGINT)
reader 先算表达式，再应用 global filter
```

## 和 DuckDB 的对应关系

```text
DuckDB BaseFileReader
  -> Doris ParquetReader

DuckDB MultiFileColumnMapper
  -> Doris IcebergTableReader 内部 column mapping 流程

DuckDB MultiFileReader::FinalizeChunk
  -> Doris IcebergTableReader::finalize_chunk

DuckDB expression_map
  -> Doris ParquetScanRequest::reader_expression_map

DuckDB IcebergMultiFileReader
  -> Doris IcebergTableReader
```

## 迁移计划

### Phase 1: API-only

- 删除旧 `vparquet_*reader*` orchestration 实现；
- 保留 decoder、schema、metadata、bloom filter、page index 等可复用模块；
- 新增 `parquet_reader.h`；
- 新增 `format/reader/table_reader.h`；
- 新增组合式 `IcebergTableReader` API；
- 明确 `ParquetReader` 和 `IcebergTableReader` 边界。

### Phase 2: ParquetReader 实现

- 从旧 `ParquetReader` / `RowGroupReader` 下沉物理读取逻辑；
- 输出 file-local block；
- 只接受 local column ids 和 local filters。

### Phase 3: IcebergTableReader 实现

- 迁移 Iceberg schema mapping；
- 实现 missing/default/partition/generated columns；
- 实现 finalize expressions。

### Phase 4: Filter/Late Materialization

- table filter localize；
- safe-cast filter；
- expression fallback；
- lazy materialization 下复用 filter column。

### Phase 5: 调用点迁移

- FileScanner 对 Iceberg 使用 `IcebergTableReader`；
- 普通 Parquet scan 直接使用 `ParquetReader`；
- delete file reader 明确使用 `ParquetReader` 或 Iceberg 专用 delete reader；
- 删除继承式 Iceberg Parquet reader。

## 非目标

当前阶段不实现：

- page decoder；
- row group pruning；
- nested definition/repetition 读取；
- dictionary filter；
- bloom filter；
- Iceberg delete；
- 实际 block materialization。

当前阶段只固化 API 和分层方向。

## 当前分支状态

当前分支是 API-only 调研分支，不是可编译完成分支。

已经删除的旧 reader orchestration 文件包括：

```text
be/src/format/parquet/vparquet_reader.{h,cpp}
be/src/format/parquet/vparquet_group_reader.{h,cpp}
be/src/format/parquet/vparquet_column_reader.{h,cpp}
be/src/format/parquet/vparquet_column_chunk_reader.{h,cpp}
be/src/format/parquet/vparquet_page_reader.{h,cpp}
```

保留的旧 Parquet 低层能力包括：

```text
schema_desc
vparquet_file_metadata
vparquet_page_index
parquet_common
parquet_column_convert
parquet_block_split_bloom_filter
plain/dict/rle/level/byte_stream_split/delta decoders
```

这些模块后续可以被新的 `ParquetReader` 实现复用。

下一步应先迁移调用点：

- `FileScanner` 不再直接构造旧 `ParquetReader`；
- Iceberg reader 不再继承旧 `ParquetReader`；
- delete file reader 明确选择 `ParquetReader` 或 Iceberg 专用 delete reader；
- 编译恢复应在调用点迁移完成后进行。
