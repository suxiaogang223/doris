# DuckDB-style Parquet Reader Refactor for Doris

本文是 Doris Parquet reader 重构的 API 调研文档。当前阶段只定义边界和核心
API，不迁移具体解码实现。

## 背景

DuckDB 的 Parquet reader 迭代历史显示，它曾经让底层 reader 直接写入
global schema chunk。后续 DuckDB 将这个设计改为：

```text
Parquet/FileReader
  读取 file-local schema
  输出 intermediate block

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

### ParquetFileReader

只负责 Parquet 物理文件读取。

职责：

- 打开文件；
- 解析 footer；
- 暴露 file-local schema；
- 根据 local column ids 初始化 scan；
- 读取 row group/page/column chunk；
- 使用 local filters 做 row group pruning、bloom filter、dictionary filter；
- 输出 file-local intermediate block。

不负责：

- table/global schema；
- Iceberg/Hive/Paimon/Hudi schema evolution；
- missing/default/partition columns；
- generated columns；
- table type cast；
- struct remap。

### ParquetTableReader

负责 table/global schema 语义。

职责：

- 根据 table schema 和 file schema 创建 column mapping；
- 将 table filters 转成 local filters；
- 对缺失列生成 default/null/partition expression；
- 对类型变化生成 cast expression；
- 对 struct evolution 生成 remap expression；
- 在必要时安装 reader expression fallback；
- 调用 `ParquetFileReader` 获取 intermediate block；
- finalize 成 table block。

### ParquetColumnMapper

负责 schema mapping 和 filter localization。

职责：

- table column -> file column 映射；
- 支持 name mapping、case-insensitive mapping、field-id mapping；
- 判断 trivial mapping；
- 生成 finalize expression；
- localize filter；
- 不能 localize 的 filter 走 expression fallback。

## API 骨架

API 定义在：

```text
be/src/format/parquet/parquet_reader_api.h
```

核心类型：

```cpp
class ParquetFileReader {
public:
    virtual Status open(io::FileReaderSPtr file, const ParquetReadOptions& options) = 0;
    virtual Status parse_footer() = 0;
    virtual Status get_file_schema(const FieldDescriptor** schema) const = 0;
    virtual Status init_scan(const ParquetScanRequest& request) = 0;
    virtual Status scan(ParquetScanResult* result) = 0;
    virtual Status close() = 0;
};

class ParquetColumnMapper {
public:
    virtual Status create_mapping(const std::vector<ParquetTableColumn>& table_columns,
                                  const FieldDescriptor& file_schema,
                                  std::vector<ParquetColumnMapping>* mappings) = 0;

    virtual Status localize_filters(const std::vector<ParquetTableFilter>& table_filters,
                                    const std::vector<ParquetColumnMapping>& mappings,
                                    ParquetScanRequest* request) = 0;
};

class ParquetTableReader {
public:
    virtual Status init(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                        const ParquetReadOptions& options) = 0;

    virtual Status next_block(Block* table_block, size_t* rows, bool* eof) = 0;

    virtual Status finalize_block(const Block& local_block, const ParquetScanResult& scan_result,
                                  Block* table_block) = 0;

    virtual Status close() = 0;
};
```

## Filter 处理策略

Table-level filters 先交给 `ParquetColumnMapper`。

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
  -> Doris ParquetFileReader

DuckDB MultiFileColumnMapper
  -> Doris ParquetColumnMapper

DuckDB MultiFileReader::FinalizeChunk
  -> Doris ParquetTableReader::finalize_block

DuckDB expression_map
  -> Doris ParquetScanRequest::reader_expression_map
```

## 迁移计划

### Phase 1: API-only

- 删除旧 `vparquet_*reader*` orchestration 实现；
- 保留 decoder、schema、metadata、bloom filter、page index 等可复用模块；
- 新增 `parquet_reader_api.h`；
- 明确 FileReader/TableReader/ColumnMapper 边界。

### Phase 2: FileReader 实现

- 从旧 `ParquetReader` / `RowGroupReader` 下沉物理读取逻辑；
- 输出 file-local intermediate block；
- 只接受 local column ids 和 local filters。

### Phase 3: TableReader 实现

- 迁移 Hive/Iceberg/Hudi/Paimon schema mapping；
- 实现 missing/default/partition/generated columns；
- 实现 finalize expressions。

### Phase 4: Filter/Late Materialization

- table filter localize；
- safe-cast filter；
- expression fallback；
- lazy materialization 下复用 filter column。

### Phase 5: 调用点迁移

- FileScanner 使用 `ParquetTableReader`；
- delete file reader 使用 `ParquetFileReader` 或专用 TableReader；
- 删除继承式 Hive/Iceberg/Hudi/Paimon Parquet reader。

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

这些模块后续可以被新的 `ParquetFileReader` 实现复用。

下一步应先迁移调用点：

- `FileScanner` 不再直接构造旧 `ParquetReader`；
- Hive/Iceberg/Hudi/Paimon reader 不再继承旧 `ParquetReader`；
- delete file reader 明确选择 `ParquetFileReader` 或专用 `ParquetTableReader`；
- 编译恢复应在调用点迁移完成后进行。
