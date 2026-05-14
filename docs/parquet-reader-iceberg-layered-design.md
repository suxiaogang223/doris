# Doris Iceberg + Parquet Reader 分层设计

本文描述 Doris 中 Iceberg + Parquet reader 的分层实现设计。当前阶段是
API-only 设计，不包含具体 Parquet page decoder、level decoder、delete file
读取等完整实现。

## 设计目标

Iceberg 表读取需要同时处理 Parquet 物理文件、Iceberg table schema、schema
evolution、delete file、虚拟列、谓词下推和延时物化。新的 reader 设计把这些职责
拆成三层：

```text
IcebergTableReader
  理解 Iceberg table 语义
  绑定 table schema
  管理 scan task、delete file、虚拟列
  将 file-local block finalize 成 table block

TableColumnMapper
  理解 table schema 与 file schema 的映射
  生成 ColumnMapping
  将 table projection/filter 转成 ParquetScanRequest
  处理 schema change、复杂列 remap、filter fallback

ParquetReader
  只理解 Parquet 文件本地 schema
  读取 projected file columns
  使用 local filters 做 row group/page/bloom/dictionary pruning
  输出 file-local block
```

核心原则是：`ParquetReader` 不理解 Iceberg/global schema。所有 table-level
schema evolution 和 filter localization 都在 `TableColumnMapper` 和
`IcebergTableReader` 中完成。

## 代码位置

当前 API 定义位置：

```text
be/src/format/parquet/parquet_reader.h
be/src/format/reader/table_reader.h
be/src/format/table/iceberg_reader.h
```

其中：

- `parquet_reader.h` 定义 Parquet 文件层读取 API；
- `table_reader.h` 定义 `TableReader`、`TableScanRequest`、`ColumnMapping`、
  `TableColumnMapper`；
- `iceberg_reader.h` 定义 `IcebergTableReader`、`IcebergScanTask` 和 Iceberg
  专用读取选项。

## 整体数据流

```text
1. IcebergTableReader::bind
   输入 Iceberg 当前 table schema
   保存 table/global schema

2. IcebergTableReader::init_scan
   输入 table projection 和 table filters
   保存 TableScanRequest

3. IcebergTableReader::initialize_reader
   打开一个 Iceberg data file
   从 ParquetReader 获取 file-local schema
   使用 TableColumnMapper(BY_FIELD_ID) 建立 ColumnMapping
   生成 ParquetScanRequest
   注入 position delete 信息
   初始化 ParquetReader

4. ParquetReader::scan
   读取 file-local columns
   应用 local filters 和 reader expression filters
   输出 ParquetScanResult(file-local block)

5. IcebergTableReader::next_block
   finalize file-local block 到 table block
   应用 equality deletes
   物化 Iceberg 虚拟列
   输出最终 Block
```

## ParquetReader

`ParquetReader` 是文件物理读取层，只负责 Parquet 文件本地语义。

输入：

- `ParquetReadOptions`
- `ParquetScanRequest`
- file-local projected column ids
- file-local filters
- reader expression map

输出：

- `ParquetScanResult`
- file-local block
- row count 和 eof 状态

主要职责：

- 打开 Parquet 文件；
- 解析 footer 和 file schema；
- 暴露 `ParquetFileColumn` 列信息；
- 初始化 row group、column chunk、page reader；
- 使用 local filters 做 stats、page index、dictionary、bloom filter pruning；
- 支持延时物化，先读 filter 依赖列，再读剩余 projection；
- 输出 file-local block。

不负责：

- Iceberg table schema；
- Iceberg field id mapping；
- missing/default/partition/generated columns；
- table type cast；
- struct/list/map 字段重排；
- equality delete 和虚拟列。

## IcebergTableReader

`IcebergTableReader` 是 Iceberg 表语义层，继承 `TableReader`，组合
`ParquetReader`。

主要职责：

- 绑定 Iceberg 当前 table schema；
- 为每个 data file 创建 scan task；
- 使用 `TableColumnMapper(BY_FIELD_ID)` 建立 table schema 到 file schema 的映射；
- 调用 `ParquetReader` 读取 data file；
- 在 `finalize_chunk` 中把 file-local block 转成 table block；
- 将 position delete 转成 file-local delete/filter request；
- 在 table block 上应用 equality delete；
- 物化 `_row_id`、`_last_updated_sequence_number` 等 Iceberg 虚拟列。

核心接口：

```cpp
class IcebergTableReader : public reader::TableReader {
public:
    Status init_iceberg(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                        const IcebergReadOptions& options,
                        std::unique_ptr<parquet::ParquetReader> data_reader);

    Status bind(const std::vector<reader::TableColumn>& iceberg_schema);
    Status init_scan(const reader::TableScanRequest& request) override;
    Status initialize_reader(const IcebergScanTask& task);
    Status next_block(Block* table_block, size_t* rows, bool* eof) override;
    Status finalize_chunk(const parquet::ParquetScanResult& scan_result, Block* table_block);
};
```

## TableColumnMapper

`TableColumnMapper` 是本设计的关键组件。它位于 table reader 层，但不是
Iceberg 专用类。Iceberg 当前使用 `BY_FIELD_ID` 模式，后续普通 multi-file
Parquet 或 Hive 场景可以使用 `BY_NAME` 模式。

### 输入

```cpp
TableColumnMapperOptions options;
std::vector<TableColumn> table_schema;
std::vector<parquet::ParquetFileColumn> file_columns;
TableScanRequest table_request;
```

其中：

- `table_schema` 是 table/global schema；
- `file_columns` 是当前 Parquet 文件暴露的 file-local schema；
- `table_request` 包含 table projection 和 table filters；
- `options.mode` 决定按 field id 还是 name 匹配列。

### 输出

```cpp
std::vector<ColumnMapping> mappings;
parquet::ParquetScanRequest parquet_request;
```

其中：

- `mappings` 描述每个 table column 如何从 file-local column 生成；
- `parquet_request.projected_file_columns` 描述 ParquetReader 需要读取哪些物理列；
- `parquet_request.local_filters` 描述可以直接下推到文件层的谓词；
- `parquet_request.reader_expression_map` 描述需要在 ParquetReader 内部先计算的
  fallback 表达式。

## ColumnMapping

`ColumnMapping` 是 table schema 和 file schema 之间的核心边界对象。

```cpp
struct ColumnMapping {
    ColumnId table_column_id;
    std::optional<ColumnId> file_column_id;
    DataTypePtr file_type;
    DataTypePtr table_type;
    VExprContextSPtr finalize_expr;
    VExprContextSPtr reader_filter_expr;
    std::vector<ColumnMapping> child_mappings;
    bool is_trivial;
    bool is_constant;
};
```

字段含义：

- `table_column_id`：table/global schema 中的列 id。Iceberg 场景下通常是
  Iceberg field id。
- `file_column_id`：当前 Parquet 文件中的 file-local column id。没有值表示该列
  不存在于当前文件，需要由 default、partition、generated 或 NULL 表达式生成。
- `file_type`：文件中的列类型。
- `table_type`：最终 table block 中的目标类型。
- `finalize_expr`：把 file-local value 转成 table/global value 的表达式。
- `reader_filter_expr`：filter fallback 使用的读时表达式。
- `child_mappings`：复杂类型的子字段映射，用于 struct/list/map 裁剪、重排、
  缺失 child 默认值和 `STRUCT_EXTRACT` filter remap。
- `is_trivial`：文件列可以原样作为 table 列使用。只有 trivial mapping 才能把
  table filter 直接复制成 file-local filter。
- `is_constant`：该列是文件级常量，例如 partition value、Iceberg 新增列默认值、
  虚拟列常量部分。

### finalize_expr

`finalize_expr` 在 `IcebergTableReader::finalize_chunk` 阶段执行，输入是
`ParquetReader` 返回的 file-local block，输出写入最终 table block。

典型情况：

```text
trivial mapping:
  finalize_expr = identity(file_col)

type schema change:
  finalize_expr = CAST(file_col AS table_type)

struct child reorder/default:
  finalize_expr = remap_struct(file_col, child_mappings, defaults)

missing column:
  finalize_expr = default/partition/generated/NULL expression
```

### reader_filter_expr

`reader_filter_expr` 只服务于读时过滤。当 table filter 不能安全转换成
file-local filter，但仍希望在延时物化第一阶段过滤数据时使用。

例子：

```text
table col BIGINT
file col  INTEGER
WHERE col = 3000000000
```

该常量不能安全转换成 `INTEGER`，因此不能生成 `file_col = 3000000000` 的
local filter。此时 mapper 可以生成：

```text
reader_filter_expr = CAST(file_col AS BIGINT)
```

并放入：

```text
ParquetScanRequest::reader_expression_map
```

`ParquetReader` 读取 file-local column 后计算该临时表达式列，再基于临时列执行
filter。这里 `ParquetReader` 仍然不理解 Iceberg schema，它只执行 request 中
已经构造好的表达式。

`reader_filter_expr` 和 `finalize_expr` 可能相同，但语义不同：

- `reader_filter_expr` 服务于读时过滤，只在 filter fallback 时出现；
- `finalize_expr` 服务于最终输出，每个输出列都可能需要。

## Mapping 流程

### create_mapping

`create_mapping` 根据 table schema 和 file schema 生成 `ColumnMapping`。

伪代码：

```cpp
for (table_col : table_schema) {
    file_col = find_file_column(table_col, file_columns, options.mode);

    if (!file_col.exists) {
        mapping.file_column_id = nullopt;
        mapping.table_type = table_col.type;
        mapping.finalize_expr = build_default_or_partition_or_generated_expr(table_col);
        mapping.is_constant = true;
        continue;
    }

    mapping.file_column_id = file_col.id;
    mapping.file_type = file_col.type;
    mapping.table_type = table_col.type;

    if (table_col.type == file_col.type && children_are_trivial(table_col, file_col)) {
        mapping.is_trivial = true;
        mapping.finalize_expr = identity(file_col);
    } else {
        mapping.is_trivial = false;
        mapping.finalize_expr = build_cast_or_remap_expr(table_col, file_col);
    }

    mapping.child_mappings = map_children(table_col.children, file_col.children);
}
```

### create_scan_request

`create_scan_request` 根据 table projection/filter 生成 `ParquetScanRequest`。

伪代码：

```cpp
for (projected_table_col : table_request.projected_table_columns) {
    mapping = mappings[projected_table_col.id];
    if (mapping.file_column_id exists) {
        parquet_request.projected_file_columns.push_back(mapping.file_column_id);
        append_selected_children(mapping.child_mappings);
    }
}

localize_filters(table_request.table_filters, parquet_request);
```

## Filter 处理

Table filter 会进入 `TableColumnMapper::localize_filters`，根据 mapping 类型选择
不同路径。

### 路径一：trivial mapping

```text
table col INTEGER
file col  INTEGER
WHERE col > 10
```

处理方式：

```text
local filter = file_col > 10
```

这种情况下 filter 可以直接下推，ParquetReader 可以使用 row group stats、page
index、dictionary filter、bloom filter 等文件层优化。

### 路径二：safe cast

```text
table col BIGINT
file col  INTEGER
WHERE col = 42
```

处理方式：

```text
filter constant: 42::BIGINT -> 42::INTEGER
local filter: file_col = 42
finalize: CAST(file_col AS BIGINT)
```

只要 filter 常量可以无损转换到 file type，就可以保留文件层 pruning 能力。

### 路径三：reader expression fallback

```text
table col BIGINT
file col  INTEGER
WHERE col = 3000000000
```

处理方式：

```text
reader_filter_expr = CAST(file_col AS BIGINT)
reader 先计算临时列，再应用 table filter
```

该路径通常不能使用 bloom filter 或 page stats 做等价 pruning，因为文件层看不到
一个合法的 file-local 常量谓词。

### 路径四：finalize-only

```text
table col country
file 中不存在
country 是 partition value
WHERE country = 'CN'
```

处理方式：

```text
mapping.is_constant = true
finalize_expr = constant('CN')
filter 可以在 table reader/mapper 层直接对常量求值
不需要 ParquetReader 读取任何物理列
```

## Schema Change 处理

### 新增列

旧文件不包含新增列：

```text
table schema: id, name, age
old file:     id, name
```

`TableColumnMapper` 生成：

```text
age.file_column_id = nullopt
age.finalize_expr = default(age) 或 NULL
age.is_constant = true
```

### 类型变化

文件类型和 table 类型不同：

```text
table col BIGINT
file col  INTEGER
```

输出时：

```text
finalize_expr = CAST(file_col AS BIGINT)
```

filter 下推时再判断常量能否安全转换到 `INTEGER`。

### 复杂列变化

struct 子字段发生新增、删除、重排：

```text
table struct s: {a INT, b STRING, c BIGINT}
file  struct s: {b STRING, a INT}
```

`TableColumnMapper` 对 `s` 生成父级 mapping，并对子字段递归生成
`child_mappings`：

```text
s.a -> file.s.a
s.b -> file.s.b
s.c -> default/NULL
```

最终：

```text
finalize_expr = remap_struct(file.s, child_mappings, defaults)
```

## 延时物化

延时物化要求 reader 先读取 filter 依赖列，过滤后再读取剩余 projection。新的分层
中，`ParquetReader` 可以继续负责这个文件层优化，但它接收的是已经 localize 后的
request。

场景：

```sql
SELECT a + b
FROM t
WHERE a + b > 1;
```

处理方式：

```text
1. TableColumnMapper 识别 filter/projection 依赖 a、b
2. 生成 reader_expression_map: tmp = a + b
3. ParquetReader 第一阶段读取 a、b 并计算 tmp
4. 用 tmp > 1 过滤 row ids
5. projection 也需要 a + b 时，直接复用 tmp
6. IcebergTableReader finalize 输出 tmp
```

这里不要求 `ParquetReader` 理解 `a + b` 是 table expression。它只执行
`ParquetScanRequest` 中已经准备好的 reader expression。

## 当前非目标

当前阶段只固化 API 和职责边界，不实现：

- Parquet page decoder；
- nested definition/repetition level 解码；
- row group/page index/bloom filter 的真实接入；
- delete file reader 的完整实现；
- expression executor 的真实集成；
- scanner 调用点的完整迁移；
- 完整可编译闭环。

## 迁移顺序

建议按以下顺序推进：

```text
1. 固化 ParquetReader / TableColumnMapper / IcebergTableReader API
2. 迁移 Parquet footer/schema/metadata 读取
3. 迁移 row group 和 column chunk 读取
4. 接入 local filter pruning
5. 实现 TableColumnMapper 的 field-id mapping 和 filter localization
6. 实现 IcebergTableReader::finalize_chunk
7. 接入 position delete、equality delete、虚拟列
8. 迁移 scanner 调用点
```
