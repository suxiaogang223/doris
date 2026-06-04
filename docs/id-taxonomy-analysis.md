# New Reader Column ID / Projection Layering Refactor Plan

本文定义 FE slot 到 new table reader / file reader / new parquet reader 的理想层级关系，并基于当前实现列出需要清理的冗余 id 和后续重构计划。

## 1. 目标层级

理想的数据流如下：

```text
FE
  -> Slot + AccessPath
  -> Scanner
  -> [TableColumn]
  -> TableReader
  -> [FieldProjection]
  -> FileReader(SchemaField)
  -> format reader, e.g. ParquetReader(ParquetColumnSchema)
```

核心边界：

- `TableColumn` 是 table/source schema 语义，由 scanner 从 FE slot 和 access path 构造。
- `SchemaField` 是 file-local schema 语义，由 file reader 从文件元数据暴露。
- `TableReader` / `ColumnMapper` 负责把 `TableColumn` 映射到 `SchemaField`，并生成 `FieldProjection`。
- `FileReader` 只根据 `SchemaField` 和 `FieldProjection` 读取文件，不理解 FE slot 或 table/global schema。
- Parquet reader 内部再把 file-local logical field 映射到 Parquet physical leaf column。

## 2. 理想结构定义

### 2.1 TableColumn

`TableColumn` 表示 table/source schema 的 projected column。

```cpp
struct TableColumn {
    // 对于 Iceberg/Paimon/Hudi 是 table/source field id。
    // 对于没有稳定 field id 的格式为 -1，mapping 需要改用 name/index/access path。
    ColumnId column_unique_id = -1;

    std::string name;
    DataTypePtr type;

    // 来自 FE access path 的 nested projection。
    // children 仍是 table/source schema 语义，不是 file schema 语义。
    std::vector<TableColumn> children {};

    VExprContextSPtr default_expr = nullptr;
    bool is_partition_key = false;
};
```

注意：

- `column_unique_id` 不应被 FileReader 直接消费。
- 非 Iceberg/Paimon/Hudi 场景下多个列都可能是 `-1`，不能用它作为唯一 lookup key。
- row-level Expr 中的 `slot_id` 仍是表达式定位概念，不等价于 `column_unique_id`。

### 2.2 SchemaField

`SchemaField` 表示 FileReader 暴露的 file-local schema。

```cpp
struct SchemaField {
    // 对于支持 field id 的格式，这是 file/source field id。
    // 对于不支持 field id 的格式，top-level 可以使用 file column position，
    // nested 可以使用 children 中的 index。
    //
    // 关键要求：在同一个 parent 的 children 下，id 必须能唯一定位 child。
    int32_t column_unique_id = -1;

    std::string name;
    DataTypePtr type;
    std::vector<SchemaField> children;
    ColumnType column_type = ColumnType::DATA_COLUMN;
};
```

`SchemaField` 的 id 是 file-local id，不是 table/global id。TableReader 可以按 field id、name 或 index 做 table-to-file mapping；FileReader 只要求 `FieldProjection` 中的 id 能在当前 `SchemaField` 树内找到对应字段。

### 2.3 FieldProjection

`FieldProjection` 是 TableReader 生成的 file-local read projection。

```cpp
struct FieldProjection {
    // 与目标 SchemaField.column_unique_id 对齐。
    // top-level projection 匹配 file_schema 中的 top-level SchemaField；
    // child projection 匹配 parent SchemaField.children 中的 child。
    ColumnId column_unique_id = -1;

    bool project_all_children = true;
    std::vector<FieldProjection> children {};
};
```

FileReader 的查找规则应非常简单：

```text
find(file_schema, projection.column_unique_id)
  -> find(schema_field.children, projection.children[i].column_unique_id)
  -> ...
  -> SchemaField
```

也就是说，`FieldProjection` 进入 FileReader 后必须已经是 file-local schema 语义。它不应再携带 table id，也不应要求 FileReader 去理解 Iceberg/Paimon/Hudi table schema。

## 3. Predicate / Non-Predicate Projection

TableReader 需要把两类需求合并到 file-local projection：

- output projection：最终要 materialize 给上层的 table columns。
- predicate projection：row-level Expr 和 delete predicate 需要读取的 columns。

期望关系：

```text
TableColumn <--- TableReader/ColumnMapper ---> SchemaField
                                      |
                                      v
                  PredicateProjection + NonPredicateProjection
```

规则：

- output child 和 filter-only child 可以属于同一个 top-level complex column。
- TableReader 合并后输出 `predicate_columns` / `non_predicate_columns`。
- FileReader 按 `FieldProjection` 读取，不关心某个 child 是 output 还是 filter-only。
- Row-level filtering 使用 localized `Expr` / `VExprContext`。
- `ColumnPredicate` 只用于 file-layer pruning：statistics / dictionary / bloom / page index。

当前最新实现已经朝这个方向收敛：

- `TableFilter` 不再只保存裸 `slot_id`，而是保存 filter 引用到的 `TableColumn` 信息：

```cpp
struct TableFilter {
    VExprContextSPtr conjunct;
    std::vector<TableColumn> column_unique_ids;
};
```

- `TableColumnPredicates` 也不再只是 `table_column_id -> predicates`，而是携带对应 `TableColumn`：

```cpp
using TableColumnPredicates =
        std::map<int32_t, std::pair<TableColumn, std::vector<std::shared_ptr<ColumnPredicate>>>>;
```

这使 `ColumnMapper` 可以根据 mapping mode 使用 `TableColumn` 的 id/name/type 做定位，而不是把 FE slot id 泄漏到 file reader 层。

## 4. DuckDB 对齐点

DuckDB 的启发是明确分层，而不是复制执行模型。

DuckDB multi-file reader 显式区分：

- global index：query/global output column index。
- local index：当前 file reader 输出 index。
- local column id：当前 file schema / reader column id。
- recursive mapping tree：global child index -> local child index。

DuckDB Parquet reader 也区分：

- logical schema tree。
- `schema_index`：Parquet schema element index。
- `column_index`：Parquet physical leaf ordinal。

对 Doris 的启发：

- TableReader/ColumnMapper 完成 table/source 到 file-local 的全部映射。
- FileReader 只消费 file-local `SchemaField` + `FieldProjection`。
- ParquetReader 内部继续区分 file-local logical field 和 Parquet physical leaf column。

## 5. 当前实现偏差

### 5.1 字段命名不一致

- `TableColumn::column_unique_id` 已经表示 table/source column unique id。
- `SchemaField::column_unique_id` 已经表示 file-local field id。
- `FieldProjection::column_unique_id` 已经表示 file-local projection id。
- `ColumnMapping::file_column_unique_id` 已经表示 mapped file-local field id。
- `ParquetColumnSchema::field_id` 实际是 Parquet logical schema tree 中 parent-scoped file field id。

当前仍需注意：`ParquetColumnSchema::field_id` 是 Parquet 内部 schema tree 的 parent-scoped
id，不是 table/source id；不要和 FileReader 边界结构混用。

### 5.2 Scanner 到 TableReader 已完成的收敛

最新代码已经完成了 `FE -> Slot + AccessPath -> Scanner -> [TableColumn] -> TableReader` 这段的关键调整：

- `FileScannerV2` 从 `SlotDescriptor` 构造 `TableColumn`，并基于 access path 构造 nested `TableColumn.children`。
- 当外部 schema 可用时，scanner 会用 schema field 辅助构造 nested children，避免只靠 FE access path 字符串猜测 child。
- `TableReader` 从 conjunct 中收集 `VSlotRef::column_uniq_id()` / name / type，构造成 `TableFilter.column_unique_ids`。
- `TableColumnPredicates` 携带 `TableColumn`，让 `ColumnMapper::_find_mapping()` 按 mapping mode 决定使用 field id、name 或 index。

因此，后续重构不应再回到 `slot_id` 驱动 mapper 的设计。

### 5.3 `ColumnId` 过宽

`using ColumnId = int32_t` 没有表达层级，当前可能表示：

- table/source column unique id。
- file-local top-level field id。
- file-local nested child id。
- synthetic row position id。

短期可以保留 alias，但需要通过字段名和注释把语义写清楚。

### 5.4 FileReader projection 字段名容易误解

当前 `FieldProjection.column_unique_id` 已与 `SchemaField.column_unique_id` 对齐，进入
FileReader 后只表达 file-local projection，不表达 Iceberg/table id。

### 5.5 Parquet reader accessor 误导

`ParquetColumnReader::file_column_id()` 当前未使用，且注释和实现不一致：

- 注释说 nested columns return `-1`。
- 实现返回 `_field_id`。
- nested `_field_id` 实际只是 parent-scoped child id，不是 top-level file column id。

该接口应删除，或改名为 `file_field_id()` 并只在确实需要时保留。

## 6. 当前明确可清理项

### 6.1 删除 `ColumnMapping::file_path`

`ColumnMapping::file_path` 在已跟踪代码中没有使用。它和 `field_id` / `child_mappings` / `file_child_id_path` 语义重叠，应删除。

### 6.2 删除 unused ParquetColumnReader id accessors

可删除：

- `ParquetColumnReader::file_column_id()`
- `ParquetColumnReader::parquet_leaf_column_id()`
- `RowPositionColumnReader` 中对应 override

如果 `_field_id` 没有内部使用，也可以删除 `_field_id` 成员。

### 6.3 修正 `column_positions` 注释

当前有注释把 `column_positions` 称为 global read-column index。正确语义是：

```text
top-level file SchemaField id -> file block position
```

这是 file-local block layout，不是 global index，也不是 Parquet leaf id。

## 7. 不应清理/合并的概念

### 7.1 `FieldProjection` 和 pruning path

`FieldProjection.children` 和 `FileColumnPredicateFilter.file_child_id_path` 都表示 nested path，但用途不同：

- `FieldProjection`：reader tree / read projection。
- `file_child_id_path`：file-layer pruning target。

短期不应强行合并。后续可以抽公共 `FileFieldPath`，让二者共享 path resolver。

### 7.2 `ParquetColumnSchema.field_id` 和 `leaf_column_id`

二者不是冗余：

- `field_id`：logical schema tree matching。
- `leaf_column_id`：physical Parquet leaf column access。

这和 DuckDB 的 logical schema tree / `column_index` 分离一致，应保留。

## 8. 推荐重构顺序

### Phase 1: 命名和冗余清理

目标：不改变行为，只减少误导。

- 删除 `ColumnMapping::file_path`。
- 删除 unused `ParquetColumnReader` id accessor。
- 修正 `column_positions` 注释。
- 保持当前 `TableFilter` / `TableColumnPredicates` 携带 `TableColumn` 的设计，不再引入裸 `slot_id` lookup。
- 局部变量按层级改名：
  - table/source 层：`table_column_id` / `column_unique_id`
  - file schema 层：`file_column_id` / `file_child_id`
  - block 层：`file_block_position`
  - parquet physical 层：`parquet_leaf_column_id`

### Phase 2: 已完成，结构字段对齐目标模型

已完成：

- `TableColumn::column_unique_id`
- `SchemaField::column_unique_id`
- `FieldProjection::column_unique_id`
- `ColumnMapping::table_column_unique_id`
- `ColumnMapping::file_column_unique_id`

public/boundary struct 使用 `column_unique_id` 对齐目标模型；内部 helper 仍使用更具体名字，
例如 `file_column_id`、`root_file_column_id`、`file_child_id_path`。

### Phase 3: 公共 FileFieldPath

目标：统一 projection / pruning 的 nested path 解析。

```cpp
struct FileFieldPath {
    ColumnId root_column_unique_id = -1;
    std::vector<ColumnId> child_column_unique_ids;
};
```

使用位置：

- `FileColumnPredicateFilter.target`
- projection merge helper 内部 path 表达。
- parquet statistics leaf resolver。

### Phase 4: 强类型边界

目标：进一步对齐 DuckDB，用类型防止误用。

可以逐步引入：

```cpp
struct TableColumnUniqueId { int32_t value = -1; };
struct FileColumnUniqueId { int32_t value = -1; };
struct FileBlockPosition { size_t value = 0; };
struct ParquetLeafColumnId { int32_t value = -1; };
```

不建议第一轮就全量替换，因为会影响 scanner/table/file/parquet 多层 API。

## 9. 验证计划

每个 phase 完成后验证：

- `git diff --check`
- Fedora `BUILD_TYPE=DEBUG ./build.sh --be`
- Fedora `./run-be-ut.sh --run '--filter=TableColumnMapperTest.*:NewParquetReaderTest.*:ParquetColumnReaderTest.*'`

## 10. 非目标

- 不让 FileReader 理解 FE slot 或 table/global schema。
- 不让 ParquetReader 处理 Iceberg/Paimon/Hudi schema change。
- 不把 struct child 注册成独立 file block slot。
- 不把 row-level filtering 改成 `ColumnPredicate`。
- 不在本轮扩展 LIST/MAP/repeated leaf pruning。
