// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <stdint.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"
#include "format/parquet/parquet_reader.h"

namespace doris {
class ColumnPredicate;
class Block;
class TFileRangeDesc;
class TFileScanRangeParams;
} // namespace doris

namespace doris::reader {

using ColumnId = int32_t;

// 表层投影列的描述，使用的是全局 table schema 的列视图。
struct TableColumn {
    ColumnId id = -1;
    std::string name;
    DataTypePtr type;
    std::vector<TableColumn> children;
};

// 表层过滤条件，仍然按 table schema 组织，后续由 TableReader 解析成文件层条件。
struct TableFilter {
    ColumnId table_column_id = -1;
    VExprContextSPtr conjunct;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};

// 表列与文件列之间的映射结果。
// 这里同时保存文件类型、表类型、finalize 表达式和读时 fallback 表达式。
struct ColumnMapping {
    ColumnId table_column_id = -1;
    std::optional<ColumnId> file_column_id;
    DataTypePtr file_type;
    DataTypePtr table_type;
    VExprContextSPtr finalize_expr;
    VExprContextSPtr reader_filter_expr;
    std::vector<ColumnMapping> child_mappings;
    bool is_trivial = false;
    bool is_constant = false;
};

enum class TableColumnMappingMode {
    BY_NAME,
    BY_FIELD_ID,
};

enum class TableFilterConversion {
    COPY_DIRECTLY,
    CAST_FILTER,
    EVALUATE_EXPRESSION,
    FINALIZE_ONLY,
};

struct TableColumnMapperOptions {
    TableColumnMappingMode mode = TableColumnMappingMode::BY_FIELD_ID;
    bool allow_missing_columns = true;
    bool enable_reader_expression_fallback = true;
};

// TableColumnMapper 对应 DuckDB 的 MultiFileColumnMapper。
//
// 它是 table/global schema 和 file-local schema 之间的通用映射组件，不属于
// Iceberg 专有逻辑。IcebergTableReader 会把 mode 配成 BY_FIELD_ID，Hive/普通
// multi-file parquet 后续可以选择 BY_NAME。
//
// 该组件负责：
// 1. 建立 table column -> file column 的映射；
// 2. 为 missing/default/partition/generated column 生成 finalize 表达式；
// 3. 为 struct/list/map 等复杂列递归建立 child mapping；
// 4. 将 table filter 转换成 file-local filter；
// 5. 无法安全本地化的 filter 通过 reader_expression_map 交给 ParquetReader
//    在 file-local block 上先计算表达式。
class TableColumnMapper {
public:
    explicit TableColumnMapper(TableColumnMapperOptions options = {}) : _options(std::move(options)) {}

    Status create_mapping(const std::vector<TableColumn>& table_schema,
                          const std::vector<parquet::ParquetFileColumn>& file_columns,
                          std::vector<ColumnMapping>* mappings) {
        // 伪逻辑，对齐 DuckDB MultiFileColumnMapper::CreateMapping：
        //
        // for table_column in table_schema:
        //   file_column = find_file_column(table_column, file_columns)
        //   if file_column missing:
        //      mapping.finalize_expr = default/partition/generated/NULL expression
        //      mapping.is_constant = true
        //      continue
        //
        //   mapping.file_column_id = file_column.id
        //   mapping.file_type = file_column.type
        //   mapping.table_type = table_column.type
        //   mapping.is_trivial = same_type && same_order && all children trivial
        //
        //   if complex column:
        //      recursively map children by field id/name
        //      if child order/type/default changed:
        //          mapping.finalize_expr = remap_struct/list/map(file_column, child_mappings)
        //   else if file_type != table_type:
        //      mapping.finalize_expr = CAST(file_column AS table_type)
        //   else:
        //      mapping.finalize_expr = identity(file_column)
        mappings->clear();
        for (const auto& table_column : table_schema) {
            ColumnMapping mapping;
            RETURN_IF_ERROR(map_column(table_column, file_columns, &mapping));
            mappings->push_back(std::move(mapping));
        }
        _mappings = *mappings;
        return Status::OK();
    }

    Status create_scan_request(const TableScanRequest& table_request,
                               parquet::ParquetScanRequest* parquet_request) const {
        // 伪逻辑：
        // 1. projection: 根据 _mappings 将 table projection 翻译成 projected_file_columns；
        // 2. filter: 调用 localize_filters，把 table filters 转成 local_filters 或
        //    reader_expression_map；
        // 3. finalize-only columns 不加入 ParquetReader projection。
        parquet_request->projected_file_columns.clear();
        parquet_request->local_filters.clear();
        parquet_request->reader_expression_map.clear();

        for (const auto& table_column : table_request.projected_table_columns) {
            const auto* mapping = find_mapping(table_column.id);
            if (mapping == nullptr || !mapping->file_column_id) {
                continue;
            }
            parquet_request->projected_file_columns.push_back(*mapping->file_column_id);
            append_child_projection(*mapping, parquet_request);
        }
        RETURN_IF_ERROR(localize_filters(table_request.table_filters, parquet_request));
        return Status::OK();
    }

    Status localize_filters(const std::vector<TableFilter>& table_filters,
                            parquet::ParquetScanRequest* parquet_request) const {
        // 伪逻辑，对齐 DuckDB MultiFileColumnMapper::CreateFilters：
        //
        // 1. trivial mapping:
        //    table col INT, file col INT, WHERE col > 10
        //    -> 直接复制成 file_col > 10，ParquetReader 可以使用 stats/bloom/page index。
        //
        // 2. safe cast:
        //    table col BIGINT, file col INT, WHERE col = 42
        //    -> 将常量 42 cast 到 INT 后下推成 file_col = 42。
        //
        // 3. unsafe cast / remap / generated expression:
        //    table col BIGINT, file col INT, WHERE col = 3000000000
        //    -> 不能转成 INT filter，安装 CAST(file_col AS BIGINT) 到
        //       reader_expression_map，再在 reader 内部对表达式结果执行 filter。
        //
        // 4. complex STRUCT_EXTRACT:
        //    WHERE s.x > 1，如果 s.x 可映射到 file struct 的某个 child，就把 child
        //    index 一起 remap；如果 struct 需要重排或补默认值，则走表达式 fallback。
        for (const auto& filter : table_filters) {
            const auto* mapping = find_mapping(filter.table_column_id);
            if (mapping == nullptr) {
                continue;
            }
            switch (filter_conversion(*mapping, filter)) {
            case TableFilterConversion::COPY_DIRECTLY:
            case TableFilterConversion::CAST_FILTER: {
                parquet::ParquetLocalFilter local_filter;
                local_filter.file_column_id = *mapping->file_column_id;
                local_filter.conjunct = filter.conjunct;
                local_filter.predicates = filter.predicates;
                parquet_request->local_filters.push_back(std::move(local_filter));
                break;
            }
            case TableFilterConversion::EVALUATE_EXPRESSION:
                parquet_request->reader_expression_map.emplace_back(*mapping->file_column_id,
                                                                    mapping->reader_filter_expr);
                break;
            case TableFilterConversion::FINALIZE_ONLY:
                // 缺失列、partition/generated column 或完全不能下推的 filter 保留到
                // TableReader finalize 后处理。
                break;
            }
        }
        return Status::OK();
    }

    const std::vector<ColumnMapping>& mappings() const { return _mappings; }

private:
    Status map_column(const TableColumn& table_column,
                      const std::vector<parquet::ParquetFileColumn>& file_columns,
                      ColumnMapping* mapping) const {
        mapping->table_column_id = table_column.id;
        mapping->table_type = table_column.type;

        const auto* file_column = find_file_column(table_column, file_columns);
        if (file_column == nullptr) {
            // 伪逻辑：
            // - Iceberg 新增列：使用 initial default 或 NULL；
            // - partition column：使用当前 data file partition value；
            // - generated column：使用 generated expression；
            // - 如果不允许缺失列，则返回 schema mismatch。
            mapping->is_constant = true;
            mapping->finalize_expr = build_default_expr(table_column);
            return Status::OK();
        }

        mapping->file_column_id = file_column->id;
        mapping->file_type = file_column->type;
        mapping->is_trivial = is_same_type(table_column.type, file_column->type);
        mapping->finalize_expr = build_finalize_expr(table_column, *file_column);

        // 复杂列递归映射对齐 DuckDB MapColumn：
        // - 按 field id/name 在 file_column.children 中查找 table child；
        // - 对未投影 child 可以跳过；
        // - 对缺失 child 生成 default/NULL expression；
        // - 对 child 顺序变化生成 remap_struct/list/map finalize expression。
        for (const auto& table_child : table_column.children) {
            ColumnMapping child_mapping;
            RETURN_IF_ERROR(map_column(table_child, file_column->children, &child_mapping));
            mapping->child_mappings.push_back(std::move(child_mapping));
            mapping->is_trivial = false;
        }
        return Status::OK();
    }

    const parquet::ParquetFileColumn* find_file_column(
            const TableColumn& table_column,
            const std::vector<parquet::ParquetFileColumn>& file_columns) const {
        for (const auto& file_column : file_columns) {
            if (_options.mode == TableColumnMappingMode::BY_FIELD_ID &&
                file_column.id == table_column.id) {
                return &file_column;
            }
            if (_options.mode == TableColumnMappingMode::BY_NAME &&
                file_column.name == table_column.name) {
                return &file_column;
            }
        }
        return nullptr;
    }

    const ColumnMapping* find_mapping(ColumnId table_column_id) const {
        for (const auto& mapping : _mappings) {
            if (mapping.table_column_id == table_column_id) {
                return &mapping;
            }
        }
        return nullptr;
    }

    TableFilterConversion filter_conversion(const ColumnMapping& mapping,
                                            const TableFilter& filter) const {
        (void)filter;
        if (!mapping.file_column_id) {
            return TableFilterConversion::FINALIZE_ONLY;
        }
        if (mapping.is_trivial) {
            return TableFilterConversion::COPY_DIRECTLY;
        }
        if (can_cast_filter_to_file_type(mapping)) {
            return TableFilterConversion::CAST_FILTER;
        }
        if (_options.enable_reader_expression_fallback && mapping.reader_filter_expr) {
            return TableFilterConversion::EVALUATE_EXPRESSION;
        }
        return TableFilterConversion::FINALIZE_ONLY;
    }

    bool is_same_type(const DataTypePtr& table_type, const DataTypePtr& file_type) const {
        // 真实实现应该调用 Doris DataType 的等价判断；这里用指针相等保留 API 骨架。
        return table_type == file_type;
    }

    bool can_cast_filter_to_file_type(const ColumnMapping& mapping) const {
        // 真实实现需要检查 filter 常量是否能无损转换到 file_type。例如：
        // BIGINT 42 -> INT 可以；BIGINT 3000000000 -> INT 不可以。
        return mapping.file_type && mapping.table_type && !mapping.child_mappings.empty();
    }

    VExprContextSPtr build_default_expr(const TableColumn& table_column) const {
        // 真实实现返回 default/partition/generated/NULL 表达式。
        (void)table_column;
        return nullptr;
    }

    VExprContextSPtr build_finalize_expr(const TableColumn& table_column,
                                         const parquet::ParquetFileColumn& file_column) const {
        // 真实实现返回 identity/cast/remap_struct/list/map 表达式。
        (void)table_column;
        (void)file_column;
        return nullptr;
    }

    void append_child_projection(const ColumnMapping& mapping,
                                 parquet::ParquetScanRequest* parquet_request) const {
        // 复杂列裁剪时，把 selected child 的 ColumnIndex/field path 放入
        // ParquetScanRequest。当前 API-only 版本只保留说明，不展开具体结构。
        (void)mapping;
        (void)parquet_request;
    }

private:
    TableColumnMapperOptions _options;
    std::vector<ColumnMapping> _mappings;
};

// 表层读取参数，当前只保留 batch size；真正的文件读取细节由底层 reader 自己决定。
struct TableReadOptions {
    size_t batch_size = 4096;
};

// 表层 scan 请求，描述本次需要哪些 table 列和哪些 table 谓词。
struct TableScanRequest {
    std::vector<TableColumn> projected_table_columns;
    std::vector<TableFilter> table_filters;
};

// 表级适配器。
// 它负责：
// 1. 绑定 table schema；
// 2. 调度底层 ParquetReader；
// 3. 把文件层结果 finalize 成表层结果。
class TableReader {
public:
    virtual ~TableReader() = default;

    virtual Status init(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                        const TableReadOptions& options) = 0;
    virtual Status init_scan(const TableScanRequest& request) = 0;
    virtual Status next_block(Block* table_block, size_t* rows, bool* eof) = 0;
    virtual Status close() = 0;
};

} // namespace doris::reader
