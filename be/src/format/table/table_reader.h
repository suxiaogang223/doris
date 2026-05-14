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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/types.h"
#include "format/generic_reader.h"
#include "storage/segment/common.h"

namespace doris {

class FileMetaCache;
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;

namespace cctz {
class time_zone;
} // namespace cctz

namespace io {
struct IOContext;
} // namespace io

// This file intentionally mirrors DuckDB's reader split while using Doris names:
//
//   DuckDB MultiFileReader      -> Doris TableReader
//   DuckDB BaseFileReader       -> Doris BaseFileFormatReader
//   DuckDB ParquetReaderScanState -> Doris FormatReaderScanState subclasses
//
// Doris BE does not enumerate files. FE already resolves table snapshots,
// manifests, data files and splits. TableReader therefore orchestrates one
// FE-planned split at a time instead of owning a multi-file list.

enum class FieldMappingKind {
    PHYSICAL,
    MISSING,
    PARTITION,
    GENERATED,
    SYNTHESIZED,
};

enum class RequiredFieldPurpose {
    OUTPUT,
    PREDICATE,
    EQUALITY_DELETE_KEY,
    ROW_LINEAGE,
    ROW_ID,
    LEVELS_ONLY,
    REFERENCE_LEVELS,
};

enum class CastSafety {
    NONE,
    SAFE_FOR_PUSHDOWN,
    RESIDUAL_IN_TABLE_READER,
};

enum class TableColumnMappingMode {
    BY_NAME,
    BY_FIELD_ID,
};

struct CastPlan {
    DataTypePtr file_type;
    DataTypePtr table_type;
    CastSafety safety = CastSafety::NONE;
};

struct PhysicalFieldRef {
    std::string file_path;
    int32_t field_id = -1;
    uint64_t column_id = 0;
    uint64_t max_column_id = 0;
};

struct FieldMappingNode {
    std::string table_path;
    std::string file_path;
    int32_t table_field_id = -1;
    FieldMappingKind kind = FieldMappingKind::MISSING;
    std::optional<PhysicalFieldRef> physical;
    std::optional<CastPlan> cast_plan;
    std::vector<FieldMappingNode> children;

    bool is_physical() const { return kind == FieldMappingKind::PHYSICAL && physical.has_value(); }
};

struct TableColumnDefinition {
    std::string name;
    DataTypePtr type;
    std::vector<TableColumnDefinition> children;
    std::optional<int32_t> field_id;
    std::optional<std::string> identifier_name;
    std::optional<std::string> default_value;

    std::string identifier_for_name_mapping() const {
        return identifier_name.has_value() ? *identifier_name : name;
    }
};

struct TableColumnMappingNode {
    std::string table_path;
    std::string file_path;
    int32_t table_field_id = -1;
    FieldMappingKind kind = FieldMappingKind::MISSING;
    std::optional<PhysicalFieldRef> physical;
    std::optional<CastPlan> cast_plan;
    std::optional<std::string> default_value;
    size_t global_ordinal = 0;
    size_t local_ordinal = 0;
    bool needs_definition_repetition_levels = false;
    std::vector<TableColumnMappingNode> children;
};

struct TableColumnMapping {
    TableColumnMappingMode mode = TableColumnMappingMode::BY_NAME;
    std::vector<TableColumnMappingNode> columns;
    FieldMappingNode mapping_root;
};

class TableColumnMapper {
public:
    TableColumnMapper(TableColumnMappingMode mode, std::vector<TableColumnDefinition> table_columns,
                      std::vector<TableColumnDefinition> file_columns)
            : _mode(mode),
              _table_columns(std::move(table_columns)),
              _file_columns(std::move(file_columns)) {}

    TableColumnMapping build_mapping() const {
        TableColumnMapping result;
        result.mode = _mode;
        result.mapping_root.table_path = "$root";
        result.mapping_root.file_path = "$root";
        result.mapping_root.kind = FieldMappingKind::PHYSICAL;

        std::unordered_map<int32_t, size_t> field_id_index;
        std::unordered_map<std::string, size_t> name_index;
        _build_indexes(_file_columns, &field_id_index, &name_index);

        for (size_t i = 0; i < _table_columns.size(); ++i) {
            auto node = _map_column(_table_columns[i], i, _file_columns, field_id_index, name_index,
                                    _table_columns[i].name);
            result.mapping_root.children.push_back(_to_field_mapping(node));
            result.columns.push_back(std::move(node));
        }
        return result;
    }

private:
    static void _build_indexes(const std::vector<TableColumnDefinition>& columns,
                               std::unordered_map<int32_t, size_t>* field_id_index,
                               std::unordered_map<std::string, size_t>* name_index) {
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].field_id.has_value()) {
                field_id_index->emplace(*columns[i].field_id, i);
            }
            name_index->emplace(columns[i].name, i);
        }
    }

    std::optional<size_t> _find_local_column(
            const TableColumnDefinition& table_column,
            const std::unordered_map<int32_t, size_t>& field_id_index,
            const std::unordered_map<std::string, size_t>& name_index) const {
        if (_mode == TableColumnMappingMode::BY_FIELD_ID && table_column.field_id.has_value()) {
            auto field_id_it = field_id_index.find(*table_column.field_id);
            if (field_id_it != field_id_index.end()) {
                return field_id_it->second;
            }
        }
        auto name_it = name_index.find(table_column.identifier_for_name_mapping());
        if (name_it != name_index.end()) {
            return name_it->second;
        }
        return std::nullopt;
    }

    TableColumnMappingNode _map_column(const TableColumnDefinition& table_column,
                                       size_t global_ordinal,
                                       const std::vector<TableColumnDefinition>& local_columns,
                                       const std::unordered_map<int32_t, size_t>& field_id_index,
                                       const std::unordered_map<std::string, size_t>& name_index,
                                       const std::string& table_path) const {
        TableColumnMappingNode node;
        node.table_path = table_path;
        node.table_field_id = table_column.field_id.value_or(-1);
        node.global_ordinal = global_ordinal;

        auto local_ordinal = _find_local_column(table_column, field_id_index, name_index);
        if (!local_ordinal.has_value()) {
            node.file_path = table_path;
            node.kind = FieldMappingKind::MISSING;
            node.default_value = table_column.default_value;
            node.needs_definition_repetition_levels = !table_column.children.empty();
            std::unordered_map<int32_t, size_t> empty_field_id_index;
            std::unordered_map<std::string, size_t> empty_name_index;
            const std::vector<TableColumnDefinition> empty_columns;
            for (size_t i = 0; i < table_column.children.size(); ++i) {
                const auto& table_child = table_column.children[i];
                node.children.push_back(_map_column(table_child, i, empty_columns,
                                                    empty_field_id_index, empty_name_index,
                                                    table_path + "." + table_child.name));
            }
            return node;
        }

        const auto& local_column = local_columns[*local_ordinal];
        node.file_path = local_column.name;
        node.local_ordinal = *local_ordinal;
        node.kind = FieldMappingKind::PHYSICAL;
        node.physical = PhysicalFieldRef {local_column.name, local_column.field_id.value_or(-1),
                                          *local_ordinal, *local_ordinal};
        node.cast_plan = CastPlan {local_column.type, table_column.type,
                                   local_column.type == table_column.type
                                           ? CastSafety::SAFE_FOR_PUSHDOWN
                                           : CastSafety::RESIDUAL_IN_TABLE_READER};

        std::unordered_map<int32_t, size_t> child_field_id_index;
        std::unordered_map<std::string, size_t> child_name_index;
        _build_indexes(local_column.children, &child_field_id_index, &child_name_index);
        for (size_t i = 0; i < table_column.children.size(); ++i) {
            const auto& table_child = table_column.children[i];
            node.children.push_back(_map_column(table_child, i, local_column.children,
                                                child_field_id_index, child_name_index,
                                                table_path + "." + table_child.name));
        }
        return node;
    }

    static FieldMappingNode _to_field_mapping(const TableColumnMappingNode& column_mapping) {
        FieldMappingNode node;
        node.table_path = column_mapping.table_path;
        node.file_path = column_mapping.file_path;
        node.table_field_id = column_mapping.table_field_id;
        node.kind = column_mapping.kind;
        node.physical = column_mapping.physical;
        node.cast_plan = column_mapping.cast_plan;
        for (const auto& child : column_mapping.children) {
            node.children.push_back(_to_field_mapping(child));
        }
        return node;
    }

    TableColumnMappingMode _mode;
    std::vector<TableColumnDefinition> _table_columns;
    std::vector<TableColumnDefinition> _file_columns;
};

struct RequiredField {
    std::string table_path;
    RequiredFieldPurpose purpose = RequiredFieldPurpose::OUTPUT;
    bool hidden = false;
    bool allow_lazy_materialization = true;
    bool needs_definition_repetition_levels = false;
};

struct SelectionVector {
    std::vector<uint8_t> selected;

    void reset(size_t rows, bool value = true) { selected.assign(rows, value ? 1 : 0); }
    size_t selected_rows() const {
        size_t rows = 0;
        for (uint8_t v : selected) {
            rows += v != 0;
        }
        return rows;
    }
};

struct RowVisibility {
    int64_t split_first_row = 0;
    int64_t split_last_row = -1;
    std::set<int64_t> deleted_rows;
    std::vector<uint8_t> deletion_vector;

    bool needs_row_positions() const {
        return split_last_row >= split_first_row || !deleted_rows.empty() ||
               !deletion_vector.empty();
    }

    bool is_visible(int64_t file_row_position) const {
        if (split_last_row >= split_first_row &&
            (file_row_position < split_first_row || file_row_position > split_last_row)) {
            return false;
        }
        if (deleted_rows.contains(file_row_position)) {
            return false;
        }
        if (file_row_position >= 0 &&
            static_cast<size_t>(file_row_position) < deletion_vector.size()) {
            return deletion_vector[static_cast<size_t>(file_row_position)] == 0;
        }
        return true;
    }
};

struct VirtualColumnPlan {
    using MaterializeFn = std::function<Status(Block*, size_t, const SelectionVector*)>;

    std::unordered_map<std::string, MaterializeFn> predicate_virtual_columns;
    std::unordered_map<std::string, MaterializeFn> payload_virtual_columns;
};

struct FormatPredicatePlan {
    std::vector<std::string> predicate_columns;
    std::vector<std::string> residual_columns;
};

struct FileReadContext {
    ReaderInitContext* legacy_init_context = nullptr;
    RuntimeState* state = nullptr;
    RuntimeProfile* profile = nullptr;
};

struct ReaderRuntimeOptions {
    FileReadContext read_context;
    size_t batch_size = 0;
    const cctz::time_zone* ctz = nullptr;
    io::IOContext* io_ctx = nullptr;
    FileMetaCache* meta_cache = nullptr;
};

struct FileSplit {
    std::string path;
    int64_t start_offset = 0;
    int64_t size = -1;
    int64_t file_size = -1;
    std::string fs_name;
    std::string format = "parquet";
};

struct TableReaderSplit {
    FileSplit data_file;
    std::unordered_map<std::string, std::string> table_properties;
};

struct PhysicalFileSchema {
    FieldMappingNode root;
    std::vector<TableColumnDefinition> columns;
    bool has_field_ids = false;
};

struct FileFormatScanProperties {
    FieldMappingNode schema_mapping_root;
    TableColumnMapping column_mapping;
    std::vector<RequiredField> required_fields;
    FormatPredicatePlan predicates;
    RowVisibility row_visibility;
    VirtualColumnPlan virtual_columns;
    bool need_row_positions = false;
    Block physical_read_template;
};

struct ColumnReadContext {
    RequiredField field;
    FieldMappingNode mapping;
    int32_t row_group_id = -1;
};

class ColumnReader {
public:
    virtual ~ColumnReader() = default;

    virtual Status initialize_read(const ColumnReadContext& context) = 0;
    virtual Status read(size_t rows, Block* block) = 0;
    virtual Status filter(size_t rows, Block* block, SelectionVector* selection,
                          size_t* selected_rows) = 0;
    virtual Status select(size_t rows, const SelectionVector& selection, size_t selected_rows,
                          Block* block) = 0;
    virtual Status skip(size_t rows) = 0;
    virtual Status read_levels(size_t rows, Block* block) = 0;
    virtual Status register_prefetch(size_t row_group, size_t page_index, bool allow_merge) = 0;
};

struct FormatReaderScanState {
    virtual ~FormatReaderScanState() = default;

    SelectionVector selection;
    size_t selected_rows = 0;
    bool finished = false;
};

class FileFormatReader {
public:
    virtual ~FileFormatReader() = default;

    virtual Status open() = 0;
    virtual const PhysicalFileSchema& physical_schema() const = 0;
    virtual Status initialize_scan(FormatReaderScanState* state) = 0;
    virtual Status scan(FormatReaderScanState* state, Block* block, bool* eof) = 0;
    virtual Status close() = 0;
};

class BaseFileFormatReader : public FileFormatReader {
public:
    explicit BaseFileFormatReader(FileSplit split, ReaderRuntimeOptions runtime_options)
            : split(std::move(split)), runtime_options(runtime_options) {}
    ~BaseFileFormatReader() override = default;

    FileSplit split;
    ReaderRuntimeOptions runtime_options;
    FileFormatScanProperties scan_properties;
};

struct TableReaderOptions {
    const TupleDescriptor* tuple_descriptor = nullptr;
    std::vector<SlotDescriptor*> output_slots;
    ReaderRuntimeOptions runtime_options;
};

struct TableReaderScanTask {
    TableReaderOptions options;
    TableReaderSplit split;
};

struct TableReaderScanState {
    virtual ~TableReaderScanState() = default;

    TableReaderScanTask task;
    std::unique_ptr<FormatReaderScanState> format_state;
    bool finished = false;
};

class TableReader {
public:
    virtual ~TableReader() = default;

    virtual Status initialize_scan(const TableReaderScanTask& task,
                                   TableReaderScanState* state) = 0;
    virtual Status scan(TableReaderScanState* state, Block* block, size_t* read_rows,
                        bool* eof) = 0;
    virtual Status finish_scan(TableReaderScanState* state) = 0;
    virtual Status close() = 0;
};

} // namespace doris
