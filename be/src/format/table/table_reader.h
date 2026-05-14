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

class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;

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

struct PhysicalFileSchema {
    FieldMappingNode root;
    bool has_field_ids = false;
};

struct FileFormatScanProperties {
    FieldMappingNode schema_mapping_root;
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
    BaseFileFormatReader(std::string path, int64_t split_start, int64_t split_size,
                         FileReadContext read_context)
            : file_path(std::move(path)),
              split_start(split_start),
              split_size(split_size),
              read_context(read_context) {}
    ~BaseFileFormatReader() override = default;

    const std::string file_path;
    const int64_t split_start = 0;
    const int64_t split_size = -1;
    FileReadContext read_context;
    FileFormatScanProperties scan_properties;
};

struct TableReaderOptions {
    const TupleDescriptor* tuple_descriptor = nullptr;
    std::vector<SlotDescriptor*> output_slots;
    FileReadContext read_context;
};

struct TableReaderScanTask {
    TableReaderOptions options;
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
