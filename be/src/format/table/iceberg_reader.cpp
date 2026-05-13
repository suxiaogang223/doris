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

#include "format/table/iceberg_reader.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/columns_common.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "exprs/aggregate/aggregate_function.h"
#include "format/format_common.h"
#include "format/generic_reader.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/schema_desc.h"
#include "format/parquet/vparquet_column_chunk_reader.h"
#include "format/table/deletion_vector_reader.h"
#include "format/table/iceberg/iceberg_orc_nested_column_utils.h"
#include "format/table/iceberg/iceberg_parquet_nested_column_utils.h"
#include "format/table/nested_column_access_helper.h"
#include "format/table/table_schema_change_helper.h"
#include "runtime/runtime_state.h"
#include "util/coding.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class RowDescriptor;
class SlotDescriptor;
class TupleDescriptor;

namespace io {
struct IOContext;
} // namespace io
class VExprContext;
} // namespace doris

namespace doris {
const std::string IcebergOrcReader::ICEBERG_ORC_ATTRIBUTE = "iceberg.id";
namespace {
constexpr const char* EQ_DELETE_PRE = "__equality_delete_column__";
constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
constexpr const char* ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER = "_last_updated_sequence_number";
} // namespace

bool IcebergTableReader::_is_fully_dictionary_encoded(
        const tparquet::ColumnMetaData& column_metadata) {
    const auto is_dictionary_encoding = [](tparquet::Encoding::type encoding) {
        return encoding == tparquet::Encoding::PLAIN_DICTIONARY ||
               encoding == tparquet::Encoding::RLE_DICTIONARY;
    };
    const auto is_data_page = [](tparquet::PageType::type page_type) {
        return page_type == tparquet::PageType::DATA_PAGE ||
               page_type == tparquet::PageType::DATA_PAGE_V2;
    };
    const auto is_level_encoding = [](tparquet::Encoding::type encoding) {
        return encoding == tparquet::Encoding::RLE || encoding == tparquet::Encoding::BIT_PACKED;
    };

    // A column chunk may have a dictionary page but still contain plain-encoded data pages.
    // Only treat it as dictionary-coded when all data pages are dictionary encoded.
    if (column_metadata.__isset.encoding_stats) {
        bool has_data_page_stats = false;
        for (const tparquet::PageEncodingStats& enc_stat : column_metadata.encoding_stats) {
            if (is_data_page(enc_stat.page_type) && enc_stat.count > 0) {
                has_data_page_stats = true;
                if (!is_dictionary_encoding(enc_stat.encoding)) {
                    return false;
                }
            }
        }
        if (has_data_page_stats) {
            return true;
        }
    }

    bool has_dict_encoding = false;
    bool has_nondict_encoding = false;
    for (const tparquet::Encoding::type& encoding : column_metadata.encodings) {
        if (is_dictionary_encoding(encoding)) {
            has_dict_encoding = true;
        }

        if (!is_dictionary_encoding(encoding) && !is_level_encoding(encoding)) {
            has_nondict_encoding = true;
            break;
        }
    }
    if (!has_dict_encoding || has_nondict_encoding) {
        return false;
    }

    return true;
}

IcebergParquetTableReader::IcebergParquetTableReader(
        ShardedKVCache* kv_cache, RuntimeProfile* profile, const TFileScanRangeParams& params,
        const TFileRangeDesc& range, size_t batch_size, const cctz::time_zone* ctz,
        io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache)
        : _kv_cache(kv_cache),
          _profile(profile),
          _scan_params(params),
          _scan_range(range),
          _batch_size(batch_size),
          _ctz(ctz),
          _io_ctx(io_ctx),
          _state(state),
          _meta_cache(meta_cache) {}

Status IcebergParquetTableReader::open(const TableReadTask& task) {
    _table_task = task;
    FormatScanTask format_task;
    RETURN_IF_ERROR(_build_format_scan_task(task, &format_task));

    _file_reader = ParquetReader::create_unique(_profile, _scan_params, _scan_range, _batch_size,
                                                _ctz, _io_ctx, _state, _meta_cache);
    RETURN_IF_ERROR(_file_reader->set_output_template(format_task.physical_read_template));
    RETURN_IF_ERROR(_file_reader->open(format_task));
    return Status::OK();
}

Status IcebergParquetTableReader::next_block(Block* block, size_t* read_rows, bool* eof) {
    DORIS_CHECK(_file_reader != nullptr);
    PhysicalReadBatch batch;
    RETURN_IF_ERROR(_file_reader->next_batch(&batch, eof));
    if (batch.physical_rows == 0) {
        *read_rows = 0;
        return Status::OK();
    }
    RETURN_IF_ERROR(_finalize_block(&batch, block, read_rows));
    return Status::OK();
}

Status IcebergParquetTableReader::close() {
    if (_file_reader != nullptr) {
        RETURN_IF_ERROR(_file_reader->close());
    }
    return Status::OK();
}

Status IcebergParquetTableReader::_build_format_scan_task(const TableReadTask& task,
                                                          FormatScanTask* format_task) {
    DORIS_CHECK(task.read_context.legacy_init_context != nullptr);
    format_task->path = _scan_range.path;
    format_task->split_start = _scan_range.start_offset;
    format_task->split_size = _scan_range.size;
    ReaderInitContext* ctx = task.read_context.legacy_init_context;
    const FieldDescriptor* parquet_schema = nullptr;
    ParquetReader schema_reader(_profile, _scan_params, _scan_range, _batch_size, _ctz, _io_ctx,
                                _state, _meta_cache);
    RETURN_IF_ERROR(schema_reader.get_file_metadata_schema(&parquet_schema));
    if (_scan_params.__isset.history_schema_info && !_scan_params.history_schema_info.empty()) {
        bool exist_field_id = true;
        RETURN_IF_ERROR(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_field_id(
                _scan_params.history_schema_info.front().root_field, *parquet_schema,
                ctx->table_info_node, exist_field_id));
        if (!exist_field_id) {
            RETURN_IF_ERROR(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_name(
                    ctx->tuple_descriptor, *parquet_schema, ctx->table_info_node));
        }
    } else {
        RETURN_IF_ERROR(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_name(
                ctx->tuple_descriptor, *parquet_schema, ctx->table_info_node));
    }

    format_task->schema_mapping_root =
            _build_schema_mapping(task.tuple_descriptor, *parquet_schema);
    format_task->required_fields =
            _collect_required_fields(task.tuple_descriptor, *parquet_schema, ctx);
    format_task->row_visibility = _build_row_visibility();
    format_task->need_row_positions = format_task->row_visibility.needs_row_positions();
    for (const auto& field : format_task->required_fields) {
        format_task->need_row_positions |= field.purpose == RequiredFieldPurpose::ROW_ID ||
                                           field.purpose == RequiredFieldPurpose::ROW_LINEAGE;
    }
    format_task->virtual_columns = _build_virtual_column_plan();
    format_task->read_context = task.read_context;

    _output_template = Block(ctx->tuple_descriptor->slots(), 0);
    format_task->physical_read_template = _output_template;
    for (const auto& field : format_task->required_fields) {
        if (!field.hidden) {
            continue;
        }
        DataTypePtr hidden_type = make_nullable(std::make_shared<DataTypeInt64>());
        ColumnWithTypeAndName hidden_column(hidden_type, field.table_path);
        format_task->physical_read_template.insert(std::move(hidden_column));
    }
    return Status::OK();
}

FieldMappingNode IcebergParquetTableReader::_build_schema_mapping(
        const TupleDescriptor* tuple_descriptor, const FieldDescriptor& parquet_schema) {
    FieldMappingNode root;
    root.table_path = "$root";
    root.file_path = "$root";
    root.kind = FieldMappingKind::PHYSICAL;
    std::unordered_map<int32_t, const FieldSchema*> field_id_to_parquet_field;
    std::unordered_map<std::string, const FieldSchema*> name_to_parquet_field;
    auto parquet_fields = parquet_schema.get_fields_schema();
    for (const auto& field : parquet_fields) {
        if (field.field_id >= 0) {
            field_id_to_parquet_field.emplace(field.field_id, &field);
        }
        name_to_parquet_field.emplace(to_lower(field.name), &field);
    }
    for (const auto* slot : tuple_descriptor->slots()) {
        FieldMappingNode child;
        child.table_path = slot->col_name();
        child.file_path = slot->col_name();
        child.iceberg_field_id = slot->col_unique_id();
        const FieldSchema* physical_field = nullptr;
        auto id_it = field_id_to_parquet_field.find(slot->col_unique_id());
        if (id_it != field_id_to_parquet_field.end()) {
            physical_field = id_it->second;
        } else {
            auto name_it = name_to_parquet_field.find(to_lower(slot->col_name()));
            if (name_it != name_to_parquet_field.end()) {
                physical_field = name_it->second;
            }
        }
        if (physical_field == nullptr) {
            child.kind = FieldMappingKind::MISSING;
        } else {
            child.kind = FieldMappingKind::PHYSICAL;
            child.file_path = physical_field->name;
            child.physical = PhysicalFieldRef {physical_field->name, physical_field->field_id,
                                               physical_field->get_column_id(),
                                               physical_field->get_max_column_id()};
        }
        child.cast_plan = CastPlan {slot->get_data_type_ptr(), slot->get_data_type_ptr(),
                                    CastSafety::SAFE_FOR_PUSHDOWN};
        if (slot->col_type() == TYPE_STRUCT || slot->col_type() == TYPE_ARRAY ||
            slot->col_type() == TYPE_MAP) {
            FieldMappingNode levels;
            levels.table_path = slot->col_name() + ".$levels";
            levels.file_path = slot->col_name();
            levels.iceberg_field_id = slot->col_unique_id();
            levels.kind = FieldMappingKind::PHYSICAL;
            levels.physical = child.physical;
            child.children.push_back(std::move(levels));
        }
        root.children.push_back(std::move(child));
    }
    return root;
}

std::vector<RequiredField> IcebergParquetTableReader::_collect_required_fields(
        const TupleDescriptor* tuple_descriptor, const FieldDescriptor& parquet_schema,
        ReaderInitContext* ctx) {
    std::vector<RequiredField> fields;
    for (const auto* slot : tuple_descriptor->slots()) {
        RequiredField field;
        field.table_path = slot->col_name();
        if (slot->col_name() == BeConsts::ICEBERG_ROWID_COL) {
            field.purpose = RequiredFieldPurpose::ROW_ID;
        } else if (slot->col_name() == ROW_LINEAGE_ROW_ID ||
                   slot->col_name() == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
            field.purpose = RequiredFieldPurpose::ROW_LINEAGE;
        } else {
            field.purpose = slot->is_predicate() ? RequiredFieldPurpose::PREDICATE
                                                 : RequiredFieldPurpose::OUTPUT;
        }
        field.allow_lazy_materialization = !slot->is_predicate();
        field.needs_definition_repetition_levels = slot->col_type() == TYPE_STRUCT ||
                                                   slot->col_type() == TYPE_ARRAY ||
                                                   slot->col_type() == TYPE_MAP;
        fields.push_back(std::move(field));
        if (fields.back().needs_definition_repetition_levels) {
            fields.push_back(RequiredField {slot->col_name() + ".$levels",
                                            RequiredFieldPurpose::REFERENCE_LEVELS, true, false,
                                            true});
        }
    }

    if (_scan_range.__isset.table_format_params) {
        std::unordered_map<int32_t, const FieldSchema*> field_id_to_parquet_field;
        auto parquet_fields = parquet_schema.get_fields_schema();
        for (const auto& field : parquet_fields) {
            if (field.field_id >= 0) {
                field_id_to_parquet_field.emplace(field.field_id, &field);
            }
        }
        const auto& iceberg_params = _scan_range.table_format_params.iceberg_params;
        for (const auto& delete_file : iceberg_params.delete_files) {
            if (!delete_file.__isset.field_ids) {
                continue;
            }
            for (int field_id : delete_file.field_ids) {
                auto it = field_id_to_parquet_field.find(field_id);
                if (it == field_id_to_parquet_field.end()) {
                    continue;
                }
                const std::string hidden_name = EQ_DELETE_PRE + it->second->name;
                fields.push_back(RequiredField {hidden_name,
                                                RequiredFieldPurpose::EQUALITY_DELETE_KEY, true,
                                                false, false});
                ctx->table_info_node->add_children(
                        hidden_name, it->second->name,
                        TableSchemaChangeHelper::ConstNode::get_instance());
            }
        }
    }
    return fields;
}

RowVisibility IcebergParquetTableReader::_build_row_visibility() {
    RowVisibility visibility;
    visibility.split_first_row = 0;
    visibility.split_last_row = -1;
    if (!_scan_range.__isset.table_format_params) {
        return visibility;
    }

    // The concrete readers for position delete files and deletion vectors stay
    // behind this table-level API. The important composition contract is that
    // their result becomes RowVisibility before Parquet reads lazy payload
    // columns, so hidden/deleted rows do not force payload materialization.
    [[maybe_unused]] const auto& iceberg_params = _scan_range.table_format_params.iceberg_params;
    visibility.split_last_row = -1;
    return visibility;
}

VirtualColumnPlan IcebergParquetTableReader::_build_virtual_column_plan() {
    VirtualColumnPlan plan;
    plan.predicate_virtual_columns.emplace(
            "__iceberg_partition_or_missing_predicate_columns__",
            [](Block*, size_t, const SelectionVector*) { return Status::OK(); });
    plan.payload_virtual_columns.emplace(
            "__iceberg_generated_payload_columns__",
            [](Block*, size_t, const SelectionVector*) { return Status::OK(); });
    return plan;
}

Status IcebergParquetTableReader::_finalize_block(PhysicalReadBatch* batch, Block* output_block,
                                                  size_t* read_rows) {
    RETURN_IF_ERROR(_apply_equality_delete(batch));
    *output_block = std::move(batch->physical_block);
    if (!batch->row_positions.empty()) {
        RETURN_IF_ERROR(_fill_iceberg_row_id(output_block, batch->row_positions));
        RETURN_IF_ERROR(_fill_row_lineage_columns(output_block, batch->row_positions));
    }
    RETURN_IF_ERROR(_project_output(output_block, batch->hidden_columns));
    *read_rows = output_block->rows();
    return Status::OK();
}

Status IcebergParquetTableReader::_apply_equality_delete(PhysicalReadBatch* batch) {
    if (batch->selection.selected.empty()) {
        return Status::OK();
    }
    IColumn::Filter filter(batch->selection.selected.begin(), batch->selection.selected.end());
    const auto selected_rows = batch->selection.selected_rows();
    for (uint32_t i = 0; i < batch->physical_block.columns(); ++i) {
        auto& column = batch->physical_block.get_by_position(i).column;
        if (column != nullptr) {
            column = column->filter(filter, selected_rows);
        }
    }
    batch->physical_rows = selected_rows;
    if (!batch->row_positions.empty()) {
        std::vector<segment_v2::rowid_t> selected_positions;
        selected_positions.reserve(selected_rows);
        for (size_t i = 0; i < batch->selection.selected.size(); ++i) {
            if (batch->selection.selected[i]) {
                selected_positions.push_back(batch->row_positions[i]);
            }
        }
        batch->row_positions = std::move(selected_positions);
    }
    return Status::OK();
}

Status IcebergParquetTableReader::_fill_iceberg_row_id(
        Block* block, const std::vector<segment_v2::rowid_t>& row_positions) {
    int row_id_pos = block->get_position_by_name(BeConsts::ICEBERG_ROWID_COL);
    if (row_id_pos < 0) {
        return Status::OK();
    }
    const auto& table_desc = _scan_range.table_format_params.iceberg_params;
    std::string file_path = table_desc.original_file_path;
    int32_t partition_spec_id =
            table_desc.__isset.partition_spec_id ? table_desc.partition_spec_id : 0;
    std::string partition_data_json =
            table_desc.__isset.partition_data_json ? table_desc.partition_data_json : "";
    auto& col_with_type = block->get_by_position(row_id_pos);
    MutableColumnPtr row_id_column;
    RETURN_IF_ERROR(_build_iceberg_rowid_column(col_with_type.type, file_path, row_positions,
                                                partition_spec_id, partition_data_json,
                                                &row_id_column));
    col_with_type.column = std::move(row_id_column);
    return Status::OK();
}

Status IcebergParquetTableReader::_build_iceberg_rowid_column(
        const DataTypePtr& type, const std::string& file_path,
        const std::vector<segment_v2::rowid_t>& row_ids, int32_t partition_spec_id,
        const std::string& partition_data_json, MutableColumnPtr* column_out) {
    DORIS_CHECK(type != nullptr);
    DORIS_CHECK(column_out != nullptr);
    MutableColumnPtr column = type->create_column();
    ColumnNullable* nullable_col = check_and_get_column<ColumnNullable>(column.get());
    ColumnStruct* struct_col = nullable_col == nullptr
                                       ? check_and_get_column<ColumnStruct>(column.get())
                                       : check_and_get_column<ColumnStruct>(
                                                 nullable_col->get_nested_column_ptr().get());
    DORIS_CHECK(struct_col != nullptr);
    DORIS_CHECK(struct_col->tuple_size() >= 4);

    auto& file_path_col = struct_col->get_column(0);
    auto& row_pos_col = struct_col->get_column(1);
    auto& spec_id_col = struct_col->get_column(2);
    auto& partition_data_col = struct_col->get_column(3);
    for (segment_v2::rowid_t row_id : row_ids) {
        file_path_col.insert_data(file_path.data(), file_path.size());
        int64_t row_pos = static_cast<int64_t>(row_id);
        row_pos_col.insert_data(reinterpret_cast<const char*>(&row_pos), sizeof(row_pos));
        spec_id_col.insert_data(reinterpret_cast<const char*>(&partition_spec_id),
                                sizeof(partition_spec_id));
        partition_data_col.insert_data(partition_data_json.data(), partition_data_json.size());
    }
    if (nullable_col != nullptr) {
        nullable_col->get_null_map_data().resize_fill(row_ids.size(), 0);
    }
    *column_out = std::move(column);
    return Status::OK();
}

Status IcebergParquetTableReader::_fill_row_lineage_columns(
        Block* block, const std::vector<segment_v2::rowid_t>& row_positions) {
    if (!_scan_range.__isset.table_format_params) {
        return Status::OK();
    }
    const auto& table_desc = _scan_range.table_format_params.iceberg_params;
    int row_id_pos = block->get_position_by_name(ROW_LINEAGE_ROW_ID);
    if (row_id_pos >= 0 && table_desc.__isset.first_row_id) {
        auto column = ColumnInt64::create();
        auto& data = column->get_data();
        data.resize(row_positions.size());
        for (size_t i = 0; i < row_positions.size(); ++i) {
            data[i] = table_desc.first_row_id + row_positions[i];
        }
        block->replace_by_position(row_id_pos, make_nullable(std::move(column)));
    }

    int seq_pos = block->get_position_by_name(ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER);
    if (seq_pos >= 0 && table_desc.__isset.last_updated_sequence_number) {
        auto column = ColumnInt64::create();
        column->insert_many_vals(table_desc.last_updated_sequence_number, row_positions.size());
        block->replace_by_position(seq_pos, make_nullable(std::move(column)));
    }
    return Status::OK();
}

Status IcebergParquetTableReader::_project_output(Block* block,
                                                  const std::vector<std::string>& hidden_columns) {
    std::set<size_t> erase_positions;
    for (const auto& name : hidden_columns) {
        int pos = block->get_position_by_name(name);
        if (pos >= 0) {
            erase_positions.insert(static_cast<size_t>(pos));
        }
    }
    if (!erase_positions.empty()) {
        block->erase(erase_positions);
    }
    return Status::OK();
}

IcebergParquetReaderAdapter::IcebergParquetReaderAdapter(
        ShardedKVCache* kv_cache, RuntimeProfile* profile, const TFileScanRangeParams& params,
        const TFileRangeDesc& range, size_t batch_size, const cctz::time_zone* ctz,
        io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache)
        : _table_reader(kv_cache, profile, params, range, batch_size, ctz, io_ctx, state,
                        meta_cache) {}

Status IcebergParquetReaderAdapter::_do_init_reader(ReaderInitContext* ctx) {
    TableReadTask task;
    task.tuple_descriptor = ctx->tuple_descriptor;
    task.output_slots = ctx->tuple_descriptor->slots();
    task.read_context.legacy_init_context = ctx;
    task.read_context.profile = nullptr;
    task.read_context.state = ctx->state;
    return _table_reader.open(task);
}

Status IcebergParquetReaderAdapter::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    return _table_reader.next_block(block, read_rows, eof);
}

// ============================================================================
// IcebergParquetReader: on_before_init_reader (Parquet-specific schema matching)
// ============================================================================
Status IcebergParquetReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    _file_format = Fileformat::PARQUET;

    // Get file metadata schema first (available because _open_file() already ran)
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(this->get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    // Build table_info_node by field_id or name matching.
    // This must happen BEFORE column classification so we can use children_column_exists
    // to check if a column exists in the file (by field ID, not name).
    if (!get_scan_params().__isset.history_schema_info ||
        get_scan_params().history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(ctx->tuple_descriptor, *field_desc,
                                                            ctx->table_info_node));
    } else {
        bool exist_field_id = true;
        RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_field_id(
                get_scan_params().history_schema_info.front().root_field, *field_desc,
                ctx->table_info_node, exist_field_id));
        if (!exist_field_id) {
            RETURN_IF_ERROR(BuildTableInfoUtil::by_parquet_name(ctx->tuple_descriptor, *field_desc,
                                                                ctx->table_info_node));
        }
    }

    std::unordered_set<std::string> partition_col_names;
    if (ctx->range->__isset.columns_from_path_keys) {
        partition_col_names.insert(ctx->range->columns_from_path_keys.begin(),
                                   ctx->range->columns_from_path_keys.end());
    }

    // Single pass: classify columns, detect $row_id, handle partition fallback.
    bool has_partition_from_path = false;
    for (auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::SYNTHESIZED) {
            if (desc.name == BeConsts::ICEBERG_ROWID_COL) {
                this->register_synthesized_column_handler(
                        BeConsts::ICEBERG_ROWID_COL, [this](Block* block, size_t rows) -> Status {
                            return _fill_iceberg_row_id(block, rows);
                        });
                continue;
            } else if (desc.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
                auto topn_row_id_column_iter = _create_topn_row_id_column_iterator();
                this->register_synthesized_column_handler(
                        desc.name,
                        [iter = std::move(topn_row_id_column_iter), this, &desc](
                                Block* block, size_t rows) -> Status {
                            return fill_topn_row_id(iter, desc.name, block, rows);
                        });
                continue;
            }
        } else if (desc.category == ColumnCategory::REGULAR) {
            // Partition fallback: if column is a partition key and NOT in the file
            // (checked via field ID matching in table_info_node), read from path instead.
            if (partition_col_names.contains(desc.name) &&
                !ctx->table_info_node->children_column_exists(desc.name)) {
                if (config::enable_iceberg_partition_column_fallback) {
                    desc.category = ColumnCategory::PARTITION_KEY;
                    has_partition_from_path = true;
                    continue;
                }
            }
            ctx->column_names.push_back(desc.name);
        } else if (desc.category == ColumnCategory::GENERATED) {
            _init_row_lineage_columns();
            if (desc.name == ROW_LINEAGE_ROW_ID) {
                ctx->column_names.push_back(desc.name);
                this->register_generated_column_handler(
                        ROW_LINEAGE_ROW_ID, [this](Block* block, size_t rows) -> Status {
                            return _fill_row_lineage_row_id(block, rows);
                        });
                continue;
            } else if (desc.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
                ctx->column_names.push_back(desc.name);
                this->register_generated_column_handler(
                        ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER,
                        [this](Block* block, size_t rows) -> Status {
                            return _fill_row_lineage_last_updated_sequence_number(block, rows);
                        });
                continue;
            }
        }
    }

    // Set up partition value extraction if any partition columns need filling from path
    if (has_partition_from_path) {
        RETURN_IF_ERROR(_extract_partition_values(*ctx->range, ctx->tuple_descriptor,
                                                  _fill_partition_values));
    }

    _all_required_col_names = ctx->column_names;

    // Create column IDs from field descriptor
    auto column_id_result = _create_column_ids(field_desc, ctx->tuple_descriptor);
    ctx->column_ids = std::move(column_id_result.column_ids);
    ctx->filter_column_ids = std::move(column_id_result.filter_column_ids);

    // Build field_id -> block_column_name mapping for equality delete filtering.
    // This was previously done in init_reader() column matching (pre-CRTP refactoring).
    for (const auto* slot : ctx->tuple_descriptor->slots()) {
        _id_to_block_column_name.emplace(slot->col_unique_id(), slot->col_name());
    }

    // Process delete files (must happen before _do_init_reader so expand col IDs are included)
    RETURN_IF_ERROR(_init_row_filters());

    // Add expand column IDs for equality delete and remap expand column names
    // to match master's behavior:
    // - Use field_id to find the actual file column name in Parquet schema
    // - Prefix with __equality_delete_column__ to avoid name conflicts
    // - Correctly map table_col_name → file_col_name in table_info_node
    const static std::string EQ_DELETE_PRE = "__equality_delete_column__";
    std::unordered_map<int, std::string> field_id_to_file_col_name;
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (field_schema) {
            field_id_to_file_col_name[field_schema->field_id] = field_schema->name;
        }
    }

    // Rebuild _expand_col_names with proper file-column-based names
    std::vector<std::string> new_expand_col_names;
    for (size_t i = 0; i < _expand_col_names.size(); ++i) {
        const auto& old_name = _expand_col_names[i];
        // Find the field_id for this expand column
        int field_id = -1;
        for (auto& [fid, name] : _id_to_block_column_name) {
            if (name == old_name) {
                field_id = fid;
                break;
            }
        }

        std::string file_col_name = old_name;
        auto it = field_id_to_file_col_name.find(field_id);
        if (it != field_id_to_file_col_name.end()) {
            file_col_name = it->second;
        }

        std::string table_col_name = EQ_DELETE_PRE + file_col_name;

        // Update _id_to_block_column_name
        if (field_id >= 0) {
            _id_to_block_column_name[field_id] = table_col_name;
        }

        // Update _expand_columns name
        if (i < _expand_columns.size()) {
            _expand_columns[i].name = table_col_name;
        }

        new_expand_col_names.push_back(table_col_name);

        // Add column IDs
        if (it != field_id_to_file_col_name.end()) {
            for (int j = 0; j < field_desc->size(); ++j) {
                auto field_schema = field_desc->get_column(j);
                if (field_schema && field_schema->field_id == field_id) {
                    ctx->column_ids.insert(field_schema->get_column_id());
                    break;
                }
            }
        }

        // Register in table_info_node: table_col_name → file_col_name
        ctx->column_names.push_back(table_col_name);
        ctx->table_info_node->add_children(table_col_name, file_col_name,
                                           TableSchemaChangeHelper::ConstNode::get_instance());
    }
    _expand_col_names = std::move(new_expand_col_names);

    // Enable group filtering for Iceberg
    _filter_groups = true;

    return Status::OK();
}

// ============================================================================
// IcebergParquetReader: _create_column_ids
// ============================================================================
ColumnIdResult IcebergParquetReader::_create_column_ids(const FieldDescriptor* field_desc,
                                                        const TupleDescriptor* tuple_descriptor) {
    auto* mutable_field_desc = const_cast<FieldDescriptor*>(field_desc);
    mutable_field_desc->assign_ids();

    std::unordered_map<int, const FieldSchema*> iceberg_id_to_field_schema_map;
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;
        int iceberg_id = field_schema->field_id;
        iceberg_id_to_field_schema_map[iceberg_id] = field_schema;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    auto process_access_paths = [](const FieldSchema* parquet_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                parquet_field, access_paths, out_ids,
                [](const FieldSchema* field) { return field->get_column_id(); },
                [](const FieldSchema* field) { return field->get_max_column_id(); },
                IcebergParquetNestedColumnUtils::extract_nested_column_ids);
    };

    for (const auto* slot : tuple_descriptor->slots()) {
        auto it = iceberg_id_to_field_schema_map.find(slot->col_unique_id());
        if (it == iceberg_id_to_field_schema_map.end()) {
            continue;
        }
        auto field_schema = it->second;

        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(field_schema->column_id);
            if (slot->is_predicate()) {
                filter_column_ids.insert(field_schema->column_id);
            }
            continue;
        }

        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(field_schema, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(field_schema, predicate_access_paths, filter_column_ids);
        }
    }
    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

// ============================================================================
// IcebergParquetReader: _read_position_delete_file
// ============================================================================
Status IcebergParquetReader::_read_position_delete_file(const TFileRangeDesc* delete_range,
                                                        DeleteFile* position_delete) {
    ParquetReader parquet_delete_reader(get_profile(), get_scan_params(), *delete_range,
                                        READ_DELETE_FILE_BATCH_SIZE, &get_state()->timezone_obj(),
                                        get_io_ctx(), get_state(), _meta_cache);
    // The delete file range has size=-1 (read whole file). We must disable
    // row group filtering before init; otherwise _do_init_reader returns EndOfFile
    // when _filter_groups && _range_size < 0.
    ParquetInitContext delete_ctx;
    delete_ctx.filter_groups = false;
    delete_ctx.column_names = delete_file_col_names;
    delete_ctx.col_name_to_block_idx =
            const_cast<std::unordered_map<std::string, uint32_t>*>(&DELETE_COL_NAME_TO_BLOCK_IDX);
    RETURN_IF_ERROR(parquet_delete_reader.init_reader(&delete_ctx));

    const tparquet::FileMetaData* meta_data = parquet_delete_reader.get_meta_data();
    bool dictionary_coded = true;
    for (const auto& row_group : meta_data->row_groups) {
        const auto& column_chunk = row_group.columns[ICEBERG_FILE_PATH_INDEX];
        if (!(column_chunk.__isset.meta_data && has_dict_page(column_chunk.meta_data))) {
            dictionary_coded = false;
            break;
        }
    }
    DataTypePtr data_type_file_path {new DataTypeString};
    DataTypePtr data_type_pos {new DataTypeInt64};
    bool eof = false;
    while (!eof) {
        Block block = {dictionary_coded
                               ? ColumnWithTypeAndName {ColumnDictI32::create(
                                                                FieldType::OLAP_FIELD_TYPE_VARCHAR),
                                                        data_type_file_path, ICEBERG_FILE_PATH}
                               : ColumnWithTypeAndName {data_type_file_path, ICEBERG_FILE_PATH},

                       {data_type_pos, ICEBERG_ROW_POS}};
        size_t read_rows = 0;
        RETURN_IF_ERROR(parquet_delete_reader.get_next_block(&block, &read_rows, &eof));

        if (read_rows <= 0) {
            break;
        }
        _gen_position_delete_file_range(block, position_delete, read_rows, dictionary_coded);
    }
    return Status::OK();
};

// ============================================================================
// IcebergOrcReader: on_before_init_reader (ORC-specific schema matching)
// ============================================================================
Status IcebergOrcReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    _file_format = Fileformat::ORC;

    // Get ORC file type first (available because _create_file_reader() already ran)
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(this->get_file_type(&orc_type_ptr));

    // Build table_info_node by field_id or name matching.
    // This must happen BEFORE column classification so we can use children_column_exists
    // to check if a column exists in the file (by field ID, not name).
    if (!get_scan_params().__isset.history_schema_info ||
        get_scan_params().history_schema_info.empty()) [[unlikely]] {
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(ctx->tuple_descriptor, orc_type_ptr,
                                                        ctx->table_info_node));
    } else {
        bool exist_field_id = true;
        RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_field_id(
                get_scan_params().history_schema_info.front().root_field, orc_type_ptr,
                ICEBERG_ORC_ATTRIBUTE, ctx->table_info_node, exist_field_id));
        if (!exist_field_id) {
            RETURN_IF_ERROR(BuildTableInfoUtil::by_orc_name(ctx->tuple_descriptor, orc_type_ptr,
                                                            ctx->table_info_node));
        }
    }

    std::unordered_set<std::string> partition_col_names;
    if (ctx->range->__isset.columns_from_path_keys) {
        partition_col_names.insert(ctx->range->columns_from_path_keys.begin(),
                                   ctx->range->columns_from_path_keys.end());
    }

    // Single pass: classify columns, detect $row_id, handle partition fallback.
    bool has_partition_from_path = false;
    for (auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::SYNTHESIZED) {
            if (desc.name == BeConsts::ICEBERG_ROWID_COL) {
                this->register_synthesized_column_handler(
                        BeConsts::ICEBERG_ROWID_COL, [this](Block* block, size_t rows) -> Status {
                            return _fill_iceberg_row_id(block, rows);
                        });
                continue;
            } else if (desc.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
                auto topn_row_id_column_iter = _create_topn_row_id_column_iterator();
                this->register_synthesized_column_handler(
                        desc.name,
                        [iter = std::move(topn_row_id_column_iter), this, &desc](
                                Block* block, size_t rows) -> Status {
                            return fill_topn_row_id(iter, desc.name, block, rows);
                        });
                continue;
            }
        } else if (desc.category == ColumnCategory::REGULAR) {
            // Partition fallback: if column is a partition key and NOT in the file
            // (checked via field ID matching in table_info_node), read from path instead.
            if (partition_col_names.contains(desc.name) &&
                !ctx->table_info_node->children_column_exists(desc.name)) {
                if (config::enable_iceberg_partition_column_fallback) {
                    desc.category = ColumnCategory::PARTITION_KEY;
                    has_partition_from_path = true;
                    continue;
                }
            }
            ctx->column_names.push_back(desc.name);
        } else if (desc.category == ColumnCategory::GENERATED) {
            _init_row_lineage_columns();
            if (desc.name == ROW_LINEAGE_ROW_ID) {
                ctx->column_names.push_back(desc.name);
                this->register_generated_column_handler(
                        ROW_LINEAGE_ROW_ID, [this](Block* block, size_t rows) -> Status {
                            return _fill_row_lineage_row_id(block, rows);
                        });
                continue;
            } else if (desc.name == ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER) {
                ctx->column_names.push_back(desc.name);
                this->register_generated_column_handler(
                        ROW_LINEAGE_LAST_UPDATED_SEQ_NUMBER,
                        [this](Block* block, size_t rows) -> Status {
                            return _fill_row_lineage_last_updated_sequence_number(block, rows);
                        });
                continue;
            }
        }
    }

    if (has_partition_from_path) {
        RETURN_IF_ERROR(_extract_partition_values(*ctx->range, ctx->tuple_descriptor,
                                                  _fill_partition_values));
    }

    _all_required_col_names = ctx->column_names;

    // Create column IDs from ORC type
    auto column_id_result = _create_column_ids(orc_type_ptr, ctx->tuple_descriptor);
    ctx->column_ids = std::move(column_id_result.column_ids);
    ctx->filter_column_ids = std::move(column_id_result.filter_column_ids);

    // Build field_id -> block_column_name mapping for equality delete filtering.
    for (const auto* slot : ctx->tuple_descriptor->slots()) {
        _id_to_block_column_name.emplace(slot->col_unique_id(), slot->col_name());
    }

    // Process delete files (must happen before _do_init_reader so expand col IDs are included)
    RETURN_IF_ERROR(_init_row_filters());

    // Add expand column IDs for equality delete and remap expand column names
    // (matching master's behavior with __equality_delete_column__ prefix)
    const static std::string EQ_DELETE_PRE = "__equality_delete_column__";
    std::unordered_map<int, std::string> field_id_to_file_col_name;
    for (uint64_t i = 0; i < orc_type_ptr->getSubtypeCount(); ++i) {
        std::string col_name = orc_type_ptr->getFieldName(i);
        const orc::Type* sub_type = orc_type_ptr->getSubtype(i);
        if (sub_type->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
            int fid = std::stoi(sub_type->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
            field_id_to_file_col_name[fid] = col_name;
        }
    }

    std::vector<std::string> new_expand_col_names;
    for (size_t i = 0; i < _expand_col_names.size(); ++i) {
        const auto& old_name = _expand_col_names[i];
        int field_id = -1;
        for (auto& [fid, name] : _id_to_block_column_name) {
            if (name == old_name) {
                field_id = fid;
                break;
            }
        }

        std::string file_col_name = old_name;
        auto it = field_id_to_file_col_name.find(field_id);
        if (it != field_id_to_file_col_name.end()) {
            file_col_name = it->second;
        }

        std::string table_col_name = EQ_DELETE_PRE + file_col_name;

        if (field_id >= 0) {
            _id_to_block_column_name[field_id] = table_col_name;
        }
        if (i < _expand_columns.size()) {
            _expand_columns[i].name = table_col_name;
        }
        new_expand_col_names.push_back(table_col_name);

        // Add column IDs
        if (it != field_id_to_file_col_name.end()) {
            for (uint64_t j = 0; j < orc_type_ptr->getSubtypeCount(); ++j) {
                const orc::Type* sub_type = orc_type_ptr->getSubtype(j);
                if (orc_type_ptr->getFieldName(j) == file_col_name) {
                    ctx->column_ids.insert(sub_type->getColumnId());
                    break;
                }
            }
        }

        ctx->column_names.push_back(table_col_name);
        ctx->table_info_node->add_children(table_col_name, file_col_name,
                                           TableSchemaChangeHelper::ConstNode::get_instance());
    }
    _expand_col_names = std::move(new_expand_col_names);

    return Status::OK();
}

// ============================================================================
// IcebergOrcReader: _create_column_ids
// ============================================================================
ColumnIdResult IcebergOrcReader::_create_column_ids(const orc::Type* orc_type,
                                                    const TupleDescriptor* tuple_descriptor) {
    std::unordered_map<int, const orc::Type*> iceberg_id_to_orc_type_map;
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        auto orc_sub_type = orc_type->getSubtype(i);
        if (!orc_sub_type) continue;
        if (!orc_sub_type->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
            continue;
        }
        int iceberg_id = std::stoi(orc_sub_type->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
        iceberg_id_to_orc_type_map[iceberg_id] = orc_sub_type;
    }

    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    auto process_access_paths = [](const orc::Type* orc_field,
                                   const std::vector<TColumnAccessPath>& access_paths,
                                   std::set<uint64_t>& out_ids) {
        process_nested_access_paths(
                orc_field, access_paths, out_ids,
                [](const orc::Type* type) { return type->getColumnId(); },
                [](const orc::Type* type) { return type->getMaximumColumnId(); },
                IcebergOrcNestedColumnUtils::extract_nested_column_ids);
    };

    for (const auto* slot : tuple_descriptor->slots()) {
        auto it = iceberg_id_to_orc_type_map.find(slot->col_unique_id());
        if (it == iceberg_id_to_orc_type_map.end()) {
            continue;
        }
        const orc::Type* orc_field = it->second;

        if ((slot->col_type() != TYPE_STRUCT && slot->col_type() != TYPE_ARRAY &&
             slot->col_type() != TYPE_MAP)) {
            column_ids.insert(orc_field->getColumnId());
            if (slot->is_predicate()) {
                filter_column_ids.insert(orc_field->getColumnId());
            }
            continue;
        }

        const auto& all_access_paths = slot->all_access_paths();
        process_access_paths(orc_field, all_access_paths, column_ids);

        const auto& predicate_access_paths = slot->predicate_access_paths();
        if (!predicate_access_paths.empty()) {
            process_access_paths(orc_field, predicate_access_paths, filter_column_ids);
        }
    }

    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids));
}

// ============================================================================
// IcebergOrcReader: _read_position_delete_file
// ============================================================================
Status IcebergOrcReader::_read_position_delete_file(const TFileRangeDesc* delete_range,
                                                    DeleteFile* position_delete) {
    OrcReader orc_delete_reader(get_profile(), get_state(), get_scan_params(), *delete_range,
                                READ_DELETE_FILE_BATCH_SIZE, get_state()->timezone(), get_io_ctx(),
                                _meta_cache);
    OrcInitContext delete_ctx;
    delete_ctx.column_names = delete_file_col_names;
    delete_ctx.col_name_to_block_idx =
            const_cast<std::unordered_map<std::string, uint32_t>*>(&DELETE_COL_NAME_TO_BLOCK_IDX);
    RETURN_IF_ERROR(orc_delete_reader.init_reader(&delete_ctx));

    bool eof = false;
    DataTypePtr data_type_file_path {new DataTypeString};
    DataTypePtr data_type_pos {new DataTypeInt64};
    while (!eof) {
        Block block = {{data_type_file_path, ICEBERG_FILE_PATH}, {data_type_pos, ICEBERG_ROW_POS}};

        size_t read_rows = 0;
        RETURN_IF_ERROR(orc_delete_reader.get_next_block(&block, &read_rows, &eof));

        _gen_position_delete_file_range(block, position_delete, read_rows, false);
    }
    return Status::OK();
}

} // namespace doris
