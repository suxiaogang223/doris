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

#include <utility>

namespace doris {

namespace {
constexpr const char* EQ_DELETE_COLUMN_PREFIX = "__iceberg_eq_delete__";
constexpr const char* ICEBERG_ROW_ID = "$row_id";
constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
constexpr const char* ROW_LINEAGE_LAST_UPDATED_SEQUENCE_NUMBER = "_last_updated_sequence_number";
} // namespace

IcebergTableReader::IcebergTableReader(ShardedKVCache* kv_cache, RuntimeProfile* profile,
                                       const TFileScanRangeParams& params,
                                       const TFileRangeDesc& range, size_t batch_size,
                                       const cctz::time_zone* ctz, io::IOContext* io_ctx,
                                       RuntimeState* state, FileMetaCache* meta_cache)
        : _kv_cache(kv_cache),
          _profile(profile),
          _params(params),
          _range(range),
          _batch_size(batch_size),
          _ctz(ctz),
          _io_ctx(io_ctx),
          _state(state),
          _meta_cache(meta_cache) {}

Status IcebergTableReader::open(const TableReadTask& task) {
    _table_task = task;
    RETURN_IF_ERROR(_load_split_context());

    auto parquet_reader = std::make_unique<ParquetReader>(_profile, _params, _range, _batch_size,
                                                          _ctz, _io_ctx, _state, _meta_cache);
    PhysicalFileSchema physical_schema;
    RETURN_IF_ERROR(parquet_reader->load_physical_schema(&physical_schema));

    FieldMappingNode mapping;
    RETURN_IF_ERROR(_build_schema_mapping(physical_schema, &mapping));

    IcebergDeletePlan delete_plan;
    RETURN_IF_ERROR(_build_delete_plan(&delete_plan));

    std::vector<RequiredField> required_fields;
    RETURN_IF_ERROR(_build_required_fields(mapping, delete_plan, &required_fields));

    VirtualColumnPlan virtual_columns;
    RETURN_IF_ERROR(_build_virtual_column_plan(&virtual_columns));

    FormatScanTask format_task;
    RETURN_IF_ERROR(_build_format_scan_task(task, mapping, delete_plan, required_fields,
                                            virtual_columns, &format_task));

    RETURN_IF_ERROR(parquet_reader->set_output_template(format_task.physical_read_template));
    RETURN_IF_ERROR(parquet_reader->open(format_task));
    _file_reader = std::move(parquet_reader);
    return Status::OK();
}

Status IcebergTableReader::next_block(Block* block, size_t* read_rows, bool* eof) {
    DORIS_CHECK(_file_reader != nullptr);
    PhysicalReadBatch batch;
    RETURN_IF_ERROR(_file_reader->next_batch(&batch, eof));
    if (*eof || batch.physical_rows == 0) {
        *read_rows = 0;
        return Status::OK();
    }
    return _finalize_block(&batch, block, read_rows);
}

Status IcebergTableReader::close() {
    if (_file_reader != nullptr) {
        RETURN_IF_ERROR(_file_reader->close());
    }
    return Status::OK();
}

Status IcebergTableReader::_load_split_context() {
    // Pseudocode:
    // TFileRangeDesc already represents one FE-planned split. FE has resolved
    // Iceberg snapshot, manifests and data files before BE sees this reader.
    //
    // Read from _range.table_format_params.iceberg_params:
    //   - original data file path
    //   - partition spec id and partition data
    //   - first_row_id / last_updated_sequence_number
    //   - delete file descriptors scoped to this split
    _split.data_file_path = _range.path;
    _split.partition_spec_id = 0;
    _split.partition_data_json.clear();
    _split.first_row_id = -1;
    _split.last_updated_sequence_number = -1;
    return Status::OK();
}

Status IcebergTableReader::_build_schema_mapping(const PhysicalFileSchema& physical_schema,
                                                 FieldMappingNode* mapping) {
    DORIS_CHECK(_table_task.tuple_descriptor != nullptr);
    *mapping = FieldMappingNode {};
    mapping->table_path = "$root";
    mapping->file_path = "$root";
    mapping->kind = FieldMappingKind::PHYSICAL;

    // Pseudocode:
    // For each slot in tuple_descriptor:
    //   1. Match by Iceberg field id against physical_schema.
    //   2. If Parquet file lacks field ids, fallback to historical name mapping.
    //   3. If still not found, create FieldMappingKind::MISSING.
    //   4. For struct/list/map, build child FieldMappingNode recursively.
    //   5. If the field requires type adaptation, attach CastPlan.
    //   6. For nested missing fields, attach REFERENCE_LEVELS to a physical sibling.
    //
    // This recursive tree is the replacement for inherited TableSchemaChangeHelper
    // state. It is explicit data passed to the file reader.
    for (const auto* slot : _table_task.tuple_descriptor->slots()) {
        FieldMappingNode child;
        child.table_path = slot->col_name();
        child.file_path = slot->col_name();
        child.iceberg_field_id = slot->col_unique_id();
        child.kind = FieldMappingKind::PHYSICAL;
        child.cast_plan = CastPlan {slot->get_data_type_ptr(), slot->get_data_type_ptr(),
                                    CastSafety::SAFE_FOR_PUSHDOWN};

        // Pseudocode placeholder for real recursive lookup:
        // child.physical = find_physical_field(physical_schema.root, slot->col_unique_id(),
        //                                      slot->col_name());
        mapping->children.push_back(std::move(child));
    }
    return Status::OK();
}

Status IcebergTableReader::_build_delete_plan(IcebergDeletePlan* delete_plan) {
    *delete_plan = IcebergDeletePlan {};

    // Pseudocode:
    // for delete_file in _range.table_format_params.iceberg_params.delete_files:
    //   if POSITION_DELETE:
    //       read position delete file and append rows to row_visibility.deleted_rows
    //   if DELETION_VECTOR:
    //       read deletion vector and set row_visibility.deletion_vector
    //   if EQUALITY_DELETE:
    //       read delete keys into equality delete matcher
    //       append hidden RequiredField for each key field id
    //
    // Position deletes and deletion vectors must be converted before Parquet
    // payload lazy materialization so deleted rows do not force payload reads.
    return Status::OK();
}

Status IcebergTableReader::_build_required_fields(const FieldMappingNode& mapping,
                                                  const IcebergDeletePlan& delete_plan,
                                                  std::vector<RequiredField>* required_fields) {
    required_fields->clear();

    std::function<void(const FieldMappingNode&)> collect = [&](const FieldMappingNode& node) {
        if (node.table_path == "$root") {
            for (const auto& child : node.children) {
                collect(child);
            }
            return;
        }

        RequiredField field;
        field.table_path = node.table_path;
        field.purpose = RequiredFieldPurpose::OUTPUT;
        field.allow_lazy_materialization = true;
        required_fields->push_back(std::move(field));

        if (!node.children.empty()) {
            RequiredField levels;
            levels.table_path = node.table_path + ".$levels";
            levels.purpose = RequiredFieldPurpose::REFERENCE_LEVELS;
            levels.hidden = true;
            levels.allow_lazy_materialization = false;
            levels.needs_definition_repetition_levels = true;
            required_fields->push_back(std::move(levels));
        }
    };
    collect(mapping);

    for (const auto& field : delete_plan.equality_delete_fields) {
        required_fields->push_back(field);
    }

    // Pseudocode:
    // Mark fields referenced by pushdown conjuncts as PREDICATE. Those fields
    // are read in ParquetReader::_read_predicate_columns before lazy payload.
    //
    // Mark $row_id, _row_id and _last_updated_sequence_number as ROW_ID /
    // ROW_LINEAGE and set FormatScanTask::need_row_positions.
    return Status::OK();
}

Status IcebergTableReader::_build_virtual_column_plan(VirtualColumnPlan* virtual_columns) {
    *virtual_columns = VirtualColumnPlan {};

    virtual_columns->predicate_virtual_columns.emplace(
            "__iceberg_predicate_virtual_columns__",
            [](Block* block, size_t rows, const SelectionVector* selection) {
                // Pseudocode:
                // Fill partition, missing and generated columns needed by predicates.
                // This keeps predicate evaluation before payload lazy materialization.
                return Status::OK();
            });

    virtual_columns->payload_virtual_columns.emplace(
            "__iceberg_payload_virtual_columns__",
            [](Block* block, size_t rows, const SelectionVector* selection) {
                // Pseudocode:
                // Fill partition, missing and generated output columns after lazy read.
                return Status::OK();
            });

    return Status::OK();
}

Status IcebergTableReader::_build_format_scan_task(
        const TableReadTask& task, const FieldMappingNode& mapping,
        const IcebergDeletePlan& delete_plan, const std::vector<RequiredField>& required_fields,
        const VirtualColumnPlan& virtual_columns, FormatScanTask* format_task) {
    format_task->path = _range.path;
    format_task->split_start = _range.start_offset;
    format_task->split_size = _range.size;
    format_task->schema_mapping_root = mapping;
    format_task->required_fields = required_fields;
    format_task->row_visibility = delete_plan.row_visibility;
    format_task->virtual_columns = virtual_columns;
    format_task->read_context = task.read_context;
    format_task->physical_read_template = Block(task.tuple_descriptor->slots(), 0);

    format_task->need_row_positions = format_task->row_visibility.needs_row_positions();
    for (const auto& field : format_task->required_fields) {
        format_task->need_row_positions |= field.purpose == RequiredFieldPurpose::ROW_ID ||
                                           field.purpose == RequiredFieldPurpose::ROW_LINEAGE;
    }
    return Status::OK();
}

Status IcebergTableReader::_finalize_block(PhysicalReadBatch* batch, Block* block,
                                           size_t* read_rows) {
    RETURN_IF_ERROR(_apply_equality_delete(batch));
    RETURN_IF_ERROR(_apply_residual_predicates(batch));
    RETURN_IF_ERROR(_fill_missing_and_partition_columns(batch));
    RETURN_IF_ERROR(_fill_generated_columns(batch));
    RETURN_IF_ERROR(_fill_row_id_columns(batch));
    RETURN_IF_ERROR(_project_final_block(batch, block));
    *read_rows = block->rows();
    return Status::OK();
}

Status IcebergTableReader::_apply_equality_delete(PhysicalReadBatch* batch) {
    // Pseudocode:
    // Use hidden equality delete key columns in batch->physical_block to filter
    // rows. Hidden columns are removed in _project_final_block.
    return Status::OK();
}

Status IcebergTableReader::_apply_residual_predicates(PhysicalReadBatch* batch) {
    // Pseudocode:
    // Casts or predicates that are not safe for Parquet pushdown are evaluated
    // here after the physical batch is materialized.
    return Status::OK();
}

Status IcebergTableReader::_fill_missing_and_partition_columns(PhysicalReadBatch* batch) {
    // Pseudocode:
    // Fill top-level missing columns by row count. Fill nested missing columns
    // with definition/repetition levels from REFERENCE_LEVELS fields so
    // array<struct<missing>> uses element cardinality instead of row cardinality.
    return Status::OK();
}

Status IcebergTableReader::_fill_generated_columns(PhysicalReadBatch* batch) {
    // Pseudocode:
    // Compute generated columns after physical and virtual payload columns are
    // present. Generated columns disable file-level lazy/dict/min-max
    // optimizations only for their dependencies.
    return Status::OK();
}

Status IcebergTableReader::_fill_row_id_columns(PhysicalReadBatch* batch) {
    // Pseudocode:
    // Use batch->row_positions plus _split metadata to fill:
    //   - $row_id
    //   - _row_id = first_row_id + row_position
    //   - _last_updated_sequence_number
    return Status::OK();
}

Status IcebergTableReader::_project_final_block(PhysicalReadBatch* batch, Block* block) {
    // Pseudocode:
    // 1. Drop hidden equality delete and levels-only columns.
    // 2. Reorder columns to match tuple descriptor.
    // 3. Move the result into block.
    *block = std::move(batch->physical_block);
    return Status::OK();
}

IcebergReaderAdapter::IcebergReaderAdapter(ShardedKVCache* kv_cache, RuntimeProfile* profile,
                                           const TFileScanRangeParams& params,
                                           const TFileRangeDesc& range, size_t batch_size,
                                           const cctz::time_zone* ctz, io::IOContext* io_ctx,
                                           RuntimeState* state, FileMetaCache* meta_cache)
        : _table_reader(kv_cache, profile, params, range, batch_size, ctz, io_ctx, state,
                        meta_cache) {}

Status IcebergReaderAdapter::_do_init_reader(ReaderInitContext* ctx) {
    TableReadTask task;
    task.tuple_descriptor = ctx->tuple_descriptor;
    task.output_slots = ctx->tuple_descriptor->slots();
    task.read_context.legacy_init_context = ctx;
    task.read_context.state = ctx->state;
    return _table_reader.open(task);
}

Status IcebergReaderAdapter::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    return _table_reader.next_block(block, read_rows, eof);
}

} // namespace doris
