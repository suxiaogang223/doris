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
constexpr const char* ICEBERG_ROW_ID = "$row_id";
constexpr const char* ROW_LINEAGE_ROW_ID = "_row_id";
constexpr const char* ROW_LINEAGE_LAST_UPDATED_SEQUENCE_NUMBER = "_last_updated_sequence_number";
} // namespace

IcebergTableReader::IcebergTableReader(ShardedKVCache* kv_cache) : _kv_cache(kv_cache) {}

Status IcebergTableReader::initialize_scan(const TableReaderScanTask& task,
                                           TableReaderScanState* state) {
    auto* iceberg_state = static_cast<IcebergTableReaderScanState*>(state);
    iceberg_state->task = task;
    iceberg_state->finished = false;

    RETURN_IF_ERROR(_load_split_context(iceberg_state));
    RETURN_IF_ERROR(_create_file_reader(iceberg_state));
    RETURN_IF_ERROR(iceberg_state->file_reader->open());
    RETURN_IF_ERROR(
            _build_schema_mapping(iceberg_state->file_reader->physical_schema(), iceberg_state));
    RETURN_IF_ERROR(_build_delete_plan(iceberg_state));
    RETURN_IF_ERROR(_build_required_fields(iceberg_state));
    RETURN_IF_ERROR(_build_virtual_column_plan(iceberg_state));
    RETURN_IF_ERROR(_configure_file_reader(task, iceberg_state));

    iceberg_state->format_state = std::make_unique<ParquetScanState>();
    return iceberg_state->file_reader->initialize_scan(iceberg_state->format_state.get());
}

Status IcebergTableReader::scan(TableReaderScanState* state, Block* block, size_t* read_rows,
                                bool* eof) {
    auto* iceberg_state = static_cast<IcebergTableReaderScanState*>(state);
    DORIS_CHECK(iceberg_state->file_reader != nullptr);
    DORIS_CHECK(iceberg_state->format_state != nullptr);

    RETURN_IF_ERROR(
            iceberg_state->file_reader->scan(iceberg_state->format_state.get(), block, eof));
    if (*eof || block->rows() == 0) {
        *read_rows = 0;
        return Status::OK();
    }
    return _finalize_block(iceberg_state, block, read_rows);
}

Status IcebergTableReader::finish_scan(TableReaderScanState* state) {
    auto* iceberg_state = static_cast<IcebergTableReaderScanState*>(state);
    iceberg_state->finished = true;
    if (iceberg_state->file_reader != nullptr) {
        RETURN_IF_ERROR(iceberg_state->file_reader->close());
    }
    return Status::OK();
}

Status IcebergTableReader::close() {
    return Status::OK();
}

Status IcebergTableReader::_load_split_context(IcebergTableReaderScanState* state) {
    // Pseudocode:
    // Doris BE receives one FE-planned split. FE has already resolved Iceberg
    // snapshot, manifests, file enumeration and split assignment. The adapter
    // extracts thrift data into TableReaderSplit before this reader sees it.
    state->split.data_file_path = state->task.split.data_file.path;
    state->split.partition_spec_id = 0;
    state->split.partition_data_json.clear();
    state->split.first_row_id = -1;
    state->split.last_updated_sequence_number = -1;
    return Status::OK();
}

Status IcebergTableReader::_create_file_reader(IcebergTableReaderScanState* state) {
    // Pseudocode:
    // Choose BaseFileFormatReader from task.split.data_file.format. This
    // experiment wires only Parquet; ORC/CSV later implement the same base API.
    state->file_reader = std::make_unique<ParquetReader>(state->task.split.data_file,
                                                         state->task.options.runtime_options);
    return Status::OK();
}

Status IcebergTableReader::_build_schema_mapping(const PhysicalFileSchema& physical_schema,
                                                 IcebergTableReaderScanState* state) {
    DORIS_CHECK(state->task.options.tuple_descriptor != nullptr);
    state->mapping = FieldMappingNode {};
    state->mapping.table_path = "$root";
    state->mapping.file_path = "$root";
    state->mapping.kind = FieldMappingKind::PHYSICAL;

    // Pseudocode:
    // For each output/predicate/delete slot:
    //   1. Match by Iceberg field id against physical_schema.
    //   2. If the file lacks ids, fallback to historical name matching.
    //   3. Missing fields become FieldMappingKind::MISSING.
    //   4. Struct/list/map recurse and attach reference-level requirements.
    //   5. Safe casts are marked for file-level pushdown; unsafe casts become
    //      residual predicates in IcebergTableReader.
    for (const auto* slot : state->task.options.tuple_descriptor->slots()) {
        FieldMappingNode child;
        child.table_path = slot->col_name();
        child.file_path = slot->col_name();
        child.table_field_id = slot->col_unique_id();
        child.kind = FieldMappingKind::PHYSICAL;
        child.cast_plan = CastPlan {slot->get_data_type_ptr(), slot->get_data_type_ptr(),
                                    CastSafety::SAFE_FOR_PUSHDOWN};
        state->mapping.children.push_back(std::move(child));
    }
    return Status::OK();
}

Status IcebergTableReader::_build_delete_plan(IcebergTableReaderScanState* state) {
    state->delete_plan = IcebergDeletePlan {};

    // Pseudocode:
    // Position deletes and deletion vectors are translated to RowVisibility so
    // FileFormatReader can apply them before payload lazy materialization.
    //
    // Equality delete files are read by a nested FileFormatReader scan with
    // hidden key RequiredFields. IcebergTableReader owns the matcher and applies
    // it during finalization.
    return Status::OK();
}

Status IcebergTableReader::_build_required_fields(IcebergTableReaderScanState* state) {
    state->required_fields.clear();

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
        state->required_fields.push_back(std::move(field));

        if (!node.children.empty()) {
            RequiredField levels;
            levels.table_path = node.table_path + ".$levels";
            levels.purpose = RequiredFieldPurpose::REFERENCE_LEVELS;
            levels.hidden = true;
            levels.allow_lazy_materialization = false;
            levels.needs_definition_repetition_levels = true;
            state->required_fields.push_back(std::move(levels));
        }
    };
    collect(state->mapping);

    for (const auto& field : state->delete_plan.equality_delete_fields) {
        state->required_fields.push_back(field);
    }

    // Pseudocode:
    // Convert scanner conjuncts into RequiredFieldPurpose::PREDICATE. Predicate
    // fields are read through ColumnReader::Filter before payload fields use
    // ColumnReader::Select.
    return Status::OK();
}

Status IcebergTableReader::_build_virtual_column_plan(IcebergTableReaderScanState* state) {
    state->virtual_columns = VirtualColumnPlan {};

    state->virtual_columns.predicate_virtual_columns.emplace(
            "__iceberg_predicate_virtual_columns__",
            [](Block* block, size_t rows, const SelectionVector* selection) {
                // Pseudocode:
                // Fill partition, missing and generated columns needed by
                // predicates before payload lazy materialization.
                return Status::OK();
            });

    state->virtual_columns.payload_virtual_columns.emplace(
            "__iceberg_payload_virtual_columns__",
            [](Block* block, size_t rows, const SelectionVector* selection) {
                // Pseudocode:
                // Fill partition, missing and generated output columns after
                // the payload columns are selected.
                return Status::OK();
            });

    return Status::OK();
}

Status IcebergTableReader::_configure_file_reader(const TableReaderScanTask& task,
                                                  IcebergTableReaderScanState* state) {
    auto& properties = state->file_reader->scan_properties;
    properties.schema_mapping_root = state->mapping;
    properties.required_fields = state->required_fields;
    properties.row_visibility = state->delete_plan.row_visibility;
    properties.virtual_columns = state->virtual_columns;
    properties.physical_read_template = Block(task.options.tuple_descriptor->slots(), 0);

    properties.need_row_positions = properties.row_visibility.needs_row_positions();
    for (const auto& field : properties.required_fields) {
        properties.need_row_positions |= field.purpose == RequiredFieldPurpose::ROW_ID ||
                                         field.purpose == RequiredFieldPurpose::ROW_LINEAGE;
    }
    return Status::OK();
}

Status IcebergTableReader::_finalize_block(IcebergTableReaderScanState* state, Block* block,
                                           size_t* read_rows) {
    RETURN_IF_ERROR(_apply_equality_delete(state, block));
    RETURN_IF_ERROR(_apply_residual_predicates(state, block));
    RETURN_IF_ERROR(_fill_missing_and_partition_columns(state, block));
    RETURN_IF_ERROR(_fill_generated_columns(state, block));
    RETURN_IF_ERROR(_fill_row_id_columns(state, block));
    RETURN_IF_ERROR(_project_final_block(state, block));
    *read_rows = block->rows();
    return Status::OK();
}

Status IcebergTableReader::_apply_equality_delete(IcebergTableReaderScanState* state,
                                                  Block* block) {
    // Pseudocode:
    // Use hidden equality delete key columns in block to filter rows. Hidden
    // columns are removed in _project_final_block.
    return Status::OK();
}

Status IcebergTableReader::_apply_residual_predicates(IcebergTableReaderScanState* state,
                                                      Block* block) {
    // Pseudocode:
    // Casts or predicates not safe for Parquet pushdown are evaluated here.
    return Status::OK();
}

Status IcebergTableReader::_fill_missing_and_partition_columns(IcebergTableReaderScanState* state,
                                                               Block* block) {
    // Pseudocode:
    // Fill top-level missing columns by row count. Fill nested missing columns
    // using REFERENCE_LEVELS fields so array<struct<missing>> follows element
    // cardinality rather than top-level row cardinality.
    return Status::OK();
}

Status IcebergTableReader::_fill_generated_columns(IcebergTableReaderScanState* state,
                                                   Block* block) {
    // Pseudocode:
    // Compute generated columns after dependencies are available.
    return Status::OK();
}

Status IcebergTableReader::_fill_row_id_columns(IcebergTableReaderScanState* state, Block* block) {
    // Pseudocode:
    // Use the hidden row-position column plus state->split metadata to fill:
    //   - $row_id
    //   - _row_id = first_row_id + row_position
    //   - _last_updated_sequence_number
    return Status::OK();
}

Status IcebergTableReader::_project_final_block(IcebergTableReaderScanState* state, Block* block) {
    // Pseudocode:
    // 1. Drop hidden equality delete, row-position and levels-only columns.
    // 2. Reorder columns to match tuple descriptor.
    return Status::OK();
}

IcebergReaderAdapter::IcebergReaderAdapter(ShardedKVCache* kv_cache, RuntimeProfile* profile,
                                           const TFileScanRangeParams& /*params*/,
                                           const TFileRangeDesc& range, size_t batch_size,
                                           const cctz::time_zone* ctz, io::IOContext* io_ctx,
                                           RuntimeState* state, FileMetaCache* meta_cache)
        : _table_reader(kv_cache),
          _runtime_options(
                  _build_runtime_options(profile, batch_size, ctz, io_ctx, state, meta_cache)),
          _split(_extract_split(range)) {}

Status IcebergReaderAdapter::_do_init_reader(ReaderInitContext* ctx) {
    TableReaderScanTask task;
    task.options.tuple_descriptor = ctx->tuple_descriptor;
    task.options.output_slots = ctx->tuple_descriptor->slots();
    task.options.runtime_options = _runtime_options;
    task.options.runtime_options.read_context.legacy_init_context = ctx;
    task.options.runtime_options.read_context.state = ctx->state;
    task.split = _split;
    return _table_reader.initialize_scan(task, &_scan_state);
}

Status IcebergReaderAdapter::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    return _table_reader.scan(&_scan_state, block, read_rows, eof);
}

ReaderRuntimeOptions IcebergReaderAdapter::_build_runtime_options(
        RuntimeProfile* profile, size_t batch_size, const cctz::time_zone* ctz,
        io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache) {
    ReaderRuntimeOptions options;
    options.read_context.state = state;
    options.read_context.profile = profile;
    options.batch_size = batch_size;
    options.ctz = ctz;
    options.io_ctx = io_ctx;
    options.meta_cache = meta_cache;
    return options;
}

TableReaderSplit IcebergReaderAdapter::_extract_split(const TFileRangeDesc& range) {
    TableReaderSplit split;
    split.data_file.path = range.path;
    split.data_file.start_offset = range.start_offset;
    split.data_file.size = range.size;
    split.data_file.format = "parquet";
    return split;
}

} // namespace doris
