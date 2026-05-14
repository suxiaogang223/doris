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

#include "format/parquet/vparquet_reader.h"

#include <algorithm>
#include <utility>

namespace doris {

ParquetReader::ParquetReader(FileSplit split, ReaderRuntimeOptions runtime_options)
        : BaseFileFormatReader(std::move(split), runtime_options),
          _batch_size(std::max<size_t>(runtime_options.batch_size, 1)),
          _ctz(runtime_options.ctz),
          _io_ctx(runtime_options.io_ctx),
          _state(runtime_options.read_context.state),
          _meta_cache(runtime_options.meta_cache) {}

Status ParquetReader::open() {
    RETURN_IF_ERROR(_open_footer());
    return Status::OK();
}

Status ParquetReader::initialize_scan(FormatReaderScanState* state) {
    auto* parquet_state = static_cast<ParquetScanState*>(state);
    parquet_state->finished = false;
    parquet_state->current_row_group = -1;
    parquet_state->offset_in_row_group = 0;
    parquet_state->row_group_first_row = 0;
    parquet_state->current_row_group_prefetched = false;
    parquet_state->output_template = scan_properties.physical_read_template;

    RETURN_IF_ERROR(_build_lazy_read_plan(&parquet_state->lazy_plan));
    RETURN_IF_ERROR(_create_column_reader_tree(parquet_state));
    return Status::OK();
}

Status ParquetReader::scan(FormatReaderScanState* state, Block* block, bool* eof) {
    auto* parquet_state = static_cast<ParquetScanState*>(state);
    while (true) {
        bool produced = false;
        RETURN_IF_ERROR(_scan_internal(parquet_state, block, &produced, eof));
        if (*eof || produced) {
            return Status::OK();
        }
        block->clear();
    }
}

Status ParquetReader::close() {
    _closed = true;
    return Status::OK();
}

Status ParquetReader::_open_footer() {
    if (!_footer.schema.root.table_path.empty()) {
        return Status::OK();
    }

    // Pseudocode:
    // 1. Open file_path through FileFactory and Doris IOContext.
    // 2. Read Parquet footer, row group metadata, page index and column chunks.
    // 3. Build a recursive PhysicalFileSchema with schema index, column index,
    //    field id, max definition level and max repetition level.
    // 4. Keep this physical schema independent from any table-format schema
    //    mapping. IcebergTableReader will consume it later.
    _footer.schema.root.table_path = "$root";
    _footer.schema.root.file_path = "$root";
    _footer.schema.root.kind = FieldMappingKind::PHYSICAL;
    _footer.schema.has_field_ids = true;
    _footer.total_rows = 0;
    _footer.row_group_first_rows.clear();
    return Status::OK();
}

Status ParquetReader::_build_lazy_read_plan(ParquetLazyReadPlan* plan) {
    *plan = ParquetLazyReadPlan {};
    for (const auto& field : scan_properties.required_fields) {
        if (field.purpose == RequiredFieldPurpose::PREDICATE) {
            plan->predicate_fields.push_back(field);
        } else if (field.purpose == RequiredFieldPurpose::LEVELS_ONLY ||
                   field.purpose == RequiredFieldPurpose::REFERENCE_LEVELS) {
            plan->levels_only_fields.push_back(field);
        } else if (field.hidden) {
            plan->hidden_fields.push_back(field);
        } else {
            plan->payload_fields.push_back(field);
        }
    }
    return Status::OK();
}

Status ParquetReader::_create_column_reader_tree(ParquetScanState* state) {
    // Pseudocode, directly aligned with DuckDB CreateReaderRecursive:
    //
    // Build a recursive ColumnReader tree from scan_properties.schema_mapping_root
    // and the physical Parquet schema. Leaf readers decode pages. Struct/list/map
    // readers own children and preserve definition/repetition level semantics.
    // Expression/virtual readers are wrappers only when the physical layer can
    // safely evaluate them before lazy payload reads.
    return Status::OK();
}

Status ParquetReader::_scan_internal(ParquetScanState* state, Block* block, bool* produced,
                                     bool* eof) {
    *produced = false;
    *eof = false;
    if (state->finished) {
        *eof = true;
        return Status::OK();
    }

    if (state->current_row_group < 0) {
        RETURN_IF_ERROR(_switch_row_group(state, eof));
        if (*eof) {
            return Status::OK();
        }
        return Status::OK();
    }

    *block = state->output_template;
    state->selection.reset(0);
    state->selected_rows = 0;

    ParquetRowGroupTask row_group;
    row_group.row_group_id = state->current_row_group;
    row_group.first_row = state->row_group_first_row + state->offset_in_row_group;
    row_group.row_count = _batch_size;
    row_group.candidate_rows.reset(row_group.row_count, true);

    RETURN_IF_ERROR(_apply_row_visibility(state, &row_group));
    RETURN_IF_ERROR(_read_predicate_columns(state, row_group, block));
    RETURN_IF_ERROR(_materialize_predicate_virtual_columns(state, block));
    RETURN_IF_ERROR(_evaluate_predicates(state, block));
    RETURN_IF_ERROR(_read_payload_columns(state, row_group, block));
    RETURN_IF_ERROR(_read_levels_only_columns(state, row_group, block));
    RETURN_IF_ERROR(_attach_row_positions(state, row_group, block));
    RETURN_IF_ERROR(_materialize_auxiliary_columns(*state, block));

    state->offset_in_row_group += row_group.row_count;
    *produced = block->rows() > 0 || state->selected_rows > 0;
    return Status::OK();
}

Status ParquetReader::_switch_row_group(ParquetScanState* state, bool* eof) {
    ++state->current_row_group;
    state->offset_in_row_group = 0;
    state->current_row_group_prefetched = false;

    if (state->current_row_group >= static_cast<int32_t>(_footer.row_group_first_rows.size())) {
        state->finished = true;
        *eof = true;
        return Status::OK();
    }

    state->row_group_first_row = _footer.row_group_first_rows[state->current_row_group];
    RETURN_IF_ERROR(_prepare_row_group(state));
    *eof = false;
    return Status::OK();
}

Status ParquetReader::_prepare_row_group(ParquetScanState* state) {
    RETURN_IF_ERROR(_apply_row_group_pruning(state));
    RETURN_IF_ERROR(_register_prefetch(state));

    // Pseudocode:
    // root_reader->InitializeRead(current_row_group, column_chunks, thrift_proto)
    return Status::OK();
}

Status ParquetReader::_register_prefetch(ParquetScanState* state) {
    // Pseudocode:
    // 1. Estimate row group span and compressed bytes for required physical
    //    columns.
    // 2. If scan ratio is high and no filters exist, prefetch the whole row
    //    group.
    // 3. Otherwise register column-wise ranges. In lazy mode, prefetch predicate
    //    columns eagerly and payload columns only when selected rows survive.
    return Status::OK();
}

Status ParquetReader::_apply_row_group_pruning(ParquetScanState* state) {
    // Pseudocode:
    // Use min/max, bloom filter, dictionary and page index for physical
    // predicate fields. This can finish the row group without invoking payload
    // column readers.
    return Status::OK();
}

Status ParquetReader::_apply_row_visibility(ParquetScanState* state,
                                            ParquetRowGroupTask* row_group) {
    if (!scan_properties.row_visibility.needs_row_positions()) {
        return Status::OK();
    }
    for (size_t i = 0; i < row_group->candidate_rows.selected.size(); ++i) {
        const int64_t file_row = row_group->first_row + static_cast<int64_t>(i);
        if (!scan_properties.row_visibility.is_visible(file_row)) {
            row_group->candidate_rows.selected[i] = 0;
        }
    }
    return Status::OK();
}

Status ParquetReader::_read_predicate_columns(ParquetScanState* state,
                                              const ParquetRowGroupTask& row_group, Block* block) {
    // Pseudocode:
    // for field in state->lazy_plan.predicate_fields:
    //   child_reader.Filter(row_group.row_count, levels, vector, predicate,
    //                       filter_state, selection, selected_rows)
    state->selected_rows = row_group.candidate_rows.selected_rows();
    state->selection = row_group.candidate_rows;
    return Status::OK();
}

Status ParquetReader::_materialize_predicate_virtual_columns(ParquetScanState* state,
                                                             Block* block) {
    for (const auto& [_, handler] : scan_properties.virtual_columns.predicate_virtual_columns) {
        RETURN_IF_ERROR(handler(block, state->selected_rows, &state->selection));
    }
    return Status::OK();
}

Status ParquetReader::_evaluate_predicates(ParquetScanState* state, Block* block) {
    // Pseudocode:
    // Intersect vectorized predicate results into state->selection. Adaptive
    // predicate ordering belongs to scan state, as in DuckDB's AdaptiveFilter.
    return Status::OK();
}

Status ParquetReader::_read_payload_columns(ParquetScanState* state,
                                            const ParquetRowGroupTask& row_group, Block* block) {
    // Pseudocode:
    // if selected_rows == 0:
    //   payload_reader.Skip(row_group.row_count)
    // else:
    //   payload_reader.Select(row_group.row_count, selection, selected_rows, block)
    //
    // This mirrors DuckDB's Filter/Select/Skip boundary and keeps table-format
    // semantics outside the Parquet reader.
    for (const auto& [_, handler] : scan_properties.virtual_columns.payload_virtual_columns) {
        RETURN_IF_ERROR(handler(block, state->selection.selected_rows(), &state->selection));
    }
    return Status::OK();
}

Status ParquetReader::_read_levels_only_columns(ParquetScanState* state,
                                                const ParquetRowGroupTask& row_group,
                                                Block* block) {
    // Pseudocode:
    // For nested missing fields, call ColumnReader::read_levels on a reference
    // physical child so IcebergTableReader can synthesize missing nested values
    // by element cardinality.
    return Status::OK();
}

Status ParquetReader::_attach_row_positions(ParquetScanState* state,
                                            const ParquetRowGroupTask& row_group, Block* block) {
    if (!scan_properties.need_row_positions) {
        return Status::OK();
    }
    // Pseudocode:
    // Add a hidden row-position column to block, aligned with the already
    // selected rows. IcebergTableReader consumes it for $row_id / row lineage
    // and removes it during final projection.
    return Status::OK();
}

Status ParquetReader::_materialize_auxiliary_columns(const ParquetScanState& state, Block* block) {
    // Pseudocode:
    // Hidden equality-delete keys, row positions and levels-only columns are
    // real columns in block. Their hidden nature is derived from
    // scan_properties.required_fields, not from a side-channel list.
    return Status::OK();
}

} // namespace doris
