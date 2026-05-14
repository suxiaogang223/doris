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

ParquetReader::ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                             const TFileRangeDesc& range, size_t batch_size,
                             const cctz::time_zone* ctz, io::IOContext* io_ctx, RuntimeState* state,
                             FileMetaCache* meta_cache)
        : _profile(profile),
          _params(params),
          _range(range),
          _batch_size(std::max<size_t>(batch_size, 1)),
          _ctz(ctz),
          _io_ctx(io_ctx),
          _state(state),
          _meta_cache(meta_cache) {}

Status ParquetReader::open() {
    RETURN_IF_ERROR(_open_footer());
    return Status::OK();
}

Status ParquetReader::initialize_scan(const FormatScanTask& task, FormatReaderScanState* state) {
    auto* parquet_state = static_cast<ParquetScanState*>(state);
    parquet_state->task = task;
    parquet_state->finished = false;
    parquet_state->current_row_group = -1;
    parquet_state->offset_in_row_group = 0;
    parquet_state->row_group_first_row = 0;
    parquet_state->current_row_group_prefetched = false;
    parquet_state->output_template = task.physical_read_template;

    RETURN_IF_ERROR(_build_lazy_read_plan(task, &parquet_state->lazy_plan));
    RETURN_IF_ERROR(_create_column_reader_tree(task, parquet_state));
    return Status::OK();
}

Status ParquetReader::scan(FormatReaderScanState* state, PhysicalReadBatch* batch, bool* eof) {
    auto* parquet_state = static_cast<ParquetScanState*>(state);
    while (true) {
        bool produced = false;
        RETURN_IF_ERROR(_scan_internal(parquet_state, batch, &produced, eof));
        if (*eof || produced) {
            return Status::OK();
        }
        batch->physical_block.clear();
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
    // 1. Open _range.path through FileFactory and Doris IOContext.
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

Status ParquetReader::_build_lazy_read_plan(const FormatScanTask& task, ParquetLazyReadPlan* plan) {
    *plan = ParquetLazyReadPlan {};
    for (const auto& field : task.required_fields) {
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

Status ParquetReader::_create_column_reader_tree(const FormatScanTask& task,
                                                 ParquetScanState* state) {
    // Pseudocode, directly aligned with DuckDB CreateReaderRecursive:
    //
    // Build a recursive ColumnReader tree from task.schema_mapping_root and the
    // physical Parquet schema. Leaf readers decode pages. Struct/list/map
    // readers own children and preserve definition/repetition level semantics.
    // Expression/virtual readers are wrappers only when the physical layer can
    // safely evaluate them before lazy payload reads.
    return Status::OK();
}

Status ParquetReader::_scan_internal(ParquetScanState* state, PhysicalReadBatch* batch,
                                     bool* produced, bool* eof) {
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

    batch->physical_block = state->output_template;
    batch->hidden_columns.clear();
    batch->row_positions.clear();
    batch->physical_rows = 0;

    ParquetRowGroupTask row_group;
    row_group.row_group_id = state->current_row_group;
    row_group.first_row = state->row_group_first_row + state->offset_in_row_group;
    row_group.row_count = _batch_size;
    row_group.candidate_rows.reset(row_group.row_count, true);

    RETURN_IF_ERROR(_apply_row_visibility(state, &row_group));
    RETURN_IF_ERROR(_read_predicate_columns(state, row_group, batch));
    RETURN_IF_ERROR(_materialize_predicate_virtual_columns(state, batch));
    RETURN_IF_ERROR(_evaluate_predicates(state, batch));
    RETURN_IF_ERROR(_read_payload_columns(state, row_group, batch));
    RETURN_IF_ERROR(_read_levels_only_columns(state, row_group, batch));
    RETURN_IF_ERROR(_attach_row_positions(state, row_group, batch));
    RETURN_IF_ERROR(_attach_hidden_columns(*state, batch));

    state->offset_in_row_group += row_group.row_count;
    *produced = batch->physical_rows > 0;
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
    if (!state->task.row_visibility.needs_row_positions()) {
        return Status::OK();
    }
    for (size_t i = 0; i < row_group->candidate_rows.selected.size(); ++i) {
        const int64_t file_row = row_group->first_row + static_cast<int64_t>(i);
        if (!state->task.row_visibility.is_visible(file_row)) {
            row_group->candidate_rows.selected[i] = 0;
        }
    }
    return Status::OK();
}

Status ParquetReader::_read_predicate_columns(ParquetScanState* state,
                                              const ParquetRowGroupTask& row_group,
                                              PhysicalReadBatch* batch) {
    // Pseudocode:
    // for field in state->lazy_plan.predicate_fields:
    //   child_reader.Filter(row_group.row_count, levels, vector, predicate,
    //                       filter_state, selection, selected_rows)
    batch->physical_rows = row_group.candidate_rows.selected_rows();
    batch->selection = row_group.candidate_rows;
    return Status::OK();
}

Status ParquetReader::_materialize_predicate_virtual_columns(ParquetScanState* state,
                                                             PhysicalReadBatch* batch) {
    for (const auto& [_, handler] : state->task.virtual_columns.predicate_virtual_columns) {
        RETURN_IF_ERROR(handler(&batch->physical_block, batch->physical_rows, &batch->selection));
    }
    return Status::OK();
}

Status ParquetReader::_evaluate_predicates(ParquetScanState* state, PhysicalReadBatch* batch) {
    // Pseudocode:
    // Intersect vectorized predicate results into batch->selection. Adaptive
    // predicate ordering belongs to scan state, as in DuckDB's AdaptiveFilter.
    return Status::OK();
}

Status ParquetReader::_read_payload_columns(ParquetScanState* state,
                                            const ParquetRowGroupTask& row_group,
                                            PhysicalReadBatch* batch) {
    // Pseudocode:
    // if selected_rows == 0:
    //   payload_reader.Skip(row_group.row_count)
    // else:
    //   payload_reader.Select(row_group.row_count, selection, selected_rows, block)
    //
    // This mirrors DuckDB's Filter/Select/Skip boundary and keeps table-format
    // semantics outside the Parquet reader.
    for (const auto& [_, handler] : state->task.virtual_columns.payload_virtual_columns) {
        RETURN_IF_ERROR(handler(&batch->physical_block, batch->selection.selected_rows(),
                                &batch->selection));
    }
    return Status::OK();
}

Status ParquetReader::_read_levels_only_columns(ParquetScanState* state,
                                                const ParquetRowGroupTask& row_group,
                                                PhysicalReadBatch* batch) {
    // Pseudocode:
    // For nested missing fields, call ColumnReader::read_levels on a reference
    // physical child so IcebergTableReader can synthesize missing nested values
    // by element cardinality.
    return Status::OK();
}

Status ParquetReader::_attach_row_positions(ParquetScanState* state,
                                            const ParquetRowGroupTask& row_group,
                                            PhysicalReadBatch* batch) {
    if (!state->task.need_row_positions) {
        return Status::OK();
    }
    batch->row_positions.clear();
    batch->row_positions.reserve(batch->selection.selected_rows());
    for (size_t i = 0; i < batch->selection.selected.size(); ++i) {
        if (batch->selection.selected[i] != 0) {
            batch->row_positions.push_back(
                    static_cast<segment_v2::rowid_t>(row_group.first_row + i));
        }
    }
    return Status::OK();
}

Status ParquetReader::_attach_hidden_columns(const ParquetScanState& state,
                                             PhysicalReadBatch* batch) {
    for (const auto& field : state.lazy_plan.hidden_fields) {
        batch->hidden_columns.push_back(field.table_path);
    }
    return Status::OK();
}

} // namespace doris
