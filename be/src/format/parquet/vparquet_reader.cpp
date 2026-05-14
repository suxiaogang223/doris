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

Status ParquetReader::open(const FormatScanTask& task) {
    _task = task;
    RETURN_IF_ERROR(_open_footer());
    RETURN_IF_ERROR(_build_lazy_read_plan());
    _next_row_group_id = 0;
    return Status::OK();
}

Status ParquetReader::set_output_template(const Block& block) {
    _output_template = block;
    return Status::OK();
}

Status ParquetReader::next_batch(PhysicalReadBatch* batch, bool* eof) {
    DORIS_CHECK(_task.has_value());
    batch->physical_block = _output_template;
    batch->hidden_columns.clear();
    batch->row_positions.clear();
    batch->physical_rows = 0;

    ParquetRowGroupTask row_group;
    RETURN_IF_ERROR(_next_row_group(&row_group, eof));
    if (*eof) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_apply_row_group_pruning(&row_group));
    RETURN_IF_ERROR(_apply_row_visibility(&row_group));
    RETURN_IF_ERROR(_read_predicate_columns(row_group, batch));
    RETURN_IF_ERROR(_materialize_predicate_virtual_columns(batch));
    RETURN_IF_ERROR(_evaluate_predicates(batch));
    RETURN_IF_ERROR(_read_payload_columns(row_group, batch));
    RETURN_IF_ERROR(_read_levels_only_columns(row_group, batch));
    RETURN_IF_ERROR(_attach_row_positions(row_group, batch));
    RETURN_IF_ERROR(_attach_hidden_columns(batch));
    return Status::OK();
}

Status ParquetReader::close() {
    _closed = true;
    return Status::OK();
}

Status ParquetReader::load_physical_schema(PhysicalFileSchema* schema) {
    RETURN_IF_ERROR(_open_footer());
    *schema = _footer.schema;
    return Status::OK();
}

Status ParquetReader::_open_footer() {
    if (!_footer.schema.root.table_path.empty()) {
        return Status::OK();
    }

    // Pseudocode:
    // 1. Open _range.path through FileFactory.
    // 2. Read Parquet footer, row group metadata, column chunk metadata.
    // 3. Build a recursive PhysicalFileSchema from Parquet schema nodes.
    // 4. Preserve Parquet field ids when present.
    _footer.schema.root.table_path = "$root";
    _footer.schema.root.file_path = "$root";
    _footer.schema.root.kind = FieldMappingKind::PHYSICAL;
    _footer.schema.has_iceberg_field_ids = true;
    _footer.total_rows = 0;
    _footer.row_group_first_rows.clear();
    return Status::OK();
}

Status ParquetReader::_build_lazy_read_plan() {
    DORIS_CHECK(_task.has_value());
    _lazy_plan = ParquetLazyReadPlan {};
    for (const auto& field : _task->required_fields) {
        if (field.purpose == RequiredFieldPurpose::PREDICATE) {
            _lazy_plan.predicate_fields.push_back(field);
        } else if (field.purpose == RequiredFieldPurpose::LEVELS_ONLY ||
                   field.purpose == RequiredFieldPurpose::REFERENCE_LEVELS) {
            _lazy_plan.levels_only_fields.push_back(field);
        } else if (field.hidden) {
            _lazy_plan.hidden_fields.push_back(field);
        } else {
            _lazy_plan.payload_fields.push_back(field);
        }
    }
    return Status::OK();
}

Status ParquetReader::_next_row_group(ParquetRowGroupTask* row_group, bool* eof) {
    DORIS_CHECK(_task.has_value());

    // Pseudocode:
    // Iterate row groups intersecting [_task->split_start, _task->split_start + split_size).
    // The real implementation should also honor byte split boundaries and empty files.
    if (_next_row_group_id >= static_cast<int32_t>(_footer.row_group_first_rows.size())) {
        *eof = true;
        return Status::OK();
    }

    row_group->row_group_id = _next_row_group_id++;
    row_group->first_row = _footer.row_group_first_rows[row_group->row_group_id];
    row_group->row_count = _batch_size;
    row_group->candidate_rows.reset(row_group->row_count, true);
    *eof = false;
    return Status::OK();
}

Status ParquetReader::_apply_row_group_pruning(ParquetRowGroupTask* row_group) {
    // Pseudocode:
    // 1. Use min/max, bloom filter and page index for predicate physical fields.
    // 2. Update row_group->candidate_rows for page-level pruning.
    // 3. Do not use table-level virtual columns here.
    return Status::OK();
}

Status ParquetReader::_apply_row_visibility(ParquetRowGroupTask* row_group) {
    DORIS_CHECK(_task.has_value());
    if (!_task->row_visibility.needs_row_positions()) {
        return Status::OK();
    }
    for (size_t i = 0; i < row_group->candidate_rows.selected.size(); ++i) {
        const int64_t file_row = row_group->first_row + static_cast<int64_t>(i);
        if (!_task->row_visibility.is_visible(file_row)) {
            row_group->candidate_rows.selected[i] = 0;
        }
    }
    return Status::OK();
}

Status ParquetReader::_read_predicate_columns(const ParquetRowGroupTask& row_group,
                                              PhysicalReadBatch* batch) {
    // Pseudocode:
    // For each predicate field:
    //   ColumnReader reader = open_column(field.mapping)
    //   reader.read(batch->physical_block, row_group.row_count, &row_group.candidate_rows)
    //
    // Complex predicate fields may also read levels so filters preserve nested
    // cardinality.
    batch->physical_rows = row_group.candidate_rows.selected_rows();
    batch->selection = row_group.candidate_rows;
    return Status::OK();
}

Status ParquetReader::_materialize_predicate_virtual_columns(PhysicalReadBatch* batch) {
    DORIS_CHECK(_task.has_value());
    for (const auto& [_, handler] : _task->virtual_columns.predicate_virtual_columns) {
        RETURN_IF_ERROR(handler(&batch->physical_block, batch->physical_rows, &batch->selection));
    }
    return Status::OK();
}

Status ParquetReader::_evaluate_predicates(PhysicalReadBatch* batch) {
    // Pseudocode:
    // 1. Evaluate vectorized predicates on predicate physical columns and
    //    predicate virtual columns.
    // 2. Intersect the result into batch->selection.
    // 3. The final selection controls payload lazy materialization.
    return Status::OK();
}

Status ParquetReader::_read_payload_columns(const ParquetRowGroupTask& row_group,
                                            PhysicalReadBatch* batch) {
    // Pseudocode:
    // For each payload field:
    //   if field.allow_lazy_materialization:
    //       reader.read(block, row_group.row_count, &batch->selection)
    //   else:
    //       reader.read(block, row_group.row_count, nullptr)
    //
    // This is the core lazy materialization boundary. Table semantics do not
    // leak into the column reader.
    for (const auto& [_, handler] : _task->virtual_columns.payload_virtual_columns) {
        RETURN_IF_ERROR(handler(&batch->physical_block, batch->selection.selected_rows(),
                                &batch->selection));
    }
    return Status::OK();
}

Status ParquetReader::_read_levels_only_columns(const ParquetRowGroupTask& row_group,
                                                PhysicalReadBatch* batch) {
    // Pseudocode:
    // For nested missing fields, read only repetition/definition levels from a
    // reference physical child. IcebergTableReader consumes these levels to fill
    // missing nested children by element cardinality, not by top-level row count.
    return Status::OK();
}

Status ParquetReader::_attach_row_positions(const ParquetRowGroupTask& row_group,
                                            PhysicalReadBatch* batch) {
    DORIS_CHECK(_task.has_value());
    if (!_task->need_row_positions) {
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

Status ParquetReader::_attach_hidden_columns(PhysicalReadBatch* batch) {
    for (const auto& field : _lazy_plan.hidden_fields) {
        batch->hidden_columns.push_back(field.table_path);
    }
    return Status::OK();
}

} // namespace doris
