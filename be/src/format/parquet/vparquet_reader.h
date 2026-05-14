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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "format/table/table_reader.h"

namespace doris {

class FileMetaCache;
class RuntimeProfile;
class RuntimeState;
class TFileRangeDesc;
class TFileScanRangeParams;

namespace cctz {
class time_zone;
} // namespace cctz

namespace io {
struct IOContext;
} // namespace io

struct ParquetFooter {
    PhysicalFileSchema schema;
    int64_t total_rows = 0;
    std::vector<int64_t> row_group_first_rows;
};

struct ParquetRowGroupTask {
    int32_t row_group_id = -1;
    int64_t first_row = 0;
    int64_t row_count = 0;
    SelectionVector candidate_rows;
};

struct ParquetLazyReadPlan {
    std::vector<RequiredField> predicate_fields;
    std::vector<RequiredField> payload_fields;
    std::vector<RequiredField> levels_only_fields;
    std::vector<RequiredField> hidden_fields;
};

struct ParquetScanState final : public FormatReaderScanState {
    int32_t current_row_group = -1;
    int64_t offset_in_row_group = 0;
    int64_t row_group_first_row = 0;
    bool current_row_group_prefetched = false;
    ParquetLazyReadPlan lazy_plan;
    Block output_template;

    // Pseudocode placeholders matching DuckDB ParquetReaderScanState:
    //   root_reader: recursive ColumnReader tree
    //   define_buf / repeat_buf: reusable level buffers
    //   scan_filters: predicate fields with per-filter state
};

class ParquetReader final : public FileFormatReader {
public:
    ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, size_t batch_size, const cctz::time_zone* ctz,
                  io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache);

    Status open() override;
    const PhysicalFileSchema& physical_schema() const override { return _footer.schema; }
    Status initialize_scan(FormatReaderScanState* state) override;
    Status scan(FormatReaderScanState* state, PhysicalReadBatch* batch, bool* eof) override;
    Status close() override;

private:
    Status _open_footer();
    Status _build_lazy_read_plan(ParquetLazyReadPlan* plan);
    Status _create_column_reader_tree(ParquetScanState* state);
    Status _scan_internal(ParquetScanState* state, PhysicalReadBatch* batch, bool* produced,
                          bool* eof);
    Status _switch_row_group(ParquetScanState* state, bool* eof);
    Status _prepare_row_group(ParquetScanState* state);
    Status _register_prefetch(ParquetScanState* state);
    Status _apply_row_group_pruning(ParquetScanState* state);
    Status _apply_row_visibility(ParquetScanState* state, ParquetRowGroupTask* row_group);
    Status _read_predicate_columns(ParquetScanState* state, const ParquetRowGroupTask& row_group,
                                   PhysicalReadBatch* batch);
    Status _materialize_predicate_virtual_columns(ParquetScanState* state,
                                                  PhysicalReadBatch* batch);
    Status _evaluate_predicates(ParquetScanState* state, PhysicalReadBatch* batch);
    Status _read_payload_columns(ParquetScanState* state, const ParquetRowGroupTask& row_group,
                                 PhysicalReadBatch* batch);
    Status _read_levels_only_columns(ParquetScanState* state, const ParquetRowGroupTask& row_group,
                                     PhysicalReadBatch* batch);
    Status _attach_row_positions(ParquetScanState* state, const ParquetRowGroupTask& row_group,
                                 PhysicalReadBatch* batch);
    Status _attach_hidden_columns(const ParquetScanState& state, PhysicalReadBatch* batch);

    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    size_t _batch_size = 0;
    const cctz::time_zone* _ctz = nullptr;
    io::IOContext* _io_ctx = nullptr;
    RuntimeState* _state = nullptr;
    FileMetaCache* _meta_cache = nullptr;

    ParquetFooter _footer;
    bool _closed = false;
};

} // namespace doris
