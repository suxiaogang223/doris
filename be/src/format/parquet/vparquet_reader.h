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
class TupleDescriptor;

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

class ParquetReader final : public FileFormatReader {
public:
    ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, size_t batch_size, const cctz::time_zone* ctz,
                  io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache);

    Status open(const FormatScanTask& task) override;
    Status set_output_template(const Block& block) override;
    Status next_batch(PhysicalReadBatch* batch, bool* eof) override;
    const PhysicalFileSchema& physical_schema() const override { return _footer.schema; }
    Status close() override;

    // Metadata-only entry used by table readers to build schema mapping before
    // creating the physical scan task.
    Status load_physical_schema(PhysicalFileSchema* schema);

private:
    Status _open_footer();
    Status _build_lazy_read_plan();
    Status _next_row_group(ParquetRowGroupTask* row_group, bool* eof);
    Status _apply_row_group_pruning(ParquetRowGroupTask* row_group);
    Status _apply_row_visibility(ParquetRowGroupTask* row_group);
    Status _read_predicate_columns(const ParquetRowGroupTask& row_group, PhysicalReadBatch* batch);
    Status _materialize_predicate_virtual_columns(PhysicalReadBatch* batch);
    Status _evaluate_predicates(PhysicalReadBatch* batch);
    Status _read_payload_columns(const ParquetRowGroupTask& row_group, PhysicalReadBatch* batch);
    Status _read_levels_only_columns(const ParquetRowGroupTask& row_group,
                                     PhysicalReadBatch* batch);
    Status _attach_row_positions(const ParquetRowGroupTask& row_group, PhysicalReadBatch* batch);
    Status _attach_hidden_columns(PhysicalReadBatch* batch);

    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    size_t _batch_size = 0;
    const cctz::time_zone* _ctz = nullptr;
    io::IOContext* _io_ctx = nullptr;
    RuntimeState* _state = nullptr;
    FileMetaCache* _meta_cache = nullptr;

    std::optional<FormatScanTask> _task;
    ParquetFooter _footer;
    ParquetLazyReadPlan _lazy_plan;
    Block _output_template;
    int32_t _next_row_group_id = 0;
    bool _closed = false;
};

} // namespace doris
