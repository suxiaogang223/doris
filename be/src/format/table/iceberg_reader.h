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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "format/generic_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/table_reader.h"

namespace doris {

class FileMetaCache;
class RuntimeProfile;
class RuntimeState;
class ShardedKVCache;
class TFileRangeDesc;
class TFileScanRangeParams;
class TupleDescriptor;

namespace cctz {
class time_zone;
} // namespace cctz

namespace io {
struct IOContext;
} // namespace io

struct IcebergSplitContext {
    std::string data_file_path;
    int32_t partition_spec_id = 0;
    std::string partition_data_json;
    int64_t first_row_id = -1;
    int64_t last_updated_sequence_number = -1;
};

struct IcebergDeletePlan {
    RowVisibility row_visibility;
    std::vector<RequiredField> equality_delete_fields;
};

class IcebergTableReader final : public TableReader {
public:
    IcebergTableReader(ShardedKVCache* kv_cache, RuntimeProfile* profile,
                       const TFileScanRangeParams& params, const TFileRangeDesc& range,
                       size_t batch_size, const cctz::time_zone* ctz, io::IOContext* io_ctx,
                       RuntimeState* state, FileMetaCache* meta_cache);

    Status open(const TableReadTask& task) override;
    Status next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status close() override;

private:
    Status _load_split_context();
    Status _build_schema_mapping(const PhysicalFileSchema& physical_schema,
                                 FieldMappingNode* mapping);
    Status _build_delete_plan(IcebergDeletePlan* delete_plan);
    Status _build_required_fields(const FieldMappingNode& mapping,
                                  const IcebergDeletePlan& delete_plan,
                                  std::vector<RequiredField>* required_fields);
    Status _build_virtual_column_plan(VirtualColumnPlan* virtual_columns);
    Status _build_format_scan_task(const TableReadTask& task, const FieldMappingNode& mapping,
                                   const IcebergDeletePlan& delete_plan,
                                   const std::vector<RequiredField>& required_fields,
                                   const VirtualColumnPlan& virtual_columns,
                                   FormatScanTask* format_task);
    Status _finalize_block(PhysicalReadBatch* batch, Block* block, size_t* read_rows);
    Status _apply_equality_delete(PhysicalReadBatch* batch);
    Status _apply_residual_predicates(PhysicalReadBatch* batch);
    Status _fill_missing_and_partition_columns(PhysicalReadBatch* batch);
    Status _fill_generated_columns(PhysicalReadBatch* batch);
    Status _fill_row_id_columns(PhysicalReadBatch* batch);
    Status _project_final_block(PhysicalReadBatch* batch, Block* block);

    ShardedKVCache* _kv_cache = nullptr;
    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    size_t _batch_size = 0;
    const cctz::time_zone* _ctz = nullptr;
    io::IOContext* _io_ctx = nullptr;
    RuntimeState* _state = nullptr;
    FileMetaCache* _meta_cache = nullptr;

    IcebergSplitContext _split;
    TableReadTask _table_task;
    std::unique_ptr<FileFormatReader> _file_reader;
    Block _output_template;
};

class IcebergReaderAdapter final : public GenericReader {
public:
    IcebergReaderAdapter(ShardedKVCache* kv_cache, RuntimeProfile* profile,
                         const TFileScanRangeParams& params, const TFileRangeDesc& range,
                         size_t batch_size, const cctz::time_zone* ctz, io::IOContext* io_ctx,
                         RuntimeState* state, FileMetaCache* meta_cache);

    Status close() override { return _table_reader.close(); }

protected:
    Status _do_init_reader(ReaderInitContext* ctx) override;
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;

private:
    IcebergTableReader _table_reader;
};

} // namespace doris
