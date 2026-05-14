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

#include <gen_cpp/parquet_types.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"
#include "format/parquet/schema_desc.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "runtime/runtime_profile.h"
#include "storage/olap_scan_common.h"

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {
class Block;
class RuntimeState;
class TFileRangeDesc;
class TFileScanRangeParams;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::parquet {

// This file defines the API target for the DuckDB-style Parquet reader rewrite.
// The implementation is intentionally stubbed during the design phase. The
// important boundary is:
//
//   ParquetFileReader:
//     reads file-local Parquet columns into an intermediate block.
//
//   ParquetTableReader:
//     maps table/global schema to file-local schema, localizes filters, and
//     finalizes the intermediate block into the table block.

using ColumnId = int32_t;

struct ParquetReadOptions {
    size_t batch_size = 4096;
    const cctz::time_zone* ctz = nullptr;
    RuntimeState* state = nullptr;
    RuntimeProfile* profile = nullptr;
    io::IOContext* io_ctx = nullptr;
};

struct ParquetFileColumn {
    ColumnId id = -1;
    std::string name;
    DataTypePtr type;
    const FieldSchema* field = nullptr;
};

struct ParquetTableColumn {
    ColumnId id = -1;
    std::string name;
    DataTypePtr type;
};

struct ParquetColumnMapping {
    ColumnId table_column_id = -1;
    std::optional<ColumnId> file_column_id;
    DataTypePtr file_type;
    DataTypePtr table_type;
    VExprContextSPtr finalize_expr;
    VExprContextSPtr reader_filter_expr;
};

struct ParquetTableFilter {
    ColumnId table_column_id = -1;
    VExprContextSPtr conjunct;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};

struct ParquetLocalFilter {
    ColumnId file_column_id = -1;
    VExprContextSPtr conjunct;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};

struct ParquetScanRequest {
    std::vector<ColumnId> projected_file_columns;
    std::vector<ParquetLocalFilter> local_filters;
    // Expressions that must be evaluated inside the file reader before applying
    // a non-localizable table filter. This is the Doris API equivalent of
    // DuckDB's expression_map fallback.
    std::unordered_map<ColumnId, VExprContextSPtr> reader_expression_map;
};

struct ParquetScanResult {
    // File-local intermediate block. Column order follows projected_file_columns.
    Block* local_block = nullptr;
    size_t rows = 0;
    bool eof = false;
};

class ParquetColumnReader {
public:
    virtual ~ParquetColumnReader() = default;

    virtual Status init(const FieldSchema& file_field) = 0;
    virtual Status read_values(size_t rows, Block* local_block, ColumnId output_column_id) = 0;
    virtual Status skip_values(size_t rows) = 0;
};

class ParquetFileReader {
public:
    virtual ~ParquetFileReader() = default;

    virtual Status open(io::FileReaderSPtr file, const ParquetReadOptions& options) = 0;
    virtual Status parse_footer() = 0;
    virtual Status get_file_schema(const FieldDescriptor** schema) const = 0;
    virtual Status init_scan(const ParquetScanRequest& request) = 0;
    virtual Status scan(ParquetScanResult* result) = 0;
    virtual Status close() = 0;
};

class ParquetColumnMapper {
public:
    virtual ~ParquetColumnMapper() = default;

    virtual Status create_mapping(const std::vector<ParquetTableColumn>& table_columns,
                                  const FieldDescriptor& file_schema,
                                  std::vector<ParquetColumnMapping>* mappings) = 0;

    virtual Status localize_filters(const std::vector<ParquetTableFilter>& table_filters,
                                    const std::vector<ParquetColumnMapping>& mappings,
                                    ParquetScanRequest* request) = 0;
};

class ParquetTableReader {
public:
    virtual ~ParquetTableReader() = default;

    virtual Status init(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                        const ParquetReadOptions& options) = 0;

    virtual Status next_block(Block* table_block, size_t* rows, bool* eof) = 0;

    virtual Status finalize_block(const Block& local_block, const ParquetScanResult& scan_result,
                                  Block* table_block) = 0;

    virtual Status close() = 0;
};

} // namespace doris::parquet
