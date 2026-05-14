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
#include <string>
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
class ColumnPredicate;
class RuntimeState;
class TFileRangeDesc;
class TFileScanRangeParams;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::parquet {

// The implementation is intentionally stubbed during the design phase. The
// important boundary is:
//
//   ParquetReader:
//     reads file-local Parquet columns into a file-local block.
//
//   IcebergTableReader:
//     maps table/global schema to file-local schema, localizes filters, and
//     finalizes the file-local block into the Iceberg table block.

using ColumnId = int32_t;

// Parquet 物理读取参数。
// 这里保留了扫描批大小、运行时上下文以及时区等和底层解码相关的信息。
struct ParquetReadOptions {
    size_t batch_size = 4096;
    RuntimeState* state = nullptr;
    RuntimeProfile* profile = nullptr;
    io::IOContext* io_ctx = nullptr;
    const cctz::time_zone* ctz = nullptr;
};

// 文件 schema 中的一个物理列描述。
// 这是 ParquetReader 对外暴露的“文件列视图”，不是 table 列。
struct ParquetFileColumn {
    ColumnId id = -1;
    std::string name;
    DataTypePtr type;
    const FieldSchema* field = nullptr;
};

// 文件层可直接下推的过滤条件。
// 这里不再理解 table schema，只接受已经被 localize 的列和表达式。
struct ParquetLocalFilter {
    ColumnId file_column_id = -1;
    VExprContextSPtr conjunct;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};

// ParquetReader 的 scan 请求。
// projected_file_columns: 需要物化的文件列。
// local_filters: 已经本地化的谓词。
// reader_expression_map: 需要先在文件层计算的表达式，既可服务于谓词下推 fallback，
//                       也可服务于延时物化时的“谓词列复用”。
struct ParquetScanRequest {
    std::vector<ColumnId> projected_file_columns;
    std::vector<ParquetLocalFilter> local_filters;
    std::vector<std::pair<ColumnId, VExprContextSPtr>> reader_expression_map;
};

// ParquetReader 输出的中间结果。
// 这个 block 仍然是文件层 block，字段顺序遵循 projected_file_columns。
struct ParquetScanResult {
    Block* local_block = nullptr;
    size_t rows = 0;
    bool eof = false;
};

// 单列解码器。
// 它负责把某个 Parquet 文件列解码成 Block 里的目标列。
class ParquetColumnReader {
public:
    virtual ~ParquetColumnReader() = default;

    virtual Status init(const FieldSchema& file_field) = 0;
    virtual Status read_values(size_t rows, Block* local_block, ColumnId output_column_id) = 0;
    virtual Status skip_values(size_t rows) = 0;
};

// Parquet 文件级 reader。
// 它只理解文件 schema、文件列和文件层谓词，不理解 table/global schema。
class ParquetReader {
public:
    virtual ~ParquetReader() = default;

    virtual Status open(io::FileReaderSPtr file, const ParquetReadOptions& options) {
        // 保存底层文件句柄和解码参数。这里不绑定任何 table/global schema。
        _file = std::move(file);
        _options = options;
        _eof = false;
        return Status::OK();
    }

    virtual Status parse_metadata() {
        // 伪逻辑：
        // 1. 读取 Parquet footer；
        // 2. 构造 FieldDescriptor / RowGroupMeta；
        // 3. 准备 row group statistics、page index、bloom filter 元数据。
        _metadata_parsed = true;
        return Status::OK();
    }

    virtual Status get_columns(std::vector<ParquetFileColumn>* columns) const {
        // 返回文件本地 schema。调用方根据这些列做 table->file mapping。
        // 这里是展示用伪实现，真实实现会从 FieldDescriptor 展开 leaf columns。
        columns->clear();
        return Status::OK();
    }

    virtual Status get_file_schema(const FieldDescriptor** schema) const {
        // 返回 Parquet 文件 schema 根节点。schema 的生命周期由 ParquetReader 持有。
        *schema = nullptr;
        return Status::OK();
    }

    virtual Status init_reader(const ParquetScanRequest& request) {
        // 伪逻辑：
        // 1. 保存 projected_file_columns，后续只解码这些文件列；
        // 2. 使用 local_filters 做 row group/page/dictionary/bloom filter pruning；
        // 3. 准备 reader_expression_map，用于无法直接本地化的谓词 fallback；
        // 4. 如果延时物化中谓词表达式同时也是 projection，则在这里分配共享输出列。
        _request = request;
        return Status::OK();
    }

    virtual Status scan(ParquetScanResult* result) {
        // 伪逻辑：
        // 1. 第一阶段只读取 local_filters 和 reader_expression_map 依赖的列；
        // 2. 应用谓词，得到 selected row ids；
        // 3. 第二阶段只对 selected rows 物化剩余 projected_file_columns；
        // 4. 如果 reader_expression_map 的结果同时被 projection 使用，直接复用第一阶段结果。
        result->local_block = nullptr;
        result->rows = 0;
        result->eof = _eof;
        _eof = true;
        return Status::OK();
    }

    virtual Status close() {
        // 释放页缓存、列 reader、footer 元数据和文件句柄。
        _file.reset();
        _metadata_parsed = false;
        return Status::OK();
    }

private:
    io::FileReaderSPtr _file;
    ParquetReadOptions _options;
    ParquetScanRequest _request;
    bool _metadata_parsed = false;
    bool _eof = false;
};

} // namespace doris::parquet
