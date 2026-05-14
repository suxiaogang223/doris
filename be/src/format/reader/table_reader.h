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
#include <stdint.h>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exprs/vexpr_fwd.h"

namespace doris {
class ColumnPredicate;
class Block;
class TFileRangeDesc;
class TFileScanRangeParams;
} // namespace doris

namespace doris::reader {

using ColumnId = int32_t;

// 表层投影列的描述，使用的是全局 table schema 的列视图。
struct TableColumn {
    ColumnId id = -1;
    std::string name;
    DataTypePtr type;
};

// 表层过滤条件，仍然按 table schema 组织，后续由 TableReader 解析成文件层条件。
struct TableFilter {
    ColumnId table_column_id = -1;
    VExprContextSPtr conjunct;
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
};

// 表列与文件列之间的映射结果。
// 这里同时保存文件类型、表类型、finalize 表达式和读时 fallback 表达式。
struct ColumnMapping {
    ColumnId table_column_id = -1;
    std::optional<ColumnId> file_column_id;
    DataTypePtr file_type;
    DataTypePtr table_type;
    VExprContextSPtr finalize_expr;
    VExprContextSPtr reader_filter_expr;
};

// 表层读取参数，当前只保留 batch size；真正的文件读取细节由底层 reader 自己决定。
struct TableReadOptions {
    size_t batch_size = 4096;
};

// 表层 scan 请求，描述本次需要哪些 table 列和哪些 table 谓词。
struct TableScanRequest {
    std::vector<TableColumn> projected_table_columns;
    std::vector<TableFilter> table_filters;
};

// 表级适配器。
// 它负责：
// 1. 绑定 table schema；
// 2. 调度底层 ParquetReader；
// 3. 把文件层结果 finalize 成表层结果。
class TableReader {
public:
    virtual ~TableReader() = default;

    virtual Status init(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                        const TableReadOptions& options) = 0;
    virtual Status init_scan(const TableScanRequest& request) = 0;
    virtual Status next_block(Block* table_block, size_t* rows, bool* eof) = 0;
    virtual Status close() = 0;
};

} // namespace doris::reader
