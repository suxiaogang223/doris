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
#include <utility>
#include <vector>

#include "common/status.h"
#include "format/parquet/parquet_reader.h"
#include "format/reader/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class Block;
class TFileRangeDesc;
class TFileScanRangeParams;
} // namespace doris

namespace doris::iceberg {

// Iceberg 数据文件的元数据摘要。
// 它只描述“这次要读哪一个 data file”，不负责具体的列映射。
struct IcebergDataFile {
    std::string path;
    std::string format;
    int64_t record_count = 0;
    int64_t file_size = 0;
    int64_t sequence_number = 0;
    int64_t first_row_id = -1;
};

// Iceberg delete file 的元数据摘要。
// equality_field_ids 记录该 delete file 关联的字段 id，供表层做 equality delete 处理。
struct IcebergDeleteFile {
    std::string path;
    std::string format;
    int64_t sequence_number = 0;
    std::vector<reader::ColumnId> equality_field_ids;
};

// 一次扫描任务所需的完整输入。
// 这里把 data file、position delete、equality delete、deletion vector 统一放在一起。
struct IcebergScanTask {
    IcebergDataFile data_file;
    std::vector<IcebergDeleteFile> positional_deletes;
    std::vector<IcebergDeleteFile> equality_deletes;
    std::vector<IcebergDeleteFile> deletion_vectors;
};

// Iceberg 表级读取配置。
// 它本质上是 TableReader 参数 + Iceberg 特有的 delete 开关。
struct IcebergReadOptions {
    reader::TableReadOptions table_options;
    bool enable_position_delete = true;
    bool enable_equality_delete = true;
    bool enable_deletion_vector = true;
};

// Iceberg 表级 reader。
// 它负责：
// 1. 绑定 Iceberg table schema；
// 2. 做 field-id 到 file column 的映射；
// 3. 安装谓词下推和 schema change 规则；
// 4. 在 finalize 阶段把 file-local block 变成 table block；
// 5. 处理 position/equality delete 和虚拟列。
class IcebergTableReader : public reader::TableReader {
public:
    ~IcebergTableReader() override = default;

    Status init(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                const reader::TableReadOptions& options) override {
        // TableReader 标准入口。Iceberg 专用实现会在 init_iceberg 中注入 ParquetReader。
        (void)params;
        (void)range;
        _options.table_options = options;
        return Status::OK();
    }

    Status init_iceberg(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                        const IcebergReadOptions& options,
                        std::unique_ptr<parquet::ParquetReader> data_reader) {
        // 绑定 Iceberg reader 的运行时参数，并组合底层 ParquetReader。
        (void)params;
        (void)range;
        _options = options;
        _data_reader = std::move(data_reader);
        return Status::OK();
    }

    Status bind(const std::vector<reader::TableColumn>& iceberg_schema) {
        // 伪逻辑：
        // 1. 保存 Iceberg 当前 snapshot schema；
        // 2. 生成 table/global columns；
        // 3. 后续 initialize_reader 会用该 schema 和 parquet file columns 做 field-id mapping。
        _iceberg_schema = iceberg_schema;
        return Status::OK();
    }

    Status init_scan(const reader::TableScanRequest& request) override {
        // 保存表层 projection/filter。这里仍然是 table/global schema 语义。
        _table_scan_request = request;
        return Status::OK();
    }

    Status initialize_reader(const IcebergScanTask& task) {
        // 伪逻辑：
        // 1. 打开 task.data_file 对应的 Parquet 文件；
        // 2. 从 ParquetReader 获取 file-local columns；
        // 3. 使用 TableColumnMapper 创建 field-id mapping；
        // 4. 将 table projection/filter 转成 ParquetScanRequest；
        // 5. 将无法本地化的 filter 放入 reader_expression_map；
        // 6. 将 position deletes 合并到 ParquetScanRequest；
        // 7. 调用 ParquetReader::init_reader。
        _scan_task = task;
        parquet::ParquetScanRequest parquet_request;
        std::vector<parquet::ParquetFileColumn> file_columns;
        if (_data_reader) {
            RETURN_IF_ERROR(_data_reader->get_columns(&file_columns));
        }
        reader::TableColumnMapperOptions mapper_options;
        mapper_options.mode = reader::TableColumnMappingMode::BY_FIELD_ID;
        _column_mapper = reader::TableColumnMapper(mapper_options);
        RETURN_IF_ERROR(_column_mapper.create_mapping(_iceberg_schema, file_columns, &_mappings));
        RETURN_IF_ERROR(_column_mapper.create_scan_request(_table_scan_request, &parquet_request));
        RETURN_IF_ERROR(apply_position_deletes(&parquet_request));
        if (_data_reader) {
            RETURN_IF_ERROR(_data_reader->init_reader(parquet_request));
        }
        return Status::OK();
    }

    Status next_block(Block* table_block, size_t* rows, bool* eof) override {
        // 伪逻辑：
        // 1. 从 ParquetReader 读取 file-local block；
        // 2. finalize_chunk 将 file-local block 转成 table block；
        // 3. 在 table block 上处理 equality deletes；
        // 4. 物化 Iceberg 虚拟列。
        parquet::ParquetScanResult scan_result;
        if (_data_reader) {
            RETURN_IF_ERROR(_data_reader->scan(&scan_result));
        }
        RETURN_IF_ERROR(finalize_chunk(scan_result, table_block));
        RETURN_IF_ERROR(apply_equality_deletes(table_block));
        RETURN_IF_ERROR(materialize_virtual_columns(table_block, scan_result.rows));
        *rows = scan_result.rows;
        *eof = scan_result.eof;
        return Status::OK();
    }

    Status finalize_chunk(const parquet::ParquetScanResult& scan_result, Block* table_block) {
        // 伪逻辑：
        // for mapping in _mappings:
        //   if mapping.file_column_id exists:
        //      value = scan_result.local_block[file_column_id]
        //      table_block[table_column_id] = eval(mapping.finalize_expr, value)
        //   else:
        //      table_block[table_column_id] = eval(default/partition/generated expr)
        (void)scan_result;
        (void)table_block;
        return Status::OK();
    }

    Status apply_position_deletes(parquet::ParquetScanRequest* request) {
        // 伪逻辑：
        // 1. 读取 task.positional_deletes；
        // 2. 生成 data file row position 的 delete bitmap；
        // 3. 将 delete bitmap 注入 ParquetScanRequest，让 ParquetReader 跳过被删除行。
        (void)request;
        return Status::OK();
    }

    Status apply_equality_deletes(Block* table_block) {
        // 伪逻辑：
        // 1. 根据 equality_field_ids 找到 table_block 中的比较列；
        // 2. 构造 equality delete hash set；
        // 3. 过滤 table_block 中命中的行。
        (void)table_block;
        return Status::OK();
    }

    Status materialize_virtual_columns(Block* table_block, size_t rows) {
        // 伪逻辑：
        // 1. _row_id = data_file.first_row_id + row_position；
        // 2. _last_updated_sequence_number = data_file.sequence_number；
        // 3. 将虚拟列追加到 table_block。
        (void)table_block;
        (void)rows;
        return Status::OK();
    }

    Status close() override {
        if (_data_reader) {
            RETURN_IF_ERROR(_data_reader->close());
        }
        _data_reader.reset();
        return Status::OK();
    }

private:
    IcebergReadOptions _options;
    IcebergScanTask _scan_task;
    reader::TableScanRequest _table_scan_request;
    std::vector<reader::TableColumn> _iceberg_schema;
    std::vector<reader::ColumnMapping> _mappings;
    reader::TableColumnMapper _column_mapper;
    std::unique_ptr<parquet::ParquetReader> _data_reader;
};

} // namespace doris::iceberg
