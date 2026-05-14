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

#include "format/table/iceberg_delete_file_reader_helper.h"

#include <cstring>
#include <stdexcept>

#include "exec/common/endian.h"
#include "format/table/deletion_vector_reader.h"
#include "io/hdfs_builder.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

Status decode_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                     roaring::Roaring64Map* rows_to_delete) {
    if (buffer_size < 12) {
        return Status::DataQualityError("Deletion vector file size too small: {}", buffer_size);
    }

    auto total_length = BigEndian::Load32(buf);
    if (total_length + 8 != buffer_size) {
        return Status::DataQualityError("Deletion vector length mismatch, expected: {}, actual: {}",
                                        total_length + 8, buffer_size);
    }

    constexpr static char MAGIC_NUMBER[] = {'\xD1', '\xD3', '\x39', '\x64'};
    if (memcmp(buf + sizeof(total_length), MAGIC_NUMBER, 4) != 0) {
        return Status::DataQualityError("Deletion vector magic number mismatch");
    }

    try {
        *rows_to_delete |= roaring::Roaring64Map::readSafe(buf + 8, buffer_size - 12);
    } catch (const std::runtime_error& e) {
        return Status::DataQualityError("Decode roaring bitmap failed, {}", e.what());
    }
    return Status::OK();
}

} // namespace

IcebergDeleteFileIOContext::IcebergDeleteFileIOContext(RuntimeState* state) {
    io_ctx.file_cache_stats = &file_cache_stats;
    io_ctx.file_reader_stats = &file_reader_stats;
    if (state != nullptr) {
        io_ctx.query_id = &state->query_id();
    }
}

TFileScanRangeParams build_iceberg_delete_scan_range_params(
        const std::map<std::string, std::string>& hadoop_conf, TFileType::type file_type,
        const std::vector<TNetworkAddress>& broker_addresses) {
    TFileScanRangeParams params;
    params.__set_file_type(file_type);
    params.__set_properties(hadoop_conf);
    if (file_type == TFileType::FILE_HDFS) {
        params.__set_hdfs_params(parse_properties(hadoop_conf));
    }
    if (!broker_addresses.empty()) {
        params.__set_broker_addresses(broker_addresses);
    }
    return params;
}

TFileRangeDesc build_iceberg_delete_file_range(const std::string& path) {
    TFileRangeDesc range;
    range.path = path;
    range.start_offset = 0;
    range.size = -1;
    range.file_size = -1;
    return range;
}

bool is_iceberg_deletion_vector(const TIcebergDeleteFileDesc& delete_file) {
    return delete_file.__isset.content && delete_file.content == 3;
}

Status read_iceberg_position_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                         const IcebergDeleteFileReaderOptions& options,
                                         IcebergPositionDeleteVisitor* visitor) {
    // Pseudocode for the composition rewrite:
    //
    // IcebergTableReader::_build_delete_plan owns position delete planning. It
    // should instantiate a FileFormatReader for the delete file format
    // (ParquetReader today, OrcReader-as-FileFormatReader later), build a
    // FormatScanTask with required fields:
    //   - file_path as hidden delete metadata
    //   - pos as hidden delete metadata
    // and stream those rows into RowVisibility::deleted_rows for the current
    // data split.
    //
    // This helper remains only for delete-sink call sites while the branch is an
    // architecture proof. It deliberately no longer initializes the old
    // ParquetReader/OrcReader GenericReader implementations.
    return Status::NotSupported(
            "Position delete file reading is now modeled inside IcebergTableReader delete "
            "planning with FileFormatReader composition. delete_file={}",
            delete_file.path);
}

Status read_iceberg_deletion_vector(const TIcebergDeleteFileDesc& delete_file,
                                    const IcebergDeleteFileReaderOptions& options,
                                    roaring::Roaring64Map* rows_to_delete) {
    if (!delete_file.__isset.content_offset || !delete_file.__isset.content_size_in_bytes) {
        return Status::InternalError("Deletion vector is missing content offset or length");
    }

    TFileRangeDesc delete_range = build_iceberg_delete_file_range(delete_file.path);
    if (options.fs_name != nullptr && !options.fs_name->empty()) {
        delete_range.__set_fs_name(*options.fs_name);
    }
    delete_range.start_offset = delete_file.content_offset;
    delete_range.size = delete_file.content_size_in_bytes;

    DeletionVectorReader dv_reader(options.state, options.profile, *options.scan_params,
                                   delete_range, options.io_ctx);
    RETURN_IF_ERROR(dv_reader.open());

    std::vector<char> buf(delete_range.size);
    RETURN_IF_ERROR(dv_reader.read_at(delete_range.start_offset,
                                      {buf.data(), cast_set<size_t>(delete_range.size)}));
    return decode_deletion_vector_buffer(buf.data(), delete_range.size, rows_to_delete);
}

} // namespace doris
