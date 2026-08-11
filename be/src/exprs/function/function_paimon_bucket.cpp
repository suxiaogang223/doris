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

#include "exprs/function/function_paimon_bucket.h"

#include <cstdlib>

#include "exprs/function/simple_function_factory.h"
#include "util/hash_util.hpp"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"

namespace doris::vectorized {

namespace {

/// Paimon MurmurHash3 seed (MurmurHashUtils.DEFAULT_SEED).
constexpr uint32_t PAIMON_MURMUR_SEED = 42;

/// Paimon BinaryRow header size for a single non-null fixed-length field:
///   8 bytes  — combined RowKind (INSERT=0) + null bitmap (64-bit aligned,
///              HEADER_SIZE_IN_BITS + arity bits, padded to 64 bits = 8 bytes
///              for arity ≤ 55).
///   8 bytes  — fixed-length field slot (arity × 8 bytes each).
///
/// The hash input is therefore 16 zero-padded bytes for a single INT/BIGINT
/// field (the first 8 bytes are always zero for a non-null INSERT row, the
/// second 8 bytes carry the field value in native byte order).
constexpr size_t PAIMON_BINARY_ROW_HEADER_BYTES = 8;
constexpr size_t PAIMON_BINARY_ROW_FIELD_SLOT_BYTES = 8;

/// Compute Paimon bucket id from a 1-column BinaryRow representation.
///
/// The caller has already serialised the bucket-key value into `field_bytes`
/// (4 bytes for INT, 8 bytes for BIGINT) in native (little-endian) byte order.
/// The two halves of the 8-byte field slot are swapped via `field_byte_order_swap`
/// when the host byte order differs from Paimon's Unsafe byte order.
///
/// Paimon chain:  murmur_hash3_x86_32(binary_row, 16, 42)
///                bucket = abs(hash % num_buckets)
int32_t compute_paimon_bucket(const void* field_bytes, size_t field_len, int32_t num_buckets) {
    // Build the 16-byte Paimon BinaryRow on the stack.
    uint8_t binary_row[PAIMON_BINARY_ROW_HEADER_BYTES + PAIMON_BINARY_ROW_FIELD_SLOT_BYTES] = {0};
    // Copy the field value into bytes 8-11 (INT) or 8-15 (BIGINT).
    memcpy(binary_row + PAIMON_BINARY_ROW_HEADER_BYTES, field_bytes, field_len);

    uint32_t hash = HashUtil::murmur_hash3_32(binary_row, sizeof(binary_row), PAIMON_MURMUR_SEED);
    // Match Java: Math.abs(hash % numBuckets)
    int32_t bucket = static_cast<int32_t>(hash % num_buckets);
    return std::abs(bucket);
}

} // namespace

Status FunctionPaimonBucketId::execute_impl(FunctionContext* /*context*/, Block& block,
                                            const ColumnNumbers& arguments, uint32_t result,
                                            size_t input_rows_count) const {
    DCHECK_EQ(arguments.size(), 2);

    const auto& key_col = block.get_by_position(arguments[0]).column;
    const auto& num_buckets_col = block.get_by_position(arguments[1]).column;

    // num_buckets is always a constant literal from the FE planner.
    int32_t num_buckets = num_buckets_col->get_int(0);
    DCHECK_GT(num_buckets, 0);

    auto res_col = ColumnInt32::create(input_rows_count);
    auto& res_data = res_col->get_data();

    // Dispatch on bucket-key type.  Phase 2a supports INT (Int32) and BIGINT
    // (Int64); unknown types cause the FE to fall back to GATHER.
    WhichDataType which(remove_nullable(block.get_by_position(arguments[0]).type));
    if (which.is_int32()) {
        const auto& key_data = assert_cast<const ColumnInt32&>(*key_col).get_data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            int32_t val = key_data[i];
            res_data[i] = compute_paimon_bucket(&val, sizeof(val), num_buckets);
        }
    } else if (which.is_int64()) {
        const auto& key_data = assert_cast<const ColumnInt64&>(*key_col).get_data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            int64_t val = key_data[i];
            res_data[i] = compute_paimon_bucket(&val, sizeof(val), num_buckets);
        }
    } else {
        return Status::NotSupported(
                "paimon_bucket_id: Phase 2a only supports INT / BIGINT bucket keys. "
                "The FE should have fallen back to GATHER for this table.");
    }

    block.replace_by_position(result, std::move(res_col));
    return Status::OK();
}

} // namespace doris::vectorized

namespace doris {

void register_function_paimon_bucket(SimpleFunctionFactory& factory) {
    factory.register_function<vectorized::FunctionPaimonBucketId>();
}

} // namespace doris
