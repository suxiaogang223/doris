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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.ExprId;

import java.util.List;

/**
 * Distribution spec for Paimon HASH_FIXED concurrent writes.
 *
 * <p>When the planner selects this spec, the translator builds a
 * {@code paimon_bucket_id(bucket_key, numBuckets)} function call as the
 * exchange partition expression, so that every row of the same Paimon bucket
 * is routed to the same pipeline instance.
 */
public class DistributionSpecPaimonTableSinkHashPartitioned extends DistributionSpec {

    private final List<ExprId> bucketKeyExprIds;
    private final int numBuckets;

    public DistributionSpecPaimonTableSinkHashPartitioned(
            List<ExprId> bucketKeyExprIds, int numBuckets) {
        this.bucketKeyExprIds = bucketKeyExprIds;
        this.numBuckets = numBuckets;
    }

    public List<ExprId> getBucketKeyExprIds() {
        return bucketKeyExprIds;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecPaimonTableSinkHashPartitioned;
    }
}
