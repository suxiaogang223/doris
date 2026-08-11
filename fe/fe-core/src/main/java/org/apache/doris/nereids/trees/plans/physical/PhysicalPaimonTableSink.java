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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.DistributionSpecPaimonTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Physical Paimon table sink.
 */
public class PhysicalPaimonTableSink<CHILD_TYPE extends Plan>
        extends PhysicalBaseExternalTableSink<CHILD_TYPE> {
    private final PaimonWriteTarget writeTarget;

    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                    PaimonWriteTarget writeTarget,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    CHILD_TYPE child) {
        this(database, writeTarget, cols, outputExprs, groupExpression, logicalProperties,
                PhysicalProperties.SINK_RANDOM_PARTITIONED, null, child);
    }

    public PhysicalPaimonTableSink(PaimonExternalDatabase database,
                                    PaimonWriteTarget writeTarget,
                                    List<Column> cols,
                                    List<NamedExpression> outputExprs,
                                    Optional<GroupExpression> groupExpression,
                                    LogicalProperties logicalProperties,
                                    PhysicalProperties physicalProperties,
                                    Statistics statistics,
                                    CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PAIMON_TABLE_SINK, database, writeTarget.getDorisTable(), cols, outputExprs,
                groupExpression, logicalProperties, physicalProperties, statistics, child);
        this.writeTarget = writeTarget;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, groupExpression,
                logicalProperties.get(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalPaimonTableSink<Plan> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics stats) {
        return new PhysicalPaimonTableSink<>(
                (PaimonExternalDatabase) database, writeTarget, cols, outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, stats, child());
    }

    @Override
    public PhysicalProperties getRequirePhysicalProperties() {
        FileStoreTable paimonTable = writeTarget.getTable();
        if (requiresSingleWriter(paimonTable)) {
            return PhysicalProperties.GATHER;
        }

        // HASH_FIXED concurrent: use paimon_bucket_id as exchange partition expr
        if (isConcurrentHashFixedWritable(paimonTable)) {
            List<String> bucketKeys = paimonTable.bucketKeys();
            List<Slot> outputSlots = child().getOutput();
            Map<String, ExprId> columnExprIds = buildColumnExprIds(outputSlots);

            List<ExprId> bucketKeyExprIds = new ArrayList<>(bucketKeys.size());
            for (String bucketKey : bucketKeys) {
                bucketKeyExprIds.add(Preconditions.checkNotNull(
                        columnExprIds.get(bucketKey),
                        "Paimon bucket-key column is missing from sink output"));
            }
            return PhysicalProperties.from(
                    new DistributionSpecPaimonTableSinkHashPartitioned(
                            bucketKeyExprIds,
                            paimonTable.bucketSpec().getNumBuckets()));
        }

        List<String> primaryKeys = paimonTable.primaryKeys();
        if (primaryKeys.isEmpty()) {
            return PhysicalProperties.SINK_RANDOM_PARTITIONED;
        }

        Map<String, ExprId> columnExprIds = buildColumnExprIds(child().getOutput());
        List<ExprId> primaryKeyExprIds = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            primaryKeyExprIds.add(Preconditions.checkNotNull(
                    columnExprIds.get(primaryKey),
                    "Paimon primary-key column is missing from sink output"));
        }
        return PhysicalProperties.createHash(primaryKeyExprIds, ShuffleType.REQUIRE);
    }

    private Map<String, ExprId> buildColumnExprIds(List<Slot> outputSlots) {
        Preconditions.checkState(cols.size() == outputSlots.size(),
                "Paimon sink columns must match child output");
        Map<String, ExprId> columnExprIds = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < cols.size(); i++) {
            columnExprIds.put(cols.get(i).getName(), outputSlots.get(i).getExprId());
        }
        return columnExprIds;
    }

    /**
     * Whether this sink must use one writer to preserve Paimon write semantics.
     *
     * <p>For dynamic bucket tables, GATHER guarantees one writer only within the current
     * INSERT. Paimon does not support concurrent write jobs to the same partition, and Doris
     * does not add a process-local lease which could not coordinate FE failover or external
     * Flink/Spark writers. Concurrent INSERTs into a dynamic bucket table are therefore
     * unsupported.
     */
    public boolean requiresSingleWriter() {
        return requiresSingleWriter(writeTarget.getTable());
    }

    /**
     * Whether a HASH_FIXED table is eligible for concurrent bucket-shuffle writes.
     *
     * <p>Phase 2a supports DEFAULT bucket function with a single INT / BIGINT
     * bucket-key column.  Other configurations fall back to GATHER until the
     * {@code paimon_bucket_id} BE function covers more types.
     */
    static boolean isConcurrentHashFixedWritable(FileStoreTable paimonTable) {
        if (paimonTable.bucketMode() != BucketMode.HASH_FIXED) {
            return false;
        }
        CoreOptions coreOptions = CoreOptions.fromMap(paimonTable.options());
        // needLookup and FULL_COMPACTION changelog still require single writer
        if (!coreOptions.writeOnly()
                && (coreOptions.needLookup()
                        || coreOptions.changelogProducer()
                                == CoreOptions.ChangelogProducer.FULL_COMPACTION)) {
            return false;
        }
        // Phase 2a: only single INT / BIGINT bucket key supported by BE function
        List<String> bucketKeys = paimonTable.bucketKeys();
        if (bucketKeys.size() != 1) {
            return false;
        }
        org.apache.paimon.types.DataType bucketKeyType =
                paimonTable.rowType().getTypeAt(
                        paimonTable.rowType().getFieldIndex(bucketKeys.get(0)));
        return bucketKeyType instanceof org.apache.paimon.types.IntType
                || bucketKeyType instanceof org.apache.paimon.types.BigIntType;
    }

    static boolean requiresSingleWriter(FileStoreTable paimonTable) {
        BucketMode bucketMode = paimonTable.bucketMode();
        CoreOptions coreOptions = CoreOptions.fromMap(paimonTable.options());

        // HASH_DYNAMIC and KEY_DYNAMIC still require single writer (Phase 2b/2c)
        if (bucketMode == BucketMode.HASH_DYNAMIC
                || bucketMode == BucketMode.KEY_DYNAMIC) {
            return true;
        }

        // HASH_FIXED: eligible tables use bucket-aware shuffle (Phase 2a)
        if (isConcurrentHashFixedWritable(paimonTable)) {
            return false;
        }

        // HASH_FIXED with non-concurrent-safe configurations falls through
        if (bucketMode == BucketMode.HASH_FIXED
                && (!paimonTable.primaryKeys().isEmpty() || !coreOptions.writeOnly())) {
            return true;
        }

        return !coreOptions.writeOnly()
                && (coreOptions.needLookup()
                        || coreOptions.changelogProducer()
                                == CoreOptions.ChangelogProducer.FULL_COMPACTION);
    }

    public PaimonWriteTarget getWriteTarget() {
        return writeTarget;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalPaimonTableSink(this, context);
    }
}
