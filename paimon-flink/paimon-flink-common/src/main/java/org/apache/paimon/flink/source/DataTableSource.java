/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.source;

import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.RuntimeFilterPushDownFieldInfo;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.connector.source.abilities.SupportsRuntimeFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsStatisticReport;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.plan.stats.TableStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * A {@link BaseDataTableSource} implements {@link SupportsStatisticReport} and {@link
 * SupportsDynamicFiltering}.
 */
public class DataTableSource extends BaseDataTableSource
        implements SupportsStatisticReport,
                SupportsDynamicFiltering,
                SupportsRuntimeFilterPushDown {

    private static final Logger LOG = LoggerFactory.getLogger(DataTableSource.class);

    @Nullable private List<String> dynamicPartitionFilteringFields;

    @Nullable private Map<String, List<String>> runtimeFilteringPushDownFields;
    @Nullable private Map<String, List<Integer>> runtimeFilteringPushDownFieldIndices;

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        this(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                null,
                null,
                null,
                null,
                null,
                false);
    }

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            @Nullable List<String> dynamicPartitionFilteringFields,
            boolean isBatchCountStar) {
        this(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                dynamicPartitionFilteringFields,
                null,
                null,
                isBatchCountStar);
    }

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy,
            @Nullable List<String> dynamicPartitionFilteringFields,
            @Nullable Map<String, List<String>> runtimeFilteringPushDownFields,
            @Nullable Map<String, List<Integer>> runtimeFilteringPushDownFieldIndices,
            boolean isBatchCountStar) {
        super(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                isBatchCountStar);
        this.dynamicPartitionFilteringFields = dynamicPartitionFilteringFields;
        this.runtimeFilteringPushDownFields = runtimeFilteringPushDownFields;
        this.runtimeFilteringPushDownFieldIndices = runtimeFilteringPushDownFieldIndices;
    }

    @Override
    public DataTableSource copy() {
        return new DataTableSource(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                dynamicPartitionFilteringFields,
                runtimeFilteringPushDownFields,
                runtimeFilteringPushDownFieldIndices,
                isBatchCountStar);
    }

    @Override
    public TableStats reportStatistics() {
        if (streaming) {
            return TableStats.UNKNOWN;
        }

        scanSplitsForInference();
        return new TableStats(splitStatistics.totalRowCount());
    }

    @Override
    public List<String> listAcceptedFilterFields() {
        // note that streaming query doesn't support dynamic filtering
        return streaming ? Collections.emptyList() : table.partitionKeys();
    }

    @Override
    public void applyDynamicFiltering(List<String> candidateFilterFields) {
        checkState(
                !streaming,
                "Cannot apply dynamic filtering to Paimon table '%s' when streaming reading.",
                table.name());

        checkState(
                !table.partitionKeys().isEmpty(),
                "Cannot apply dynamic filtering to non-partitioned Paimon table '%s'.",
                table.name());

        this.dynamicPartitionFilteringFields = candidateFilterFields;
    }

    @Override
    protected List<String> dynamicPartitionFilteringFields() {
        return dynamicPartitionFilteringFields;
    }

    @Override
    public List<RuntimeFilterPushDownFieldInfo> listAcceptedRuntimeFilterPushDownFields() {
        // note that streaming query doesn't support runtime filter push down
        if (streaming || !(table instanceof DataTable)) {
            return Collections.emptyList();
        }

        List<RuntimeFilterPushDownFieldInfo> result = new ArrayList<>();
        for (Map.Entry<FileIndexOptions.Column, Map<String, Options>> entry :
                ((DataTable) table).coreOptions().indexColumnsOptions().entrySet()) {
            FileIndexOptions.Column column = entry.getKey();
            if (column.isNestedColumn()) {
                // TODO how to handle nested columns?
                continue;
            }
            entry.getValue()
                    .keySet()
                    .forEach(
                            indexType ->
                                    result.add(
                                            new RuntimeFilterPushDownFieldInfo(
                                                    column.getColumnName(), indexType)));
        }
        return result;
    }

    @Override
    public void applyRuntimeFiltering(
            Map<String, List<String>> pushDownFields,
            Map<String, List<Integer>> pushDownFieldIndices) {
        checkState(
                !streaming,
                "Cannot apply dynamic filtering to Paimon table '%s' when streaming reading.",
                table.name());

        LOG.info(
                "Apply runtime filtering to table {} with fields: {}, field indices: {}",
                table,
                pushDownFields,
                pushDownFieldIndices);
        this.runtimeFilteringPushDownFields = pushDownFields;
        this.runtimeFilteringPushDownFieldIndices = pushDownFieldIndices;
    }

    @Override
    protected Map<String, List<String>> runtimeFilteringFields() {
        return runtimeFilteringPushDownFields;
    }

    @Override
    protected Map<String, List<Integer>> runtimeFilteringFieldIndices() {
        return runtimeFilteringPushDownFieldIndices;
    }
}
