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
import org.apache.paimon.flink.incremental.IncrementalProcessingUtils;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.BloomFilterRuntimeFilterType;
import org.apache.flink.table.connector.source.InFilterRuntimeFilterType;
import org.apache.flink.table.connector.source.RuntimeFilterPushDownFieldInfo;
import org.apache.flink.table.connector.source.RuntimeFilterType;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.connector.source.abilities.SupportsRuntimeFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsScanRange;
import org.apache.flink.table.connector.source.abilities.SupportsStatisticReport;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory.BITMAP_INDEX;
import static org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndexFactory.BLOOM_FILTER;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * A {@link BaseDataTableSource} implements {@link SupportsStatisticReport} and {@link
 * SupportsDynamicFiltering}.
 */
public class DataTableSource extends BaseDataTableSource
        implements SupportsStatisticReport,
                SupportsDynamicFiltering,
                SupportsScanRange,
                SupportsRuntimeFilterPushDown {

    private static final Logger LOG = LoggerFactory.getLogger(DataTableSource.class);

    @Nullable private List<String> dynamicPartitionFilteringFields;

    @Nullable private Map<RuntimeFilterType, List<String>> runtimeFilterPushDownFieldNames;
    @Nullable private Map<RuntimeFilterType, List<Integer>> runtimeFilterPushDownFieldIndices;

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
                null);
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
            @Nullable Long countPushed) {
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
                countPushed);
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
            @Nullable Map<RuntimeFilterType, List<String>> runtimeFilterPushDownFieldNames,
            @Nullable Map<RuntimeFilterType, List<Integer>> runtimeFilterPushDownFieldIndices,
            @Nullable Long countPushed) {
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
                countPushed);
        this.dynamicPartitionFilteringFields = dynamicPartitionFilteringFields;
        this.runtimeFilterPushDownFieldNames = runtimeFilterPushDownFieldNames;
        this.runtimeFilterPushDownFieldIndices = runtimeFilterPushDownFieldIndices;
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
                runtimeFilterPushDownFieldNames,
                runtimeFilterPushDownFieldIndices,
                countPushed);
    }

    @Override
    public TableStats reportStatistics() {
        if (streaming) {
            return TableStats.UNKNOWN;
        }
        Optional<Statistics> optionStatistics = table.statistics();
        if (optionStatistics.isPresent()) {
            Statistics statistics = optionStatistics.get();
            if (statistics.mergedRecordCount().isPresent()) {
                Map<String, ColumnStats> flinkColStats =
                        statistics.colStats().entrySet().stream()
                                .map(
                                        entry ->
                                                new AbstractMap.SimpleEntry<>(
                                                        entry.getKey(),
                                                        toFlinkColumnStats(entry.getValue())))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return new TableStats(statistics.mergedRecordCount().getAsLong(), flinkColStats);
            }
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

    private ColumnStats toFlinkColumnStats(ColStats<?> colStats) {
        return ColumnStats.Builder.builder()
                .setNdv(
                        colStats.distinctCount().isPresent()
                                ? colStats.distinctCount().getAsLong()
                                : null)
                .setNullCount(
                        colStats.nullCount().isPresent() ? colStats.nullCount().getAsLong() : null)
                .setAvgLen(
                        colStats.avgLen().isPresent()
                                ? (double) colStats.avgLen().getAsLong()
                                : null)
                .setMaxLen(
                        colStats.maxLen().isPresent() ? (int) colStats.maxLen().getAsLong() : null)
                .setMax(colStats.max().isPresent() ? colStats.max().get() : null)
                .setMin(colStats.min().isPresent() ? colStats.min().get() : null)
                .build();
    }

    @Override
    public ScanTableSource applyScanRange(long start, long end) {
        Table newTable = IncrementalProcessingUtils.newTableWithScanRange(table, start, end);
        return new DataTableSource(
                tableIdentifier,
                newTable,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy,
                dynamicPartitionFilteringFields,
                countPushed);
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
                // cannot support nested columns now
                continue;
            }

            for (Map.Entry<String, Options> indexType : entry.getValue().entrySet()) {
                switch (indexType.getKey()) {
                    case BLOOM_FILTER:
                        result.add(
                                new RuntimeFilterPushDownFieldInfo(
                                        column.getColumnName(),
                                        new BloomFilterRuntimeFilterType()));
                        break;
                    case BITMAP_INDEX:
                        result.add(
                                new RuntimeFilterPushDownFieldInfo(
                                        column.getColumnName(), new InFilterRuntimeFilterType()));
                        break;
                    default:
                        LOG.info(
                                "Cannot support runtime filter push down with type {} for column {}",
                                indexType,
                                column);
                }
            }
        }
        return result;
    }

    @Override
    public void applyRuntimeFiltering(
            Map<RuntimeFilterType, List<String>> pushDownFields,
            Map<RuntimeFilterType, List<Integer>> pushDownFieldIndices) {
        checkState(
                !streaming,
                "Cannot apply dynamic filtering to Paimon table '%s' when streaming reading.",
                table.name());
        LOG.info(
                "Apply runtime filtering to table {} with fields: {}, field indices: {}",
                table,
                pushDownFields,
                pushDownFieldIndices);
        this.runtimeFilterPushDownFieldNames = pushDownFields;
        this.runtimeFilterPushDownFieldIndices = pushDownFieldIndices;
    }

    @Override
    protected Map<RuntimeFilterType, List<String>> runtimeFilterPushDownFieldNames() {
        return runtimeFilterPushDownFieldNames;
    }

    @Override
    protected Map<RuntimeFilterType, List<Integer>> runtimeFilterPushDownFieldIndices() {
        return runtimeFilterPushDownFieldIndices;
    }
}
