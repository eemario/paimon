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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexMeta;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.source.assigners.DynamicPartitionPruningAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.table.connector.source.RuntimeFilteringData;
import org.apache.flink.table.connector.source.RuntimeFilteringEvent;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID;
import static org.apache.paimon.flink.source.StaticFileStoreSource.createSplitAssigner;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** A {@link SplitEnumerator} implementation for {@link StaticFileStoreSource} input. */
public class StaticFileStoreSplitEnumerator
        implements SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(StaticFileStoreSplitEnumerator.class);

    private final SplitEnumeratorContext<FileStoreSourceSplit> context;

    @Nullable private final Snapshot snapshot;

    private SplitAssigner splitAssigner;

    @Nullable private final DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo;
    @Nullable Map<String, List<String>> runtimeFilteringPushDownFields;
    @Nullable Map<String, List<Integer>> runtimeFilteringPushDownFieldIndices;

    @Nullable private final ReadBuilder readBuilder;

    @Nullable private final Integer splitBatchSize;

    @Nullable private final FlinkConnectorOptions.SplitAssignMode splitAssignMode;

    @Nullable private final Table table;

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            SplitAssigner splitAssigner) {
        this(context, snapshot, splitAssigner, null);
    }

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            SplitAssigner splitAssigner,
            @Nullable DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo) {
        this(
                context,
                snapshot,
                splitAssigner,
                dynamicPartitionFilteringInfo,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            SplitAssigner splitAssigner,
            @Nullable DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo,
            @Nullable ReadBuilder readBuilder,
            @Nullable Integer splitBatchSize,
            @Nullable FlinkConnectorOptions.SplitAssignMode splitAssignMode,
            @Nullable Map<String, List<String>> runtimeFilteringPushDownFields,
            @Nullable Map<String, List<Integer>> runtimeFilteringPushDownFieldIndices,
            @Nullable Table table) {
        this.context = context;
        this.snapshot = snapshot;
        this.splitAssigner = splitAssigner;
        this.dynamicPartitionFilteringInfo = dynamicPartitionFilteringInfo;
        this.readBuilder = readBuilder;
        this.splitBatchSize = splitBatchSize;
        this.splitAssignMode = splitAssignMode;
        this.runtimeFilteringPushDownFields = runtimeFilteringPushDownFields;
        this.runtimeFilteringPushDownFieldIndices = runtimeFilteringPushDownFieldIndices;
        this.table = table;
    }

    @Override
    public void start() {
        // no resources to start
    }

    @Override
    public void handleSplitRequest(int subtask, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtask)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        List<FileStoreSourceSplit> assignment = splitAssigner.getNext(subtask, hostname);
        if (assignment.size() > 0) {
            context.assignSplits(
                    new SplitsAssignment<>(Collections.singletonMap(subtask, assignment)));
        } else {
            context.signalNoMoreSplits(subtask);
        }
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> backSplits, int subtaskId) {
        splitAssigner.addSplitsBack(subtaskId, backSplits);
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) {
        return new PendingSplitsCheckpoint(
                splitAssigner.remainingSplits(), snapshot == null ? null : snapshot.id());
    }

    @Override
    public void close() {
        // no resources to close
    }

    @Nullable
    public Snapshot snapshot() {
        return snapshot;
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof ReaderConsumeProgressEvent) {
            // batch reading doesn't handle consumer
            // avoid meaningless error logs
            return;
        }

        if (sourceEvent.getClass().getSimpleName().equals("DynamicFilteringEvent")) {
            checkNotNull(
                    dynamicPartitionFilteringInfo,
                    "Cannot apply dynamic filtering because dynamicPartitionFilteringInfo hasn't been set.");
            this.splitAssigner =
                    DynamicPartitionPruningAssigner.createDynamicPartitionPruningAssignerIfNeeded(
                            subtaskId,
                            splitAssigner,
                            dynamicPartitionFilteringInfo.getPartitionRowProjection(),
                            sourceEvent,
                            LOG);
        } else if (sourceEvent.getClass().getSimpleName().equals("RuntimeFilteringEvent")) {
            // uses class name for backward compatibility
            this.splitAssigner =
                    createSplitAssignerForRuntimeFilteringIfNeeded(
                            subtaskId, (RuntimeFilteringEvent) sourceEvent);
        } else {
            LOG.error("Received unrecognized event: {}", sourceEvent);
        }
    }

    private SplitAssigner createSplitAssignerForRuntimeFilteringIfNeeded(
            int subtaskId, RuntimeFilteringEvent runtimeFilteringEvent) {
        RuntimeFilteringData runtimeFilteringData = runtimeFilteringEvent.getData();
        LOG.info(
                "Source for table {} received subtask {} with RuntimeFilteringData: {}.",
                table.name(),
                subtaskId,
                runtimeFilteringData);
        if (!runtimeFilteringData.isFiltering()) {
            return splitAssigner;
        }

        Collection<RowData> data = runtimeFilteringData.getData();
        if (data.isEmpty()) {
            LOG.info("Received RuntimeFilteringData with no data, filter everything.");
            return createSplitAssigner(
                    context, splitBatchSize, splitAssignMode, Collections.emptyList());
        }

        if (runtimeFilteringPushDownFields == null
                || runtimeFilteringPushDownFieldIndices == null) {
            LOG.info(
                    "Cannot apply runtime filtering because runtimeFilteringPushDownFields hasn't been set.");
            return splitAssigner;
        }

        String filterType = runtimeFilteringData.getFilterType();
        checkNotNull(
                filterType, "Cannot apply runtime filtering because filterType hasn't been set.");
        checkNotNull(
                readBuilder, "Cannot apply runtime filtering because readBuilder hasn't been set.");
        checkNotNull(
                splitBatchSize,
                "Cannot apply runtime filtering because splitBatchSize hasn't been set.");
        checkNotNull(
                splitAssignMode,
                "Cannot apply runtime filtering because splitAssignMode hasn't been set.");

        List<String> fieldNames = runtimeFilteringPushDownFields.get(filterType);
        List<Integer> filterFieldIndices = runtimeFilteringPushDownFieldIndices.get(filterType);
        RowData.FieldGetter[] fieldGetters = runtimeFilteringData.getFieldGetters();

        RowType readType = readBuilder.readType();
        int[] readFieldIndices = fieldNames.stream().mapToInt(readType::getFieldIndex).toArray();

        Map<String, List<Object>> filterValues = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            for (RowData rowData : data) {
                filterValues
                        .computeIfAbsent(fieldNames.get(i), k -> new ArrayList<>())
                        .add(fieldGetters[filterFieldIndices.get(i)].getFieldOrNull(rowData));
            }
        }
        LOG.info(
                "Source for table {} received RuntimeFilteringData: {}, current push down fields: {} and indices: {}.",
                table,
                runtimeFilteringData,
                fieldNames,
                filterFieldIndices);

        checkState(
                splitAssigner.remainingSplits().stream()
                        .allMatch(s -> s.split() instanceof DataSplit),
                "Only supports DataSplit.");
        checkState(table instanceof FileStoreTable, "Only supports FileStoreTable.");
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileStorePathFactory pathFactory = fileStoreTable.store().pathFactory();

        Map<BinaryRow, Map<Integer, List<DataFileMeta>>> files = new LinkedHashMap<>();
        for (FileStoreSourceSplit split : splitAssigner.remainingSplits()) {
            DataSplit dataSplit = (DataSplit) split.split();
            DataFilePathFactory dataFilePathFactory =
                    pathFactory.createDataFilePathFactory(
                            dataSplit.partition(), dataSplit.bucket());

            // TODO handle schema evolution
            for (DataFileMeta dataFile : dataSplit.dataFiles()) {
                if (dataFile.embeddedIndex() != null) {
                    // TODO handle embedded index
                    LOG.info(
                            "Do not skip data file {} because of embedded index",
                            dataFile.fileName());
                    files.computeIfAbsent(dataSplit.partition(), k -> new LinkedHashMap<>())
                            .computeIfAbsent(dataSplit.bucket(), k -> new ArrayList<>())
                            .add(dataFile);
                } else {
                    boolean skip = false;
                    List<String> indexFiles =
                            dataFile.extraFiles().stream()
                                    .filter(
                                            name ->
                                                    name.endsWith(
                                                            DataFilePathFactory.INDEX_PATH_SUFFIX))
                                    .collect(Collectors.toList());
                    if (!indexFiles.isEmpty()) {
                        if (indexFiles.size() > 1) {
                            throw new RuntimeException(
                                    "Found more than one index file for one data file: "
                                            + String.join(" and ", indexFiles));
                        }
                        // go to file index check
                        Path indexFilePath = dataFilePathFactory.toPath(indexFiles.get(0));
                        FileIO fileIO = fileStoreTable.fileIO();
                        RowType fileRowType = fileStoreTable.schema().logicalRowType();
                        try (FileIndexFormat.Reader reader =
                                FileIndexFormat.createReader(
                                        fileIO.newInputStream(indexFilePath), fileRowType)) {
                            // if one column does not contain any of the values, we can skip the
                            // data file
                            boolean remain = true;
                            for (int i = 0; i < fieldNames.size(); i++) {
                                String fieldName = fieldNames.get(i);
                                Set<FileIndexReader> fileIndexReaders =
                                        reader.readColumnIndex(fieldName);
                                for (FileIndexReader fileIndexReader : fileIndexReaders) {
                                    if (fileIndexReader instanceof BitmapFileIndex.Reader) {
                                        BitmapFileIndex.Reader bitmapFileIndexReader =
                                                (BitmapFileIndex.Reader) fileIndexReader;
                                        BitmapFileIndexMeta bitmapFileIndexMeta =
                                                bitmapFileIndexReader.getBitmapFileIndexMeta(
                                                        fileRowType.getTypeAt(readFieldIndices[i]));
                                        List<Object> filterValuesForField =
                                                filterValues.get(fieldName);
                                        if (filterValuesForField.stream()
                                                .allMatch(
                                                        value ->
                                                                !bitmapFileIndexMeta.contains(
                                                                        value))) {
                                            remain = false;
                                        }
                                    }
                                }
                            }
                            if (!remain) {
                                skip = true;
                            }
                        } catch (Exception e) {
                            LOG.warn(
                                    "Exception when trying to skip data file {}",
                                    dataFile.fileName(),
                                    e);
                        }
                    } else {
                        LOG.info("Found no index file for {}", dataFile.fileName());
                    }

                    if (!skip) {
                        LOG.info("Do not skip data file {}", dataFile.fileName());
                        files.computeIfAbsent(dataSplit.partition(), k -> new LinkedHashMap<>())
                                .computeIfAbsent(dataSplit.bucket(), k -> new ArrayList<>())
                                .add(dataFile);
                    } else {
                        LOG.info("Skip data file {}", dataFile.fileName());
                    }
                }
            }
        }

        SplitGenerator splitGenerator = fileStoreTable.newSnapshotReader().splitGenerator();

        List<Split> splits = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> entry : files.entrySet()) {
            BinaryRow partition = entry.getKey();
            Map<Integer, List<DataFileMeta>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry : buckets.entrySet()) {
                int bucket = bucketEntry.getKey();
                List<DataFileMeta> bucketFiles = bucketEntry.getValue();
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(
                                        snapshot == null ? FIRST_SNAPSHOT_ID - 1 : snapshot.id())
                                .withPartition(partition)
                                .withBucket(bucket);
                List<SplitGenerator.SplitGroup> splitGroups =
                        splitGenerator.splitForBatch(bucketFiles);
                // TODO handle deletion index files

                for (SplitGenerator.SplitGroup splitGroup : splitGroups) {
                    List<DataFileMeta> dataFiles = splitGroup.files;
                    String bucketPath = pathFactory.bucketPath(partition, bucket).toString();
                    builder.withDataFiles(dataFiles)
                            .rawConvertible(splitGroup.rawConvertible)
                            .withBucketPath(bucketPath);
                    // TODO handle deletion vectors

                    splits.add(builder.build());
                }
            }
        }

        FileStoreSourceSplitGenerator fileStoreSourceSplitGenerator =
                new FileStoreSourceSplitGenerator();

        List<FileStoreSourceSplit> fileStoreSourceSplits =
                fileStoreSourceSplitGenerator.createSplits(splits);

        return createSplitAssigner(context, splitBatchSize, splitAssignMode, fileStoreSourceSplits);
    }

    @VisibleForTesting
    public SplitAssigner getSplitAssigner() {
        return splitAssigner;
    }
}
