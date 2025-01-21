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

package org.apache.paimon.flink.incremental;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IncrementalProcessingUtils}. */
public class IncrementalProcessingUtilsTest {
    @TempDir java.nio.file.Path tempDir;
    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.STRING()}, new String[] {"k", "v0"});
    private static final List<String> PRIMARY_KEYS = Collections.singletonList("k");
    private FileStoreTable table;

    @BeforeEach
    public void beforeEach() throws Exception {
        Identifier identifier = new Identifier("test_db", "T");
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempDir.toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        catalog.createDatabase(identifier.getDatabaseName(), true);
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BUCKET.key(), "1");
        catalog.createTable(
                identifier,
                new Schema(
                        ROW_TYPE.getFields(),
                        Collections.emptyList(),
                        PRIMARY_KEYS,
                        tableOptions,
                        ""),
                false);
        table = (FileStoreTable) catalog.getTable(identifier);
    }

    @Test
    public void testNewTableWithScanRangeDeltaScan() throws Exception {
        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("one"));
        GenericRow row2 = GenericRow.of(2, BinaryString.fromString("two"));
        writeTable(table, new GenericRow[] {row1});
        long ts1 = table.snapshotManager().latestSnapshot().timeMillis(); // timestamp of snapshot 1
        writeTable(table, new GenericRow[] {row2});
        long ts2 = table.snapshotManager().latestSnapshot().timeMillis(); // timestamp of snapshot 2
        long start = ts1;
        long end = ts2;
        FileStoreTable newTable =
                (FileStoreTable)
                        IncrementalProcessingUtils.newTableWithScanRange(table, start, end);
        List<InternalRow> actual = readTable(newTable);
        assertThat(actual.size()).isEqualTo(1);
        validateResult(actual.get(0), row2);
    }

    @Test
    public void testNewTableWithScanRangeFullScan() throws Exception {
        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("one"));
        GenericRow row2 = GenericRow.of(2, BinaryString.fromString("two"));
        writeTable(table, new GenericRow[] {row1});
        writeTable(table, new GenericRow[] {row2});
        long ts2 = table.snapshotManager().latestSnapshot().timeMillis(); // timestamp of snapshot 2
        long start = -1;
        long end = ts2;
        FileStoreTable newTable =
                (FileStoreTable)
                        IncrementalProcessingUtils.newTableWithScanRange(table, start, end);
        List<InternalRow> actual = readTable(newTable);
        assertThat(actual.size()).isEqualTo(2);
        validateResult(actual.get(0), row1);
        validateResult(actual.get(1), row2);
    }

    @Test
    public void testNewTableWithScanRangeEmptyScan() throws Exception {
        long start = -1;
        long end = -1;
        FileStoreTable newTable =
                (FileStoreTable)
                        IncrementalProcessingUtils.newTableWithScanRange(table, start, end);
        List<InternalRow> actual = readTable(newTable);
        assertThat(actual.size()).isEqualTo(0);
    }

    @Test
    public void testNewTableWithScanRangeEmptyDeltaScan() throws Exception {
        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("one"));
        writeTable(table, new GenericRow[] {row1});
        long ts1 = table.snapshotManager().latestSnapshot().timeMillis(); // timestamp of snapshot 1
        long start = ts1;
        long end = ts1;
        FileStoreTable newTable =
                (FileStoreTable)
                        IncrementalProcessingUtils.newTableWithScanRange(table, start, end);
        List<InternalRow> actual = readTable(newTable);
        assertThat(actual.size()).isEqualTo(0);
    }

    @Test
    public void testNewTableWithScanRangeInvalidScanRange() throws Exception {
        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("one"));
        GenericRow row2 = GenericRow.of(2, BinaryString.fromString("two"));
        writeTable(table, new GenericRow[] {row1});
        long ts1 = table.snapshotManager().latestSnapshot().timeMillis(); // timestamp of snapshot 1
        writeTable(table, new GenericRow[] {row2});
        long ts2 = table.snapshotManager().latestSnapshot().timeMillis(); // timestamp of snapshot 2
        long start = ts2;
        long end = ts1;
        assertThatThrownBy(
                        () -> IncrementalProcessingUtils.newTableWithScanRange(table, start, end))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNewTableWithScanRangeDeltaScanAffectedByExpiredSnapshot() throws Exception {
        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("one"));
        GenericRow row2 = GenericRow.of(2, BinaryString.fromString("two"));
        writeTable(table, new GenericRow[] {row1});
        long ts1 = table.snapshotManager().latestSnapshot().timeMillis(); // timestamp of snapshot 1
        writeTable(table, new GenericRow[] {row2});
        long ts2 = table.snapshotManager().latestSnapshot().timeMillis(); // timestamp of snapshot 2
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) table.newExpireSnapshots();
        expire.expireUntil(1, 2); // expire snapshot 1
        long start = ts1, end = ts2;
        assertThatThrownBy(
                        () -> IncrementalProcessingUtils.newTableWithScanRange(table, start, end))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testNewTableWithScanRangeFullScanAffectedByExpiredSnapshot() throws Exception {
        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("one"));
        GenericRow row2 = GenericRow.of(2, BinaryString.fromString("two"));
        writeTable(table, new GenericRow[] {row1});
        long ts1 = table.snapshotManager().latestSnapshot().timeMillis(); // timestamp of snapshot 1
        writeTable(table, new GenericRow[] {row2});
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) table.newExpireSnapshots();
        expire.expireUntil(1, 2); // expire snapshot 1
        long start = -1, end = ts1;
        assertThatThrownBy(
                        () -> IncrementalProcessingUtils.newTableWithScanRange(table, start, end))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testNewTableWithScanRangeNonEmptyScanForEmptyTable() throws Exception {
        long start1 = -1, end1 = System.currentTimeMillis();
        assertThatThrownBy(
                        () -> IncrementalProcessingUtils.newTableWithScanRange(table, start1, end1))
                .isInstanceOf(IllegalStateException.class);
    }

    private void writeTable(FileStoreTable fileStoreTable, GenericRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = fileStoreTable.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();
        for (GenericRow row : rows) {
            writer.write(row);
        }
        commit.commit(writer.prepareCommit());
        writer.close();
        commit.close();
    }

    private List<InternalRow> readTable(FileStoreTable fileStoreTable) throws Exception {
        List<InternalRow> result = new ArrayList<>();
        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
        TableScan scan = readBuilder.newScan();
        TableRead read = readBuilder.newRead();
        read.createReader(scan.plan()).forEachRemaining(result::add);
        return result;
    }

    private void validateResult(InternalRow actual, InternalRow expected) {
        InternalRow.FieldGetter[] fieldGetters = ROW_TYPE.fieldGetters();
        for (int i = 0; i < fieldGetters.length; i++) {
            Object actualField = fieldGetters[i].getFieldOrNull(actual);
            Object expectedField = fieldGetters[i].getFieldOrNull(expected);
            assertThat(actualField).isEqualTo(expectedField);
        }
    }
}
