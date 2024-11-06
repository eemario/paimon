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
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;

/** Utilities for incremental processing. */
public class IncrementalProcessingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalProcessingUtils.class);
    private static final long EARLIEST = -1;
    private static final String SNAPSHOT_EXPIRED_MSG =
            "This usually indicates that the snapshots retained time or number is too small compared to the scheduling interval of this job.";

    public static Table newTableWithScanRange(Table table, long start, long end) {
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    "Cannot set scan range for " + table.getClass());
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        if (fileStoreTable.coreOptions().toConfiguration().get(SCAN_MODE)
                != CoreOptions.StartupMode.DEFAULT) {
            throw new UnsupportedOperationException(
                    "Cannot set scan range for table with specified scan mode");
        }
        if (start > end) {
            throw new IllegalArgumentException(
                    "start timestamp " + start + " > end timestamp " + end + " in scan range");
        }
        if (start == EARLIEST && end == EARLIEST) {
            // special case to indicate empty scan range, which can happen when incremental
            // processing is performed on empty source tables
            Map<String, String> newOptions = new HashMap<>();
            newOptions.put(INCREMENTAL_BETWEEN.key(), "0,0");
            return fileStoreTable.copyWithoutTimeTravel(newOptions);
        }
        if (start == EARLIEST) {
            return newTableWithFullScan(fileStoreTable, end);
        }
        return newTableWithDeltaScan(fileStoreTable, start, end);
    }

    private static Table newTableWithFullScan(FileStoreTable fileStoreTable, long timestamp) {
        Snapshot earliestSnapshot = fileStoreTable.snapshotManager().earliestSnapshot();
        if (earliestSnapshot == null) {
            throw new IllegalStateException(
                    "No snapshot is found for table "
                            + fileStoreTable.name()
                            + ", full scan timestamp "
                            + timestamp
                            + ". "
                            + SNAPSHOT_EXPIRED_MSG);
        }
        long earliestSnapshotTimestamp = earliestSnapshot.timeMillis();
        if (earliestSnapshotTimestamp > timestamp) {
            throw new IllegalStateException(
                    "The full scan timestamp "
                            + timestamp
                            + " is smaller than the earliest snapshot timestamp of table "
                            + fileStoreTable.name()
                            + ". "
                            + SNAPSHOT_EXPIRED_MSG);
        }
        LOG.info(
                "start is EARLIEST ({}), use full scan at timestamp {} for table {}.",
                EARLIEST,
                timestamp,
                fileStoreTable.name());
        Map<String, String> newOptions = new HashMap<>();
        newOptions.put(SCAN_TIMESTAMP_MILLIS.key(), Long.toString(timestamp));
        // incremental processing does not support time travel now
        return fileStoreTable.copyWithoutTimeTravel(newOptions);
    }

    private static Table newTableWithDeltaScan(
            FileStoreTable fileStoreTable, long start, long end) {
        Snapshot earliestSnapshot = fileStoreTable.snapshotManager().earliestSnapshot();
        if (earliestSnapshot == null) {
            throw new IllegalStateException(
                    "No snapshot is found for table "
                            + fileStoreTable.name()
                            + ", scan range ("
                            + start
                            + ","
                            + end
                            + "]. "
                            + SNAPSHOT_EXPIRED_MSG);
        }
        long earliestSnapshotTimestamp = earliestSnapshot.timeMillis();
        if (earliestSnapshotTimestamp > start) {
            throw new IllegalStateException(
                    "The delta scan range start timestamp "
                            + start
                            + " is smaller than the earliest snapshot timestamp of table "
                            + fileStoreTable.name()
                            + ". "
                            + SNAPSHOT_EXPIRED_MSG);
        }
        Map<String, String> newOptions = new HashMap<>();
        newOptions.put(INCREMENTAL_BETWEEN_TIMESTAMP.key(), start + "," + end);
        // incremental processing does not support time travel now
        return fileStoreTable.copyWithoutTimeTravel(newOptions);
    }
}
