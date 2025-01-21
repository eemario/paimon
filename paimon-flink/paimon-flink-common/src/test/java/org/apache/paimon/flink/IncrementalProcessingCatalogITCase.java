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

package org.apache.paimon.flink;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for catalogs with incremental processing. */
public class IncrementalProcessingCatalogITCase extends CatalogITCaseBase {
    @Timeout(60)
    @Test
    public void testGetTableTimestamp() throws Exception {
        tEnv.executeSql(
                        "CREATE TABLE t ( k INT, v INT, PRIMARY KEY (k) NOT ENFORCED ) "
                                + "WITH ( 'bucket' = '1')")
                .await();
        long startTime = System.currentTimeMillis();
        tEnv.executeSql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)").await();
        long endTime = System.currentTimeMillis();
        Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        long timestamp =
                ((FlinkCatalog) catalog)
                        .getTableTimestamp(
                                new ObjectPath(tEnv.getCurrentDatabase(), "t"),
                                System.currentTimeMillis());
        assertThat(timestamp).isBetween(startTime, endTime);
    }

    @Timeout(60)
    @Test
    public void testGetTableTimestampNotLatest() throws Exception {
        tEnv.executeSql(
                        "CREATE TABLE t ( k INT, v INT, PRIMARY KEY (k) NOT ENFORCED ) "
                                + "WITH ( 'bucket' = '1')")
                .await();
        long startTime = System.currentTimeMillis();
        tEnv.executeSql("INSERT INTO t VALUES (1, 10), (2, 20)").await();
        long endTime = System.currentTimeMillis();
        tEnv.executeSql("INSERT INTO t VALUES (3, 30), (4, 40)").await();
        Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        long timestamp =
                ((FlinkCatalog) catalog)
                        .getTableTimestamp(new ObjectPath(tEnv.getCurrentDatabase(), "t"), endTime);
        assertThat(timestamp).isBetween(startTime, endTime);
    }

    @Timeout(60)
    @Test
    public void testGetTableTimestampWithEarlierTimestamp() throws Exception {
        tEnv.executeSql(
                        "CREATE TABLE t ( k INT, v INT, PRIMARY KEY (k) NOT ENFORCED ) "
                                + "WITH ( 'bucket' = '1')")
                .await();
        long startTime = System.currentTimeMillis();
        tEnv.executeSql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)").await();
        Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        long timestamp =
                ((FlinkCatalog) catalog)
                        .getTableTimestamp(
                                new ObjectPath(tEnv.getCurrentDatabase(), "t"), startTime);
        assertThat(timestamp).isEqualTo(-1);
    }

    @Timeout(60)
    @Test
    public void testGetTableTimestampForEmptyTable() throws Exception {
        tEnv.executeSql(
                        "CREATE TABLE t ( k INT, v INT, PRIMARY KEY (k) NOT ENFORCED ) "
                                + "WITH ( 'bucket' = '1')")
                .await();
        Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        long timestamp =
                ((FlinkCatalog) catalog)
                        .getTableTimestamp(
                                new ObjectPath(tEnv.getCurrentDatabase(), "t"),
                                System.currentTimeMillis());
        assertThat(timestamp).isEqualTo(-1);
    }

    @Timeout(60)
    @Test
    public void testGetTableTimestampTableNotExist() {
        Catalog catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
        assertThatThrownBy(
                        () ->
                                ((FlinkCatalog) catalog)
                                        .getTableTimestamp(
                                                new ObjectPath(tEnv.getCurrentDatabase(), "t"),
                                                System.currentTimeMillis()))
                .isInstanceOf(TableNotExistException.class);
    }
}
