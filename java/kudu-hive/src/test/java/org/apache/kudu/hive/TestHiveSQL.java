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

package org.apache.kudu.hive;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class TestHiveSQL {

  private static final String hiveDatabaseName = "kudu_db";
  private static final String hiveTableName = hiveDatabaseName + ".test_table";
  private static final String kuduTableName = "kudu_table";

  private KuduClient client;

  @HiveSQL(files = {})
  private HiveShell shell;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    client = harness.getClient();

    // Create and load a basic Kudu table.
    ClientTestUtil.createDefaultTable(client, kuduTableName);
    ClientTestUtil.loadDefaultTable(client, kuduTableName, 100);

    // Create an external Hive table.
    shell.execute(String.format("CREATE DATABASE %s", hiveDatabaseName));
    shell.execute(String.format(
        "CREATE EXTERNAL TABLE %s " +
            "STORED BY 'org.apache.kudu.hive.KuduStorageHandler' " +
            "TBLPROPERTIES (\"kudu.table_name\"=\"%s\", \"kudu.master_addresses\"=\"%s\")",
        hiveTableName, kuduTableName, harness.getMasterAddressesAsString()));
  }

  @Test
  public void testBasicRead() {
    List<Object[]> result = shell.executeStatement(String.format("SELECT * FROM %s",
        hiveTableName));
    assertEquals(100, result.size());

    List<Object[]> result2 = shell.executeStatement(String.format("SELECT count(*) FROM %s",
        hiveTableName));
    assertEquals(1, result2.size());
    assertEquals(100L, (long) result2.get(0)[0]);
  }

  @Test
  public void testBasicWrite() {
    shell.execute(String.format("INSERT INTO TABLE %s VALUES (101, 2, 3, 'a string', true)",
        hiveTableName));
    List<Object[]> result = shell.executeStatement(String.format("SELECT * FROM %s",
        hiveTableName));
    assertEquals(101, result.size());
  }

  @Test
  public void testInsertAsSelect() throws Exception {
    // Create and load a basic Kudu table.
    String copyKuduTable = "kudu_copy";
    String copyHiveTable = hiveDatabaseName + ".kudu_copy";
    ClientTestUtil.createDefaultTable(client, copyKuduTable);
    shell.execute(String.format(
        "CREATE EXTERNAL TABLE %s " +
            "STORED BY 'org.apache.kudu.hive.KuduStorageHandler' " +
            "TBLPROPERTIES (\"kudu.table_name\"=\"%s\", \"kudu.master_addresses\"=\"%s\")",
        copyHiveTable, copyKuduTable, harness.getMasterAddressesAsString()));
    shell.execute(String.format("INSERT INTO TABLE %s SELECT * FROM %s",
        copyHiveTable, hiveTableName));
    List<Object[]> result = shell.executeStatement(String.format("SELECT * FROM %s",
        copyHiveTable));
    assertEquals(100, result.size());
  }

  @Test
  public void testBasicProjection() {
    List<Object[]> result = shell.executeStatement(String.format("SELECT column3_s, key FROM %s",
        hiveTableName));
    assertEquals(100, result.size());
  }

  @Test
  public void testBasicPredicate() throws Exception {
    List<Object[]> result = shell.executeStatement(String.format("SELECT * FROM %s WHERE key < 50",
        hiveTableName));
    assertEquals(50, result.size());
  }
}