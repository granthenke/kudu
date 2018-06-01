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
package org.apache.kudu;

import org.apache.kudu.client.BaseKuduTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

public class TestSchema {

  @Test
  public void testEquals() {
    assertEquals(BaseKuduTest.getBasicSchema(), BaseKuduTest.getBasicSchema());
    assertNotSame(BaseKuduTest.getBasicSchema(), BaseKuduTest.getBasicSchema());

    assertEquals(BaseKuduTest.getSchemaWithAllTypes(), BaseKuduTest.getSchemaWithAllTypes());
    assertNotSame(BaseKuduTest.getSchemaWithAllTypes(), BaseKuduTest.getSchemaWithAllTypes());

    // One less column
    List<ColumnSchema> fewerColumns =
        new ArrayList<>(BaseKuduTest.getBasicSchema().getColumns());
    fewerColumns.remove(fewerColumns.size() - 1);
    Schema fewerSchema = new Schema(fewerColumns);
    assertNotEquals(BaseKuduTest.getBasicSchema(), fewerSchema);

    // Different column (nullable)
    List<ColumnSchema> differentColumns =
        new ArrayList<>(BaseKuduTest.getBasicSchema().getColumns());
    differentColumns.remove(differentColumns.size() - 1);
    differentColumns.add(
        new ColumnSchema.ColumnSchemaBuilder("column4_b", Type.BOOL)
            .nullable(true)
            .build()
    );
    Schema differentSchema = new Schema(differentColumns);
    assertNotEquals(BaseKuduTest.getBasicSchema(), differentSchema);
  }
}