/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.config.table;

import java.io.IOException;
import java.util.Map;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseReadVisibilityMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseSidecarConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseTabletConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseWriteMode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class LakehouseTableConfigTest {
  @Test
  public void testLakehouseConfigBuilderAndJsonRoundTrip()
      throws IOException {
    LakehouseConfig lakehouseConfig = buildLakehouseConfig();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("lakehouseTable")
        .setLakehouseConfig(lakehouseConfig)
        .build();

    assertNotNull(tableConfig.getLakehouseConfig());
    assertEquals(tableConfig.getLakehouseConfig(), lakehouseConfig);
    assertFalse(tableConfig.getLakehouseConfig().isEnabled());
    assertEquals(tableConfig.getLakehouseConfig().getMode(), LakehouseMode.ICEBERG_NATIVE);
    assertEquals(tableConfig.getLakehouseConfig().getReadVisibilityMode(),
        LakehouseReadVisibilityMode.HOT_PLUS_SNAPSHOT);
    assertEquals(tableConfig.getLakehouseConfig().getWriteMode(), LakehouseWriteMode.DISABLED);

    TableConfig jsonRoundTrip = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
    assertEquals(jsonRoundTrip, tableConfig);
    assertEquals(jsonRoundTrip.getLakehouseConfig().getCatalogConfig().getTableIdentifier(), "db.table");
    assertEquals(jsonRoundTrip.getLakehouseConfig().getTabletConfig().getTargetFilesPerTablet(), 96);
    assertEquals(jsonRoundTrip.getLakehouseConfig().getSidecarConfig().getStorageUri(), "s3://bucket/sidecars");

    TableConfig copy = new TableConfig(tableConfig);
    assertEquals(copy, tableConfig);
    assertEquals(copy.getLakehouseConfig(), lakehouseConfig);
  }

  @Test
  public void testOldJsonWithoutLakehouseConfigDeserializesCleanly()
      throws IOException {
    String oldJson = "{\"tableName\":\"legacy_OFFLINE\",\"tableType\":\"OFFLINE\","
        + "\"segmentsConfig\":{},\"tenants\":{},\"tableIndexConfig\":{}}";
    TableConfig config = JsonUtils.stringToObject(oldJson, TableConfig.class);
    assertNull(config.getLakehouseConfig());
  }

  private static LakehouseConfig buildLakehouseConfig() {
    IcebergCatalogConfig catalogConfig = new IcebergCatalogConfig();
    catalogConfig.setCatalogType(IcebergCatalogType.REST);
    catalogConfig.setCatalogName("pinot-rest");
    catalogConfig.setCatalogUri("http://catalog:8181");
    catalogConfig.setWarehouseUri("s3://bucket/warehouse");
    catalogConfig.setTableIdentifier("db.table");
    catalogConfig.setProperties(Map.of("credential", "token"));

    LakehouseTabletConfig tabletConfig = new LakehouseTabletConfig();
    tabletConfig.setTargetFilesPerTablet(96);
    tabletConfig.setTargetBytesPerTablet(8_589_934_592L);
    tabletConfig.setTimePartitionColumn("eventTime");
    tabletConfig.setTimePartitionBucket("DAY");
    tabletConfig.setMaxEnvelopeBytes(4096);

    LakehouseSidecarConfig sidecarConfig = new LakehouseSidecarConfig();
    sidecarConfig.setEnabled(true);
    sidecarConfig.setFailOpen(true);
    sidecarConfig.setProvider("pinot-sidecar");
    sidecarConfig.setStorageUri("s3://bucket/sidecars");

    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(false);
    lakehouseConfig.setMode(LakehouseMode.ICEBERG_NATIVE);
    lakehouseConfig.setCatalogConfig(catalogConfig);
    lakehouseConfig.setReadVisibilityMode(LakehouseReadVisibilityMode.HOT_PLUS_SNAPSHOT);
    lakehouseConfig.setWriteMode(LakehouseWriteMode.DISABLED);
    lakehouseConfig.setTabletConfig(tabletConfig);
    lakehouseConfig.setSidecarConfig(sidecarConfig);
    return lakehouseConfig;
  }
}
