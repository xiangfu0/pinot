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
package org.apache.pinot.controller.api.resources;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseReadVisibilityMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseTabletConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseWriteMode;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotLakehouseTabletMetadataRestletResourceTest extends ControllerTest {
  private static final String BASE_TABLE_NAME = "lakehouse_inspect";

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(2, true);
    _controllerRequestURLBuilder = getControllerRequestURLBuilder();
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    cleanup();
  }

  @AfterClass
  public void tearDownClass() {
    stopFakeInstances();
    stopController();
    stopZk();
  }

  @Test
  public void testListAndGetLakehouseTabletMetadataEnvelopes()
      throws Exception {
    String tableName = uniqueTableName("list_get");
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    addLakehouseOfflineTable(tableName);

    TabletMetadataEnvelope tabletOne = buildEnvelope(tableNameWithType, "tablet-1", 101L, 11L);
    TabletMetadataEnvelope tabletTwo = buildEnvelope(tableNameWithType, "tablet-2", 102L, 12L);
    assertTrue(ZKMetadataProvider.createTabletMetadataEnvelope(getPropertyStore(), tabletOne));
    assertTrue(ZKMetadataProvider.createTabletMetadataEnvelope(getPropertyStore(), tabletTwo));

    String listUrl = lakehouseTabletUrl(tableNameWithType);
    List<TabletMetadataEnvelope> envelopes =
        JsonUtils.stringToObject(ControllerTest.sendGetRequest(listUrl),
            new com.fasterxml.jackson.core.type.TypeReference<List<TabletMetadataEnvelope>>() { });
    assertEquals(envelopes.size(), 2);
    assertEquals(envelopes.get(0).getTabletId(), "tablet-1");
    assertEquals(envelopes.get(1).getTabletId(), "tablet-2");

    TabletMetadataEnvelope fetched =
        JsonUtils.stringToObject(ControllerTest.sendGetRequest(listUrl + "/tablet-2"), TabletMetadataEnvelope.class);
    assertEquals(fetched, tabletTwo);
  }

  @Test
  public void testMissingLakehouseTabletReturnsNotFound()
      throws Exception {
    String tableName = uniqueTableName("missing_tablet");
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    addLakehouseOfflineTable(tableName);

    try {
      ControllerTest.sendGetRequest(lakehouseTabletUrl(tableNameWithType) + "/tablet-does-not-exist");
      fail("Expected tablet lookup to fail");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("not found"), e.getMessage());
    }
  }

  @Test
  public void testRejectsNonLakehouseTables()
      throws Exception {
    String tableName = uniqueTableName("legacy");
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    addDummySchema(tableName);
    addTableConfig(createLegacyOfflineTable(tableName));

    try {
      ControllerTest.sendGetRequest(lakehouseTabletUrl(tableNameWithType));
      fail("Expected non-lakehouse table lookup to fail");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("only available for enabled ICEBERG_NATIVE tables in Phase 1"),
          e.getMessage());
    }
  }

  private String uniqueTableName(String suffix) {
    return BASE_TABLE_NAME + "_" + suffix + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  private String lakehouseTabletUrl(String tableNameWithType) {
    return _controllerRequestURLBuilder.getBaseUrl() + "/tables/" + tableNameWithType + "/lakehouse/tablets";
  }

  private void addLakehouseOfflineTable(String tableName)
      throws IOException {
    addDummySchema(tableName);
    addTableConfig(createLakehouseOfflineTable(tableName));
  }

  private static TableConfig createLakehouseOfflineTable(String tableName) {
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .setLakehouseConfig(buildLakehouseConfig());
    return builder.build();
  }

  private static TableConfig createLegacyOfflineTable(String tableName) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .build();
  }

  private static LakehouseConfig buildLakehouseConfig() {
    IcebergCatalogConfig catalogConfig = new IcebergCatalogConfig();
    catalogConfig.setCatalogType(IcebergCatalogType.REST);
    catalogConfig.setCatalogName("testCatalog");
    catalogConfig.setCatalogUri("http://localhost:8181");
    catalogConfig.setTableIdentifier("test.table");

    LakehouseTabletConfig tabletConfig = new LakehouseTabletConfig();
    tabletConfig.setTargetFilesPerTablet(2);
    tabletConfig.setTargetBytesPerTablet(1024L);
    tabletConfig.setMaxEnvelopeBytes(4096);

    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    lakehouseConfig.setMode(LakehouseMode.ICEBERG_NATIVE);
    lakehouseConfig.setCatalogConfig(catalogConfig);
    lakehouseConfig.setReadVisibilityMode(LakehouseReadVisibilityMode.SNAPSHOT_ONLY);
    lakehouseConfig.setWriteMode(LakehouseWriteMode.DISABLED);
    lakehouseConfig.setTabletConfig(tabletConfig);
    return lakehouseConfig;
  }

  private static TabletMetadataEnvelope buildEnvelope(String tableNameWithType, String tabletId, long snapshotId,
      long manifestVersion) {
    TabletMetadataEnvelope tabletMetadataEnvelope = new TabletMetadataEnvelope();
    tabletMetadataEnvelope.setVersion(1);
    tabletMetadataEnvelope.setTabletId(tabletId);
    tabletMetadataEnvelope.setTableNameWithType(tableNameWithType);
    tabletMetadataEnvelope.setSnapshotId(snapshotId);
    tabletMetadataEnvelope.setSpecId(7);
    tabletMetadataEnvelope.setGeneration(3);
    tabletMetadataEnvelope.setMinTimeMillis(10_000L + snapshotId);
    tabletMetadataEnvelope.setMaxTimeMillis(20_000L + snapshotId);
    tabletMetadataEnvelope.setApproximateRowCount(1_000L + snapshotId);
    tabletMetadataEnvelope.setApproximateSizeBytes(2_000L + snapshotId);
    tabletMetadataEnvelope.setMicrosegmentCount(4);
    tabletMetadataEnvelope.setManifestUri("s3://lakehouse/" + tableNameWithType + "/" + tabletId + ".json");
    tabletMetadataEnvelope.setManifestVersion(manifestVersion);
    tabletMetadataEnvelope.setState("COMMITTED");
    tabletMetadataEnvelope.setPartitionTuple(Map.of("region", "us-west", "tabletId", tabletId));
    return tabletMetadataEnvelope;
  }
}
