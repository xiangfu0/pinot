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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.lakehouse.LakehouseTableRefreshResponse;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseReadVisibilityMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseTabletConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseWriteMode;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotDescriptor;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration tests for the controller-side lakehouse refresh and refs APIs.
 */
public class PinotLakehouseRestletResourceTest extends ControllerTest {
  private static final String BASE_TABLE_NAME = "lakehouse_refresh";

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
  public void testResolveLakehouseRefs()
      throws Exception {
    String tableName = uniqueTableName("refs");
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    addLakehouseOfflineTable(tableName);

    LakehouseSnapshotDescriptor snapshotDescriptor = JsonUtils.stringToObject(
        ControllerTest.sendGetRequest(lakehouseBaseUrl(tableNameWithType) + "/refs?snapshotId=222"),
        LakehouseSnapshotDescriptor.class);
    assertEquals(snapshotDescriptor.getSnapshotId(), 222L);
    assertEquals(snapshotDescriptor.getSpecId(), 7);
    assertEquals(snapshotDescriptor.getTableIdentifier(), "db." + tableName);
  }

  @Test
  public void testResolveLakehouseRefsRejectsConflictingSelectors()
      throws Exception {
    String tableName = uniqueTableName("refs_conflict");
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    addLakehouseOfflineTable(tableName);

    try {
      ControllerTest.sendGetRequest(lakehouseBaseUrl(tableNameWithType) + "/refs?snapshotId=222&branch=dev");
      org.testng.Assert.fail("Expected conflicting lakehouse snapshot selectors to be rejected");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Only one lakehouse snapshot selector can be set"), e.getMessage());
    }
  }

  @Test
  public void testRefreshLakehouseTablePublishesTabletsAndManifests()
      throws Exception {
    String tableName = uniqueTableName("refresh");
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    addLakehouseOfflineTable(tableName);

    LakehouseTableRefreshResponse firstResponse = JsonUtils.stringToObject(
        ControllerTest.sendPostRequest(lakehouseBaseUrl(tableNameWithType) + "/refresh?snapshotId=333"),
        LakehouseTableRefreshResponse.class);
    assertEquals(firstResponse.getSnapshotDescriptor().getSnapshotId(), 333L);
    assertEquals(firstResponse.getTabletCount(), 2);
    assertEquals(firstResponse.getTabletIds(), List.of("snapshot-333-tablet-0", "snapshot-333-tablet-1"));
    assertTrue(firstResponse.getRemovedTabletIds().isEmpty());

    LakehouseTableRefreshResponse secondResponse = JsonUtils.stringToObject(
        ControllerTest.sendPostRequest(lakehouseBaseUrl(tableNameWithType) + "/refresh?snapshotId=334"),
        LakehouseTableRefreshResponse.class);
    assertEquals(secondResponse.getSnapshotDescriptor().getSnapshotId(), 334L);
    assertEquals(secondResponse.getTabletCount(), 2);
    assertEquals(secondResponse.getTabletIds(), List.of("snapshot-334-tablet-0", "snapshot-334-tablet-1"));
    assertEquals(secondResponse.getRemovedTabletIds(), firstResponse.getTabletIds());

    IdealState idealState = getHelixResourceManager().getTableIdealState(tableNameWithType);
    assertTrue(idealState != null);
    assertEquals(idealState.getPartitionSet(), Set.of("snapshot-334-tablet-0", "snapshot-334-tablet-1"));

    List<TabletMetadataEnvelope> tabletMetadataEnvelopes =
        ZKMetadataProvider.getTabletMetadataEnvelopes(getPropertyStore(), tableNameWithType);
    assertEquals(tabletMetadataEnvelopes.size(), 2);
    assertTrue(tabletMetadataEnvelopes.stream().allMatch(tabletMetadataEnvelope ->
        tabletMetadataEnvelope.getSnapshotId() == 334L));
    TabletMetadataEnvelope refreshedTabletMetadataEnvelope =
        ZKMetadataProvider.getTabletMetadataEnvelope(getPropertyStore(), tableNameWithType,
            "snapshot-334-tablet-0");
    assertTrue(refreshedTabletMetadataEnvelope != null);
    assertTrue(getHelixResourceManager().getSegmentZKMetadata(tableNameWithType, "snapshot-334-tablet-0") != null);
    assertEquals(refreshedTabletMetadataEnvelope.getManifestUri(),
        getHelixResourceManager().getSegmentZKMetadata(tableNameWithType, "snapshot-334-tablet-0").getCustomMap()
            .get(CommonConstants.Segment.Lakehouse.MANIFEST_URI));
    for (TabletMetadataEnvelope tabletMetadataEnvelope : tabletMetadataEnvelopes) {
      assertTrue(new File(new URI(tabletMetadataEnvelope.getManifestUri())).exists());
      assertEquals(tabletMetadataEnvelope.getSnapshotId(), 334L);
      assertTrue(tabletMetadataEnvelope.getMicrosegmentCount() > 0);
    }
  }

  private String uniqueTableName(String suffix) {
    return BASE_TABLE_NAME + "_" + suffix + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  private String lakehouseBaseUrl(String tableNameWithType) {
    return _controllerRequestURLBuilder.getBaseUrl() + "/tables/" + tableNameWithType + "/lakehouse";
  }

  private void addLakehouseOfflineTable(String tableName)
      throws IOException {
    addDummySchema(tableName);
    addTableConfig(createLakehouseOfflineTable(tableName));
  }

  private static TableConfig createLakehouseOfflineTable(String tableName) {
    IcebergCatalogConfig catalogConfig = new IcebergCatalogConfig();
    catalogConfig.setCatalogType(IcebergCatalogType.CUSTOM);
    catalogConfig.setCatalogName("testCatalog");
    catalogConfig.setTableIdentifier("db." + tableName);
    catalogConfig.setPluginName(TestingLakehouseCatalogAdapter.class.getName());

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

    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .setLakehouseConfig(lakehouseConfig)
        .build();
  }
}
