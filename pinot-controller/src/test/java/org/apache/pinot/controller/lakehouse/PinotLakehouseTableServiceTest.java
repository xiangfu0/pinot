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
package org.apache.pinot.controller.lakehouse;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataUtils;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.pinot.controller.api.resources.TestingLakehouseCatalogAdapter;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseReadVisibilityMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseTabletConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseWriteMode;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotRequest;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Unit tests for controller-side lakehouse tablet publication semantics.
 */
public class PinotLakehouseTableServiceTest {
  @Test
  public void testRefreshTablePublishesOrderedTabletsAndCleansUpStaleMetadata()
      throws Exception {
    String tableName = "lakehouse_service_" + System.nanoTime();
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    TableConfig tableConfig = createLakehouseOfflineTable(tableName);

    FakePropertyStore propertyStore = new FakePropertyStore();
    ZKMetadataProvider.createTabletMetadataEnvelope(propertyStore,
        buildEnvelope(tableNameWithType, "stale-b", 111L, 11L));
    ZKMetadataProvider.createTabletMetadataEnvelope(propertyStore,
        buildEnvelope(tableNameWithType, "stale-a", 112L, 12L));
    ZKMetadataProvider.createSegmentZkMetadata(propertyStore, tableNameWithType, buildSegmentMetadata("stale-b"));
    ZKMetadataProvider.createSegmentZkMetadata(propertyStore, tableNameWithType, buildSegmentMetadata("stale-a"));

    PinotHelixResourceManager resourceManager = Mockito.mock(PinotHelixResourceManager.class);
    ControllerMetrics controllerMetrics = Mockito.mock(ControllerMetrics.class);
    when(resourceManager.getPropertyStore()).thenReturn(propertyStore);
    when(resourceManager.getTableConfig(tableNameWithType)).thenReturn(tableConfig);
    doAnswer(invocation -> ZKMetadataProvider.createSegmentZkMetadata(propertyStore, tableNameWithType,
        invocation.getArgument(1))).when(resourceManager).createSegmentZkMetadata(eq(tableNameWithType), any());
    doAnswer(invocation -> ZKMetadataProvider.setSegmentZKMetadata(propertyStore, tableNameWithType,
        invocation.getArgument(1))).when(resourceManager).updateZkMetadata(eq(tableNameWithType), any());
    doAnswer(invocation -> ZKMetadataProvider.removeSegmentZKMetadata(propertyStore, tableNameWithType,
        invocation.getArgument(1))).when(resourceManager).removeSegmentZKMetadata(eq(tableNameWithType), any());
    doNothing().when(resourceManager).assignSegments(eq(tableConfig), anyMap());
    doNothing().when(resourceManager).removeSegmentsFromIdealState(eq(tableNameWithType), any());

    Path manifestRootDirectory = Files.createTempDirectory("lakehouse-manifests");
    try {
      PinotLakehouseTableService service = new PinotLakehouseTableService(resourceManager, controllerMetrics,
          new LakehouseCatalogAdapterProvider(), new LakehouseTabletPlanner(),
          new LocalFileSystemLakehouseManifestStore(manifestRootDirectory));

      LakehouseTableRefreshResponse response = service.refreshTable(tableNameWithType, snapshotRequest(333L));

      Assert.assertEquals(response.getSnapshotDescriptor().getSnapshotId(), 333L);
      Assert.assertEquals(response.getTabletCount(), 2);
      Assert.assertEquals(response.getTabletIds(), List.of("snapshot-333-tablet-0", "snapshot-333-tablet-1"));
      Assert.assertEquals(response.getRemovedTabletIds(), List.of("stale-a", "stale-b"));

      List<TabletMetadataEnvelope> tabletMetadataEnvelopes =
          ZKMetadataProvider.getTabletMetadataEnvelopes(propertyStore, tableNameWithType);
      Assert.assertEquals(new HashSet<>(ZKMetadataProvider.getTabletMetadataEnvelopeIds(propertyStore,
          tableNameWithType)), new HashSet<>(List.of("snapshot-333-tablet-0", "snapshot-333-tablet-1")));
      Assert.assertEquals(tabletMetadataEnvelopes.size(), 2);
      for (TabletMetadataEnvelope tabletMetadataEnvelope : tabletMetadataEnvelopes) {
        Assert.assertEquals(tabletMetadataEnvelope.getSnapshotId(), 333L);
        Assert.assertTrue(new File(new URI(tabletMetadataEnvelope.getManifestUri())).exists());
      }

      SegmentZKMetadata tabletSegmentMetadata =
          ZKMetadataProvider.getSegmentZKMetadata(propertyStore, tableNameWithType, "snapshot-333-tablet-0");
      Assert.assertNotNull(tabletSegmentMetadata);
      TabletMetadataEnvelope tabletEnvelope =
          ZKMetadataProvider.getTabletMetadataEnvelope(propertyStore, tableNameWithType, "snapshot-333-tablet-0");
      Assert.assertNotNull(tabletEnvelope);
      Assert.assertTrue(SegmentZKMetadataUtils.isLakehouseTabletSegment(tabletSegmentMetadata));
      Assert.assertEquals(SegmentZKMetadataUtils.getLakehouseManifestUri(tabletSegmentMetadata),
          tabletEnvelope.getManifestUri());
      Assert.assertEquals(tabletSegmentMetadata.getCustomMap().get(CommonConstants.Segment.Lakehouse.SNAPSHOT_ID),
          "333");

      @SuppressWarnings("unchecked")
      ArgumentCaptor<Map<String, SegmentZKMetadata>> tabletMetadataCaptor = ArgumentCaptor.forClass(Map.class);
      verify(resourceManager).assignSegments(eq(tableConfig), tabletMetadataCaptor.capture());
      Assert.assertEquals(tabletMetadataCaptor.getValue().keySet(),
          Set.of("snapshot-333-tablet-0", "snapshot-333-tablet-1"));

      @SuppressWarnings("unchecked")
      ArgumentCaptor<List<String>> removedTabletIdCaptor = ArgumentCaptor.forClass(List.class);
      verify(resourceManager).removeSegmentsFromIdealState(eq(tableNameWithType), removedTabletIdCaptor.capture());
      Assert.assertEquals(new HashSet<>(removedTabletIdCaptor.getValue()), Set.of("stale-a", "stale-b"));

      verify(controllerMetrics).setValueOfTableGauge(tableNameWithType, ControllerGauge.LAKEHOUSE_TABLET_COUNT, 2L);
      verify(controllerMetrics).setValueOfTableGauge(tableNameWithType, ControllerGauge.LAKEHOUSE_STALE_MANIFEST_COUNT,
          0L);
      verify(controllerMetrics, times(2)).addMeteredTableValue(tableNameWithType,
          ControllerMeter.LAKEHOUSE_MANIFEST_PUBLISH_SUCCESS, 1L);
      verify(controllerMetrics)
          .addMeteredTableValue(tableNameWithType, ControllerMeter.LAKEHOUSE_SNAPSHOT_REFRESH_SUCCESS, 1L);
      verify(controllerMetrics, never()).addMeteredTableValue(eq(tableNameWithType),
          eq(ControllerMeter.LAKEHOUSE_SNAPSHOT_REFRESH_FAILURE), anyLong());
      verify(controllerMetrics, never()).addMeteredTableValue(eq(tableNameWithType),
          eq(ControllerMeter.LAKEHOUSE_MANIFEST_PUBLISH_FAILURE), anyLong());
    } finally {
      deleteRecursively(manifestRootDirectory);
    }
  }

  private static SegmentZKMetadata buildSegmentMetadata(String segmentName) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
    segmentZKMetadata.setCustomMap(Map.of(CommonConstants.Segment.Lakehouse.MANIFEST_URI,
        "file:/tmp/" + segmentName + ".json"));
    return segmentZKMetadata;
  }

  private static LakehouseSnapshotRequest snapshotRequest(Long snapshotId) {
    LakehouseSnapshotRequest snapshotRequest = new LakehouseSnapshotRequest();
    snapshotRequest.setSnapshotId(snapshotId);
    return snapshotRequest;
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

  private static TabletMetadataEnvelope buildEnvelope(String tableNameWithType, String tabletId, long snapshotId,
      long manifestVersion) {
    TabletMetadataEnvelope tabletMetadataEnvelope = new TabletMetadataEnvelope();
    tabletMetadataEnvelope.setVersion(1);
    tabletMetadataEnvelope.setTabletId(tabletId);
    tabletMetadataEnvelope.setTableNameWithType(tableNameWithType);
    tabletMetadataEnvelope.setSnapshotId(snapshotId);
    tabletMetadataEnvelope.setSpecId(7);
    tabletMetadataEnvelope.setGeneration(1);
    tabletMetadataEnvelope.setMinTimeMillis(10_000L + snapshotId);
    tabletMetadataEnvelope.setMaxTimeMillis(20_000L + snapshotId);
    tabletMetadataEnvelope.setApproximateRowCount(1_000L + snapshotId);
    tabletMetadataEnvelope.setApproximateSizeBytes(2_000L + snapshotId);
    tabletMetadataEnvelope.setMicrosegmentCount(4);
    tabletMetadataEnvelope.setManifestUri("file:/tmp/" + tabletId + ".json");
    tabletMetadataEnvelope.setManifestVersion(manifestVersion);
    tabletMetadataEnvelope.setState("COMMITTED");
    tabletMetadataEnvelope.setPartitionTuple(Map.of("region", "us-west", "tabletId", tabletId));
    return tabletMetadataEnvelope;
  }

  private static void deleteRecursively(Path rootDirectory)
      throws IOException {
    if (rootDirectory == null || !Files.exists(rootDirectory)) {
      return;
    }

    try (var paths = Files.walk(rootDirectory)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }
}
