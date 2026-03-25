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

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseTabletConfig;
import org.apache.pinot.spi.lakehouse.LakehouseFileFormat;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotDescriptor;
import org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor;
import org.apache.pinot.spi.lakehouse.TabletManifest;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for controller-side lakehouse tablet planning.
 */
public class LakehouseTabletPlannerTest {
  private final LakehouseTabletPlanner _planner = new LakehouseTabletPlanner();

  @Test
  public void testPlanTabletManifestsGroupsByPartitionAndSizing() {
    LakehouseConfig lakehouseConfig = buildLakehouseConfig(2, 1024L, 16 * 1024);
    LakehouseSnapshotDescriptor snapshotDescriptor = buildSnapshotDescriptor(123L);

    List<TabletManifest> tabletManifests = _planner.planTabletManifests("table_OFFLINE", lakehouseConfig,
        snapshotDescriptor, List.of(
            buildMicrosegment("west-1", "file:/west-1.parquet", 400L, 10L, 100L, 150L, Map.of("region", "west")),
            buildMicrosegment("west-2", "file:/west-2.parquet", 500L, 20L, 151L, 200L, Map.of("region", "west")),
            buildMicrosegment("east-1", "file:/east-1.parquet", 300L, 15L, 50L, 120L, Map.of("region", "east"))));

    Assert.assertEquals(tabletManifests.size(), 2);
    Assert.assertEquals(tabletManifests.get(0).getMicrosegments().size(), 1);
    Assert.assertEquals(tabletManifests.get(0).getPartitionTuple(), Map.of("region", "east"));
    Assert.assertEquals(tabletManifests.get(1).getMicrosegments().size(), 2);
    Assert.assertEquals(tabletManifests.get(1).getPartitionTuple(), Map.of("region", "west"));
  }

  @Test
  public void testBuildEnvelopeRejectsOversizedEnvelope() {
    LakehouseConfig lakehouseConfig = buildLakehouseConfig(2, 1024L, 1);
    TabletManifest tabletManifest = new TabletManifest();
    tabletManifest.setVersion(1);
    tabletManifest.setTabletId("tablet-1");
    tabletManifest.setTableNameWithType("table_OFFLINE");
    tabletManifest.setSnapshotId(99L);
    tabletManifest.setSpecId(7);
    tabletManifest.setGeneration(1);
    tabletManifest.setMicrosegments(List.of(buildMicrosegment("segment", "file:/segment.parquet", 100L, 10L, 1L, 2L,
        Map.of("region", "west"))));

    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class,
        () -> _planner.buildEnvelope(tabletManifest, "file:/tmp/tablet-1.json", lakehouseConfig));
    Assert.assertTrue(exception.getMessage().contains("maxEnvelopeBytes"));
  }

  private static LakehouseConfig buildLakehouseConfig(int targetFilesPerTablet, long targetBytesPerTablet,
      int maxEnvelopeBytes) {
    IcebergCatalogConfig catalogConfig = new IcebergCatalogConfig();
    catalogConfig.setTableIdentifier("db.table");
    LakehouseTabletConfig tabletConfig = new LakehouseTabletConfig();
    tabletConfig.setTargetFilesPerTablet(targetFilesPerTablet);
    tabletConfig.setTargetBytesPerTablet(targetBytesPerTablet);
    tabletConfig.setMaxEnvelopeBytes(maxEnvelopeBytes);

    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    lakehouseConfig.setMode(LakehouseMode.ICEBERG_NATIVE);
    lakehouseConfig.setCatalogConfig(catalogConfig);
    lakehouseConfig.setTabletConfig(tabletConfig);
    return lakehouseConfig;
  }

  private static LakehouseSnapshotDescriptor buildSnapshotDescriptor(long snapshotId) {
    LakehouseSnapshotDescriptor snapshotDescriptor = new LakehouseSnapshotDescriptor();
    snapshotDescriptor.setTableIdentifier("db.table");
    snapshotDescriptor.setSnapshotId(snapshotId);
    snapshotDescriptor.setSpecId(7);
    return snapshotDescriptor;
  }

  private static MicrosegmentDescriptor buildMicrosegment(String microsegmentId, String filePath, long fileSizeBytes,
      long recordCount, long minTimeMillis, long maxTimeMillis, Map<String, String> partitionTuple) {
    MicrosegmentDescriptor microsegmentDescriptor = new MicrosegmentDescriptor();
    microsegmentDescriptor.setVersion(1);
    microsegmentDescriptor.setMicrosegmentId(microsegmentId);
    microsegmentDescriptor.setFilePath(filePath);
    microsegmentDescriptor.setFileFormat(LakehouseFileFormat.PARQUET);
    microsegmentDescriptor.setFileSizeBytes(fileSizeBytes);
    microsegmentDescriptor.setRecordCount(recordCount);
    microsegmentDescriptor.setMinTimeMillis(minTimeMillis);
    microsegmentDescriptor.setMaxTimeMillis(maxTimeMillis);
    microsegmentDescriptor.setPartitionTuple(partitionTuple);
    return microsegmentDescriptor;
  }
}
