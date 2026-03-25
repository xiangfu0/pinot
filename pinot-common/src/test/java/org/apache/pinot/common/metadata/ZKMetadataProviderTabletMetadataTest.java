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
package org.apache.pinot.common.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ZKMetadataProviderTabletMetadataTest {
  private static final String TABLE_NAME_WITH_TYPE = "lakehouseTable_OFFLINE";
  private static final String TABLET_ID_1 = "tablet-1";
  private static final String TABLET_ID_2 = "tablet-2";

  @Test
  public void testTabletMetadataPathConstants() {
    assertEquals(CommonConstants.ZkPaths.LAKEHOUSE_TABLET_METADATA_PARENT_PATH, "/LAKEHOUSE/TABLET_METADATA");
    assertEquals(CommonConstants.ZkPaths.LAKEHOUSE_TABLET_METADATA_PATH_PREFIX, "/LAKEHOUSE/TABLET_METADATA/");
    assertEquals(ZKMetadataProvider.constructPropertyStorePathForTabletMetadata(TABLE_NAME_WITH_TYPE),
        "/LAKEHOUSE/TABLET_METADATA/" + TABLE_NAME_WITH_TYPE);
    assertEquals(ZKMetadataProvider.constructPropertyStorePathForTabletMetadata(TABLE_NAME_WITH_TYPE, TABLET_ID_1),
        "/LAKEHOUSE/TABLET_METADATA/" + TABLE_NAME_WITH_TYPE + "/" + TABLET_ID_1);
  }

  @Test
  public void testCreateGetUpdateListAndRemoveTabletMetadataEnvelope() {
    FakePropertyStore propertyStore = new FakePropertyStore();
    TabletMetadataEnvelope firstEnvelope = buildEnvelope(TABLET_ID_1, 101L, 7L);
    TabletMetadataEnvelope secondEnvelope = buildEnvelope(TABLET_ID_2, 102L, 8L);

    assertTrue(ZKMetadataProvider.setTabletMetadataEnvelope(propertyStore, firstEnvelope));
    assertEquals(ZKMetadataProvider.getTabletMetadataEnvelope(propertyStore, TABLE_NAME_WITH_TYPE, TABLET_ID_1),
        firstEnvelope);
    assertEquals(ZKMetadataProvider.getTabletMetadataEnvelopeIds(propertyStore, TABLE_NAME_WITH_TYPE),
        List.of(TABLET_ID_1));

    assertTrue(ZKMetadataProvider.setTabletMetadataEnvelope(propertyStore, secondEnvelope));
    assertEquals(ZKMetadataProvider.getTabletMetadataEnvelope(propertyStore, TABLE_NAME_WITH_TYPE, TABLET_ID_2),
        secondEnvelope);

    TabletMetadataEnvelope updatedFirstEnvelope = buildEnvelope(TABLET_ID_1, 201L, 17L);
    updatedFirstEnvelope.setManifestVersion(9L);
    assertTrue(ZKMetadataProvider.setTabletMetadataEnvelope(propertyStore, updatedFirstEnvelope));
    assertEquals(ZKMetadataProvider.getTabletMetadataEnvelope(propertyStore, TABLE_NAME_WITH_TYPE, TABLET_ID_1),
        updatedFirstEnvelope);

    List<TabletMetadataEnvelope> tabletMetadataEnvelopes =
        ZKMetadataProvider.getTabletMetadataEnvelopes(propertyStore, TABLE_NAME_WITH_TYPE);
    assertEquals(tabletMetadataEnvelopes.size(), 2);
    assertTrue(tabletMetadataEnvelopes.contains(updatedFirstEnvelope));
    assertTrue(tabletMetadataEnvelopes.contains(secondEnvelope));

    assertTrue(ZKMetadataProvider.removeTabletMetadataEnvelope(propertyStore, TABLE_NAME_WITH_TYPE, TABLET_ID_1));
    assertNull(ZKMetadataProvider.getTabletMetadataEnvelope(propertyStore, TABLE_NAME_WITH_TYPE, TABLET_ID_1));
    assertEquals(ZKMetadataProvider.getTabletMetadataEnvelopeIds(propertyStore, TABLE_NAME_WITH_TYPE),
        List.of(TABLET_ID_2));
  }

  @Test
  public void testListTabletMetadataEnvelopeForMissingTableReturnsEmptyList() {
    FakePropertyStore propertyStore = new FakePropertyStore();

    assertTrue(ZKMetadataProvider.getTabletMetadataEnvelopeIds(propertyStore, TABLE_NAME_WITH_TYPE).isEmpty());
    assertTrue(ZKMetadataProvider.getTabletMetadataEnvelopes(propertyStore, TABLE_NAME_WITH_TYPE).isEmpty());
  }

  @Test
  public void testCreateTabletMetadataEnvelope() {
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    TabletMetadataEnvelope envelope = buildEnvelope(TABLET_ID_1, 101L, 7L);

    Mockito.when(propertyStore.create(Mockito.eq(ZKMetadataProvider.constructPropertyStorePathForTabletMetadata(
        TABLE_NAME_WITH_TYPE, TABLET_ID_1)), Mockito.any(ZNRecord.class), Mockito.eq(AccessOption.PERSISTENT)))
        .thenReturn(true);

    assertTrue(ZKMetadataProvider.createTabletMetadataEnvelope(propertyStore, envelope));
  }

  private static TabletMetadataEnvelope buildEnvelope(String tabletId, long snapshotId, long manifestVersion) {
    TabletMetadataEnvelope tabletMetadataEnvelope = new TabletMetadataEnvelope();
    tabletMetadataEnvelope.setTabletId(tabletId);
    tabletMetadataEnvelope.setTableNameWithType(TABLE_NAME_WITH_TYPE);
    tabletMetadataEnvelope.setSnapshotId(snapshotId);
    tabletMetadataEnvelope.setSpecId(3);
    tabletMetadataEnvelope.setGeneration(1);
    tabletMetadataEnvelope.setMicrosegmentCount(4);
    tabletMetadataEnvelope.setApproximateRowCount(10_000L + snapshotId);
    tabletMetadataEnvelope.setApproximateSizeBytes(20_000L + snapshotId);
    tabletMetadataEnvelope.setManifestUri("s3://bucket/" + TABLE_NAME_WITH_TYPE + "/" + tabletId + ".json");
    tabletMetadataEnvelope.setManifestVersion(manifestVersion);
    tabletMetadataEnvelope.setState("ACTIVE");
    Map<String, String> partitionTuple = new HashMap<>();
    partitionTuple.put("ds", "2026-03-25");
    partitionTuple.put("region", tabletId);
    tabletMetadataEnvelope.setPartitionTuple(partitionTuple);
    return tabletMetadataEnvelope;
  }
}
