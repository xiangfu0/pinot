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
package org.apache.pinot.materializedview.consistency;

import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.PartitionFingerprint;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class MaterializedViewConsistencyManagerTest {
  private static final String BASE_TABLE = "baseTable";
  private static final String MV_TABLE = "mvTable_OFFLINE";
  private static final long BUCKET_MS = 86_400_000L;

  @Test
  public void testEpochZeroRangeMarksPartitionStale()
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String runtimePath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(MV_TABLE);
    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MV_TABLE, 2 * BUCKET_MS,
        Map.of(
            0L, validInfo(),
            BUCKET_MS, validInfo()));
    when(propertyStore.get(eq(runtimePath), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(runtime.toZNRecord());
    when(propertyStore.set(eq(runtimePath), any(ZNRecord.class), eq(0), eq(AccessOption.PERSISTENT)))
        .thenReturn(true);

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);
    manager.onMaterializedViewTableCreated(MV_TABLE, List.of(BASE_TABLE));
    manager.onBaseTableDataChange(BASE_TABLE, 0L, BUCKET_MS - 1);

    manager.flush(BASE_TABLE);
    manager.stop();

    ArgumentCaptor<ZNRecord> recordCaptor = ArgumentCaptor.forClass(ZNRecord.class);
    verify(propertyStore).set(eq(runtimePath), recordCaptor.capture(), eq(0), eq(AccessOption.PERSISTENT));
    MaterializedViewRuntimeMetadata updated =
        MaterializedViewRuntimeMetadata.fromZNRecord(recordCaptor.getValue());
    assertEquals(updated.getPartitions().get(0L).getState(), PartitionState.STALE);
    assertEquals(updated.getPartitions().get(BUCKET_MS).getState(), PartitionState.VALID);
  }

  @Test
  public void testDefinitionCreatedAfterInitRegistersBaseTableMapping()
      throws Exception {
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String definitionParentPath = ZKMetadataProvider.getPropertyStorePathForMaterializedViewDefinitionPrefix();
    String definitionPath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewDefinition(MV_TABLE);
    String runtimePath = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(MV_TABLE);

    when(propertyStore.getChildNames(eq(definitionParentPath), eq(AccessOption.PERSISTENT)))
        .thenReturn(List.of(), List.of(), List.of(MV_TABLE));
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        MV_TABLE, List.of(BASE_TABLE), "SELECT count(*) FROM baseTable", Map.of(), null);
    when(propertyStore.get(eq(definitionPath), any(), eq(AccessOption.PERSISTENT)))
        .thenReturn(definition.toZNRecord());

    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MV_TABLE, 2 * BUCKET_MS,
        Map.of(
            0L, validInfo(),
            BUCKET_MS, validInfo()));
    when(propertyStore.get(eq(runtimePath), any(Stat.class), eq(AccessOption.PERSISTENT)))
        .thenReturn(runtime.toZNRecord());
    when(propertyStore.set(eq(runtimePath), any(ZNRecord.class), eq(0), eq(AccessOption.PERSISTENT)))
        .thenReturn(true);

    MaterializedViewConsistencyManager manager = new MaterializedViewConsistencyManager();
    manager.init(propertyStore);

    ArgumentCaptor<IZkChildListener> childListenerCaptor = ArgumentCaptor.forClass(IZkChildListener.class);
    verify(propertyStore).subscribeChildChanges(eq(definitionParentPath), childListenerCaptor.capture());
    childListenerCaptor.getValue().handleChildChange(definitionParentPath, List.of(MV_TABLE));

    manager.onBaseTableDataChange(BASE_TABLE, 0L, BUCKET_MS - 1);
    manager.flush(BASE_TABLE);
    manager.stop();

    ArgumentCaptor<ZNRecord> recordCaptor = ArgumentCaptor.forClass(ZNRecord.class);
    verify(propertyStore).set(eq(runtimePath), recordCaptor.capture(), eq(0), eq(AccessOption.PERSISTENT));
    MaterializedViewRuntimeMetadata updated =
        MaterializedViewRuntimeMetadata.fromZNRecord(recordCaptor.getValue());
    assertEquals(updated.getPartitions().get(0L).getState(), PartitionState.STALE);
    assertEquals(updated.getPartitions().get(BUCKET_MS).getState(), PartitionState.VALID);
  }

  private static PartitionInfo validInfo() {
    return new PartitionInfo(PartitionState.VALID, new PartitionFingerprint(1, 1234L), 10L);
  }
}
