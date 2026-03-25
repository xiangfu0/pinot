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
package org.apache.pinot.controller.helix.core.lakehouse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link TabletAssignmentManager} verifying IdealState updates for lakehouse tablets.
 *
 * <p>Tests exercise the extracted updater function ({@link TabletAssignmentManager#buildIdealStateUpdater})
 * directly, allowing full verification of the IdealState mutation logic without requiring a live
 * Helix cluster or complex ZooKeeper mocks.</p>
 */
public class TabletAssignmentManagerTest {
  private static final String TABLE_NAME = "lakehouse_test_OFFLINE";

  private List<String> _serverInstances;

  @BeforeMethod
  public void setUp() {
    _serverInstances = Arrays.asList("Server_host1_8099", "Server_host2_8099", "Server_host3_8099");
  }

  @Test
  public void testNewTabletsAreAddedToIdealState() {
    IdealState idealState = createEmptyIdealState();
    List<TabletMetadataEnvelope> tablets = createTabletEnvelopes("tablet_0", "tablet_1", "tablet_2");

    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, 1);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    Map<String, Map<String, String>> assignment = result.getRecord().getMapFields();
    assertEquals(assignment.size(), 3);
    assertTrue(assignment.containsKey("tablet_0"));
    assertTrue(assignment.containsKey("tablet_1"));
    assertTrue(assignment.containsKey("tablet_2"));

    // Each tablet should have exactly 1 replica (replication=1)
    for (Map.Entry<String, Map<String, String>> entry : assignment.entrySet()) {
      assertEquals(entry.getValue().size(), 1);
      assertTrue(entry.getValue().containsValue(SegmentStateModel.ONLINE));
    }
  }

  @Test
  public void testStaleTabletsAreRemoved() {
    IdealState idealState = createEmptyIdealState();
    // Pre-populate with some existing tablets
    idealState.getRecord().getMapFields().put("stale_tablet_1", createOnlineMap("Server_host1_8099"));
    idealState.getRecord().getMapFields().put("stale_tablet_2", createOnlineMap("Server_host2_8099"));
    idealState.getRecord().getMapFields().put("keep_tablet", createOnlineMap("Server_host3_8099"));

    // Only keep_tablet and new_tablet should remain
    List<TabletMetadataEnvelope> tablets = createTabletEnvelopes("keep_tablet", "new_tablet");

    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, 1);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    Map<String, Map<String, String>> assignment = result.getRecord().getMapFields();
    assertEquals(assignment.size(), 2);
    assertTrue(assignment.containsKey("keep_tablet"));
    assertTrue(assignment.containsKey("new_tablet"));
    assertFalse(assignment.containsKey("stale_tablet_1"));
    assertFalse(assignment.containsKey("stale_tablet_2"));
  }

  @Test
  public void testExistingTabletsArePreserved() {
    IdealState idealState = createEmptyIdealState();
    // Pre-populate with existing assignment for tablet_0 on a specific server
    Map<String, String> existingAssignment = createOnlineMap("Server_host2_8099");
    idealState.getRecord().getMapFields().put("tablet_0", existingAssignment);

    List<TabletMetadataEnvelope> tablets = createTabletEnvelopes("tablet_0", "tablet_1");

    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, 1);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    Map<String, Map<String, String>> assignment = result.getRecord().getMapFields();
    assertEquals(assignment.size(), 2);

    // tablet_0 should keep its original assignment (Server_host2_8099), not be reassigned
    Map<String, String> tablet0Assignment = assignment.get("tablet_0");
    assertNotNull(tablet0Assignment);
    assertTrue(tablet0Assignment.containsKey("Server_host2_8099"));
    assertEquals(tablet0Assignment.get("Server_host2_8099"), SegmentStateModel.ONLINE);
    assertEquals(tablet0Assignment.size(), 1, "Preserved tablet should have unchanged replica count");
  }

  @Test
  public void testReplicationIsRespected() {
    IdealState idealState = createEmptyIdealState();
    List<TabletMetadataEnvelope> tablets = createTabletEnvelopes("tablet_0");

    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, 2);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    Map<String, Map<String, String>> assignment = result.getRecord().getMapFields();
    assertEquals(assignment.size(), 1);

    // tablet_0 should be assigned to 2 servers
    Map<String, String> tablet0Assignment = assignment.get("tablet_0");
    assertNotNull(tablet0Assignment);
    assertEquals(tablet0Assignment.size(), 2);
    for (String state : tablet0Assignment.values()) {
      assertEquals(state, SegmentStateModel.ONLINE);
    }
  }

  @Test
  public void testReplicationCappedAtServerCount() {
    IdealState idealState = createEmptyIdealState();
    // Request replication=5 but only 3 servers available; effectiveReplication is computed
    // by the caller, so we pass min(5, 3) = 3
    List<TabletMetadataEnvelope> tablets = createTabletEnvelopes("tablet_0");

    int effectiveReplication = Math.min(5, _serverInstances.size());
    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, effectiveReplication);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    Map<String, String> tablet0Assignment = result.getRecord().getMapFields().get("tablet_0");
    assertNotNull(tablet0Assignment);
    // Should be capped at 3 (number of available servers)
    assertEquals(tablet0Assignment.size(), 3);
  }

  @Test
  public void testEmptyTabletListClearsAllPartitions() {
    IdealState idealState = createEmptyIdealState();
    // Pre-populate with existing tablets
    idealState.getRecord().getMapFields().put("tablet_0", createOnlineMap("Server_host1_8099"));
    idealState.getRecord().getMapFields().put("tablet_1", createOnlineMap("Server_host2_8099"));

    List<TabletMetadataEnvelope> emptyTablets = Collections.emptyList();

    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, emptyTablets, _serverInstances, 1);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    Map<String, Map<String, String>> assignment = result.getRecord().getMapFields();
    assertTrue(assignment.isEmpty());
    assertEquals(result.getNumPartitions(), 0);
  }

  @Test
  public void testNullIdealStateReturnsNull() {
    List<TabletMetadataEnvelope> tablets = createTabletEnvelopes("tablet_0");

    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, 1);
    IdealState result = updater.apply(null);

    assertNull(result);
  }

  @Test
  public void testRoundRobinDistribution() {
    IdealState idealState = createEmptyIdealState();
    // Create multiple tablets and verify they are distributed across servers
    List<TabletMetadataEnvelope> tablets =
        createTabletEnvelopes("tablet_0", "tablet_1", "tablet_2", "tablet_3", "tablet_4", "tablet_5");

    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, 1);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    Map<String, Map<String, String>> assignment = result.getRecord().getMapFields();
    assertEquals(assignment.size(), 6);

    // Count how many tablets each server is assigned
    Map<String, Integer> serverCounts = new HashMap<>();
    for (Map<String, String> instanceMap : assignment.values()) {
      for (String server : instanceMap.keySet()) {
        serverCounts.merge(server, 1, Integer::sum);
      }
    }

    // Each server should have 2 tablets (6 tablets / 3 servers = 2 each)
    assertEquals(serverCounts.get("Server_host1_8099").intValue(), 2);
    assertEquals(serverCounts.get("Server_host2_8099").intValue(), 2);
    assertEquals(serverCounts.get("Server_host3_8099").intValue(), 2);
  }

  @Test
  public void testBuildInstanceStateMap() {
    Map<String, String> stateMap =
        TabletAssignmentManager.buildInstanceStateMap(_serverInstances, 0, 2);

    assertEquals(stateMap.size(), 2);
    assertTrue(stateMap.containsKey("Server_host1_8099"));
    assertTrue(stateMap.containsKey("Server_host2_8099"));
    assertEquals(stateMap.get("Server_host1_8099"), SegmentStateModel.ONLINE);
    assertEquals(stateMap.get("Server_host2_8099"), SegmentStateModel.ONLINE);
  }

  @Test
  public void testBuildInstanceStateMapWrapsAround() {
    Map<String, String> stateMap =
        TabletAssignmentManager.buildInstanceStateMap(_serverInstances, 2, 2);

    assertEquals(stateMap.size(), 2);
    // Starting at index 2 -> Server_host3_8099, then wraps to Server_host1_8099
    assertTrue(stateMap.containsKey("Server_host3_8099"));
    assertTrue(stateMap.containsKey("Server_host1_8099"));
  }

  @Test
  public void testBuildInstanceStateMapCapsReplication() {
    // Replication=5 with only 3 servers should cap at 3
    Map<String, String> stateMap =
        TabletAssignmentManager.buildInstanceStateMap(_serverInstances, 0, 5);

    assertEquals(stateMap.size(), 3);
  }

  @Test
  public void testNumPartitionsUpdated() {
    IdealState idealState = createEmptyIdealState();
    List<TabletMetadataEnvelope> tablets = createTabletEnvelopes("tablet_0", "tablet_1", "tablet_2");

    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, 1);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    assertEquals(result.getNumPartitions(), 3);
  }

  @Test
  public void testMixedAddAndRemove() {
    IdealState idealState = createEmptyIdealState();
    // Pre-populate: tablet_A exists, tablet_B exists
    idealState.getRecord().getMapFields().put("tablet_A", createOnlineMap("Server_host1_8099"));
    idealState.getRecord().getMapFields().put("tablet_B", createOnlineMap("Server_host2_8099"));

    // New state: tablet_B stays, tablet_C is added, tablet_A is removed
    List<TabletMetadataEnvelope> tablets = createTabletEnvelopes("tablet_B", "tablet_C");

    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, 1);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    Map<String, Map<String, String>> assignment = result.getRecord().getMapFields();
    assertEquals(assignment.size(), 2);
    assertFalse(assignment.containsKey("tablet_A"), "tablet_A should have been removed");
    assertTrue(assignment.containsKey("tablet_B"), "tablet_B should have been preserved");
    assertTrue(assignment.containsKey("tablet_C"), "tablet_C should have been added");

    // tablet_B should retain its original server assignment
    assertEquals(assignment.get("tablet_B").size(), 1);
    assertTrue(assignment.get("tablet_B").containsKey("Server_host2_8099"));
  }

  @Test
  public void testReplicationWithMultipleNewTablets() {
    IdealState idealState = createEmptyIdealState();
    List<TabletMetadataEnvelope> tablets = createTabletEnvelopes("tablet_0", "tablet_1", "tablet_2");

    // Replication=2, 3 servers: each tablet gets 2 replicas
    Function<IdealState, IdealState> updater =
        TabletAssignmentManager.buildIdealStateUpdater(TABLE_NAME, tablets, _serverInstances, 2);
    IdealState result = updater.apply(idealState);

    assertNotNull(result);
    Map<String, Map<String, String>> assignment = result.getRecord().getMapFields();
    assertEquals(assignment.size(), 3);

    // Each tablet should have exactly 2 replicas
    for (Map.Entry<String, Map<String, String>> entry : assignment.entrySet()) {
      assertEquals(entry.getValue().size(), 2, "Tablet " + entry.getKey() + " should have 2 replicas");
    }

    // Count total assignments per server; with 3 tablets * 2 replicas = 6 total
    Map<String, Integer> serverCounts = new HashMap<>();
    for (Map<String, String> instanceMap : assignment.values()) {
      for (String server : instanceMap.keySet()) {
        serverCounts.merge(server, 1, Integer::sum);
      }
    }
    // With round-robin and replication=2, each server should get 2 assignments
    assertEquals(serverCounts.get("Server_host1_8099").intValue(), 2);
    assertEquals(serverCounts.get("Server_host2_8099").intValue(), 2);
    assertEquals(serverCounts.get("Server_host3_8099").intValue(), 2);
  }

  // --- Helper methods ---

  private IdealState createEmptyIdealState() {
    ZNRecord znRecord = new ZNRecord(TABLE_NAME);
    IdealState idealState = new IdealState(znRecord);
    idealState.setNumPartitions(0);
    return idealState;
  }

  private List<TabletMetadataEnvelope> createTabletEnvelopes(String... tabletIds) {
    List<TabletMetadataEnvelope> envelopes = new ArrayList<>();
    for (String tabletId : tabletIds) {
      envelopes.add(new TabletMetadataEnvelope(
          tabletId,
          TABLE_NAME,
          TabletMetadataEnvelope.CURRENT_VERSION,
          1001L,  // snapshotId
          0,      // specId
          null,   // partitionValues
          0L,     // minTimeMs
          0L,     // maxTimeMs
          1000L,  // rowCount
          10000L, // sizeBytes
          5,      // microsegmentCount
          "mem://" + TABLE_NAME + "/" + tabletId + "/manifest.json"
      ));
    }
    return envelopes;
  }

  private Map<String, String> createOnlineMap(String... instances) {
    Map<String, String> map = new HashMap<>();
    for (String instance : instances) {
      map.put(instance, SegmentStateModel.ONLINE);
    }
    return map;
  }
}
