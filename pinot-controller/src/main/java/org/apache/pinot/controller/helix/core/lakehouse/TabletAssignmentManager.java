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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages Helix IdealState for lakehouse tablets.
 *
 * <p>For lakehouse-native tables, tablets (not individual files) are the Helix partitions.
 * This keeps ZooKeeper metadata bounded by tablet count rather than file count.
 * Each tablet is assigned to server instances according to the table's replication config.</p>
 *
 * <p>Assignment uses a simple round-robin strategy across the provided list of server instances.
 * Existing assignments are preserved when a tablet's partition already exists in the IdealState,
 * and stale partitions for tablets that no longer exist are removed.</p>
 *
 * <p>This class is stateless and all methods are static.</p>
 */
public class TabletAssignmentManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TabletAssignmentManager.class);

  private TabletAssignmentManager() {
  }

  /**
   * Updates the Helix IdealState for a lakehouse table to reflect the current set of tablets.
   *
   * <ul>
   *   <li>Adds new tablets as ONLINE partitions assigned to servers via round-robin</li>
   *   <li>Removes tablets that no longer exist in the current envelope list</li>
   *   <li>Preserves existing assignments where tablets have not changed</li>
   * </ul>
   *
   * @param helixManager the Helix manager for accessing and updating IdealState
   * @param tableNameWithType table name with type suffix (the Helix resource name)
   * @param tablets current tablet metadata envelopes
   * @param serverInstances list of enabled server instances available for assignment
   * @param replication number of replicas per tablet
   */
  public static void updateTabletIdealState(HelixManager helixManager, String tableNameWithType,
      List<TabletMetadataEnvelope> tablets, List<String> serverInstances, int replication) {
    if (serverInstances.isEmpty()) {
      LOGGER.warn("No server instances available for table: {}, skipping tablet assignment", tableNameWithType);
      return;
    }

    int effectiveReplication = Math.min(replication, serverInstances.size());
    Function<IdealState, IdealState> updater =
        buildIdealStateUpdater(tableNameWithType, tablets, serverInstances, effectiveReplication);

    HelixHelper.updateIdealState(helixManager, tableNameWithType, updater);

    LOGGER.info("Updated tablet IdealState for table: {} with {} tablets (replication={})", tableNameWithType,
        tablets.size(), effectiveReplication);
  }

  /**
   * Builds the IdealState updater function for a given set of tablets.
   *
   * <p>This method is extracted from {@link #updateTabletIdealState} so that the update logic
   * can be unit-tested without a live Helix cluster.</p>
   *
   * @param tableNameWithType table name with type suffix
   * @param tablets current tablet metadata envelopes
   * @param serverInstances list of enabled server instances available for assignment
   * @param effectiveReplication effective number of replicas (capped at server count)
   * @return function that transforms an IdealState to reflect the current tablet set
   */
  @VisibleForTesting
  static Function<IdealState, IdealState> buildIdealStateUpdater(String tableNameWithType,
      List<TabletMetadataEnvelope> tablets, List<String> serverInstances, int effectiveReplication) {
    // Build the set of current tablet IDs for fast lookup
    Set<String> currentTabletIds = new HashSet<>(tablets.size());
    for (TabletMetadataEnvelope envelope : tablets) {
      currentTabletIds.add(envelope.getTabletId());
    }

    return idealState -> {
      if (idealState == null) {
        LOGGER.warn("IdealState not found for table: {}, cannot update tablet assignments", tableNameWithType);
        return null;
      }

      Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();

      // Step 1: Remove stale tablet partitions that no longer exist
      Set<String> existingPartitions = new HashSet<>(currentAssignment.keySet());
      int removedCount = 0;
      for (String partition : existingPartitions) {
        if (!currentTabletIds.contains(partition)) {
          currentAssignment.remove(partition);
          removedCount++;
        }
      }
      if (removedCount > 0) {
        LOGGER.info("Removed {} stale tablet partitions from IdealState for table: {}", removedCount,
            tableNameWithType);
      }

      // Step 2: Add new tablet partitions using round-robin assignment
      // Use a counter based on the current number of assignments to distribute tablets evenly
      int serverIndex = currentAssignment.size() % serverInstances.size();
      int addedCount = 0;
      for (TabletMetadataEnvelope envelope : tablets) {
        String tabletId = envelope.getTabletId();
        if (!currentAssignment.containsKey(tabletId)) {
          Map<String, String> instanceStateMap = buildInstanceStateMap(serverInstances, serverIndex,
              effectiveReplication);
          currentAssignment.put(tabletId, instanceStateMap);
          serverIndex = (serverIndex + effectiveReplication) % serverInstances.size();
          addedCount++;
        }
      }

      if (addedCount > 0) {
        LOGGER.info("Added {} new tablet partitions to IdealState for table: {}", addedCount, tableNameWithType);
      }

      // Update the number of partitions
      idealState.setNumPartitions(currentAssignment.size());

      return idealState;
    };
  }

  /**
   * Builds a round-robin instance-to-state map for a single tablet partition.
   *
   * @param serverInstances available server instances
   * @param startIndex starting index for round-robin
   * @param replication number of replicas
   * @return map of instance name to ONLINE state
   */
  @VisibleForTesting
  static Map<String, String> buildInstanceStateMap(List<String> serverInstances, int startIndex, int replication) {
    int effectiveReplication = Math.min(replication, serverInstances.size());
    Map<String, String> instanceStateMap = new TreeMap<>();
    for (int r = 0; r < effectiveReplication; r++) {
      String instance = serverInstances.get((startIndex + r) % serverInstances.size());
      instanceStateMap.put(instance, SegmentStateModel.ONLINE);
    }
    return instanceStateMap;
  }
}
