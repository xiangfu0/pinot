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

import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task that refreshes lakehouse tablet metadata for all lakehouse-enabled tables.
 *
 * <p>This task runs on the lead controller for each table. On each execution it iterates
 * over all tables, checks if lakehouse mode is enabled, and delegates to
 * {@link LakehouseTableManager#refreshTableTablets(String, TableConfig)} to detect
 * snapshot changes and rebuild tablet metadata as needed.</p>
 *
 * <p>The default run frequency is 60 seconds. The task is registered with the controller's
 * periodic task scheduler and follows standard {@link ControllerPeriodicTask} leader election
 * semantics.</p>
 *
 * <p>This class is thread-safe (all mutable state is in the delegate {@link LakehouseTableManager}).</p>
 */
public class TabletRefreshTask extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TabletRefreshTask.class);

  public static final String TASK_NAME = "TabletRefreshTask";
  public static final long DEFAULT_RUN_FREQUENCY_SECONDS = 60L;
  public static final long DEFAULT_INITIAL_DELAY_SECONDS = 120L;

  private final LakehouseTableManager _lakehouseTableManager;

  /**
   * Creates a new TabletRefreshTask.
   *
   * @param lakehouseTableManager the lakehouse table manager for refreshing tablets
   * @param pinotHelixResourceManager Helix resource manager for table discovery
   * @param leadControllerManager leader election manager
   * @param controllerMetrics controller metrics
   * @param runFrequencyInSeconds how often to run (seconds)
   * @param initialDelayInSeconds initial delay before first run (seconds)
   */
  public TabletRefreshTask(LakehouseTableManager lakehouseTableManager,
      PinotHelixResourceManager pinotHelixResourceManager, LeadControllerManager leadControllerManager,
      ControllerMetrics controllerMetrics, long runFrequencyInSeconds, long initialDelayInSeconds) {
    super(TASK_NAME, runFrequencyInSeconds, initialDelayInSeconds, pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _lakehouseTableManager = lakehouseTableManager;
  }

  /**
   * Creates a new TabletRefreshTask with default run frequency and initial delay.
   *
   * @param lakehouseTableManager the lakehouse table manager for refreshing tablets
   * @param pinotHelixResourceManager Helix resource manager for table discovery
   * @param leadControllerManager leader election manager
   * @param controllerMetrics controller metrics
   */
  public TabletRefreshTask(LakehouseTableManager lakehouseTableManager,
      PinotHelixResourceManager pinotHelixResourceManager, LeadControllerManager leadControllerManager,
      ControllerMetrics controllerMetrics) {
    this(lakehouseTableManager, pinotHelixResourceManager, leadControllerManager, controllerMetrics,
        DEFAULT_RUN_FREQUENCY_SECONDS, DEFAULT_INITIAL_DELAY_SECONDS);
  }

  @Override
  protected void processTable(String tableNameWithType) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.warn("Table config not found for table: {}, skipping tablet refresh", tableNameWithType);
      return;
    }

    if (!tableConfig.isLakehouseEnabled()) {
      return;
    }

    LOGGER.debug("Refreshing tablets for lakehouse table: {}", tableNameWithType);
    List<TabletMetadataEnvelope> envelopes =
        _lakehouseTableManager.refreshTableTablets(tableNameWithType, tableConfig);
    LOGGER.info("Table: {} has {} tablets after refresh", tableNameWithType, envelopes.size());

    // Update Helix IdealState so servers and brokers can discover tablet assignments
    if (!envelopes.isEmpty()) {
      try {
        HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();
        TenantConfig tenantConfig = tableConfig.getTenantConfig();
        String serverTag = TagNameUtils.extractOfflineServerTag(tenantConfig);
        List<String> serverInstances = HelixHelper.getEnabledInstancesWithTag(helixManager, serverTag);
        int replication = tableConfig.getReplication();
        TabletAssignmentManager.updateTabletIdealState(helixManager, tableNameWithType, envelopes, serverInstances,
            replication);
      } catch (Exception e) {
        LOGGER.error("Failed to update Helix IdealState for lakehouse table: {}", tableNameWithType, e);
      }
    }
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    for (String tableNameWithType : tableNamesWithType) {
      _lakehouseTableManager.removeTable(tableNameWithType);
      LOGGER.info("Cleaned up lakehouse state for table: {} (no longer leader)", tableNameWithType);
    }
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Cleaning up TabletRefreshTask");
    try {
      _lakehouseTableManager.close();
    } catch (Exception e) {
      LOGGER.error("Error closing LakehouseTableManager during task cleanup", e);
    }
  }
}
