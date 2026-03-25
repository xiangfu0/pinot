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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataUtils;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.api.resources.TableConfigValidationUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.lakehouse.LakehouseCatalogAdapter;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotDescriptor;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotRequest;
import org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor;
import org.apache.pinot.spi.lakehouse.TabletManifest;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;


/**
 * Controller-side service for lakehouse snapshot resolution and tablet publication.
 *
 * <p>Thread-safety relies on Pinot's controller leadership discipline. The service does not attempt a full atomic
 * pointer swap in Phase 1; it persists manifests first, publishes new envelopes second, and removes stale envelopes
 * last.</p>
 */
public class PinotLakehouseTableService {
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final LakehouseCatalogAdapterProvider _catalogAdapterProvider;
  private final LakehouseTabletPlanner _tabletPlanner;
  private final LocalFileSystemLakehouseManifestStore _manifestStore;

  public PinotLakehouseTableService(PinotHelixResourceManager pinotHelixResourceManager,
      ControllerMetrics controllerMetrics, LakehouseCatalogAdapterProvider catalogAdapterProvider,
      LakehouseTabletPlanner tabletPlanner, LocalFileSystemLakehouseManifestStore manifestStore) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerMetrics = controllerMetrics;
    _catalogAdapterProvider = catalogAdapterProvider;
    _tabletPlanner = tabletPlanner;
    _manifestStore = manifestStore;
  }

  public LakehouseSnapshotDescriptor resolveSnapshot(String tableNameWithType, LakehouseSnapshotRequest snapshotRequest)
      throws IOException {
    TableConfig tableConfig = getValidatedLakehouseTableConfig(tableNameWithType);
    LakehouseCatalogAdapter catalogAdapter = _catalogAdapterProvider.getAdapter(tableConfig.getLakehouseConfig());
    return catalogAdapter.resolveSnapshot(tableConfig.getLakehouseConfig(), snapshotRequest);
  }

  public LakehouseTableRefreshResponse refreshTable(String tableNameWithType, LakehouseSnapshotRequest snapshotRequest)
      throws IOException {
    TableConfig tableConfig = getValidatedLakehouseTableConfig(tableNameWithType);
    LakehouseConfig lakehouseConfig = tableConfig.getLakehouseConfig();
    LakehouseCatalogAdapter catalogAdapter = _catalogAdapterProvider.getAdapter(lakehouseConfig);
    try {
      LakehouseSnapshotDescriptor snapshotDescriptor = catalogAdapter.resolveSnapshot(lakehouseConfig, snapshotRequest);
      List<MicrosegmentDescriptor> microsegments =
          catalogAdapter.listMicrosegments(lakehouseConfig, snapshotDescriptor);
      List<TabletManifest> tabletManifests =
          _tabletPlanner.planTabletManifests(tableNameWithType, lakehouseConfig, snapshotDescriptor, microsegments);

      List<String> plannedTabletIds = new ArrayList<>(tabletManifests.size());
      Map<String, SegmentZKMetadata> segmentMetadataByTabletId = new LinkedHashMap<>();
      Set<String> plannedTabletIdSet = new HashSet<>();
      for (TabletManifest tabletManifest : tabletManifests) {
        plannedTabletIds.add(tabletManifest.getTabletId());
        plannedTabletIdSet.add(tabletManifest.getTabletId());
        String manifestUri = _manifestStore.persistTabletManifest(tabletManifest);
        TabletMetadataEnvelope envelope = _tabletPlanner.buildEnvelope(tabletManifest, manifestUri, lakehouseConfig);
        SegmentZKMetadata segmentZKMetadata = buildTabletSegmentMetadata(envelope);
        upsertTabletSegmentMetadata(tableNameWithType, segmentZKMetadata);
        segmentMetadataByTabletId.put(tabletManifest.getTabletId(), segmentZKMetadata);
        publishTabletEnvelope(envelope);
        _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.LAKEHOUSE_MANIFEST_PUBLISH_SUCCESS,
            1L);
      }

      _pinotHelixResourceManager.assignSegments(tableConfig, segmentMetadataByTabletId);
      List<String> staleTabletIds = removeStaleTablets(tableNameWithType, plannedTabletIdSet);
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.LAKEHOUSE_TABLET_COUNT,
          tabletManifests.size());
      _controllerMetrics.setValueOfTableGauge(tableNameWithType, ControllerGauge.LAKEHOUSE_STALE_MANIFEST_COUNT, 0L);
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.LAKEHOUSE_SNAPSHOT_REFRESH_SUCCESS,
          1L);

      LakehouseTableRefreshResponse response = new LakehouseTableRefreshResponse();
      response.setTableNameWithType(tableNameWithType);
      response.setSnapshotDescriptor(snapshotDescriptor);
      response.setTabletCount(tabletManifests.size());
      response.setTabletIds(new ArrayList<>(plannedTabletIds));
      response.setRemovedTabletIds(staleTabletIds);
      return response;
    } catch (Exception e) {
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.LAKEHOUSE_SNAPSHOT_REFRESH_FAILURE,
          1L);
      _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.LAKEHOUSE_MANIFEST_PUBLISH_FAILURE,
          1L);
      if (e instanceof IllegalStateException) {
        throw (IllegalStateException) e;
      }
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("Failed to refresh lakehouse tablets for table: " + tableNameWithType, e);
    }
  }

  private void publishTabletEnvelope(TabletMetadataEnvelope envelope)
      throws IOException {
    boolean created =
        ZKMetadataProvider.createTabletMetadataEnvelope(_pinotHelixResourceManager.getPropertyStore(), envelope);
    if (!created && !ZKMetadataProvider.setTabletMetadataEnvelope(_pinotHelixResourceManager.getPropertyStore(),
        envelope)) {
      throw new IOException("Failed to publish lakehouse tablet envelope: " + envelope.getTabletId());
    }
  }

  private void upsertTabletSegmentMetadata(String tableNameWithType, SegmentZKMetadata segmentZKMetadata)
      throws IOException {
    boolean created = _pinotHelixResourceManager.createSegmentZkMetadata(tableNameWithType, segmentZKMetadata);
    if (!created && !_pinotHelixResourceManager.updateZkMetadata(tableNameWithType, segmentZKMetadata)) {
      throw new IOException(
          "Failed to publish lakehouse tablet segment metadata: " + segmentZKMetadata.getSegmentName());
    }
  }

  private List<String> removeStaleTablets(String tableNameWithType, Set<String> plannedTabletIds)
      throws IOException {
    List<String> existingTabletIds = ZKMetadataProvider.getTabletMetadataEnvelopeIds(
        _pinotHelixResourceManager.getPropertyStore(), tableNameWithType);
    List<String> removedTabletIds = new ArrayList<>();
    for (String existingTabletId : existingTabletIds) {
      if (!plannedTabletIds.contains(existingTabletId)) {
        if (!ZKMetadataProvider.removeTabletMetadataEnvelope(_pinotHelixResourceManager.getPropertyStore(),
            tableNameWithType, existingTabletId)) {
          throw new IOException("Failed to remove stale lakehouse tablet envelope: " + existingTabletId);
        }
        if (!_pinotHelixResourceManager.removeSegmentZKMetadata(tableNameWithType, existingTabletId)) {
          throw new IOException("Failed to remove stale lakehouse tablet segment metadata: " + existingTabletId);
        }
        removedTabletIds.add(existingTabletId);
      }
    }
    removedTabletIds.sort(String::compareTo);
    if (!removedTabletIds.isEmpty()) {
      _pinotHelixResourceManager.removeSegmentsFromIdealState(tableNameWithType, removedTabletIds);
    }
    return removedTabletIds;
  }

  private static SegmentZKMetadata buildTabletSegmentMetadata(TabletMetadataEnvelope tabletMetadataEnvelope) {
    return SegmentZKMetadataUtils.createLakehouseTabletSegmentZKMetadata(tabletMetadataEnvelope);
  }

  private TableConfig getValidatedLakehouseTableConfig(String tableNameWithType) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      throw new IllegalStateException("Table not found: " + tableNameWithType);
    }
    TableConfigValidationUtils.validateLakehouseNativeConfig(tableConfig);
    return tableConfig;
  }
}
