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
package org.apache.pinot.core.data.manager.offline;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataUtils;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.lakehouse.TabletManifest;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * OFFLINE table-data-manager specialization for lakehouse tablet stubs.
 *
 * <p>This class keeps the Helix-visible routing key as {@code tabletId}, resolves the tablet envelope/manifest from
 * ZK plus the segment custom-map, and registers one multi-segment {@link SegmentDataManager} per tablet. The actual
 * parquet-backed execution path is a later phase; this slice only establishes the bounded load/reload seam.</p>
 */
@ThreadSafe
public class LakehouseOfflineTableDataManager extends OfflineTableDataManager {

  @Override
  protected void doAddOnlineSegment(String segmentName)
      throws Exception {
    SegmentZKMetadata zkMetadata = fetchZKMetadata(segmentName);
    if (!SegmentZKMetadataUtils.isLakehouseTabletSegment(zkMetadata)) {
      super.doAddOnlineSegment(segmentName);
      return;
    }
    addOrReplaceLakehouseTablet(zkMetadata);
  }

  @Override
  protected void doReplaceSegment(String segmentName)
      throws Exception {
    SegmentZKMetadata zkMetadata = fetchZKMetadataNullable(segmentName);
    if (!SegmentZKMetadataUtils.isLakehouseTabletSegment(zkMetadata)) {
      super.doReplaceSegment(segmentName);
      return;
    }
    Preconditions.checkState(zkMetadata != null, "Failed to find ZK metadata for lakehouse tablet: %s", segmentName);
    addOrReplaceLakehouseTablet(zkMetadata);
  }

  @Override
  public void reloadSegment(String segmentName, boolean forceDownload, String reloadJobId)
      throws Exception {
    SegmentZKMetadata zkMetadata = fetchZKMetadataNullable(segmentName);
    if (!SegmentZKMetadataUtils.isLakehouseTabletSegment(zkMetadata)) {
      super.reloadSegment(segmentName, forceDownload, reloadJobId);
      return;
    }

    Preconditions.checkState(!_shutDown,
        "Table data manager is already shut down, cannot reload lakehouse tablet: %s in table: %s", segmentName,
        _tableNameWithType);
    _logger.info("Reloading lakehouse tablet: {} with forceDownload: {}", segmentName, forceDownload);
    Lock segmentLock = getSegmentLock(segmentName);
    segmentLock.lock();
    try {
      addOrReplaceLakehouseTablet(Preconditions.checkNotNull(zkMetadata,
          "Failed to find ZK metadata for lakehouse tablet: %s of table: %s", segmentName, _tableNameWithType));
    } catch (Exception e) {
      addSegmentError(segmentName,
          new SegmentErrorInfo(System.currentTimeMillis(), "Caught exception while reloading lakehouse tablet", e));
      throw e;
    } finally {
      segmentLock.unlock();
    }
  }

  private void addOrReplaceLakehouseTablet(SegmentZKMetadata zkMetadata)
      throws Exception {
    String tabletId = zkMetadata.getSegmentName();
    SegmentDataManager existingSegmentDataManager = _segmentDataManagerMap.get(tabletId);
    if (existingSegmentDataManager != null && hasSameLakehouseVersion(existingSegmentDataManager, zkMetadata)) {
      _logger.info("Lakehouse tablet: {} already loaded with manifest version: {}, skipping replace", tabletId,
          zkMetadata.getCrc());
      return;
    }

    LakehouseTabletSegmentDataManager newSegmentDataManager = loadLakehouseTabletSegment(zkMetadata);
    _logger.info("Adding lakehouse tablet: {} with manifest uri: {}", tabletId,
        SegmentZKMetadataUtils.getLakehouseManifestUri(zkMetadata));
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        newSegmentDataManager.getSegment().getSegmentMetadata().getTotalDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);

    SegmentDataManager oldSegmentDataManager = registerSegment(tabletId, newSegmentDataManager);
    if (oldSegmentDataManager == null) {
      _logger.info("Added new lakehouse tablet: {}", tabletId);
      return;
    }

    _logger.info("Replaced lakehouse tablet: {}", tabletId);
    oldSegmentDataManager.offload();
    releaseSegment(oldSegmentDataManager);
  }

  private boolean hasSameLakehouseVersion(SegmentDataManager existingSegmentDataManager, SegmentZKMetadata zkMetadata) {
    return Long.toString(zkMetadata.getCrc()).equals(existingSegmentDataManager.getSegment().getSegmentMetadata()
        .getCrc());
  }

  private LakehouseTabletSegmentDataManager loadLakehouseTabletSegment(SegmentZKMetadata zkMetadata)
      throws Exception {
    TabletMetadataEnvelope tabletMetadataEnvelope = resolveTabletMetadataEnvelope(zkMetadata);
    String manifestUri = tabletMetadataEnvelope.getManifestUri();
    Preconditions.checkState(manifestUri != null, "Missing manifest URI for lakehouse tablet: %s in table: %s",
        zkMetadata.getSegmentName(), _tableNameWithType);
    TabletManifest tabletManifest = fetchTabletManifest(manifestUri);
    TableConfig tableConfig = getCachedTableConfigAndSchema().getLeft();
    Schema schema = getCachedTableConfigAndSchema().getRight();
    Preconditions.checkState(schema != null, "Missing schema while loading lakehouse tablet: %s for table: %s",
        zkMetadata.getSegmentName(), _tableNameWithType);
    return LakehouseTabletSegmentDataManager.create(_tableNameWithType, tableConfig, schema, zkMetadata,
        tabletMetadataEnvelope, tabletManifest);
  }

  protected TabletManifest fetchTabletManifest(String manifestUri)
      throws IOException {
    return JsonUtils.fileToObject(Path.of(URI.create(manifestUri)).toFile(), TabletManifest.class);
  }

  private TabletMetadataEnvelope resolveTabletMetadataEnvelope(SegmentZKMetadata zkMetadata) {
    String tabletId = zkMetadata.getSegmentName();
    TabletMetadataEnvelope tabletMetadataEnvelope =
        ZKMetadataProvider.getTabletMetadataEnvelope(_propertyStore, _tableNameWithType, tabletId);
    if (tabletMetadataEnvelope != null) {
      return tabletMetadataEnvelope;
    }
    return buildEnvelopeFromCustomMap(zkMetadata);
  }

  private TabletMetadataEnvelope buildEnvelopeFromCustomMap(SegmentZKMetadata zkMetadata) {
    Map<String, String> customMap = zkMetadata.getCustomMap();
    Preconditions.checkState(customMap != null, "Missing lakehouse custom map for segment: %s of table: %s",
        zkMetadata.getSegmentName(), _tableNameWithType);

    TabletMetadataEnvelope tabletMetadataEnvelope = new TabletMetadataEnvelope();
    tabletMetadataEnvelope.setTableNameWithType(_tableNameWithType);
    tabletMetadataEnvelope.setTabletId(
        customMap.getOrDefault(CommonConstants.Segment.Lakehouse.TABLET_ID, zkMetadata.getSegmentName()));
    tabletMetadataEnvelope.setManifestUri(customMap.get(CommonConstants.Segment.Lakehouse.MANIFEST_URI));
    tabletMetadataEnvelope.setManifestVersion(parseLong(
        customMap.get(CommonConstants.Segment.Lakehouse.MANIFEST_VERSION), zkMetadata.getCrc()));
    tabletMetadataEnvelope.setSnapshotId(
        parseLong(customMap.get(CommonConstants.Segment.Lakehouse.SNAPSHOT_ID), zkMetadata.getCrc()));
    tabletMetadataEnvelope.setSpecId(parseInt(customMap.get(CommonConstants.Segment.Lakehouse.SPEC_ID), 0));
    if (zkMetadata.getStartTimeMs() >= 0) {
      tabletMetadataEnvelope.setMinTimeMillis(zkMetadata.getStartTimeMs());
    }
    if (zkMetadata.getEndTimeMs() >= 0) {
      tabletMetadataEnvelope.setMaxTimeMillis(zkMetadata.getEndTimeMs());
    }
    if (zkMetadata.getTotalDocs() >= 0) {
      tabletMetadataEnvelope.setApproximateRowCount(zkMetadata.getTotalDocs());
    }
    if (zkMetadata.getSizeInBytes() >= 0) {
      tabletMetadataEnvelope.setApproximateSizeBytes(zkMetadata.getSizeInBytes());
    }
    return tabletMetadataEnvelope;
  }

  private static long parseLong(@Nullable String value, long defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return Long.parseLong(value);
  }

  private static int parseInt(@Nullable String value, int defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }
}
