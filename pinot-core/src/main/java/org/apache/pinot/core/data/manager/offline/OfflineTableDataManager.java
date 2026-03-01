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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.core.data.manager.DuoSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Table data manager for OFFLINE table.
 */
@ThreadSafe
public class OfflineTableDataManager extends BaseTableDataManager {

  private TableUpsertMetadataManager _tableUpsertMetadataManager;

  @Override
  protected void doInit() {
    Pair<TableConfig, Schema> tableConfigAndSchema = getCachedTableConfigAndSchema();
    TableConfig tableConfig = tableConfigAndSchema.getLeft();
    Schema schema = tableConfigAndSchema.getRight();
    if (tableConfig.isUpsertEnabled()) {
      _tableUpsertMetadataManager =
          TableUpsertMetadataManagerFactory.create(_instanceDataManagerConfig.getUpsertConfig(), tableConfig, schema,
              this, _segmentOperationsThrottler);
    }
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doShutdown() {
    if (_tableUpsertMetadataManager != null) {
      _tableUpsertMetadataManager.stop();
      releaseAndRemoveAllSegments();
      try {
        _tableUpsertMetadataManager.close();
      } catch (IOException e) {
        _logger.warn("Caught exception while closing upsert metadata manager", e);
      }
    } else {
      releaseAndRemoveAllSegments();
    }
  }

  protected void doAddOnlineSegment(String segmentName)
      throws Exception {
    SegmentZKMetadata zkMetadata = fetchZKMetadata(segmentName);
    IndexLoadingConfig indexLoadingConfig = fetchIndexLoadingConfig();
    indexLoadingConfig.setSegmentTier(zkMetadata.getTier());
    SegmentDataManager segmentDataManager = _segmentDataManagerMap.get(segmentName);
    if (segmentDataManager == null) {
      addNewOnlineSegment(zkMetadata, indexLoadingConfig);
    } else {
      replaceSegmentIfCrcMismatch(segmentDataManager, zkMetadata, indexLoadingConfig);
    }
  }

  @Override
  public void addSegment(ImmutableSegment immutableSegment, @Nullable SegmentZKMetadata zkMetadata) {
    String segmentName = immutableSegment.getSegmentName();
    Preconditions.checkState(!_shutDown,
        "Table data manager is already shut down, cannot add segment: %s to table: %s",
        segmentName, _tableNameWithType);
    if (isUpsertEnabled()) {
      handleUpsert(immutableSegment, zkMetadata);
      return;
    }
    super.addSegment(immutableSegment, zkMetadata);
  }

  @Override
  public List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments,
      Map<String, String> queryOptions) {
    List<SegmentContext> segmentContexts = new ArrayList<>(selectedSegments.size());
    selectedSegments.forEach(s -> segmentContexts.add(new SegmentContext(s)));
    if (isUpsertEnabled() && !QueryOptionsUtils.isSkipUpsert(queryOptions)) {
      _tableUpsertMetadataManager.setSegmentContexts(segmentContexts, queryOptions);
    }
    return segmentContexts;
  }

  @Override
  public void addConsumingSegment(String segmentName) {
    throw new UnsupportedOperationException("Cannot add CONSUMING segment to OFFLINE table");
  }

  public boolean isUpsertEnabled() {
    return _tableUpsertMetadataManager != null;
  }

  @VisibleForTesting
  public TableUpsertMetadataManager getTableUpsertMetadataManager() {
    return _tableUpsertMetadataManager;
  }

  public Map<Integer, Long> getPartitionToPrimaryKeyCount() {
    if (isUpsertEnabled()) {
      return _tableUpsertMetadataManager.getPartitionToPrimaryKeyCount();
    }
    return Collections.emptyMap();
  }

  private void handleUpsert(ImmutableSegment immutableSegment, @Nullable SegmentZKMetadata zkMetadata) {
    String segmentName = immutableSegment.getSegmentName();
    _logger.info("Adding immutable segment: {} with upsert enabled", segmentName);

    // Set the ZK creation time so that same creation time can be used to break the comparison ties across replicas,
    // to ensure data consistency of replica.
    setZkCreationTimeIfAvailable(immutableSegment, zkMetadata);

    Integer partitionId = SegmentUtils.getSegmentPartitionId(segmentName, _tableNameWithType, _helixManager, null);
    Preconditions.checkNotNull(partitionId, "Failed to get partition id for segment: " + segmentName
        + " (upsert-enabled table: " + _tableNameWithType + ")");
    PartitionUpsertMetadataManager partitionUpsertMetadataManager =
        _tableUpsertMetadataManager.getOrCreatePartitionManager(partitionId);

    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        immutableSegment.getSegmentMetadata().getTotalDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);
    ImmutableSegmentDataManager newSegmentManager = new ImmutableSegmentDataManager(immutableSegment);
    SegmentDataManager oldSegmentManager = _segmentDataManagerMap.get(segmentName);
    if (oldSegmentManager == null) {
      // When adding a new segment, we should register it 'before' it is fully initialized by
      // partitionUpsertMetadataManager. Because when processing docs in the new segment, the docs in the other
      // segments may be invalidated, making the queries see less valid docs than expected. We should let query
      // access the new segment asap even though its validDocId bitmap is still being filled by
      // partitionUpsertMetadataManager.
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
      partitionUpsertMetadataManager.trackNewlyAddedSegment(segmentName);
      partitionUpsertMetadataManager.addSegment(immutableSegment);
      _logger.info("Added new immutable segment: {} with upsert enabled", segmentName);
    } else {
      replaceUpsertSegment(segmentName, oldSegmentManager, newSegmentManager, partitionUpsertMetadataManager);
    }
  }

  private void replaceUpsertSegment(String segmentName, SegmentDataManager oldSegmentManager,
      ImmutableSegmentDataManager newSegmentManager, PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    IndexSegment oldSegment = oldSegmentManager.getSegment();
    ImmutableSegment immutableSegment = newSegmentManager.getSegment();
    UpsertConfig.ConsistencyMode consistencyMode = _tableUpsertMetadataManager.getContext().getConsistencyMode();
    if (consistencyMode == UpsertConfig.ConsistencyMode.NONE) {
      partitionUpsertMetadataManager.replaceSegment(immutableSegment, oldSegment);
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
    } else {
      SegmentDataManager duoSegmentDataManager = new DuoSegmentDataManager(newSegmentManager, oldSegmentManager);
      registerSegment(segmentName, duoSegmentDataManager, partitionUpsertMetadataManager);
      partitionUpsertMetadataManager.replaceSegment(immutableSegment, oldSegment);
      registerSegment(segmentName, newSegmentManager, partitionUpsertMetadataManager);
    }
    _logger.info("Replaced {} segment: {} with upsert enabled and consistency mode: {}",
        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, consistencyMode);
    oldSegmentManager.offload();
    releaseSegment(oldSegmentManager);
  }

  private void registerSegment(String segmentName, SegmentDataManager segmentDataManager,
      @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager) {
    if (partitionUpsertMetadataManager != null) {
      partitionUpsertMetadataManager.trackSegmentForUpsertView(segmentDataManager.getSegment());
    }
    registerSegment(segmentName, segmentDataManager);
  }

  private void setZkCreationTimeIfAvailable(ImmutableSegment segment, @Nullable SegmentZKMetadata zkMetadata) {
    if (zkMetadata != null && zkMetadata.getCreationTime() > 0) {
      SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
      if (segmentMetadata instanceof SegmentMetadataImpl) {
        SegmentMetadataImpl segmentMetadataImpl = (SegmentMetadataImpl) segmentMetadata;
        segmentMetadataImpl.setZkCreationTime(zkMetadata.getCreationTime());
        _logger.info("Set ZK creation time {} for segment: {} in upsert table", zkMetadata.getCreationTime(),
            zkMetadata.getSegmentName());
      }
    }
  }
}
