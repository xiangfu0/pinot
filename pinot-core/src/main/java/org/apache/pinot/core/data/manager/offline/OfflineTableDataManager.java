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
import java.util.List;
import java.util.TreeMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.core.data.manager.BaseTableDataManager;
import org.apache.pinot.core.data.manager.TabletSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Table data manager for OFFLINE table.
 */
@ThreadSafe
public class OfflineTableDataManager extends BaseTableDataManager {

  @Override
  protected void doInit() {
    Pair<TableConfig, Schema> tableConfigAndSchema = getCachedTableConfigAndSchema();
    TableConfig tableConfig = tableConfigAndSchema.getLeft();
    Schema schema = tableConfigAndSchema.getRight();
    if (tableConfig.isUpsertEnabled()) {
      _tableUpsertMetadataManager =
          TableUpsertMetadataManagerFactory.create(_instanceDataManagerConfig.getUpsertConfig(), tableConfig, schema,
              this, _segmentOperationsThrottlerSet);
    }
  }

  @Override
  protected void doStart() {
  }

  @Override
  protected void doShutdown() {
    if (_tableUpsertMetadataManager != null) {
      _tableUpsertMetadataManager.stop();
    }
    releaseAndRemoveAllSegments();
    if (_tableUpsertMetadataManager != null) {
      try {
        _tableUpsertMetadataManager.close();
      } catch (IOException e) {
        _logger.warn("Caught exception while closing upsert metadata manager", e);
      }
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
  protected void addNewLakehouseTabletSegment(SegmentZKMetadata zkMetadata, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    Pair<TableConfig, Schema> tableConfigAndSchema = getCachedTableConfigAndSchema();
    Schema schema = Preconditions.checkNotNull(tableConfigAndSchema.getRight(),
        "Failed to find schema for tablet segment: %s in table: %s", zkMetadata.getSegmentName(), _tableNameWithType);
    String rawTableName = TableNameBuilder.extractRawTableName(_tableNameWithType);
    long creationTime = zkMetadata.getCreationTime() > 0 ? zkMetadata.getCreationTime() : System.currentTimeMillis();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(rawTableName, zkMetadata.getSegmentName(), schema,
        creationTime) {
      private final TreeMap<String, ColumnMetadata> _columnMetadataMap = createColumnMetadataMap(schema);

      @Override
      public TreeMap<String, ColumnMetadata> getColumnMetadataMap() {
        return _columnMetadataMap;
      }
    };
    EmptyIndexSegment tabletSegment = new EmptyIndexSegment(segmentMetadata);
    setZkOperationTimeIfAvailable(tabletSegment, zkMetadata);

    ImmutableSegmentDataManager childSegmentDataManager = new ImmutableSegmentDataManager(tabletSegment);
    TabletSegmentDataManager tabletSegmentDataManager =
        new TabletSegmentDataManager(zkMetadata.getSegmentName(), List.of(childSegmentDataManager));
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.DOCUMENT_COUNT,
        tabletSegment.getSegmentMetadata().getTotalDocs());
    _serverMetrics.addValueToTableGauge(_tableNameWithType, ServerGauge.SEGMENT_COUNT, 1L);

    SegmentDataManager oldSegmentManager = registerSegment(zkMetadata.getSegmentName(), tabletSegmentDataManager);
    if (oldSegmentManager != null) {
      oldSegmentManager.offload();
      releaseSegment(oldSegmentManager);
    }
    _logger.info("Added lakehouse tablet segment: {} as a tablet container with {} child segment(s)",
        zkMetadata.getSegmentName(), tabletSegmentDataManager.getSegments().size());
  }

  private static TreeMap<String, ColumnMetadata> createColumnMetadataMap(Schema schema) {
    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      columnMetadataMap.put(fieldSpec.getName(), new ColumnMetadataImpl.Builder().setFieldSpec(fieldSpec).build());
    }
    return columnMetadataMap;
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
  public void addConsumingSegment(String segmentName) {
    throw new UnsupportedOperationException("Cannot add CONSUMING segment to OFFLINE table");
  }
}
