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
package org.apache.pinot.segment.local.indexsegment.lakehouse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.lakehouse.TabletManifest;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;


/**
 * Metadata for a tablet treated as a logical segment in the Pinot query execution path.
 *
 * <p>Wraps a {@link TabletManifest} and {@link TabletMetadataEnvelope} to provide
 * {@link SegmentMetadata}-compatible access. This allows the query engine to treat tablets
 * as ordinary segments for planning, pruning, and execution.</p>
 *
 * <p>Most index-specific metadata (star-tree, column metadata, etc.) returns empty/null defaults
 * for Phase 1. A full implementation will populate column metadata from Parquet schema
 * discovery.</p>
 *
 * <p>This class is thread-safe when the underlying manifest and envelope are immutable.</p>
 */
public class TabletSegmentMetadata implements SegmentMetadata {

  private final TabletMetadataEnvelope _envelope;
  private final TabletManifest _manifest;
  private final Schema _schema;
  private final String _rawTableName;

  /**
   * Creates tablet segment metadata from the envelope and manifest.
   *
   * @param envelope the ZK-stored tablet metadata envelope
   * @param manifest the full tablet manifest loaded from deep store
   * @param schema the table schema
   */
  public TabletSegmentMetadata(TabletMetadataEnvelope envelope, TabletManifest manifest, Schema schema) {
    _envelope = envelope;
    _manifest = manifest;
    _schema = schema;
    // Extract raw table name by removing the type suffix (e.g. "_OFFLINE" or "_REALTIME")
    String tableNameWithType = envelope.getTableNameWithType();
    int lastUnderscore = tableNameWithType.lastIndexOf('_');
    _rawTableName = lastUnderscore > 0 ? tableNameWithType.substring(0, lastUnderscore) : tableNameWithType;
  }

  @Deprecated
  @Override
  public String getTableName() {
    return _rawTableName;
  }

  @Override
  public String getName() {
    return _envelope.getTabletId();
  }

  @Override
  public String getTimeColumn() {
    // Check the legacy TimeFieldSpec first
    if (_schema.getTimeFieldSpec() != null) {
      return _schema.getTimeFieldSpec().getName();
    }
    // Fall back to the first DateTimeFieldSpec (primary time column)
    if (!_schema.getDateTimeFieldSpecs().isEmpty()) {
      return _schema.getDateTimeFieldSpecs().get(0).getName();
    }
    return null;
  }

  @Override
  public long getStartTime() {
    return _envelope.getMinTimeMs();
  }

  @Override
  public long getEndTime() {
    return _envelope.getMaxTimeMs();
  }

  @Override
  public TimeUnit getTimeUnit() {
    return TimeUnit.MILLISECONDS;
  }

  @Override
  public Duration getTimeGranularity() {
    return new Duration(1L);
  }

  @Nullable
  @Override
  public Interval getTimeInterval() {
    long startMs = _envelope.getMinTimeMs();
    long endMs = _envelope.getMaxTimeMs();
    if (startMs > 0 && endMs > 0 && endMs >= startMs) {
      return new Interval(startMs, endMs, DateTimeZone.UTC);
    }
    return null;
  }

  @Override
  public String getCrc() {
    // Use snapshot ID as a surrogate CRC for tablet identity
    return String.valueOf(_envelope.getSnapshotId());
  }

  @Override
  public String getDataCrc() {
    return String.valueOf(Long.MIN_VALUE);
  }

  @Override
  public SegmentVersion getVersion() {
    return SegmentVersion.v3;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public int getTotalDocs() {
    long totalRows = _manifest.getTotalRowCount();
    // Clamp to Integer.MAX_VALUE to match SegmentMetadata contract
    return (int) Math.min(totalRows, Integer.MAX_VALUE);
  }

  @Nullable
  @Override
  public File getIndexDir() {
    // Tablets are backed by remote files, not a local index directory
    return null;
  }

  @Nullable
  @Override
  public String getCreatorName() {
    return "lakehouse-tablet";
  }

  @Override
  public long getIndexCreationTime() {
    return _manifest.getCreatedAtMs();
  }

  @Override
  public long getLastIndexedTimestamp() {
    return Long.MIN_VALUE;
  }

  @Override
  public long getLatestIngestionTimestamp() {
    return Long.MIN_VALUE;
  }

  @Override
  public long getMinimumIngestionLagMs() {
    return Long.MAX_VALUE;
  }

  @Nullable
  @Override
  public List<StarTreeV2Metadata> getStarTreeV2MetadataList() {
    return null;
  }

  @Nullable
  @Override
  public MultiColumnTextMetadata getMultiColumnTextMetadata() {
    return null;
  }

  @Override
  public Map<String, String> getCustomMap() {
    return Collections.emptyMap();
  }

  @Override
  public String getStartOffset() {
    return null;
  }

  @Override
  public String getEndOffset() {
    return null;
  }

  @Override
  public TreeMap<String, ColumnMetadata> getColumnMetadataMap() {
    // Phase 1: no per-column metadata from Parquet files yet
    return new TreeMap<>();
  }

  @Override
  public void removeColumn(String column) {
    // No-op for tablet metadata; column metadata is not populated in Phase 1
  }

  @Override
  public JsonNode toJson(@Nullable Set<String> columnFilter) {
    ObjectNode node = JsonUtils.newObjectNode();
    node.put("segmentName", getName());
    node.put("tableName", _rawTableName);
    node.put("tabletId", _envelope.getTabletId());
    node.put("snapshotId", _envelope.getSnapshotId());
    node.put("totalDocs", getTotalDocs());
    node.put("startTimeMs", _envelope.getMinTimeMs());
    node.put("endTimeMs", _envelope.getMaxTimeMs());
    node.put("microsegmentCount", _envelope.getMicrosegmentCount());
    node.put("manifestUri", _envelope.getManifestPointerUri());
    node.put("creationTimeMs", _manifest.getCreatedAtMs());
    return node;
  }

  /**
   * Returns the underlying tablet manifest.
   */
  public TabletManifest getManifest() {
    return _manifest;
  }

  /**
   * Returns the underlying tablet metadata envelope.
   */
  public TabletMetadataEnvelope getEnvelope() {
    return _envelope;
  }

  @Override
  public String toString() {
    return toJson(null).toString();
  }
}
