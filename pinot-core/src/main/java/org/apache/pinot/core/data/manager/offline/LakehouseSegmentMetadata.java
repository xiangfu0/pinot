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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.joda.time.Duration;
import org.joda.time.Interval;


/**
 * Minimal in-memory metadata for lakehouse tablet placeholders.
 *
 * <p>This metadata is intentionally lightweight. It provides enough segment-level state for table-data-manager
 * accounting and query-time flattening before the Parquet-native execution path is added.</p>
 */
class LakehouseSegmentMetadata implements SegmentMetadata {
  private final String _rawTableName;
  private final String _segmentName;
  @Nullable
  private final String _timeColumn;
  private final long _startTime;
  private final long _endTime;
  @Nullable
  private final TimeUnit _timeUnit;
  @Nullable
  private final Duration _timeGranularity;
  @Nullable
  private final Interval _timeInterval;
  private final String _crc;
  private final String _dataCrc;
  private final Schema _schema;
  private final int _totalDocs;
  private final File _indexDir;
  private final long _creationTime;
  private final Map<String, String> _customMap;
  private final TreeMap<String, ColumnMetadata> _columnMetadataMap = new TreeMap<>();

  LakehouseSegmentMetadata(String rawTableName, String segmentName, @Nullable String timeColumn, long startTime,
      long endTime, @Nullable TimeUnit timeUnit, String crc, String dataCrc, Schema schema, int totalDocs,
      long creationTime, Map<String, String> customMap) {
    _rawTableName = rawTableName;
    _segmentName = segmentName;
    _timeColumn = timeColumn;
    _startTime = startTime;
    _endTime = endTime;
    _timeUnit = timeUnit;
    _timeGranularity = timeUnit != null ? new Duration(timeUnit.toMillis(1)) : null;
    _timeInterval = timeUnit != null && startTime >= 0 && endTime >= 0
        ? new Interval(timeUnit.toMillis(startTime), timeUnit.toMillis(endTime))
        : null;
    _crc = crc;
    _dataCrc = dataCrc;
    _schema = schema;
    _totalDocs = totalDocs;
    _indexDir = new File(System.getProperty("java.io.tmpdir"), segmentName);
    _creationTime = creationTime;
    _customMap = Collections.unmodifiableMap(new TreeMap<>(customMap));
  }

  @Deprecated
  @Override
  public String getTableName() {
    return _rawTableName;
  }

  @Override
  public String getName() {
    return _segmentName;
  }

  @Nullable
  @Override
  public String getTimeColumn() {
    return _timeColumn;
  }

  @Override
  public long getStartTime() {
    return _startTime;
  }

  @Override
  public long getEndTime() {
    return _endTime;
  }

  @Nullable
  @Override
  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  @Nullable
  @Override
  public Duration getTimeGranularity() {
    return _timeGranularity;
  }

  @Nullable
  @Override
  public Interval getTimeInterval() {
    return _timeInterval;
  }

  @Override
  public String getCrc() {
    return _crc;
  }

  @Override
  public String getDataCrc() {
    return _dataCrc;
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
    return _totalDocs;
  }

  @Override
  public File getIndexDir() {
    return _indexDir;
  }

  @Nullable
  @Override
  public String getCreatorName() {
    return "LakehousePlaceholderImmutableSegment";
  }

  @Override
  public long getIndexCreationTime() {
    return _creationTime;
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
    return Long.MIN_VALUE;
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
    return _customMap;
  }

  @Nullable
  @Override
  public String getStartOffset() {
    return null;
  }

  @Nullable
  @Override
  public String getEndOffset() {
    return null;
  }

  @Override
  public NavigableSet<String> getAllColumns() {
    return new TreeSet<>(_schema.getColumnNames());
  }

  @Override
  public TreeMap<String, ColumnMetadata> getColumnMetadataMap() {
    return _columnMetadataMap;
  }

  @Override
  public void removeColumn(String column) {
    _columnMetadataMap.remove(column);
  }

  @Override
  public JsonNode toJson(@Nullable Set<String> columnFilter) {
    ObjectNode objectNode = JsonUtils.newObjectNode();
    objectNode.put("segmentName", _segmentName);
    objectNode.put("tableName", _rawTableName);
    objectNode.put("totalDocs", _totalDocs);
    objectNode.put("crc", _crc);
    return objectNode;
  }
}
