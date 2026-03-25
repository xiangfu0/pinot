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
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.EmptyDataSource;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Metadata-only immutable segment used as a temporary tablet execution placeholder.
 *
 * <p>The real Phase 1 Parquet path will replace these placeholders with parquet-backed index segments. Until then,
 * the segment carries schema and coarse metadata so tablet loading and query-time flattening can be wired cleanly.</p>
 */
class LakehousePlaceholderImmutableSegment implements ImmutableSegment {
  private final LakehouseSegmentMetadata _segmentMetadata;
  private final long _segmentSizeBytes;

  LakehousePlaceholderImmutableSegment(LakehouseSegmentMetadata segmentMetadata, long segmentSizeBytes) {
    _segmentMetadata = segmentMetadata;
    _segmentSizeBytes = segmentSizeBytes;
  }

  @Override
  public String getSegmentName() {
    return _segmentMetadata.getName();
  }

  @Override
  public LakehouseSegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public Set<String> getColumnNames() {
    return _segmentMetadata.getSchema().getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    return _segmentMetadata.getSchema().getPhysicalColumnNames();
  }

  @Nullable
  @Override
  public DataSource getDataSourceNullable(String column) {
    Schema schema = _segmentMetadata.getSchema();
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    if (fieldSpec == null) {
      return null;
    }
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
    return new EmptyDataSource(columnMetadata != null ? columnMetadata.getFieldSpec() : fieldSpec);
  }

  @Override
  public DataSource getDataSource(String column, Schema schema) {
    DataSource dataSource = getDataSourceNullable(column);
    if (dataSource != null) {
      return dataSource;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    Preconditions.checkState(fieldSpec != null, "Failed to find column: %s in schema: %s", column,
        schema.getSchemaName());
    return new EmptyDataSource(fieldSpec);
  }

  @Nullable
  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  @Nullable
  @Override
  public TextIndexReader getMultiColumnTextIndex() {
    return null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getQueryableDocIds() {
    return null;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    throw new UnsupportedOperationException("Lakehouse placeholder segments do not support row reads yet");
  }

  @Override
  public Object getValue(int docId, String column) {
    throw new UnsupportedOperationException("Lakehouse placeholder segments do not support value reads yet");
  }

  @Override
  public void offload() {
  }

  @Override
  public void destroy() {
  }

  @Nullable
  @Override
  public <I extends IndexReader> I getIndex(String column, IndexType<?, I, ?> type) {
    return null;
  }

  @Nullable
  @Override
  public Dictionary getDictionary(String column) {
    return null;
  }

  @Nullable
  @Override
  public ForwardIndexReader getForwardIndex(String column) {
    return null;
  }

  @Nullable
  @Override
  public InvertedIndexReader getInvertedIndex(String column) {
    return null;
  }

  @Override
  public long getSegmentSizeBytes() {
    return _segmentSizeBytes;
  }

  @Nullable
  @Override
  public String getTier() {
    return null;
  }
}
