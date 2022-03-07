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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.IndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.creator.TimestampIndexCreatorProvider;
import org.apache.pinot.segment.spi.index.creator.TimestampIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.TimestampIndexGranularity;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class TimestampIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestampIndexHandler.class);

  private final SegmentMetadata _segmentMetadata;
  private final Set<String> _columnsToAddIdx;
  private final Map<String, Set<TimestampIndexGranularity>> _timestampIndexConfigs;

  public TimestampIndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _timestampIndexConfigs = indexLoadingConfig.getTimestampIndexConfigs();
    _columnsToAddIdx = _timestampIndexConfigs.keySet();
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns =
        segmentReader.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.TIMESTAMP_INDEX);
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing timestamp index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (shouldCreateTimestampIndex(columnMetadata)) {
        LOGGER.info("Need to create new timestamp index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider)
      throws IOException {
    // Remove indices not set in table config any more
    String segmentName = _segmentMetadata.getName();
    Set<String> existingColumns =
        segmentWriter.toSegmentDirectory().getColumnsWithIndex(ColumnIndexType.TIMESTAMP_INDEX);
    for (String column : existingColumns) {
      if (!_columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing timestamp index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, ColumnIndexType.RANGE_INDEX);
        LOGGER.info("Removed existing timestamp index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : _columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (shouldCreateTimestampIndex(columnMetadata)) {
        createTimestampIndexForColumn(segmentWriter, columnMetadata, indexCreatorProvider);
      }
    }
  }

  private boolean shouldCreateTimestampIndex(ColumnMetadata columnMetadata) {
    return columnMetadata != null;
  }

  private void createTimestampIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      TimestampIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    String segmentName = _segmentMetadata.getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + ".timestamp.inprogress");
    File timestampIndexFile = new File(indexDir, columnName + V1Constants.Indexes.TIMESTAMP_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove range index if exists.
      // For v1 and v2, it's the actual range index. For v3, it's the temporary range index.
      FileUtils.deleteQuietly(timestampIndexFile);
    }

    // Create new timestamp index for the column.
    LOGGER.info("Creating new timestamp index for segment: {}, column: {}", segmentName, columnName);
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(segmentWriter, columnMetadata, indexCreatorProvider);
    } else {
      handleNonDictionaryBasedColumn(segmentWriter, columnMetadata, indexCreatorProvider);
    }

    // For v3, write the generated range index file into the single file and remove it.
    if (_segmentMetadata.getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, timestampIndexFile, ColumnIndexType.TIMESTAMP_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created timestamp index for segment: {}, column: {}", segmentName, columnName);
  }

  private void handleDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      TimestampIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    int numDocs = columnMetadata.getTotalDocs();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(segmentWriter, columnMetadata);
        BaseImmutableDictionary dictionary = LoaderUtils.getDictionary(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        TimestampIndexCreator timestampIndexCreator = newTimestampIndexCreator(columnMetadata, indexCreatorProvider)) {
      for (int i = 0; i < numDocs; i++) {
        timestampIndexCreator.add(dictionary.getLongValue(forwardIndexReader.getDictId(i, readerContext)));
      }
      timestampIndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      TimestampIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    int numDocs = columnMetadata.getTotalDocs();
    try (ForwardIndexReader forwardIndexReader = LoaderUtils.getForwardIndexReader(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        TimestampIndexCreator timestampIndexCreator = newTimestampIndexCreator(columnMetadata, indexCreatorProvider)) {
      for (int i = 0; i < numDocs; i++) {
        timestampIndexCreator.add(forwardIndexReader.getLong(i, readerContext));
      }
      timestampIndexCreator.seal();
    }
  }

  private TimestampIndexCreator newTimestampIndexCreator(ColumnMetadata columnMetadata,
      TimestampIndexCreatorProvider indexCreatorProvider)
      throws IOException {
    File indexDir = _segmentMetadata.getIndexDir();
    Set<TimestampIndexGranularity> granularities = _timestampIndexConfigs.get(columnMetadata.getColumnName());
    return indexCreatorProvider.newTimestampIndexCreator(
        IndexCreationContext.builder().withIndexDir(indexDir).withColumnMetadata(columnMetadata).build()
            .forTimestamp(granularities));
  }
}
