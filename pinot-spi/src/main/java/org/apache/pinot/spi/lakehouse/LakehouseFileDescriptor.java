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
package org.apache.pinot.spi.lakehouse;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Describes a single lakehouse data or delete file, extracted from Iceberg manifests.
 *
 * <p>Uses plain types to avoid leaking Iceberg API classes into foundational modules.</p>
 */
public class LakehouseFileDescriptor {

  /** Iceberg content types. */
  public static final int CONTENT_DATA = 0;
  public static final int CONTENT_POSITION_DELETES = 1;
  public static final int CONTENT_EQUALITY_DELETES = 2;

  private final String _filePath;
  private final String _fileFormat;
  private final long _fileSizeBytes;
  private final long _rowCount;
  private final int _contentType;
  private final int _specId;
  private final List<String> _partitionValues;
  private final Map<String, String> _columnLowerBounds;
  private final Map<String, String> _columnUpperBounds;
  private final Map<String, Long> _columnNullCounts;

  public LakehouseFileDescriptor(String filePath, String fileFormat, long fileSizeBytes, long rowCount,
      int contentType, int specId, @Nullable List<String> partitionValues,
      @Nullable Map<String, String> columnLowerBounds, @Nullable Map<String, String> columnUpperBounds,
      @Nullable Map<String, Long> columnNullCounts) {
    _filePath = filePath;
    _fileFormat = fileFormat;
    _fileSizeBytes = fileSizeBytes;
    _rowCount = rowCount;
    _contentType = contentType;
    _specId = specId;
    _partitionValues = partitionValues;
    _columnLowerBounds = columnLowerBounds;
    _columnUpperBounds = columnUpperBounds;
    _columnNullCounts = columnNullCounts;
  }

  public String getFilePath() {
    return _filePath;
  }

  public String getFileFormat() {
    return _fileFormat;
  }

  public long getFileSizeBytes() {
    return _fileSizeBytes;
  }

  public long getRowCount() {
    return _rowCount;
  }

  public int getContentType() {
    return _contentType;
  }

  public int getSpecId() {
    return _specId;
  }

  @Nullable
  public List<String> getPartitionValues() {
    return _partitionValues;
  }

  @Nullable
  public Map<String, String> getColumnLowerBounds() {
    return _columnLowerBounds;
  }

  @Nullable
  public Map<String, String> getColumnUpperBounds() {
    return _columnUpperBounds;
  }

  @Nullable
  public Map<String, Long> getColumnNullCounts() {
    return _columnNullCounts;
  }

  public boolean isDataFile() {
    return _contentType == CONTENT_DATA;
  }

  public boolean isPositionDeleteFile() {
    return _contentType == CONTENT_POSITION_DELETES;
  }
}
