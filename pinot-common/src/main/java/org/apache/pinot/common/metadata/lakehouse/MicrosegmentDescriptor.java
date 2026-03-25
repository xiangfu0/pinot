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
package org.apache.pinot.common.metadata.lakehouse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Descriptor for a single microsegment within a tablet.
 *
 * <p>A microsegment typically corresponds to one Iceberg data file. This descriptor holds the
 * metadata needed for server-side file-level pruning and execution without referencing Iceberg
 * APIs directly (keeping Iceberg dependencies out of foundational modules).</p>
 *
 * <p>This class is immutable.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MicrosegmentDescriptor {

  @JsonPropertyDescription("Unique microsegment identifier within the tablet")
  private final String _microsegmentId;

  @JsonPropertyDescription("Data file path (e.g. s3://bucket/path/to/file.parquet)")
  private final String _filePath;

  @JsonPropertyDescription("File format (e.g. PARQUET)")
  private final String _fileFormat;

  @JsonPropertyDescription("File size in bytes")
  private final long _fileSizeBytes;

  @JsonPropertyDescription("Row count in this data file")
  private final long _rowCount;

  @JsonPropertyDescription("Minimum time value in this file (epoch ms)")
  private final long _minTimeMs;

  @JsonPropertyDescription("Maximum time value in this file (epoch ms)")
  private final long _maxTimeMs;

  @JsonPropertyDescription("Per-column lower bounds from Iceberg manifest (column name -> value string)")
  @Nullable
  private final Map<String, String> _columnLowerBounds;

  @JsonPropertyDescription("Per-column upper bounds from Iceberg manifest (column name -> value string)")
  @Nullable
  private final Map<String, String> _columnUpperBounds;

  @JsonPropertyDescription("Per-column null counts from Iceberg manifest")
  @Nullable
  private final Map<String, Long> _columnNullCounts;

  @JsonPropertyDescription("Iceberg content type: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES")
  private final int _contentType;

  @JsonCreator
  public MicrosegmentDescriptor(
      @JsonProperty(value = "microsegmentId", required = true) String microsegmentId,
      @JsonProperty(value = "filePath", required = true) String filePath,
      @JsonProperty("fileFormat") String fileFormat,
      @JsonProperty("fileSizeBytes") long fileSizeBytes,
      @JsonProperty("rowCount") long rowCount,
      @JsonProperty("minTimeMs") long minTimeMs,
      @JsonProperty("maxTimeMs") long maxTimeMs,
      @JsonProperty("columnLowerBounds") @Nullable Map<String, String> columnLowerBounds,
      @JsonProperty("columnUpperBounds") @Nullable Map<String, String> columnUpperBounds,
      @JsonProperty("columnNullCounts") @Nullable Map<String, Long> columnNullCounts,
      @JsonProperty("contentType") int contentType) {
    _microsegmentId = microsegmentId;
    _filePath = filePath;
    _fileFormat = fileFormat != null ? fileFormat : "PARQUET";
    _fileSizeBytes = fileSizeBytes;
    _rowCount = rowCount;
    _minTimeMs = minTimeMs;
    _maxTimeMs = maxTimeMs;
    _columnLowerBounds = columnLowerBounds;
    _columnUpperBounds = columnUpperBounds;
    _columnNullCounts = columnNullCounts;
    _contentType = contentType;
  }

  @JsonProperty("microsegmentId")
  public String getMicrosegmentId() {
    return _microsegmentId;
  }

  @JsonProperty("filePath")
  public String getFilePath() {
    return _filePath;
  }

  @JsonProperty("fileFormat")
  public String getFileFormat() {
    return _fileFormat;
  }

  @JsonProperty("fileSizeBytes")
  public long getFileSizeBytes() {
    return _fileSizeBytes;
  }

  @JsonProperty("rowCount")
  public long getRowCount() {
    return _rowCount;
  }

  @JsonProperty("minTimeMs")
  public long getMinTimeMs() {
    return _minTimeMs;
  }

  @JsonProperty("maxTimeMs")
  public long getMaxTimeMs() {
    return _maxTimeMs;
  }

  @JsonProperty("columnLowerBounds")
  @Nullable
  public Map<String, String> getColumnLowerBounds() {
    return _columnLowerBounds;
  }

  @JsonProperty("columnUpperBounds")
  @Nullable
  public Map<String, String> getColumnUpperBounds() {
    return _columnUpperBounds;
  }

  @JsonProperty("columnNullCounts")
  @Nullable
  public Map<String, Long> getColumnNullCounts() {
    return _columnNullCounts;
  }

  @JsonProperty("contentType")
  public int getContentType() {
    return _contentType;
  }

  /**
   * @return true if this is a data file (not a delete file)
   */
  public boolean isDataFile() {
    return _contentType == 0;
  }

  /**
   * @return true if this is a position delete file
   */
  public boolean isPositionDeleteFile() {
    return _contentType == 1;
  }

  /**
   * @return true if this is an equality delete file
   */
  public boolean isEqualityDeleteFile() {
    return _contentType == 2;
  }
}
