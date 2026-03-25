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
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Describes one data-plane unit inside a lakehouse tablet.
 *
 * <p>In Phase 1 this is typically a single Parquet data file. The contract leaves room for later row-group and
 * delete-file richness without exposing those internals to Helix.</p>
 */
public class MicrosegmentDescriptor extends BaseJsonConfig {
  private int _version = 1;
  private String _microsegmentId;
  private String _filePath;
  private LakehouseFileFormat _fileFormat = LakehouseFileFormat.PARQUET;
  private Long _fileSizeBytes;
  private Long _recordCount;
  private Integer _rowGroupCount;
  private Long _minTimeMillis;
  private Long _maxTimeMillis;
  private Long _dataSequenceNumber;
  private Map<String, String> _partitionTuple;
  private List<String> _deleteFilePaths;

  public int getVersion() {
    return _version;
  }

  public void setVersion(int version) {
    _version = version;
  }

  @Nullable
  public String getMicrosegmentId() {
    return _microsegmentId;
  }

  public void setMicrosegmentId(@Nullable String microsegmentId) {
    _microsegmentId = microsegmentId;
  }

  @Nullable
  public String getFilePath() {
    return _filePath;
  }

  public void setFilePath(@Nullable String filePath) {
    _filePath = filePath;
  }

  public LakehouseFileFormat getFileFormat() {
    return _fileFormat;
  }

  public void setFileFormat(@Nullable LakehouseFileFormat fileFormat) {
    _fileFormat = fileFormat;
  }

  @Nullable
  public Long getFileSizeBytes() {
    return _fileSizeBytes;
  }

  public void setFileSizeBytes(@Nullable Long fileSizeBytes) {
    _fileSizeBytes = fileSizeBytes;
  }

  @Nullable
  public Long getRecordCount() {
    return _recordCount;
  }

  public void setRecordCount(@Nullable Long recordCount) {
    _recordCount = recordCount;
  }

  @Nullable
  public Integer getRowGroupCount() {
    return _rowGroupCount;
  }

  public void setRowGroupCount(@Nullable Integer rowGroupCount) {
    _rowGroupCount = rowGroupCount;
  }

  @Nullable
  public Long getMinTimeMillis() {
    return _minTimeMillis;
  }

  public void setMinTimeMillis(@Nullable Long minTimeMillis) {
    _minTimeMillis = minTimeMillis;
  }

  @Nullable
  public Long getMaxTimeMillis() {
    return _maxTimeMillis;
  }

  public void setMaxTimeMillis(@Nullable Long maxTimeMillis) {
    _maxTimeMillis = maxTimeMillis;
  }

  @Nullable
  public Long getDataSequenceNumber() {
    return _dataSequenceNumber;
  }

  public void setDataSequenceNumber(@Nullable Long dataSequenceNumber) {
    _dataSequenceNumber = dataSequenceNumber;
  }

  @Nullable
  public Map<String, String> getPartitionTuple() {
    return _partitionTuple;
  }

  public void setPartitionTuple(@Nullable Map<String, String> partitionTuple) {
    _partitionTuple = partitionTuple;
  }

  @Nullable
  public List<String> getDeleteFilePaths() {
    return _deleteFilePaths;
  }

  public void setDeleteFilePaths(@Nullable List<String> deleteFilePaths) {
    _deleteFilePaths = deleteFilePaths;
  }
}
