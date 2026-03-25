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
 * Stores the full file-level metadata Pinot needs for one lakehouse tablet.
 */
public class TabletManifest extends BaseJsonConfig {
  private int _version = 1;
  private String _tabletId;
  private String _tableNameWithType;
  private String _tableIdentifier;
  private long _snapshotId;
  private int _specId;
  private Integer _generation;
  private Map<String, String> _partitionTuple;
  private List<MicrosegmentDescriptor> _microsegments;
  private List<String> _sidecarManifestUris;
  private Map<String, String> _metadata;

  public int getVersion() {
    return _version;
  }

  public void setVersion(int version) {
    _version = version;
  }

  @Nullable
  public String getTabletId() {
    return _tabletId;
  }

  public void setTabletId(@Nullable String tabletId) {
    _tabletId = tabletId;
  }

  @Nullable
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public void setTableNameWithType(@Nullable String tableNameWithType) {
    _tableNameWithType = tableNameWithType;
  }

  @Nullable
  public String getTableIdentifier() {
    return _tableIdentifier;
  }

  public void setTableIdentifier(@Nullable String tableIdentifier) {
    _tableIdentifier = tableIdentifier;
  }

  public long getSnapshotId() {
    return _snapshotId;
  }

  public void setSnapshotId(long snapshotId) {
    _snapshotId = snapshotId;
  }

  public int getSpecId() {
    return _specId;
  }

  public void setSpecId(int specId) {
    _specId = specId;
  }

  @Nullable
  public Integer getGeneration() {
    return _generation;
  }

  public void setGeneration(@Nullable Integer generation) {
    _generation = generation;
  }

  @Nullable
  public Map<String, String> getPartitionTuple() {
    return _partitionTuple;
  }

  public void setPartitionTuple(@Nullable Map<String, String> partitionTuple) {
    _partitionTuple = partitionTuple;
  }

  @Nullable
  public List<MicrosegmentDescriptor> getMicrosegments() {
    return _microsegments;
  }

  public void setMicrosegments(@Nullable List<MicrosegmentDescriptor> microsegments) {
    _microsegments = microsegments;
  }

  @Nullable
  public List<String> getSidecarManifestUris() {
    return _sidecarManifestUris;
  }

  public void setSidecarManifestUris(@Nullable List<String> sidecarManifestUris) {
    _sidecarManifestUris = sidecarManifestUris;
  }

  @Nullable
  public Map<String, String> getMetadata() {
    return _metadata;
  }

  public void setMetadata(@Nullable Map<String, String> metadata) {
    _metadata = metadata;
  }
}
