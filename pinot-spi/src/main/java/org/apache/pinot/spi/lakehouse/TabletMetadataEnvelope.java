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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Stores the bounded, Helix-visible metadata Pinot brokers need to route a lakehouse tablet.
 */
public class TabletMetadataEnvelope extends BaseJsonConfig {
  private int _version = 1;
  private String _tabletId;
  private String _tableNameWithType;
  private long _snapshotId;
  private int _specId;
  private Integer _generation;
  private Long _minTimeMillis;
  private Long _maxTimeMillis;
  private Long _approximateRowCount;
  private Long _approximateSizeBytes;
  private Integer _microsegmentCount;
  private String _manifestUri;
  private Long _manifestVersion;
  private String _state;
  private Map<String, String> _partitionTuple;

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
  public Long getApproximateRowCount() {
    return _approximateRowCount;
  }

  public void setApproximateRowCount(@Nullable Long approximateRowCount) {
    _approximateRowCount = approximateRowCount;
  }

  @Nullable
  public Long getApproximateSizeBytes() {
    return _approximateSizeBytes;
  }

  public void setApproximateSizeBytes(@Nullable Long approximateSizeBytes) {
    _approximateSizeBytes = approximateSizeBytes;
  }

  @Nullable
  public Integer getMicrosegmentCount() {
    return _microsegmentCount;
  }

  public void setMicrosegmentCount(@Nullable Integer microsegmentCount) {
    _microsegmentCount = microsegmentCount;
  }

  @Nullable
  public String getManifestUri() {
    return _manifestUri;
  }

  public void setManifestUri(@Nullable String manifestUri) {
    _manifestUri = manifestUri;
  }

  @Nullable
  public Long getManifestVersion() {
    return _manifestVersion;
  }

  public void setManifestVersion(@Nullable Long manifestVersion) {
    _manifestVersion = manifestVersion;
  }

  @Nullable
  public String getState() {
    return _state;
  }

  public void setState(@Nullable String state) {
    _state = state;
  }

  @Nullable
  public Map<String, String> getPartitionTuple() {
    return _partitionTuple;
  }

  public void setPartitionTuple(@Nullable Map<String, String> partitionTuple) {
    _partitionTuple = partitionTuple;
  }
}
