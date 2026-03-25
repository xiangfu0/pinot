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
 * Describes optional Pinot-managed acceleration artifacts associated with a tablet or microsegment set.
 */
public class SidecarManifest extends BaseJsonConfig {
  private int _version = 1;
  private String _tabletId;
  private long _snapshotId;
  private String _sidecarVersion;
  private Long _generatedAtMillis;
  private Map<String, String> _artifactUris;

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

  public long getSnapshotId() {
    return _snapshotId;
  }

  public void setSnapshotId(long snapshotId) {
    _snapshotId = snapshotId;
  }

  @Nullable
  public String getSidecarVersion() {
    return _sidecarVersion;
  }

  public void setSidecarVersion(@Nullable String sidecarVersion) {
    _sidecarVersion = sidecarVersion;
  }

  @Nullable
  public Long getGeneratedAtMillis() {
    return _generatedAtMillis;
  }

  public void setGeneratedAtMillis(@Nullable Long generatedAtMillis) {
    _generatedAtMillis = generatedAtMillis;
  }

  @Nullable
  public Map<String, String> getArtifactUris() {
    return _artifactUris;
  }

  public void setArtifactUris(@Nullable Map<String, String> artifactUris) {
    _artifactUris = artifactUris;
  }
}
