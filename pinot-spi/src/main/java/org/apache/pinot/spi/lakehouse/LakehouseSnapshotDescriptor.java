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
 * Describes the resolved lakehouse snapshot the rest of Pinot should pin a query or tablet-refresh operation to.
 */
public class LakehouseSnapshotDescriptor extends BaseJsonConfig {
  private String _tableIdentifier;
  private long _snapshotId;
  private int _specId;
  private Long _committedAtMillis;
  private String _manifestListUri;
  private Map<String, String> _summary;

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
  public Long getCommittedAtMillis() {
    return _committedAtMillis;
  }

  public void setCommittedAtMillis(@Nullable Long committedAtMillis) {
    _committedAtMillis = committedAtMillis;
  }

  @Nullable
  public String getManifestListUri() {
    return _manifestListUri;
  }

  public void setManifestListUri(@Nullable String manifestListUri) {
    _manifestListUri = manifestListUri;
  }

  @Nullable
  public Map<String, String> getSummary() {
    return _summary;
  }

  public void setSummary(@Nullable Map<String, String> summary) {
    _summary = summary;
  }
}
