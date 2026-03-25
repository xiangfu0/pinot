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

import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Encodes the query-scoped snapshot selector that broker and controller components will pass to the catalog adapter.
 */
public class LakehouseSnapshotRequest extends BaseJsonConfig {
  private Long _snapshotId;
  private String _branch;
  private String _tag;
  private Long _asOfTimeMillis;

  @Nullable
  public Long getSnapshotId() {
    return _snapshotId;
  }

  public void setSnapshotId(@Nullable Long snapshotId) {
    _snapshotId = snapshotId;
  }

  @Nullable
  public String getBranch() {
    return _branch;
  }

  public void setBranch(@Nullable String branch) {
    _branch = branch;
  }

  @Nullable
  public String getTag() {
    return _tag;
  }

  public void setTag(@Nullable String tag) {
    _tag = tag;
  }

  @Nullable
  public Long getAsOfTimeMillis() {
    return _asOfTimeMillis;
  }

  public void setAsOfTimeMillis(@Nullable Long asOfTimeMillis) {
    _asOfTimeMillis = asOfTimeMillis;
  }
}
