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
package org.apache.pinot.controller.lakehouse;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotDescriptor;


/**
 * Returns the outcome of a controller-side lakehouse refresh operation.
 */
public class LakehouseTableRefreshResponse extends BaseJsonConfig {
  private String _tableNameWithType;
  private LakehouseSnapshotDescriptor _snapshotDescriptor;
  private int _tabletCount;
  private List<String> _tabletIds;
  private List<String> _removedTabletIds;

  @Nullable
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public void setTableNameWithType(@Nullable String tableNameWithType) {
    _tableNameWithType = tableNameWithType;
  }

  @Nullable
  public LakehouseSnapshotDescriptor getSnapshotDescriptor() {
    return _snapshotDescriptor;
  }

  public void setSnapshotDescriptor(@Nullable LakehouseSnapshotDescriptor snapshotDescriptor) {
    _snapshotDescriptor = snapshotDescriptor;
  }

  public int getTabletCount() {
    return _tabletCount;
  }

  public void setTabletCount(int tabletCount) {
    _tabletCount = tabletCount;
  }

  @Nullable
  public List<String> getTabletIds() {
    return _tabletIds;
  }

  public void setTabletIds(@Nullable List<String> tabletIds) {
    _tabletIds = tabletIds;
  }

  @Nullable
  public List<String> getRemovedTabletIds() {
    return _removedTabletIds;
  }

  public void setRemovedTabletIds(@Nullable List<String> removedTabletIds) {
    _removedTabletIds = removedTabletIds;
  }
}
