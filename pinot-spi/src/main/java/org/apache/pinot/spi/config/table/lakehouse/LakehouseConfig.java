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
package org.apache.pinot.spi.config.table.lakehouse;

import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Top-level table config section for Pinot lakehouse-native mode.
 *
 * <p>The config is disabled by default so existing Pinot-native tables remain unchanged unless they opt in.</p>
 */
public class LakehouseConfig extends BaseJsonConfig {
  private boolean _enabled;
  private LakehouseMode _mode = LakehouseMode.ICEBERG_NATIVE;
  private IcebergCatalogConfig _catalogConfig;
  private LakehouseReadVisibilityMode _readVisibilityMode = LakehouseReadVisibilityMode.SNAPSHOT_ONLY;
  private LakehouseWriteMode _writeMode = LakehouseWriteMode.DISABLED;
  private LakehouseTabletConfig _tabletConfig;
  private LakehouseSidecarConfig _sidecarConfig;

  public boolean isEnabled() {
    return _enabled;
  }

  public void setEnabled(boolean enabled) {
    _enabled = enabled;
  }

  public LakehouseMode getMode() {
    return _mode;
  }

  public void setMode(@Nullable LakehouseMode mode) {
    _mode = mode;
  }

  @Nullable
  public IcebergCatalogConfig getCatalogConfig() {
    return _catalogConfig;
  }

  public void setCatalogConfig(@Nullable IcebergCatalogConfig catalogConfig) {
    _catalogConfig = catalogConfig;
  }

  public LakehouseReadVisibilityMode getReadVisibilityMode() {
    return _readVisibilityMode;
  }

  public void setReadVisibilityMode(@Nullable LakehouseReadVisibilityMode readVisibilityMode) {
    _readVisibilityMode = readVisibilityMode;
  }

  public LakehouseWriteMode getWriteMode() {
    return _writeMode;
  }

  public void setWriteMode(@Nullable LakehouseWriteMode writeMode) {
    _writeMode = writeMode;
  }

  @Nullable
  public LakehouseTabletConfig getTabletConfig() {
    return _tabletConfig;
  }

  public void setTabletConfig(@Nullable LakehouseTabletConfig tabletConfig) {
    _tabletConfig = tabletConfig;
  }

  @Nullable
  public LakehouseSidecarConfig getSidecarConfig() {
    return _sidecarConfig;
  }

  public void setSidecarConfig(@Nullable LakehouseSidecarConfig sidecarConfig) {
    _sidecarConfig = sidecarConfig;
  }
}
