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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Captures the minimum Iceberg catalog information Pinot needs in order to resolve snapshots and data files.
 *
 * <p>This type intentionally carries only neutral configuration state so the actual catalog client can live in an
 * extension module without forcing Iceberg dependencies into foundational modules.</p>
 */
public class IcebergCatalogConfig extends BaseJsonConfig {
  private IcebergCatalogType _catalogType = IcebergCatalogType.REST;
  private String _catalogName;
  private String _catalogUri;
  private String _warehouseUri;
  private String _tableIdentifier;
  private String _pluginName;
  private Map<String, String> _properties;

  public IcebergCatalogType getCatalogType() {
    return _catalogType;
  }

  public void setCatalogType(IcebergCatalogType catalogType) {
    _catalogType = catalogType;
  }

  @Nullable
  public String getCatalogName() {
    return _catalogName;
  }

  public void setCatalogName(@Nullable String catalogName) {
    _catalogName = catalogName;
  }

  @Nullable
  public String getCatalogUri() {
    return _catalogUri;
  }

  public void setCatalogUri(@Nullable String catalogUri) {
    _catalogUri = catalogUri;
  }

  @Nullable
  public String getWarehouseUri() {
    return _warehouseUri;
  }

  public void setWarehouseUri(@Nullable String warehouseUri) {
    _warehouseUri = warehouseUri;
  }

  @Nullable
  public String getTableIdentifier() {
    return _tableIdentifier;
  }

  public void setTableIdentifier(@Nullable String tableIdentifier) {
    _tableIdentifier = tableIdentifier;
  }

  @Nullable
  public String getPluginName() {
    return _pluginName;
  }

  public void setPluginName(@Nullable String pluginName) {
    _pluginName = pluginName;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return _properties;
  }

  public void setProperties(@Nullable Map<String, String> properties) {
    _properties = properties;
  }
}
