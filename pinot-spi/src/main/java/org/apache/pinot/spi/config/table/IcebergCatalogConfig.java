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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Configuration for connecting to an Iceberg catalog.
 *
 * <p>Supports REST catalogs, Hadoop catalogs, and other Iceberg catalog implementations.
 * Catalog-specific properties can be passed via the {@code properties} map.</p>
 */
public class IcebergCatalogConfig extends BaseJsonConfig {

  /**
   * Supported Iceberg catalog types.
   */
  public enum CatalogType {
    REST,
    HADOOP,
    HIVE,
    GLUE,
    NESSIE,
    JDBC
  }

  @JsonPropertyDescription("Iceberg catalog type")
  private final CatalogType _type;

  @JsonPropertyDescription("Catalog URI (e.g. REST endpoint URL or metastore URI)")
  @Nullable
  private final String _uri;

  @JsonPropertyDescription("Warehouse location (e.g. s3://warehouse)")
  @Nullable
  private final String _warehouse;

  @JsonPropertyDescription("Fully qualified Iceberg table identifier (e.g. analytics.events)")
  private final String _tableIdentifier;

  @JsonPropertyDescription("Additional catalog-specific properties")
  @Nullable
  private final Map<String, String> _properties;

  @JsonCreator
  public IcebergCatalogConfig(
      @JsonProperty(value = "type", required = true) CatalogType type,
      @JsonProperty("uri") @Nullable String uri,
      @JsonProperty("warehouse") @Nullable String warehouse,
      @JsonProperty(value = "tableIdentifier", required = true) String tableIdentifier,
      @JsonProperty("properties") @Nullable Map<String, String> properties) {
    _type = type;
    _uri = uri;
    _warehouse = warehouse;
    _tableIdentifier = tableIdentifier;
    _properties = properties;
  }

  @JsonProperty("type")
  public CatalogType getType() {
    return _type;
  }

  @JsonProperty("uri")
  @Nullable
  public String getUri() {
    return _uri;
  }

  @JsonProperty("warehouse")
  @Nullable
  public String getWarehouse() {
    return _warehouse;
  }

  @JsonProperty("tableIdentifier")
  public String getTableIdentifier() {
    return _tableIdentifier;
  }

  @JsonProperty("properties")
  @Nullable
  public Map<String, String> getProperties() {
    return _properties;
  }
}
