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
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Top-level configuration for lakehouse-native mode.
 *
 * <p>When enabled, Pinot queries Iceberg tables in place using Parquet as the base storage format.
 * Pinot groups Iceberg data files into tablets (supersegments) as the Helix-visible routing unit,
 * avoiding the one-file-one-segment control-plane bottleneck. Pinot indexes remain available as
 * optional sidecar accelerators.</p>
 *
 * <p>This configuration is additive and opt-in. Existing Pinot-native OFFLINE/REALTIME/HYBRID
 * tables are completely unaffected.</p>
 */
public class LakehouseConfig extends BaseJsonConfig {

  /**
   * Supported lakehouse storage modes.
   */
  public enum Mode {
    /**
     * Iceberg is the metadata and snapshot source of truth; Parquet is the base columnar format.
     */
    ICEBERG_NATIVE
  }

  /**
   * Read visibility modes for lakehouse-native realtime tables.
   */
  public enum VisibilityMode {
    /**
     * Query committed Iceberg snapshot plus hot rows beyond the committed watermark.
     * Preserves Pinot-style freshness. Not fully transactional before commit.
     */
    HOT_PLUS_SNAPSHOT,

    /**
     * Query only committed Iceberg snapshots. Stronger semantics, slower freshness.
     */
    SNAPSHOT_ONLY
  }

  /**
   * Write modes for lakehouse-native realtime tables.
   */
  public enum WriteMode {
    /**
     * Append-only writes. No deletes or updates.
     */
    APPEND,

    /**
     * CDC changelog write mode for mutable workloads.
     */
    CDC
  }

  /**
   * Iceberg ref types for snapshot resolution.
   */
  public enum RefType {
    BRANCH,
    TAG,
    SNAPSHOT_ID
  }

  @JsonPropertyDescription("Whether lakehouse-native mode is enabled for this table")
  private final boolean _enabled;

  @JsonPropertyDescription("Lakehouse storage mode")
  private final Mode _mode;

  @JsonPropertyDescription("Iceberg catalog configuration")
  private final IcebergCatalogConfig _catalog;

  @JsonPropertyDescription("Read path configuration")
  @Nullable
  private final LakehouseReadConfig _read;

  @JsonPropertyDescription("Write path configuration")
  @Nullable
  private final LakehouseWriteConfig _write;

  @JsonPropertyDescription("Tablet sizing and grouping configuration")
  @Nullable
  private final TabletConfig _tablet;

  @JsonPropertyDescription("Sidecar index configuration")
  @Nullable
  private final SidecarConfig _sidecars;

  @JsonCreator
  public LakehouseConfig(
      @JsonProperty(value = "enabled", required = true) boolean enabled,
      @JsonProperty(value = "mode", required = true) Mode mode,
      @JsonProperty(value = "catalog", required = true) IcebergCatalogConfig catalog,
      @JsonProperty("read") @Nullable LakehouseReadConfig read,
      @JsonProperty("write") @Nullable LakehouseWriteConfig write,
      @JsonProperty("tablet") @Nullable TabletConfig tablet,
      @JsonProperty("sidecars") @Nullable SidecarConfig sidecars) {
    _enabled = enabled;
    _mode = mode;
    _catalog = catalog;
    _read = read;
    _write = write;
    _tablet = tablet;
    _sidecars = sidecars;
  }

  @JsonProperty("enabled")
  public boolean isEnabled() {
    return _enabled;
  }

  @JsonProperty("mode")
  public Mode getMode() {
    return _mode;
  }

  @JsonProperty("catalog")
  public IcebergCatalogConfig getCatalog() {
    return _catalog;
  }

  @JsonProperty("read")
  @Nullable
  public LakehouseReadConfig getRead() {
    return _read;
  }

  @JsonProperty("write")
  @Nullable
  public LakehouseWriteConfig getWrite() {
    return _write;
  }

  @JsonProperty("tablet")
  @Nullable
  public TabletConfig getTablet() {
    return _tablet;
  }

  @JsonProperty("sidecars")
  @Nullable
  public SidecarConfig getSidecars() {
    return _sidecars;
  }
}
