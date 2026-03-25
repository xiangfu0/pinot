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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Configuration for Pinot sidecar indexes in lakehouse-native mode.
 *
 * <p>Sidecars are optional Pinot-specific acceleration artifacts layered on top of Iceberg/Parquet.
 * They are never required for correctness; missing, stale, or corrupt sidecars fail open to the
 * base Parquet scan path on the pinned snapshot.</p>
 */
public class SidecarConfig extends BaseJsonConfig {

  /**
   * Supported sidecar index types.
   */
  public enum SidecarIndexType {
    INVERTED,
    RANGE,
    JSON,
    NATIVE_TEXT,
    VECTOR,
    STAR_TREE,
    BLOOM_FILTER,
    TIMESTAMP,
    H3
  }

  @JsonPropertyDescription("URI for sidecar index storage location")
  @Nullable
  private final String _storeURI;

  @JsonPropertyDescription("List of sidecar index types to build/maintain")
  @Nullable
  private final List<SidecarIndexType> _indexes;

  @JsonCreator
  public SidecarConfig(
      @JsonProperty("storeURI") @Nullable String storeURI,
      @JsonProperty("indexes") @Nullable List<SidecarIndexType> indexes) {
    _storeURI = storeURI;
    _indexes = indexes;
  }

  @JsonProperty("storeURI")
  @Nullable
  public String getStoreURI() {
    return _storeURI;
  }

  @JsonProperty("indexes")
  @Nullable
  public List<SidecarIndexType> getIndexes() {
    return _indexes;
  }
}
