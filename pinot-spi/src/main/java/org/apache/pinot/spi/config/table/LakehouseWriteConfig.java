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
 * Write path configuration for lakehouse-native realtime tables.
 *
 * <p>Controls whether the Pinot realtime writer directly appends to Iceberg
 * and the write mode (append-only or CDC changelog).</p>
 */
public class LakehouseWriteConfig extends BaseJsonConfig {

  public static final LakehouseConfig.WriteMode DEFAULT_WRITE_MODE = LakehouseConfig.WriteMode.APPEND;
  public static final String DEFAULT_FILE_FORMAT = "PARQUET";

  @JsonPropertyDescription("Whether direct Iceberg write is enabled")
  private final boolean _enabled;

  @JsonPropertyDescription("Write mode (APPEND or CDC)")
  @Nullable
  private final LakehouseConfig.WriteMode _mode;

  @JsonPropertyDescription("Base file format for written data files")
  @Nullable
  private final String _fileFormat;

  @JsonCreator
  public LakehouseWriteConfig(
      @JsonProperty(value = "enabled", required = true) boolean enabled,
      @JsonProperty("mode") @Nullable LakehouseConfig.WriteMode mode,
      @JsonProperty("fileFormat") @Nullable String fileFormat) {
    _enabled = enabled;
    _mode = mode;
    _fileFormat = fileFormat;
  }

  @JsonProperty("enabled")
  public boolean isEnabled() {
    return _enabled;
  }

  @JsonProperty("mode")
  public LakehouseConfig.WriteMode getMode() {
    return _mode != null ? _mode : DEFAULT_WRITE_MODE;
  }

  @JsonProperty("fileFormat")
  public String getFileFormat() {
    return _fileFormat != null ? _fileFormat : DEFAULT_FILE_FORMAT;
  }
}
