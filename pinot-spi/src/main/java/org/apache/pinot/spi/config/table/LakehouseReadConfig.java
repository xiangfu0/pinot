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
 * Read path configuration for lakehouse-native tables.
 *
 * <p>Controls visibility mode and default Iceberg ref resolution for queries.</p>
 */
public class LakehouseReadConfig extends BaseJsonConfig {

  public static final LakehouseConfig.VisibilityMode DEFAULT_VISIBILITY_MODE =
      LakehouseConfig.VisibilityMode.HOT_PLUS_SNAPSHOT;
  public static final LakehouseConfig.RefType DEFAULT_REF_TYPE = LakehouseConfig.RefType.BRANCH;
  public static final String DEFAULT_REF_NAME = "main";

  @JsonPropertyDescription("Read visibility mode")
  @Nullable
  private final LakehouseConfig.VisibilityMode _visibilityMode;

  @JsonPropertyDescription("Default Iceberg ref type for queries")
  @Nullable
  private final LakehouseConfig.RefType _defaultRefType;

  @JsonPropertyDescription("Default Iceberg ref name (e.g. 'main' branch)")
  @Nullable
  private final String _defaultRefName;

  @JsonCreator
  public LakehouseReadConfig(
      @JsonProperty("visibilityMode") @Nullable LakehouseConfig.VisibilityMode visibilityMode,
      @JsonProperty("defaultRefType") @Nullable LakehouseConfig.RefType defaultRefType,
      @JsonProperty("defaultRefName") @Nullable String defaultRefName) {
    _visibilityMode = visibilityMode;
    _defaultRefType = defaultRefType;
    _defaultRefName = defaultRefName;
  }

  @JsonProperty("visibilityMode")
  public LakehouseConfig.VisibilityMode getVisibilityMode() {
    return _visibilityMode != null ? _visibilityMode : DEFAULT_VISIBILITY_MODE;
  }

  @JsonProperty("defaultRefType")
  public LakehouseConfig.RefType getDefaultRefType() {
    return _defaultRefType != null ? _defaultRefType : DEFAULT_REF_TYPE;
  }

  @JsonProperty("defaultRefName")
  public String getDefaultRefName() {
    return _defaultRefName != null ? _defaultRefName : DEFAULT_REF_NAME;
  }
}
