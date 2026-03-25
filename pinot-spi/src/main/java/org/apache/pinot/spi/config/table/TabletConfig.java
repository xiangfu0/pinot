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
 * Configuration for tablet (supersegment) sizing and grouping in lakehouse-native mode.
 *
 * <p>A tablet is the Helix-visible routing and assignment unit. It groups multiple Iceberg data
 * files together to avoid the one-file-one-Helix-partition control-plane bottleneck. These
 * settings control how files are grouped into tablets.</p>
 */
public class TabletConfig extends BaseJsonConfig {

  public static final int DEFAULT_TARGET_FILES_PER_TABLET = 128;
  public static final long DEFAULT_TARGET_BYTES_PER_TABLET = 64L * 1024 * 1024 * 1024; // 64 GiB
  public static final String DEFAULT_TIME_BUCKET = "DAY";

  @JsonPropertyDescription("Target number of Iceberg data files per tablet")
  @Nullable
  private final Integer _targetFilesPerTablet;

  @JsonPropertyDescription("Target total bytes per tablet")
  @Nullable
  private final Long _targetBytesPerTablet;

  @JsonPropertyDescription("Time bucket granularity for tablet grouping (e.g. DAY, HOUR)")
  @Nullable
  private final String _timeBucket;

  @JsonCreator
  public TabletConfig(
      @JsonProperty("targetFilesPerTablet") @Nullable Integer targetFilesPerTablet,
      @JsonProperty("targetBytesPerTablet") @Nullable Long targetBytesPerTablet,
      @JsonProperty("timeBucket") @Nullable String timeBucket) {
    _targetFilesPerTablet = targetFilesPerTablet;
    _targetBytesPerTablet = targetBytesPerTablet;
    _timeBucket = timeBucket;
  }

  @JsonProperty("targetFilesPerTablet")
  public int getTargetFilesPerTablet() {
    return _targetFilesPerTablet != null ? _targetFilesPerTablet : DEFAULT_TARGET_FILES_PER_TABLET;
  }

  @JsonProperty("targetBytesPerTablet")
  public long getTargetBytesPerTablet() {
    return _targetBytesPerTablet != null ? _targetBytesPerTablet : DEFAULT_TARGET_BYTES_PER_TABLET;
  }

  @JsonProperty("timeBucket")
  public String getTimeBucket() {
    return _timeBucket != null ? _timeBucket : DEFAULT_TIME_BUCKET;
  }
}
