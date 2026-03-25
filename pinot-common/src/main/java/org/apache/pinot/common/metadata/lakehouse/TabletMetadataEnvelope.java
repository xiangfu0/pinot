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
package org.apache.pinot.common.metadata.lakehouse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Small metadata envelope for a tablet, stored in ZooKeeper / controller metadata.
 *
 * <p>This is the broker-visible routing unit for lakehouse-native tables. It contains only the
 * coarse metadata needed for tablet-level pruning and routing. The full tablet manifest
 * (with per-file details) is stored outside ZK, referenced by {@link #getManifestPointerUri()}.</p>
 *
 * <p>Design rationale: Iceberg tables can have hundreds of thousands of data files. Storing
 * per-file metadata in ZooKeeper would cause znode growth and broker heap pressure. The envelope
 * keeps ZK metadata bounded by tablet count, not file count.</p>
 *
 * <p>This class is thread-safe and immutable.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TabletMetadataEnvelope {

  public static final int CURRENT_VERSION = 1;

  @JsonPropertyDescription("Unique tablet identifier")
  private final String _tabletId;

  @JsonPropertyDescription("Table name with type suffix")
  private final String _tableNameWithType;

  @JsonPropertyDescription("Envelope schema version for forward/backward compatibility")
  private final int _version;

  @JsonPropertyDescription("Iceberg snapshot ID this tablet is pinned to")
  private final long _snapshotId;

  @JsonPropertyDescription("Iceberg partition spec ID")
  private final int _specId;

  @JsonPropertyDescription("Partition values as strings for the grouping key")
  @Nullable
  private final List<String> _partitionValues;

  @JsonPropertyDescription("Minimum time value across all files in this tablet (epoch ms)")
  private final long _minTimeMs;

  @JsonPropertyDescription("Maximum time value across all files in this tablet (epoch ms)")
  private final long _maxTimeMs;

  @JsonPropertyDescription("Approximate total row count in this tablet")
  private final long _rowCount;

  @JsonPropertyDescription("Approximate total size in bytes")
  private final long _sizeBytes;

  @JsonPropertyDescription("Number of microsegments (data files) in this tablet")
  private final int _microsegmentCount;

  @JsonPropertyDescription("URI pointing to the full TabletManifest outside ZK")
  private final String _manifestPointerUri;

  @JsonCreator
  public TabletMetadataEnvelope(
      @JsonProperty(value = "tabletId", required = true) String tabletId,
      @JsonProperty(value = "tableNameWithType", required = true) String tableNameWithType,
      @JsonProperty("version") int version,
      @JsonProperty(value = "snapshotId", required = true) long snapshotId,
      @JsonProperty("specId") int specId,
      @JsonProperty("partitionValues") @Nullable List<String> partitionValues,
      @JsonProperty("minTimeMs") long minTimeMs,
      @JsonProperty("maxTimeMs") long maxTimeMs,
      @JsonProperty("rowCount") long rowCount,
      @JsonProperty("sizeBytes") long sizeBytes,
      @JsonProperty("microsegmentCount") int microsegmentCount,
      @JsonProperty(value = "manifestPointerUri", required = true) String manifestPointerUri) {
    _tabletId = tabletId;
    _tableNameWithType = tableNameWithType;
    _version = version > 0 ? version : CURRENT_VERSION;
    _snapshotId = snapshotId;
    _specId = specId;
    _partitionValues = partitionValues;
    _minTimeMs = minTimeMs;
    _maxTimeMs = maxTimeMs;
    _rowCount = rowCount;
    _sizeBytes = sizeBytes;
    _microsegmentCount = microsegmentCount;
    _manifestPointerUri = manifestPointerUri;
  }

  @JsonProperty("tabletId")
  public String getTabletId() {
    return _tabletId;
  }

  @JsonProperty("tableNameWithType")
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  @JsonProperty("version")
  public int getVersion() {
    return _version;
  }

  @JsonProperty("snapshotId")
  public long getSnapshotId() {
    return _snapshotId;
  }

  @JsonProperty("specId")
  public int getSpecId() {
    return _specId;
  }

  @JsonProperty("partitionValues")
  @Nullable
  public List<String> getPartitionValues() {
    return _partitionValues;
  }

  @JsonProperty("minTimeMs")
  public long getMinTimeMs() {
    return _minTimeMs;
  }

  @JsonProperty("maxTimeMs")
  public long getMaxTimeMs() {
    return _maxTimeMs;
  }

  @JsonProperty("rowCount")
  public long getRowCount() {
    return _rowCount;
  }

  @JsonProperty("sizeBytes")
  public long getSizeBytes() {
    return _sizeBytes;
  }

  @JsonProperty("microsegmentCount")
  public int getMicrosegmentCount() {
    return _microsegmentCount;
  }

  @JsonProperty("manifestPointerUri")
  public String getManifestPointerUri() {
    return _manifestPointerUri;
  }

  public String toJsonString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize TabletMetadataEnvelope", e);
    }
  }

  public static TabletMetadataEnvelope fromJsonString(String json) {
    try {
      return JsonUtils.stringToObject(json, TabletMetadataEnvelope.class);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize TabletMetadataEnvelope", e);
    }
  }

  @Override
  public String toString() {
    return toJsonString();
  }
}
