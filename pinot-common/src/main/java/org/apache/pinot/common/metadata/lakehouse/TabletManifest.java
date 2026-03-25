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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Full manifest for a tablet, stored outside ZooKeeper (e.g. in deep store or controller metadata).
 *
 * <p>Contains per-microsegment descriptors, delete file references, sidecar references, and
 * watermark metadata. The corresponding {@link TabletMetadataEnvelope} in ZK points to this
 * manifest via a URI.</p>
 *
 * <p>The manifest is versioned for forward/backward compatibility. Readers should tolerate
 * unknown fields (via {@code @JsonIgnoreProperties(ignoreUnknown = true)}).</p>
 *
 * <p>This class is immutable.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TabletManifest {

  public static final int CURRENT_VERSION = 1;

  @JsonPropertyDescription("Tablet identifier (matches TabletMetadataEnvelope.tabletId)")
  private final String _tabletId;

  @JsonPropertyDescription("Manifest schema version")
  private final int _version;

  @JsonPropertyDescription("Iceberg table UUID")
  @Nullable
  private final String _icebergTableUuid;

  @JsonPropertyDescription("Pinned Iceberg snapshot ID")
  private final long _snapshotId;

  @JsonPropertyDescription("Iceberg partition spec ID")
  private final int _specId;

  @JsonPropertyDescription("Data file microsegment descriptors")
  private final List<MicrosegmentDescriptor> _microsegments;

  @JsonPropertyDescription("Position/equality delete file descriptors")
  @Nullable
  private final List<MicrosegmentDescriptor> _deleteFiles;

  @JsonPropertyDescription("Sidecar manifest references for optional acceleration")
  @Nullable
  private final List<SidecarReference> _sidecarReferences;

  @JsonPropertyDescription("Committed snapshot watermark for hot/cold boundary (epoch ms)")
  private final long _committedWatermarkMs;

  @JsonPropertyDescription("Creation timestamp of this manifest (epoch ms)")
  private final long _createdAtMs;

  @JsonCreator
  public TabletManifest(
      @JsonProperty(value = "tabletId", required = true) String tabletId,
      @JsonProperty("version") int version,
      @JsonProperty("icebergTableUuid") @Nullable String icebergTableUuid,
      @JsonProperty(value = "snapshotId", required = true) long snapshotId,
      @JsonProperty("specId") int specId,
      @JsonProperty(value = "microsegments", required = true) List<MicrosegmentDescriptor> microsegments,
      @JsonProperty("deleteFiles") @Nullable List<MicrosegmentDescriptor> deleteFiles,
      @JsonProperty("sidecarReferences") @Nullable List<SidecarReference> sidecarReferences,
      @JsonProperty("committedWatermarkMs") long committedWatermarkMs,
      @JsonProperty("createdAtMs") long createdAtMs) {
    _tabletId = tabletId;
    _version = version > 0 ? version : CURRENT_VERSION;
    _icebergTableUuid = icebergTableUuid;
    _snapshotId = snapshotId;
    _specId = specId;
    _microsegments = microsegments != null ? Collections.unmodifiableList(microsegments) : Collections.emptyList();
    _deleteFiles = deleteFiles;
    _sidecarReferences = sidecarReferences;
    _committedWatermarkMs = committedWatermarkMs;
    _createdAtMs = createdAtMs > 0 ? createdAtMs : System.currentTimeMillis();
  }

  @JsonProperty("tabletId")
  public String getTabletId() {
    return _tabletId;
  }

  @JsonProperty("version")
  public int getVersion() {
    return _version;
  }

  @JsonProperty("icebergTableUuid")
  @Nullable
  public String getIcebergTableUuid() {
    return _icebergTableUuid;
  }

  @JsonProperty("snapshotId")
  public long getSnapshotId() {
    return _snapshotId;
  }

  @JsonProperty("specId")
  public int getSpecId() {
    return _specId;
  }

  @JsonProperty("microsegments")
  public List<MicrosegmentDescriptor> getMicrosegments() {
    return _microsegments;
  }

  @JsonProperty("deleteFiles")
  @Nullable
  public List<MicrosegmentDescriptor> getDeleteFiles() {
    return _deleteFiles;
  }

  @JsonProperty("sidecarReferences")
  @Nullable
  public List<SidecarReference> getSidecarReferences() {
    return _sidecarReferences;
  }

  @JsonProperty("committedWatermarkMs")
  public long getCommittedWatermarkMs() {
    return _committedWatermarkMs;
  }

  @JsonProperty("createdAtMs")
  public long getCreatedAtMs() {
    return _createdAtMs;
  }

  /**
   * @return total row count across all data file microsegments
   */
  public long getTotalRowCount() {
    long total = 0;
    for (MicrosegmentDescriptor ms : _microsegments) {
      if (ms.isDataFile()) {
        total += ms.getRowCount();
      }
    }
    return total;
  }

  /**
   * @return total size in bytes across all data file microsegments
   */
  public long getTotalSizeBytes() {
    long total = 0;
    for (MicrosegmentDescriptor ms : _microsegments) {
      if (ms.isDataFile()) {
        total += ms.getFileSizeBytes();
      }
    }
    return total;
  }

  public String toJsonString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize TabletManifest", e);
    }
  }

  public static TabletManifest fromJsonString(String json) {
    try {
      return JsonUtils.stringToObject(json, TabletManifest.class);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize TabletManifest", e);
    }
  }

  @Override
  public String toString() {
    return toJsonString();
  }
}
