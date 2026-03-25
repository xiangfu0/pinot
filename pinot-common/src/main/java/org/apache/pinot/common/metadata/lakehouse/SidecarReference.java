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
import javax.annotation.Nullable;


/**
 * Reference to a sidecar index artifact within a tablet manifest.
 *
 * <p>Sidecars are optional Pinot-specific acceleration artifacts. This reference describes where
 * the sidecar is stored, which microsegment it accelerates, and its version for staleness
 * detection.</p>
 *
 * <p>This class is immutable.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SidecarReference {

  @JsonPropertyDescription("Sidecar index type (e.g. INVERTED, RANGE, JSON)")
  private final String _indexType;

  @JsonPropertyDescription("Microsegment ID this sidecar applies to, or null for tablet-wide sidecars")
  @Nullable
  private final String _microsegmentId;

  @JsonPropertyDescription("URI to the sidecar artifact")
  private final String _sidecarUri;

  @JsonPropertyDescription("Sidecar version for staleness detection")
  private final String _sidecarVersion;

  @JsonPropertyDescription("Snapshot ID the sidecar was built from")
  private final long _builtFromSnapshotId;

  @JsonCreator
  public SidecarReference(
      @JsonProperty(value = "indexType", required = true) String indexType,
      @JsonProperty("microsegmentId") @Nullable String microsegmentId,
      @JsonProperty(value = "sidecarUri", required = true) String sidecarUri,
      @JsonProperty(value = "sidecarVersion", required = true) String sidecarVersion,
      @JsonProperty("builtFromSnapshotId") long builtFromSnapshotId) {
    _indexType = indexType;
    _microsegmentId = microsegmentId;
    _sidecarUri = sidecarUri;
    _sidecarVersion = sidecarVersion;
    _builtFromSnapshotId = builtFromSnapshotId;
  }

  @JsonProperty("indexType")
  public String getIndexType() {
    return _indexType;
  }

  @JsonProperty("microsegmentId")
  @Nullable
  public String getMicrosegmentId() {
    return _microsegmentId;
  }

  @JsonProperty("sidecarUri")
  public String getSidecarUri() {
    return _sidecarUri;
  }

  @JsonProperty("sidecarVersion")
  public String getSidecarVersion() {
    return _sidecarVersion;
  }

  @JsonProperty("builtFromSnapshotId")
  public long getBuiltFromSnapshotId() {
    return _builtFromSnapshotId;
  }
}
