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
package org.apache.pinot.controller.lakehouse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseTabletConfig;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotDescriptor;
import org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor;
import org.apache.pinot.spi.lakehouse.TabletManifest;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Groups microsegments into Pinot tablets and materializes both the full manifest and the broker-visible envelope.
 *
 * <p>The planner is deterministic for a fixed snapshot and tablet config. It first groups by partition tuple, then
 * batches by the configured file-count and byte-size targets, and finally enforces the maximum serialized envelope
 * size.</p>
 */
public class LakehouseTabletPlanner {
  private static final String COMMITTED_STATE = "COMMITTED";

  public List<TabletManifest> planTabletManifests(String tableNameWithType, LakehouseConfig lakehouseConfig,
      LakehouseSnapshotDescriptor snapshotDescriptor, List<MicrosegmentDescriptor> microsegments) {
    LakehouseTabletConfig tabletConfig = lakehouseConfig.getTabletConfig() != null ? lakehouseConfig.getTabletConfig()
        : new LakehouseTabletConfig();
    Map<String, List<MicrosegmentDescriptor>> microsegmentsByPartition = groupByPartitionTuple(microsegments);
    List<TabletManifest> tabletManifests = new ArrayList<>();
    int tabletOrdinal = 0;
    for (List<MicrosegmentDescriptor> partitionMicrosegments : microsegmentsByPartition.values()) {
      List<MicrosegmentDescriptor> currentTablet = new ArrayList<>();
      long currentSizeBytes = 0L;
      for (MicrosegmentDescriptor microsegmentDescriptor : partitionMicrosegments) {
        boolean shouldFlushCurrentTablet = !currentTablet.isEmpty() && (currentTablet.size()
            >= tabletConfig.getTargetFilesPerTablet() || currentSizeBytes >= tabletConfig.getTargetBytesPerTablet());
        if (shouldFlushCurrentTablet) {
          tabletManifests.add(buildManifest(tableNameWithType, snapshotDescriptor, currentTablet, tabletOrdinal++));
          currentTablet = new ArrayList<>();
          currentSizeBytes = 0L;
        }

        currentTablet.add(copyMicrosegment(microsegmentDescriptor));
        currentSizeBytes += nullToZero(microsegmentDescriptor.getFileSizeBytes());

        if (currentTablet.size() >= tabletConfig.getTargetFilesPerTablet()
            || currentSizeBytes >= tabletConfig.getTargetBytesPerTablet()) {
          tabletManifests.add(buildManifest(tableNameWithType, snapshotDescriptor, currentTablet, tabletOrdinal++));
          currentTablet = new ArrayList<>();
          currentSizeBytes = 0L;
        }
      }

      if (!currentTablet.isEmpty()) {
        tabletManifests.add(buildManifest(tableNameWithType, snapshotDescriptor, currentTablet, tabletOrdinal++));
      }
    }
    return tabletManifests;
  }

  public TabletMetadataEnvelope buildEnvelope(TabletManifest manifest, String manifestUri,
      LakehouseConfig lakehouseConfig) {
    TabletMetadataEnvelope tabletMetadataEnvelope = new TabletMetadataEnvelope();
    tabletMetadataEnvelope.setVersion(1);
    tabletMetadataEnvelope.setTabletId(manifest.getTabletId());
    tabletMetadataEnvelope.setTableNameWithType(manifest.getTableNameWithType());
    tabletMetadataEnvelope.setSnapshotId(manifest.getSnapshotId());
    tabletMetadataEnvelope.setSpecId(manifest.getSpecId());
    tabletMetadataEnvelope.setGeneration(manifest.getGeneration());
    tabletMetadataEnvelope.setMinTimeMillis(minTimeMillis(manifest.getMicrosegments()));
    tabletMetadataEnvelope.setMaxTimeMillis(maxTimeMillis(manifest.getMicrosegments()));
    tabletMetadataEnvelope.setApproximateRowCount(sumRecordCount(manifest.getMicrosegments()));
    tabletMetadataEnvelope.setApproximateSizeBytes(sumFileSizeBytes(manifest.getMicrosegments()));
    tabletMetadataEnvelope.setMicrosegmentCount(manifest.getMicrosegments() != null ? manifest.getMicrosegments().size()
        : 0);
    tabletMetadataEnvelope.setManifestUri(manifestUri);
    tabletMetadataEnvelope.setManifestVersion(calculateManifestVersion(manifest));
    tabletMetadataEnvelope.setState(COMMITTED_STATE);
    tabletMetadataEnvelope.setPartitionTuple(manifest.getPartitionTuple());

    int maxEnvelopeBytes = lakehouseConfig.getTabletConfig() != null ? lakehouseConfig.getTabletConfig()
        .getMaxEnvelopeBytes() : new LakehouseTabletConfig().getMaxEnvelopeBytes();
    ensureEnvelopeFits(tabletMetadataEnvelope, maxEnvelopeBytes);
    return tabletMetadataEnvelope;
  }

  private Map<String, List<MicrosegmentDescriptor>> groupByPartitionTuple(List<MicrosegmentDescriptor> microsegments) {
    Map<String, List<MicrosegmentDescriptor>> grouped = new LinkedHashMap<>();
    List<MicrosegmentDescriptor> sortedMicrosegments = new ArrayList<>(microsegments);
    sortedMicrosegments.sort(Comparator.comparing(LakehouseTabletPlanner::partitionKey)
        .thenComparing(MicrosegmentDescriptor::getFilePath, Comparator.nullsLast(String::compareTo))
        .thenComparing(MicrosegmentDescriptor::getMicrosegmentId, Comparator.nullsLast(String::compareTo)));
    for (MicrosegmentDescriptor microsegmentDescriptor : sortedMicrosegments) {
      grouped.computeIfAbsent(partitionKey(microsegmentDescriptor), ignored -> new ArrayList<>())
          .add(microsegmentDescriptor);
    }
    return grouped;
  }

  private TabletManifest buildManifest(String tableNameWithType, LakehouseSnapshotDescriptor snapshotDescriptor,
      List<MicrosegmentDescriptor> microsegments, int tabletOrdinal) {
    TabletManifest tabletManifest = new TabletManifest();
    tabletManifest.setVersion(1);
    tabletManifest.setTabletId("snapshot-" + snapshotDescriptor.getSnapshotId() + "-tablet-" + tabletOrdinal);
    tabletManifest.setTableNameWithType(tableNameWithType);
    tabletManifest.setTableIdentifier(snapshotDescriptor.getTableIdentifier());
    tabletManifest.setSnapshotId(snapshotDescriptor.getSnapshotId());
    tabletManifest.setSpecId(snapshotDescriptor.getSpecId());
    tabletManifest.setGeneration(1);
    tabletManifest.setPartitionTuple(normalizePartitionTuple(microsegments.get(0).getPartitionTuple()));
    tabletManifest.setMicrosegments(microsegments);
    return tabletManifest;
  }

  private void ensureEnvelopeFits(TabletMetadataEnvelope tabletMetadataEnvelope, int maxEnvelopeBytes) {
    try {
      int envelopeBytes = JsonUtils.objectToBytes(tabletMetadataEnvelope).length;
      if (envelopeBytes > maxEnvelopeBytes) {
        throw new IllegalStateException("Planned lakehouse tablet envelope '" + tabletMetadataEnvelope.getTabletId()
            + "' exceeds lakehouseConfig.tabletConfig.maxEnvelopeBytes: " + envelopeBytes + " > "
            + maxEnvelopeBytes);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize lakehouse tablet envelope", e);
    }
  }

  private long calculateManifestVersion(TabletManifest manifest) {
    try {
      return Integer.toUnsignedLong(JsonUtils.objectToString(manifest).hashCode());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to hash lakehouse tablet manifest", e);
    }
  }

  private static String partitionKey(MicrosegmentDescriptor microsegmentDescriptor) {
    Map<String, String> partitionTuple = normalizePartitionTuple(microsegmentDescriptor.getPartitionTuple());
    try {
      return partitionTuple != null ? JsonUtils.objectToString(partitionTuple) : "";
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to build lakehouse partition key", e);
    }
  }

  @Nullable
  private static Map<String, String> normalizePartitionTuple(@Nullable Map<String, String> partitionTuple) {
    return partitionTuple != null && !partitionTuple.isEmpty() ? new TreeMap<>(partitionTuple) : null;
  }

  private static MicrosegmentDescriptor copyMicrosegment(MicrosegmentDescriptor source) {
    MicrosegmentDescriptor copy = new MicrosegmentDescriptor();
    copy.setVersion(source.getVersion());
    copy.setMicrosegmentId(source.getMicrosegmentId());
    copy.setFilePath(source.getFilePath());
    copy.setFileFormat(source.getFileFormat());
    copy.setFileSizeBytes(source.getFileSizeBytes());
    copy.setRecordCount(source.getRecordCount());
    copy.setRowGroupCount(source.getRowGroupCount());
    copy.setMinTimeMillis(source.getMinTimeMillis());
    copy.setMaxTimeMillis(source.getMaxTimeMillis());
    copy.setDataSequenceNumber(source.getDataSequenceNumber());
    copy.setPartitionTuple(normalizePartitionTuple(source.getPartitionTuple()));
    copy.setDeleteFilePaths(source.getDeleteFilePaths());
    return copy;
  }

  @Nullable
  private static Long minTimeMillis(@Nullable List<MicrosegmentDescriptor> microsegments) {
    Long minTimeMillis = null;
    if (microsegments == null) {
      return null;
    }
    for (MicrosegmentDescriptor microsegmentDescriptor : microsegments) {
      if (microsegmentDescriptor.getMinTimeMillis() != null) {
        minTimeMillis = minTimeMillis == null ? microsegmentDescriptor.getMinTimeMillis()
            : Math.min(minTimeMillis, microsegmentDescriptor.getMinTimeMillis());
      }
    }
    return minTimeMillis;
  }

  @Nullable
  private static Long maxTimeMillis(@Nullable List<MicrosegmentDescriptor> microsegments) {
    Long maxTimeMillis = null;
    if (microsegments == null) {
      return null;
    }
    for (MicrosegmentDescriptor microsegmentDescriptor : microsegments) {
      if (microsegmentDescriptor.getMaxTimeMillis() != null) {
        maxTimeMillis = maxTimeMillis == null ? microsegmentDescriptor.getMaxTimeMillis()
            : Math.max(maxTimeMillis, microsegmentDescriptor.getMaxTimeMillis());
      }
    }
    return maxTimeMillis;
  }

  private static long sumRecordCount(@Nullable List<MicrosegmentDescriptor> microsegments) {
    long recordCount = 0L;
    if (microsegments != null) {
      for (MicrosegmentDescriptor microsegmentDescriptor : microsegments) {
        recordCount += nullToZero(microsegmentDescriptor.getRecordCount());
      }
    }
    return recordCount;
  }

  private static long sumFileSizeBytes(@Nullable List<MicrosegmentDescriptor> microsegments) {
    long fileSizeBytes = 0L;
    if (microsegments != null) {
      for (MicrosegmentDescriptor microsegmentDescriptor : microsegments) {
        fileSizeBytes += nullToZero(microsegmentDescriptor.getFileSizeBytes());
      }
    }
    return fileSizeBytes;
  }

  private static long nullToZero(@Nullable Long value) {
    return value != null ? value : 0L;
  }
}
