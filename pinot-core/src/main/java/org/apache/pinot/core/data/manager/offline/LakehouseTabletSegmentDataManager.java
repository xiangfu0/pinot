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
package org.apache.pinot.core.data.manager.offline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor;
import org.apache.pinot.spi.lakehouse.TabletManifest;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Segment-data-manager wrapper for one tablet that expands into multiple lakehouse microsegments at query time.
 *
 * <p>The primary segment represents the tablet envelope for metadata/accounting, while {@link #getSegments()} exposes
 * per-microsegment placeholders to the existing query-time flattening seam.</p>
 */
public class LakehouseTabletSegmentDataManager extends SegmentDataManager {
  private static final String MICROSEGMENT_ID_KEY = "segment.lakehouse.microsegmentId";
  private static final String MICROSEGMENT_FILE_PATH_KEY = "segment.lakehouse.microsegmentFilePath";
  private static final String MICROSEGMENT_FILE_FORMAT_KEY = "segment.lakehouse.microsegmentFileFormat";

  private final ImmutableSegment _tabletSegment;
  private final List<IndexSegment> _microsegments;

  LakehouseTabletSegmentDataManager(ImmutableSegment tabletSegment, List<IndexSegment> microsegments) {
    _tabletSegment = tabletSegment;
    _microsegments = List.copyOf(microsegments);
  }

  public static LakehouseTabletSegmentDataManager create(String tableNameWithType, @Nullable TableConfig tableConfig,
      Schema schema, SegmentZKMetadata zkMetadata, TabletMetadataEnvelope envelope, TabletManifest manifest) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    String tabletId = manifest.getTabletId() != null ? manifest.getTabletId() : zkMetadata.getSegmentName();
    long manifestVersion =
        envelope.getManifestVersion() != null ? envelope.getManifestVersion() : Math.max(0L, zkMetadata.getCrc());
    long snapshotId = envelope.getSnapshotId() > 0 ? envelope.getSnapshotId() : manifest.getSnapshotId();
    @Nullable String timeColumn = tableConfig != null ? tableConfig.getValidationConfig().getTimeColumnName() : null;
    long creationTime = zkMetadata.getCreationTime() > 0 ? zkMetadata.getCreationTime() : System.currentTimeMillis();
    Map<String, String> baseCustomMap =
        zkMetadata.getCustomMap() != null ? new HashMap<>(zkMetadata.getCustomMap()) : new HashMap<>();

    List<MicrosegmentDescriptor> microsegmentDescriptors =
        manifest.getMicrosegments() != null ? manifest.getMicrosegments() : Collections.emptyList();
    List<IndexSegment> microsegments = new ArrayList<>(microsegmentDescriptors.size());
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    long totalSizeBytes = 0L;
    long totalDocs = 0L;
    int ordinal = 0;
    for (MicrosegmentDescriptor descriptor : microsegmentDescriptors) {
      long microsegmentSize = nullToZero(descriptor.getFileSizeBytes());
      long microsegmentDocs = nullToZero(descriptor.getRecordCount());
      totalSizeBytes += microsegmentSize;
      totalDocs += microsegmentDocs;
      minTime = min(minTime, descriptor.getMinTimeMillis());
      maxTime = max(maxTime, descriptor.getMaxTimeMillis());

      String microsegmentName = buildMicrosegmentName(tabletId, descriptor, ordinal++);
      Map<String, String> microsegmentCustomMap = new HashMap<>(baseCustomMap);
      if (descriptor.getMicrosegmentId() != null) {
        microsegmentCustomMap.put(MICROSEGMENT_ID_KEY, descriptor.getMicrosegmentId());
      }
      if (descriptor.getFilePath() != null) {
        microsegmentCustomMap.put(MICROSEGMENT_FILE_PATH_KEY, descriptor.getFilePath());
      }
      if (descriptor.getFileFormat() != null) {
        microsegmentCustomMap.put(MICROSEGMENT_FILE_FORMAT_KEY, descriptor.getFileFormat().name());
      }
      microsegmentCustomMap.put(CommonConstants.Segment.Lakehouse.TABLET_ID, tabletId);
      microsegments.add(new LakehousePlaceholderImmutableSegment(new LakehouseSegmentMetadata(rawTableName,
          microsegmentName, timeColumn, normalizeTime(descriptor.getMinTimeMillis()),
          normalizeTime(descriptor.getMaxTimeMillis()), descriptor.getMinTimeMillis() != null
          && descriptor.getMaxTimeMillis() != null ? TimeUnit.MILLISECONDS : null,
          Long.toString(resolveMicrosegmentVersion(snapshotId, descriptor)), Long.toString(snapshotId), schema,
          toDocCount(microsegmentDocs), creationTime, microsegmentCustomMap), microsegmentSize));
    }

    long envelopeDocs = nullToZero(envelope.getApproximateRowCount());
    long envelopeSizeBytes = nullToZero(envelope.getApproximateSizeBytes());
    long primaryDocs = envelopeDocs > 0 ? envelopeDocs : totalDocs;
    long primarySizeBytes = envelopeSizeBytes > 0 ? envelopeSizeBytes : totalSizeBytes;
    long primaryMinTime = envelope.getMinTimeMillis() != null ? envelope.getMinTimeMillis()
        : (minTime != Long.MAX_VALUE ? minTime : -1L);
    long primaryMaxTime = envelope.getMaxTimeMillis() != null ? envelope.getMaxTimeMillis()
        : (maxTime != Long.MIN_VALUE ? maxTime : -1L);

    ImmutableSegment tabletSegment = new LakehousePlaceholderImmutableSegment(new LakehouseSegmentMetadata(rawTableName,
        tabletId, timeColumn, primaryMinTime, primaryMaxTime, primaryMinTime >= 0 && primaryMaxTime >= 0
        ? TimeUnit.MILLISECONDS : null, Long.toString(manifestVersion), Long.toString(snapshotId), schema,
        toDocCount(primaryDocs), creationTime, baseCustomMap), primarySizeBytes);
    return new LakehouseTabletSegmentDataManager(tabletSegment, microsegments);
  }

  @Override
  public String getSegmentName() {
    return _tabletSegment.getSegmentName();
  }

  @Override
  public ImmutableSegment getSegment() {
    return _tabletSegment;
  }

  @Override
  public boolean hasMultiSegments() {
    return !_microsegments.isEmpty();
  }

  @Override
  public List<IndexSegment> getSegments() {
    return _microsegments;
  }

  @Override
  public void doOffload() {
    _tabletSegment.offload();
    for (IndexSegment microsegment : _microsegments) {
      microsegment.offload();
    }
  }

  @Override
  protected void doDestroy() {
    _tabletSegment.destroy();
    for (IndexSegment microsegment : _microsegments) {
      microsegment.destroy();
    }
  }

  private static String buildMicrosegmentName(String tabletId, MicrosegmentDescriptor descriptor, int ordinal) {
    if (descriptor.getMicrosegmentId() != null) {
      return tabletId + "$" + descriptor.getMicrosegmentId();
    }
    return tabletId + "$m" + ordinal;
  }

  private static long resolveMicrosegmentVersion(long snapshotId, MicrosegmentDescriptor descriptor) {
    if (descriptor.getDataSequenceNumber() != null && descriptor.getDataSequenceNumber() >= 0) {
      return descriptor.getDataSequenceNumber();
    }
    return snapshotId;
  }

  private static int toDocCount(long value) {
    if (value <= 0) {
      return 0;
    }
    return (int) Math.min(Integer.MAX_VALUE, value);
  }

  private static long nullToZero(@Nullable Long value) {
    return value != null ? value : 0L;
  }

  private static long normalizeTime(@Nullable Long value) {
    return value != null ? value : -1L;
  }

  private static long min(long currentMin, @Nullable Long candidate) {
    return candidate != null ? Math.min(currentMin, candidate) : currentMin;
  }

  private static long max(long currentMax, @Nullable Long candidate) {
    return candidate != null ? Math.max(currentMax, candidate) : currentMax;
  }
}
