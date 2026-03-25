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
package org.apache.pinot.common.metadata.segment;

import java.util.Map;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SegmentZKMetadataUtilsTest {
  @Test
  public void testCreateLakehouseTabletSegmentZKMetadata() {
    TabletMetadataEnvelope tabletMetadataEnvelope = new TabletMetadataEnvelope();
    tabletMetadataEnvelope.setTabletId("tablet-7");
    tabletMetadataEnvelope.setSnapshotId(1234L);
    tabletMetadataEnvelope.setSpecId(9);
    tabletMetadataEnvelope.setMinTimeMillis(100L);
    tabletMetadataEnvelope.setMaxTimeMillis(200L);
    tabletMetadataEnvelope.setApproximateRowCount(300L);
    tabletMetadataEnvelope.setApproximateSizeBytes(400L);
    tabletMetadataEnvelope.setManifestUri("file:/tmp/tablet-7.json");
    tabletMetadataEnvelope.setManifestVersion(500L);

    SegmentZKMetadata segmentZKMetadata =
        SegmentZKMetadataUtils.createLakehouseTabletSegmentZKMetadata(tabletMetadataEnvelope);

    assertEquals(segmentZKMetadata.getSegmentName(), "tablet-7");
    assertEquals(segmentZKMetadata.getStartTimeMs(), 100L);
    assertEquals(segmentZKMetadata.getEndTimeMs(), 200L);
    assertEquals(segmentZKMetadata.toZNRecord().getSimpleField(CommonConstants.Segment.TIME_UNIT), "MILLISECONDS");
    assertEquals(segmentZKMetadata.getTotalDocs(), 300L);
    assertEquals(segmentZKMetadata.getSizeInBytes(), 400L);
    assertEquals(segmentZKMetadata.getCrc(), 500L);
    assertTrue(SegmentZKMetadataUtils.isLakehouseTabletSegment(segmentZKMetadata));
    assertEquals(SegmentZKMetadataUtils.getLakehouseManifestUri(segmentZKMetadata), "file:/tmp/tablet-7.json");
    assertEquals(segmentZKMetadata.getCustomMap(), Map.of(
        CommonConstants.Segment.Lakehouse.SEGMENT_KIND, CommonConstants.Segment.Lakehouse.TABLET_SEGMENT_KIND,
        CommonConstants.Segment.Lakehouse.TABLET_ID, "tablet-7",
        CommonConstants.Segment.Lakehouse.MANIFEST_URI, "file:/tmp/tablet-7.json",
        CommonConstants.Segment.Lakehouse.MANIFEST_VERSION, "500",
        CommonConstants.Segment.Lakehouse.SNAPSHOT_ID, "1234",
        CommonConstants.Segment.Lakehouse.SPEC_ID, "9"));
  }
}
