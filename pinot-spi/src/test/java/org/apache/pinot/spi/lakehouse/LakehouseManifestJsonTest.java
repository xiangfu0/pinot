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
package org.apache.pinot.spi.lakehouse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class LakehouseManifestJsonTest {
  @Test
  public void testTabletManifestJsonRoundTrip()
      throws IOException {
    MicrosegmentDescriptor microsegmentDescriptor = new MicrosegmentDescriptor();
    microsegmentDescriptor.setVersion(1);
    microsegmentDescriptor.setMicrosegmentId("micro-0");
    microsegmentDescriptor.setFilePath("s3://bucket/table/data/file-0.parquet");
    microsegmentDescriptor.setFileFormat(LakehouseFileFormat.PARQUET);
    microsegmentDescriptor.setFileSizeBytes(1024L);
    microsegmentDescriptor.setRecordCount(55L);
    microsegmentDescriptor.setRowGroupCount(3);
    microsegmentDescriptor.setMinTimeMillis(10L);
    microsegmentDescriptor.setMaxTimeMillis(20L);
    microsegmentDescriptor.setDataSequenceNumber(7L);
    microsegmentDescriptor.setPartitionTuple(Map.of("dt", "2026-03-25"));
    microsegmentDescriptor.setDeleteFilePaths(List.of());

    TabletManifest tabletManifest = new TabletManifest();
    tabletManifest.setVersion(1);
    tabletManifest.setTabletId("tablet-1");
    tabletManifest.setTableNameWithType("lakehouse_OFFLINE");
    tabletManifest.setTableIdentifier("db.table");
    tabletManifest.setSnapshotId(12345L);
    tabletManifest.setSpecId(4);
    tabletManifest.setGeneration(2);
    tabletManifest.setPartitionTuple(Map.of("dt", "2026-03-25"));
    tabletManifest.setMicrosegments(List.of(microsegmentDescriptor));
    tabletManifest.setSidecarManifestUris(List.of("s3://bucket/sidecars/tablet-1.json"));
    tabletManifest.setMetadata(Map.of("planner", "phase1"));

    TabletManifest roundTrip = JsonUtils.stringToObject(tabletManifest.toJsonString(), TabletManifest.class);
    assertEquals(roundTrip, tabletManifest);
    assertEquals(roundTrip.getMicrosegments().get(0).getMicrosegmentId(), "micro-0");
    assertEquals(roundTrip.getMicrosegments().get(0).getFileFormat(), LakehouseFileFormat.PARQUET);

    TabletMetadataEnvelope envelope = new TabletMetadataEnvelope();
    envelope.setVersion(1);
    envelope.setTabletId("tablet-1");
    envelope.setTableNameWithType("lakehouse_OFFLINE");
    envelope.setSnapshotId(12345L);
    envelope.setSpecId(4);
    envelope.setGeneration(2);
    envelope.setMinTimeMillis(10L);
    envelope.setMaxTimeMillis(20L);
    envelope.setApproximateRowCount(55L);
    envelope.setApproximateSizeBytes(1024L);
    envelope.setMicrosegmentCount(1);
    envelope.setManifestUri("s3://bucket/manifests/tablet-1.json");
    envelope.setManifestVersion(9L);
    envelope.setState("ONLINE");
    envelope.setPartitionTuple(Map.of("dt", "2026-03-25"));

    TabletMetadataEnvelope envelopeRoundTrip =
        JsonUtils.stringToObject(envelope.toJsonString(), TabletMetadataEnvelope.class);
    assertEquals(envelopeRoundTrip, envelope);
    assertEquals(envelopeRoundTrip.getManifestVersion(), Long.valueOf(9L));

    SidecarManifest sidecarManifest = new SidecarManifest();
    sidecarManifest.setVersion(1);
    sidecarManifest.setTabletId("tablet-1");
    sidecarManifest.setSnapshotId(12345L);
    sidecarManifest.setSidecarVersion("v1");
    sidecarManifest.setGeneratedAtMillis(999L);
    sidecarManifest.setArtifactUris(Map.of("range", "s3://bucket/sidecars/range.idx"));

    SidecarManifest sidecarRoundTrip =
        JsonUtils.stringToObject(sidecarManifest.toJsonString(), SidecarManifest.class);
    assertEquals(sidecarRoundTrip, sidecarManifest);
    assertNotNull(sidecarRoundTrip.getArtifactUris());
  }
}
