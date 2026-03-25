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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class TabletManifestTest {

  private MicrosegmentDescriptor createDataFile(String id, String path, long size, long rows) {
    return new MicrosegmentDescriptor(id, path, "PARQUET", size, rows, 1705276800000L, 1705363200000L, null, null, null,
        0);
  }

  private MicrosegmentDescriptor createDeleteFile(String id, String path, long size, long rows) {
    return new MicrosegmentDescriptor(id, path, "PARQUET", size, rows, 0L, 0L, null, null, null, 1);
  }

  @Test
  public void testBasicManifest() {
    List<MicrosegmentDescriptor> microsegments = Arrays.asList(
        createDataFile("ms-001", "s3://data/file1.parquet", 100_000_000L, 500000L),
        createDataFile("ms-002", "s3://data/file2.parquet", 150_000_000L, 750000L)
    );

    TabletManifest manifest =
        new TabletManifest("tablet-001", 1, "uuid-123", 1234567890L, 0, microsegments, null, null, 0L, 0L);

    assertEquals(manifest.getTabletId(), "tablet-001");
    assertEquals(manifest.getVersion(), TabletManifest.CURRENT_VERSION);
    assertEquals(manifest.getIcebergTableUuid(), "uuid-123");
    assertEquals(manifest.getSnapshotId(), 1234567890L);
    assertEquals(manifest.getMicrosegments().size(), 2);
    assertNull(manifest.getDeleteFiles());
    assertNull(manifest.getSidecarReferences());
    assertEquals(manifest.getTotalRowCount(), 1250000L);
    assertEquals(manifest.getTotalSizeBytes(), 250_000_000L);
  }

  @Test
  public void testManifestWithDeleteFiles() {
    List<MicrosegmentDescriptor> dataFiles = Collections.singletonList(
        createDataFile("ms-001", "s3://data/file1.parquet", 100_000_000L, 500000L));
    List<MicrosegmentDescriptor> deleteFiles = Collections.singletonList(
        createDeleteFile("del-001", "s3://data/delete1.parquet", 1000L, 100L));

    TabletManifest manifest =
        new TabletManifest("tablet-002", 1, "uuid-456", 9876543210L, 0, dataFiles, deleteFiles, null, 0L, 0L);

    assertNotNull(manifest.getDeleteFiles());
    assertEquals(manifest.getDeleteFiles().size(), 1);
    assertTrue(manifest.getDeleteFiles().get(0).isPositionDeleteFile());
  }

  @Test
  public void testManifestWithSidecarReferences() {
    List<MicrosegmentDescriptor> dataFiles = Collections.singletonList(
        createDataFile("ms-001", "s3://data/file1.parquet", 100_000_000L, 500000L));
    List<SidecarReference> sidecars = Arrays.asList(
        new SidecarReference("INVERTED", "ms-001", "s3://sidecars/ms-001/inverted.idx", "v1", 1234567890L),
        new SidecarReference("RANGE", "ms-001", "s3://sidecars/ms-001/range.idx", "v1", 1234567890L));

    TabletManifest manifest =
        new TabletManifest("tablet-003", 1, null, 1234567890L, 0, dataFiles, null, sidecars, 0L, 0L);

    assertNotNull(manifest.getSidecarReferences());
    assertEquals(manifest.getSidecarReferences().size(), 2);
    assertEquals(manifest.getSidecarReferences().get(0).getIndexType(), "INVERTED");
    assertEquals(manifest.getSidecarReferences().get(0).getMicrosegmentId(), "ms-001");
  }

  @Test
  public void testJsonRoundTrip() {
    List<MicrosegmentDescriptor> microsegments = Arrays.asList(
        createDataFile("ms-001", "s3://data/file1.parquet", 100_000_000L, 500000L),
        createDataFile("ms-002", "s3://data/file2.parquet", 200_000_000L, 1000000L));

    TabletManifest original =
        new TabletManifest("tablet-004", 1, "uuid-789", 1111111111L, 2, microsegments, null, null,
            1705276800000L, 1705363200000L);

    String json = original.toJsonString();
    assertNotNull(json);

    TabletManifest deserialized = TabletManifest.fromJsonString(json);
    assertEquals(deserialized.getTabletId(), "tablet-004");
    assertEquals(deserialized.getIcebergTableUuid(), "uuid-789");
    assertEquals(deserialized.getSnapshotId(), 1111111111L);
    assertEquals(deserialized.getSpecId(), 2);
    assertEquals(deserialized.getMicrosegments().size(), 2);
    assertEquals(deserialized.getMicrosegments().get(0).getFilePath(), "s3://data/file1.parquet");
    assertEquals(deserialized.getTotalRowCount(), 1500000L);
    assertEquals(deserialized.getCommittedWatermarkMs(), 1705276800000L);
  }

  @Test
  public void testMicrosegmentDescriptorWithStats() {
    MicrosegmentDescriptor ms = new MicrosegmentDescriptor("ms-stats", "s3://data/stats.parquet", "PARQUET",
        500_000_000L, 2000000L, 1705276800000L, 1705363200000L, Map.of("col_a", "100", "col_b", "abc"),
        Map.of("col_a", "999", "col_b", "xyz"), Map.of("col_a", 0L, "col_b", 50L), 0);

    assertTrue(ms.isDataFile());
    assertNotNull(ms.getColumnLowerBounds());
    assertEquals(ms.getColumnLowerBounds().get("col_a"), "100");
    assertNotNull(ms.getColumnUpperBounds());
    assertEquals(ms.getColumnUpperBounds().get("col_b"), "xyz");
    assertNotNull(ms.getColumnNullCounts());
    assertEquals(ms.getColumnNullCounts().get("col_b").longValue(), 50L);
  }

  @Test
  public void testMicrosegmentContentTypes() {
    MicrosegmentDescriptor data = new MicrosegmentDescriptor("d", "path", "PARQUET", 0, 0, 0, 0, null, null, null, 0);
    MicrosegmentDescriptor posDelete =
        new MicrosegmentDescriptor("p", "path", "PARQUET", 0, 0, 0, 0, null, null, null, 1);
    MicrosegmentDescriptor eqDelete =
        new MicrosegmentDescriptor("e", "path", "PARQUET", 0, 0, 0, 0, null, null, null, 2);

    assertTrue(data.isDataFile());
    assertTrue(posDelete.isPositionDeleteFile());
    assertTrue(eqDelete.isEqualityDeleteFile());
  }

  @Test
  public void testForwardCompatibility()
      throws Exception {
    String futureJson = "{\"tabletId\":\"t-future\",\"version\":99,\"snapshotId\":1,"
        + "\"specId\":0,\"microsegments\":[],\"newUnknownField\":\"hello\","
        + "\"committedWatermarkMs\":0,\"createdAtMs\":0}";
    TabletManifest manifest = TabletManifest.fromJsonString(futureJson);
    assertEquals(manifest.getTabletId(), "t-future");
    assertEquals(manifest.getVersion(), 99);
    assertTrue(manifest.getMicrosegments().isEmpty());
  }

  @Test
  public void testEmptyMicrosegmentsList() {
    TabletManifest manifest =
        new TabletManifest("tablet-empty", 1, null, 100L, 0, Collections.emptyList(), null, null, 0L, 0L);
    assertEquals(manifest.getTotalRowCount(), 0L);
    assertEquals(manifest.getTotalSizeBytes(), 0L);
    assertTrue(manifest.getMicrosegments().isEmpty());
  }
}
