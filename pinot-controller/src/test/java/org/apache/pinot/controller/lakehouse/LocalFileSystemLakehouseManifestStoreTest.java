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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.lakehouse.LakehouseFileFormat;
import org.apache.pinot.spi.lakehouse.TabletManifest;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for controller-side lakehouse manifest persistence.
 */
public class LocalFileSystemLakehouseManifestStoreTest {
  @Test
  public void testPersistAndFetchTabletManifestRoundTrips()
      throws Exception {
    Path rootDirectory = Files.createTempDirectory("lakehouse-manifest-store");
    try {
      LocalFileSystemLakehouseManifestStore manifestStore =
          new LocalFileSystemLakehouseManifestStore(rootDirectory);
      TabletManifest manifest = buildManifest();

      String manifestUri = manifestStore.persistTabletManifest(manifest);
      Path manifestPath = Path.of(java.net.URI.create(manifestUri));

      Assert.assertTrue(Files.exists(manifestPath));
      Assert.assertTrue(manifestPath.startsWith(rootDirectory));

      TabletManifest reloadedManifest = manifestStore.fetchTabletManifest(manifestUri);
      Assert.assertEquals(reloadedManifest.getTabletId(), manifest.getTabletId());
      Assert.assertEquals(reloadedManifest.getTableNameWithType(), manifest.getTableNameWithType());
      Assert.assertEquals(reloadedManifest.getTableIdentifier(), manifest.getTableIdentifier());
      Assert.assertEquals(reloadedManifest.getSnapshotId(), manifest.getSnapshotId());
      Assert.assertEquals(reloadedManifest.getPartitionTuple(), manifest.getPartitionTuple());
      Assert.assertEquals(reloadedManifest.getMicrosegments().size(), manifest.getMicrosegments().size());
    } finally {
      deleteRecursively(rootDirectory);
    }
  }

  private static TabletManifest buildManifest()
      throws IOException {
    TabletManifest manifest = new TabletManifest();
    manifest.setVersion(1);
    manifest.setTabletId("snapshot-123-tablet-0");
    manifest.setTableNameWithType("lakehouse_OFFLINE");
    manifest.setTableIdentifier("db.lakehouse");
    manifest.setSnapshotId(123L);
    manifest.setSpecId(7);
    manifest.setGeneration(1);
    manifest.setPartitionTuple(Map.of("region", "west"));
    manifest.setMicrosegments(List.of(buildMicrosegment("segment-1")));
    manifest.setSidecarManifestUris(List.of("file:/tmp/sidecar.avro"));
    manifest.setMetadata(Map.of("branch", "main"));
    return manifest;
  }

  private static org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor buildMicrosegment(String microsegmentId) {
    org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor microsegmentDescriptor =
        new org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor();
    microsegmentDescriptor.setVersion(1);
    microsegmentDescriptor.setMicrosegmentId(microsegmentId);
    microsegmentDescriptor.setFilePath("file:/tmp/" + microsegmentId + ".parquet");
    microsegmentDescriptor.setFileFormat(LakehouseFileFormat.PARQUET);
    microsegmentDescriptor.setFileSizeBytes(128L);
    microsegmentDescriptor.setRecordCount(12L);
    return microsegmentDescriptor;
  }

  private static void deleteRecursively(Path rootDirectory)
      throws IOException {
    try (var paths = Files.walk(rootDirectory)) {
      paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }
}
