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
package org.apache.pinot.controller.api.resources;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.lakehouse.LakehouseCatalogAdapter;
import org.apache.pinot.spi.lakehouse.LakehouseFileFormat;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotDescriptor;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotRequest;
import org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor;


/**
 * Test-only lakehouse catalog adapter used by controller API integration tests.
 */
public class TestingLakehouseCatalogAdapter
    implements LakehouseCatalogAdapter {
  @Override
  public LakehouseSnapshotDescriptor resolveSnapshot(LakehouseConfig lakehouseConfig, LakehouseSnapshotRequest request)
      throws IOException {
    LakehouseSnapshotDescriptor snapshotDescriptor = new LakehouseSnapshotDescriptor();
    snapshotDescriptor.setTableIdentifier(lakehouseConfig.getCatalogConfig().getTableIdentifier());
    snapshotDescriptor.setSnapshotId(request.getSnapshotId() != null ? request.getSnapshotId() : 101L);
    snapshotDescriptor.setSpecId(7);
    snapshotDescriptor.setCommittedAtMillis(1_700_000_000_000L);
    snapshotDescriptor.setManifestListUri("file:/tmp/testing-manifest-list.avro");
    snapshotDescriptor.setSummary(Map.of("branch", request.getBranch() != null ? request.getBranch() : "main",
        "tag", request.getTag() != null ? request.getTag() : ""));
    return snapshotDescriptor;
  }

  @Override
  public List<MicrosegmentDescriptor> listMicrosegments(LakehouseConfig lakehouseConfig,
      LakehouseSnapshotDescriptor snapshotDescriptor)
      throws IOException {
    return List.of(
        buildMicrosegment("west-1", "file:/west-1.parquet", 400L, 10L, 100L, 150L, Map.of("region", "west")),
        buildMicrosegment("west-2", "file:/west-2.parquet", 500L, 20L, 151L, 220L, Map.of("region", "west")),
        buildMicrosegment("east-1", "file:/east-1.parquet", 300L, 15L, 50L, 120L, Map.of("region", "east")));
  }

  private static MicrosegmentDescriptor buildMicrosegment(String microsegmentId, String filePath, long fileSizeBytes,
      long recordCount, long minTimeMillis, long maxTimeMillis, Map<String, String> partitionTuple) {
    MicrosegmentDescriptor microsegmentDescriptor = new MicrosegmentDescriptor();
    microsegmentDescriptor.setVersion(1);
    microsegmentDescriptor.setMicrosegmentId(microsegmentId);
    microsegmentDescriptor.setFilePath(filePath);
    microsegmentDescriptor.setFileFormat(LakehouseFileFormat.PARQUET);
    microsegmentDescriptor.setFileSizeBytes(fileSizeBytes);
    microsegmentDescriptor.setRecordCount(recordCount);
    microsegmentDescriptor.setMinTimeMillis(minTimeMillis);
    microsegmentDescriptor.setMaxTimeMillis(maxTimeMillis);
    microsegmentDescriptor.setPartitionTuple(partitionTuple);
    return microsegmentDescriptor;
  }
}
