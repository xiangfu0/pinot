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
package org.apache.pinot.controller.helix.core.lakehouse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.config.table.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.LakehouseConfig;
import org.apache.pinot.spi.config.table.LakehouseReadConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TabletConfig;
import org.apache.pinot.spi.lakehouse.LakehouseCatalogAdapter;
import org.apache.pinot.spi.lakehouse.LakehouseFieldDescriptor;
import org.apache.pinot.spi.lakehouse.LakehouseFileDescriptor;
import org.apache.pinot.spi.lakehouse.TabletManifestStore;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Scale tests for the lakehouse pipeline that validate bounded tablet count, envelope serialization
 * size, total ZK metadata size, and grouping performance with large file counts.
 *
 * <p>These tests verify the core design constraint: "don't create one Helix partition per file."
 * With 100,000 files and default config (128 files per tablet), we expect ~782 tablets, keeping
 * control-plane metadata bounded.</p>
 */
public class LakehouseScaleTest {
  private InMemoryManifestStore _manifestStore;
  private MockCatalogAdapter _mockAdapter;
  private LakehouseTableManager _manager;

  @BeforeMethod
  public void setUp() {
    _manifestStore = new InMemoryManifestStore();
    _mockAdapter = new MockCatalogAdapter();
    LakehouseTableManager.CatalogAdapterFactory factory = config -> _mockAdapter;
    _manager = new LakehouseTableManager(_manifestStore, factory);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    _manager.close();
  }

  /**
   * 100,000 files with default config (128 files per tablet) -> ~782 tablets.
   * This validates the core "don't create one Helix partition per file" constraint.
   */
  @Test
  public void testLargeFileCountBoundedTablets() {
    int fileCount = 100_000;
    _mockAdapter.setSnapshotId(1001L);
    _mockAdapter.setDataFiles(createDataFiles(fileCount, 50_000_000L, 10000L));

    TableConfig tableConfig = createLakehouseTableConfig("scale_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("scale_OFFLINE", tableConfig);

    // Default: 128 files per tablet -> ceil(100000/128) = 782 tablets
    int expectedTablets = (fileCount + TabletConfig.DEFAULT_TARGET_FILES_PER_TABLET - 1)
        / TabletConfig.DEFAULT_TARGET_FILES_PER_TABLET;
    assertEquals(tablets.size(), expectedTablets);

    // Verify the bound: tablets << files
    assertTrue(tablets.size() < fileCount / 10, "Tablet count should be much less than file count");
    assertTrue(tablets.size() < 1000, "Tablet count should be bounded under 1000 for 100k files");

    // Verify total microsegment count matches file count
    int totalMicrosegments = 0;
    for (TabletMetadataEnvelope envelope : tablets) {
      totalMicrosegments += envelope.getMicrosegmentCount();
    }
    assertEquals(totalMicrosegments, fileCount);
  }

  /**
   * Each envelope should be small enough for ZooKeeper (< 10KB per envelope).
   * This ensures we don't bloat ZK znodes.
   */
  @Test
  public void testEnvelopeSizeIsBounded() {
    int fileCount = 1000;
    _mockAdapter.setSnapshotId(2001L);
    _mockAdapter.setDataFiles(createDataFiles(fileCount, 100_000_000L, 50000L));

    TableConfig tableConfig = createLakehouseTableConfig("envelopeSize_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("envelopeSize_OFFLINE", tableConfig);

    long maxEnvelopeSize = 0;
    for (TabletMetadataEnvelope envelope : tablets) {
      String json = envelope.toJsonString();
      long size = json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
      if (size > maxEnvelopeSize) {
        maxEnvelopeSize = size;
      }
      assertTrue(size < 10_000, "Envelope JSON should be under 10KB, but was " + size + " bytes");
    }

    // The envelope should be compact since it only has summary metadata, not per-file details
    // Typical envelope is a few hundred bytes
    assertTrue(maxEnvelopeSize < 2000,
        "Max envelope size should be well under 2KB for summary-only metadata, but was " + maxEnvelopeSize);
  }

  /**
   * Total ZK metadata size across all envelopes should stay bounded even with many tablets.
   */
  @Test
  public void testTotalZkMetadataSizeBounded() {
    int fileCount = 100_000;
    _mockAdapter.setSnapshotId(3001L);
    _mockAdapter.setDataFiles(createDataFiles(fileCount, 50_000_000L, 10000L));

    TableConfig tableConfig = createLakehouseTableConfig("zkSize_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("zkSize_OFFLINE", tableConfig);

    long totalZkBytes = 0;
    for (TabletMetadataEnvelope envelope : tablets) {
      totalZkBytes += envelope.toJsonString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    // With ~782 tablets at ~500 bytes each, total should be well under 1MB
    assertTrue(totalZkBytes < 1_000_000,
        "Total ZK metadata for 100k files should be under 1MB, but was " + totalZkBytes + " bytes");

    // Log the actual size for visibility
    double totalKb = totalZkBytes / 1024.0;
    // ~782 envelopes * ~500 bytes = ~400KB expected
    assertTrue(totalKb > 0, "Total ZK metadata should be non-zero");
  }

  /**
   * 100k files should group in < 5 seconds. This validates that the grouping algorithm
   * is efficient and doesn't have pathological behavior with large inputs.
   */
  @Test
  public void testGroupingPerformance() {
    int fileCount = 100_000;
    List<LakehouseFileDescriptor> files = createDataFiles(fileCount, 50_000_000L, 10000L);
    TabletConfig tabletConfig = new TabletConfig(128, null, "NONE");

    long startNanos = System.nanoTime();
    Map<String, List<LakehouseFileDescriptor>> tablets = TabletGrouper.groupFiles(files, tabletConfig, null);
    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

    assertTrue(elapsedMs < 5000, "Grouping 100k files should complete in < 5 seconds, but took " + elapsedMs + " ms");
    assertFalse(tablets.isEmpty());

    // Verify correctness along with performance
    int totalFiles = 0;
    for (List<LakehouseFileDescriptor> group : tablets.values()) {
      totalFiles += group.size();
    }
    assertEquals(totalFiles, fileCount);
  }

  /**
   * Tests grouping performance with partitioned data at scale.
   * Files spread across 100 partitions with time bucketing.
   */
  @Test
  public void testPartitionedGroupingPerformance() {
    int filesPerPartition = 1000;
    int partitionCount = 100;
    long baseTime = 1705276800000L;

    List<LakehouseFileDescriptor> files = new ArrayList<>();
    for (int p = 0; p < partitionCount; p++) {
      for (int f = 0; f < filesPerPartition; f++) {
        // Spread files across 10 days
        long ts = baseTime + (f % 10) * 86400000L;
        files.add(new LakehouseFileDescriptor("s3://data/p" + p + "/file" + f + ".parquet", "PARQUET", 50_000_000L,
            10000L, LakehouseFileDescriptor.CONTENT_DATA, 0, Arrays.asList("partition_" + p),
            Map.of("ts", String.valueOf(ts)), Map.of("ts", String.valueOf(ts + 86399999L)), null));
      }
    }

    TabletConfig tabletConfig = new TabletConfig(128, null, "DAY");

    long startNanos = System.nanoTime();
    Map<String, List<LakehouseFileDescriptor>> tablets = TabletGrouper.groupFiles(files, tabletConfig, "ts");
    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

    assertTrue(elapsedMs < 5000,
        "Grouping 100k partitioned files should complete in < 5 seconds, but took " + elapsedMs + " ms");

    // 100 partitions * 10 days = up to 1000 buckets, split by file count
    // Each bucket has ~100 files (1000/10), well within 128 limit -> 1000 tablets
    assertTrue(tablets.size() >= partitionCount, "Should have at least one tablet per partition");
    assertTrue(tablets.size() <= partitionCount * 10 + partitionCount,
        "Should not have excessively many tablets");

    int totalFiles = 0;
    for (List<LakehouseFileDescriptor> group : tablets.values()) {
      totalFiles += group.size();
    }
    assertEquals(totalFiles, filesPerPartition * partitionCount);
  }

  /**
   * Full pipeline test at scale: 100k files through the manager, verifying that manifest
   * store usage is bounded.
   */
  @Test
  public void testFullPipelineAtScale() {
    int fileCount = 100_000;
    _mockAdapter.setSnapshotId(5001L);
    _mockAdapter.setDataFiles(createDataFiles(fileCount, 50_000_000L, 10000L));

    TableConfig tableConfig = createLakehouseTableConfig("fullScale_OFFLINE");

    long startNanos = System.nanoTime();
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("fullScale_OFFLINE", tableConfig);
    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

    assertFalse(tablets.isEmpty());
    // The full pipeline (grouping + manifest creation + envelope creation) should complete quickly
    // Allow more time than pure grouping since manifests are serialized
    assertTrue(elapsedMs < 30000,
        "Full pipeline for 100k files should complete in < 30 seconds, but took " + elapsedMs + " ms");

    // Number of manifests should equal number of tablets
    assertEquals(_manifestStore.size(), tablets.size());

    // Verify manifests are not excessively large
    // Each manifest contains microsegment descriptors, so it's larger than an envelope
    // but should still be reasonable
    for (TabletMetadataEnvelope envelope : tablets) {
      String manifestJson = _manifestStore.readManifest(envelope.getManifestPointerUri());
      assertNotNull(manifestJson);
      int manifestSizeBytes = manifestJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
      // A manifest with 128 files at ~200 bytes each should be < 50KB
      assertTrue(manifestSizeBytes < 50_000,
          "Manifest should be under 50KB, but was " + manifestSizeBytes + " bytes");
    }
  }

  // --- Helpers ---

  private List<LakehouseFileDescriptor> createDataFiles(int count, long sizeEach, long rowCountEach) {
    List<LakehouseFileDescriptor> files = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      files.add(
          new LakehouseFileDescriptor("s3://data/file" + i + ".parquet", "PARQUET", sizeEach, rowCountEach,
              LakehouseFileDescriptor.CONTENT_DATA, 0, null, null, null, null));
    }
    return files;
  }

  private TableConfig createLakehouseTableConfig(String tableName) {
    IcebergCatalogConfig catalogConfig =
        new IcebergCatalogConfig(IcebergCatalogConfig.CatalogType.REST, "http://localhost:8181", "s3://warehouse",
            "db.events", null);
    LakehouseReadConfig readConfig =
        new LakehouseReadConfig(LakehouseConfig.VisibilityMode.SNAPSHOT_ONLY, LakehouseConfig.RefType.BRANCH, "main");
    LakehouseConfig lakehouseConfig =
        new LakehouseConfig(true, LakehouseConfig.Mode.ICEBERG_NATIVE, catalogConfig, readConfig, null, null, null);
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName.replace("_OFFLINE", ""))
        .setLakehouseConfig(lakehouseConfig).build();
  }

  // --- Mock implementations ---

  private static class MockCatalogAdapter implements LakehouseCatalogAdapter {
    private long _snapshotId = -1;
    private List<LakehouseFileDescriptor> _dataFiles = Collections.emptyList();

    void setSnapshotId(long snapshotId) {
      _snapshotId = snapshotId;
    }

    void setDataFiles(List<LakehouseFileDescriptor> dataFiles) {
      _dataFiles = dataFiles;
    }

    @Override
    public void init(IcebergCatalogConfig config) {
    }

    @Override
    public long getCurrentSnapshotId() {
      return _snapshotId;
    }

    @Override
    public long resolveSnapshotForBranch(String branch) {
      return _snapshotId;
    }

    @Override
    public long resolveSnapshotForTag(String tag) {
      return _snapshotId;
    }

    @Override
    public long resolveSnapshotAsOf(long timestampMs) {
      return _snapshotId;
    }

    @Override
    public List<LakehouseFileDescriptor> listDataFiles(long snapshotId) {
      return _dataFiles;
    }

    @Override
    public List<LakehouseFileDescriptor> listDeleteFiles(long snapshotId) {
      return Collections.emptyList();
    }

    @Nullable
    @Override
    public String getTableUuid() {
      return "mock-uuid-scale-test";
    }

    @Override
    public List<LakehouseFieldDescriptor> getSchemaFields() {
      return Arrays.asList(new LakehouseFieldDescriptor(1, "id", "long", true),
          new LakehouseFieldDescriptor(2, "name", "string", false));
    }

    @Override
    public void close() {
    }
  }

  private static class InMemoryManifestStore implements TabletManifestStore {
    private final ConcurrentHashMap<String, String> _store = new ConcurrentHashMap<>();

    @Override
    public void init(String baseUri) {
    }

    @Override
    public String writeManifest(String tableNameWithType, String tabletId, String manifestJson) {
      String uri = "mem://" + tableNameWithType + "/" + tabletId + "/manifest.json";
      _store.put(uri, manifestJson);
      return uri;
    }

    @Nullable
    @Override
    public String readManifest(String manifestUri) {
      return _store.get(manifestUri);
    }

    @Override
    public void deleteManifest(String manifestUri) {
      _store.remove(manifestUri);
    }

    @Override
    public void close() {
    }

    int size() {
      return _store.size();
    }
  }
}
