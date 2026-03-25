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
import org.apache.pinot.common.metadata.lakehouse.TabletManifest;
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
 * Integration-style test that exercises the full lakehouse pipeline from table config through
 * tablet creation, manifest persistence, envelope serialization, and tablet pruning.
 *
 * <p>Uses mock implementations (no real Iceberg catalog or Parquet files required).</p>
 */
public class LakehouseIntegrationTest {
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

  // ---- Scenario 1: Small table (10 files) -> 1 tablet ----

  @Test
  public void testSmallTableSingleTablet() {
    _mockAdapter.setSnapshotId(1001L);
    _mockAdapter.setDataFiles(createDataFiles(10, 100_000_000L, 50000L));

    TableConfig tableConfig = createLakehouseTableConfig("smallTable_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("smallTable_OFFLINE", tableConfig);

    assertNotNull(tablets);
    // Default target is 128 files per tablet, so 10 files -> 1 tablet
    assertEquals(tablets.size(), 1);

    TabletMetadataEnvelope envelope = tablets.get(0);
    assertEquals(envelope.getSnapshotId(), 1001L);
    assertEquals(envelope.getTableNameWithType(), "smallTable_OFFLINE");
    assertEquals(envelope.getMicrosegmentCount(), 10);
    assertEquals(envelope.getRowCount(), 500000L);
    assertEquals(envelope.getSizeBytes(), 1_000_000_000L);
    assertNotNull(envelope.getManifestPointerUri());

    // Verify manifest was persisted and can be read back
    String manifestJson = _manifestStore.readManifest(envelope.getManifestPointerUri());
    assertNotNull(manifestJson);
    TabletManifest manifest = TabletManifest.fromJsonString(manifestJson);
    assertEquals(manifest.getSnapshotId(), 1001L);
    assertEquals(manifest.getMicrosegments().size(), 10);
    assertEquals(manifest.getTotalRowCount(), 500000L);
    assertEquals(manifest.getTotalSizeBytes(), 1_000_000_000L);
  }

  // ---- Scenario 2: Medium table (500 files) -> multiple tablets ----

  @Test
  public void testMediumTableMultipleTablets() {
    _mockAdapter.setSnapshotId(2001L);
    _mockAdapter.setDataFiles(createDataFiles(500, 50_000_000L, 10000L));

    TableConfig tableConfig = createLakehouseTableConfig("mediumTable_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("mediumTable_OFFLINE", tableConfig);

    // Default 128 files per tablet: 500 / 128 = 3 full tablets + 1 remainder = 4 tablets
    assertEquals(tablets.size(), 4);

    long totalRows = 0;
    long totalBytes = 0;
    int totalMicrosegments = 0;
    for (TabletMetadataEnvelope envelope : tablets) {
      assertEquals(envelope.getSnapshotId(), 2001L);
      assertTrue(envelope.getMicrosegmentCount() > 0);
      assertTrue(envelope.getMicrosegmentCount() <= 128);
      totalRows += envelope.getRowCount();
      totalBytes += envelope.getSizeBytes();
      totalMicrosegments += envelope.getMicrosegmentCount();

      // Verify each tablet's manifest is persisted
      String manifestJson = _manifestStore.readManifest(envelope.getManifestPointerUri());
      assertNotNull(manifestJson);
    }

    assertEquals(totalMicrosegments, 500);
    assertEquals(totalRows, 5_000_000L);
    assertEquals(totalBytes, 25_000_000_000L);
  }

  // ---- Scenario 3: Multiple partitions -> tablets per partition ----

  @Test
  public void testMultiplePartitionsCreatesSeparateTablets() {
    _mockAdapter.setSnapshotId(3001L);
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    // 5 files in partition "US"
    for (int i = 0; i < 5; i++) {
      files.add(new LakehouseFileDescriptor("s3://data/us/file" + i + ".parquet", "PARQUET", 100_000_000L, 10000L,
          LakehouseFileDescriptor.CONTENT_DATA, 0, Arrays.asList("US"), null, null, null));
    }
    // 5 files in partition "EU"
    for (int i = 0; i < 5; i++) {
      files.add(new LakehouseFileDescriptor("s3://data/eu/file" + i + ".parquet", "PARQUET", 100_000_000L, 10000L,
          LakehouseFileDescriptor.CONTENT_DATA, 0, Arrays.asList("EU"), null, null, null));
    }
    // 3 files in partition "APAC"
    for (int i = 0; i < 3; i++) {
      files.add(new LakehouseFileDescriptor("s3://data/apac/file" + i + ".parquet", "PARQUET", 100_000_000L, 10000L,
          LakehouseFileDescriptor.CONTENT_DATA, 0, Arrays.asList("APAC"), null, null, null));
    }
    _mockAdapter.setDataFiles(files);

    TabletConfig tabletConfig = new TabletConfig(128, null, "NONE");
    TableConfig tableConfig = createLakehouseTableConfigWithTablet("partitioned_OFFLINE", tabletConfig);
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("partitioned_OFFLINE", tableConfig);

    // 3 distinct partition values -> 3 tablets (each within the 128-file limit)
    assertEquals(tablets.size(), 3);

    // Verify each partition has its own tablet
    int totalMicrosegments = 0;
    for (TabletMetadataEnvelope envelope : tablets) {
      totalMicrosegments += envelope.getMicrosegmentCount();
    }
    assertEquals(totalMicrosegments, 13);
  }

  // ---- Scenario 4: Time-bucketed data -> tablets per time bucket ----

  @Test
  public void testTimeBucketedDataCreatesSeparateTablets() {
    _mockAdapter.setSnapshotId(4001L);
    long day1Start = 1705276800000L; // 2024-01-15 00:00:00 UTC
    long day2Start = 1705363200000L; // 2024-01-16 00:00:00 UTC
    long day3Start = 1705449600000L; // 2024-01-17 00:00:00 UTC

    List<LakehouseFileDescriptor> files = new ArrayList<>();
    // 3 files for day 1
    for (int i = 0; i < 3; i++) {
      long ts = day1Start + i * 3600000L;
      files.add(new LakehouseFileDescriptor("s3://data/d1/file" + i + ".parquet", "PARQUET", 100_000_000L, 10000L,
          LakehouseFileDescriptor.CONTENT_DATA, 0, null, Map.of("ts", String.valueOf(ts)),
          Map.of("ts", String.valueOf(ts + 3600000L)), null));
    }
    // 4 files for day 2
    for (int i = 0; i < 4; i++) {
      long ts = day2Start + i * 3600000L;
      files.add(new LakehouseFileDescriptor("s3://data/d2/file" + i + ".parquet", "PARQUET", 100_000_000L, 10000L,
          LakehouseFileDescriptor.CONTENT_DATA, 0, null, Map.of("ts", String.valueOf(ts)),
          Map.of("ts", String.valueOf(ts + 3600000L)), null));
    }
    // 2 files for day 3
    for (int i = 0; i < 2; i++) {
      long ts = day3Start + i * 3600000L;
      files.add(new LakehouseFileDescriptor("s3://data/d3/file" + i + ".parquet", "PARQUET", 100_000_000L, 10000L,
          LakehouseFileDescriptor.CONTENT_DATA, 0, null, Map.of("ts", String.valueOf(ts)),
          Map.of("ts", String.valueOf(ts + 3600000L)), null));
    }
    _mockAdapter.setDataFiles(files);

    TabletConfig tabletConfig = new TabletConfig(128, null, "DAY");
    TableConfig tableConfig =
        createLakehouseTableConfigWithTabletAndTimeColumn("timeBucketed_OFFLINE", tabletConfig, "ts");
    List<TabletMetadataEnvelope> tablets =
        _manager.refreshTableTablets("timeBucketed_OFFLINE", tableConfig);

    // 3 distinct days -> 3 tablets
    assertEquals(tablets.size(), 3);

    int totalMicrosegments = 0;
    for (TabletMetadataEnvelope envelope : tablets) {
      totalMicrosegments += envelope.getMicrosegmentCount();
    }
    assertEquals(totalMicrosegments, 9);
  }

  // ---- Scenario 5: Snapshot change detection ----

  @Test
  public void testSnapshotChangeDetectionRefreshesTablets() {
    _mockAdapter.setSnapshotId(5001L);
    _mockAdapter.setDataFiles(createDataFiles(5, 50_000_000L, 5000L));

    TableConfig tableConfig = createLakehouseTableConfig("snapChange_OFFLINE");

    // First refresh
    List<TabletMetadataEnvelope> tablets1 = _manager.refreshTableTablets("snapChange_OFFLINE", tableConfig);
    assertEquals(tablets1.get(0).getSnapshotId(), 5001L);
    String firstManifestUri = tablets1.get(0).getManifestPointerUri();
    String firstManifestJson = _manifestStore.readManifest(firstManifestUri);
    assertNotNull(firstManifestJson);

    // Second refresh with same snapshot -> should return cached envelopes
    List<TabletMetadataEnvelope> tablets2 = _manager.refreshTableTablets("snapChange_OFFLINE", tableConfig);
    assertEquals(tablets2.size(), tablets1.size());
    assertEquals(tablets2.get(0).getSnapshotId(), 5001L);

    // Change snapshot and add more files
    _mockAdapter.setSnapshotId(5002L);
    _mockAdapter.setDataFiles(createDataFiles(8, 60_000_000L, 6000L));

    List<TabletMetadataEnvelope> tablets3 = _manager.refreshTableTablets("snapChange_OFFLINE", tableConfig);
    assertEquals(tablets3.get(0).getSnapshotId(), 5002L);

    // Verify the manifest content was updated (new snapshot ID in the manifest)
    String updatedManifestJson = _manifestStore.readManifest(tablets3.get(0).getManifestPointerUri());
    assertNotNull(updatedManifestJson);
    TabletManifest updatedManifest = TabletManifest.fromJsonString(updatedManifestJson);
    assertEquals(updatedManifest.getSnapshotId(), 5002L);
    assertEquals(updatedManifest.getMicrosegments().size(), 8);

    // Verify row counts updated
    long totalRows = 0;
    for (TabletMetadataEnvelope env : tablets3) {
      totalRows += env.getRowCount();
    }
    assertEquals(totalRows, 48000L);
  }

  // ---- Scenario 6: Envelope metadata accuracy ----

  @Test
  public void testEnvelopeMetadataCorrectness() {
    _mockAdapter.setSnapshotId(6001L);
    long baseTimeMs = 1705276800000L;
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    files.add(new LakehouseFileDescriptor("s3://data/a.parquet", "PARQUET", 200_000_000L, 1000000L,
        LakehouseFileDescriptor.CONTENT_DATA, 0, null, Map.of("ts", String.valueOf(baseTimeMs)),
        Map.of("ts", String.valueOf(baseTimeMs + 10000L)), null));
    files.add(new LakehouseFileDescriptor("s3://data/b.parquet", "PARQUET", 300_000_000L, 1500000L,
        LakehouseFileDescriptor.CONTENT_DATA, 0, null, Map.of("ts", String.valueOf(baseTimeMs + 20000L)),
        Map.of("ts", String.valueOf(baseTimeMs + 50000L)), null));
    _mockAdapter.setDataFiles(files);

    TabletConfig tabletConfig = new TabletConfig(128, null, "NONE");
    TableConfig tableConfig = createLakehouseTableConfigWithTablet("accurate_OFFLINE", tabletConfig);
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("accurate_OFFLINE", tableConfig);

    assertEquals(tablets.size(), 1);
    TabletMetadataEnvelope envelope = tablets.get(0);
    assertEquals(envelope.getRowCount(), 2500000L);
    assertEquals(envelope.getSizeBytes(), 500_000_000L);
    assertEquals(envelope.getMicrosegmentCount(), 2);
    assertEquals(envelope.getSnapshotId(), 6001L);
    assertEquals(envelope.getVersion(), TabletMetadataEnvelope.CURRENT_VERSION);
    assertEquals(envelope.getMinTimeMs(), baseTimeMs);
    assertEquals(envelope.getMaxTimeMs(), baseTimeMs + 50000L);
  }

  // ---- Scenario 7: Manifest persist and read-back round trip ----

  @Test
  public void testManifestPersistenceRoundTrip() {
    _mockAdapter.setSnapshotId(7001L);
    _mockAdapter.setDataFiles(createDataFiles(3, 100_000_000L, 25000L));

    TableConfig tableConfig = createLakehouseTableConfig("manifestRt_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("manifestRt_OFFLINE", tableConfig);

    for (TabletMetadataEnvelope envelope : tablets) {
      String manifestUri = envelope.getManifestPointerUri();
      assertNotNull(manifestUri);

      String manifestJson = _manifestStore.readManifest(manifestUri);
      assertNotNull(manifestJson);

      // Round-trip: deserialize and verify
      TabletManifest manifest = TabletManifest.fromJsonString(manifestJson);
      assertEquals(manifest.getTabletId(), envelope.getTabletId());
      assertEquals(manifest.getSnapshotId(), envelope.getSnapshotId());
      assertEquals(manifest.getMicrosegments().size(), envelope.getMicrosegmentCount());
      assertEquals(manifest.getVersion(), TabletManifest.CURRENT_VERSION);
      assertNotNull(manifest.getIcebergTableUuid());
      assertTrue(manifest.getCreatedAtMs() > 0);

      // Verify microsegment details
      manifest.getMicrosegments().forEach(ms -> {
        assertNotNull(ms.getMicrosegmentId());
        assertNotNull(ms.getFilePath());
        assertTrue(ms.getFilePath().startsWith("s3://data/"));
        assertEquals(ms.getFileFormat(), "PARQUET");
        assertTrue(ms.getFileSizeBytes() > 0);
        assertTrue(ms.getRowCount() > 0);
        assertTrue(ms.isDataFile());
      });

      // Verify JSON re-serialization produces valid JSON
      String reserializedJson = manifest.toJsonString();
      TabletManifest reManifest = TabletManifest.fromJsonString(reserializedJson);
      assertEquals(reManifest.getTabletId(), manifest.getTabletId());
      assertEquals(reManifest.getMicrosegments().size(), manifest.getMicrosegments().size());
    }
  }

  // ---- Scenario 8: Envelope JSON serialization round-trip ----

  @Test
  public void testEnvelopeJsonSerializationRoundTrip() {
    _mockAdapter.setSnapshotId(8001L);
    _mockAdapter.setDataFiles(createDataFiles(5, 80_000_000L, 15000L));

    TableConfig tableConfig = createLakehouseTableConfig("envelopeRt_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("envelopeRt_OFFLINE", tableConfig);

    for (TabletMetadataEnvelope envelope : tablets) {
      String json = envelope.toJsonString();
      assertNotNull(json);
      assertTrue(json.contains("tabletId"));
      assertTrue(json.contains("snapshotId"));
      assertTrue(json.contains("manifestPointerUri"));

      // Round-trip deserialization
      TabletMetadataEnvelope deserialized = TabletMetadataEnvelope.fromJsonString(json);
      assertEquals(deserialized.getTabletId(), envelope.getTabletId());
      assertEquals(deserialized.getTableNameWithType(), envelope.getTableNameWithType());
      assertEquals(deserialized.getSnapshotId(), envelope.getSnapshotId());
      assertEquals(deserialized.getSpecId(), envelope.getSpecId());
      assertEquals(deserialized.getMinTimeMs(), envelope.getMinTimeMs());
      assertEquals(deserialized.getMaxTimeMs(), envelope.getMaxTimeMs());
      assertEquals(deserialized.getRowCount(), envelope.getRowCount());
      assertEquals(deserialized.getSizeBytes(), envelope.getSizeBytes());
      assertEquals(deserialized.getMicrosegmentCount(), envelope.getMicrosegmentCount());
      assertEquals(deserialized.getManifestPointerUri(), envelope.getManifestPointerUri());
      assertEquals(deserialized.getVersion(), envelope.getVersion());
    }
  }

  // ---- Scenario 9: TabletPruner with generated envelopes ----

  @Test
  public void testTabletPrunerWithGeneratedEnvelopes() {
    _mockAdapter.setSnapshotId(9001L);
    long day1Start = 1705276800000L; // 2024-01-15 00:00:00 UTC
    long day2Start = 1705363200000L; // 2024-01-16 00:00:00 UTC
    long day3Start = 1705449600000L; // 2024-01-17 00:00:00 UTC

    List<LakehouseFileDescriptor> files = new ArrayList<>();
    // Day 1 file
    files.add(new LakehouseFileDescriptor("s3://data/d1.parquet", "PARQUET", 100_000_000L, 10000L,
        LakehouseFileDescriptor.CONTENT_DATA, 0, null, Map.of("ts", String.valueOf(day1Start)),
        Map.of("ts", String.valueOf(day1Start + 86399999L)), null));
    // Day 2 file
    files.add(new LakehouseFileDescriptor("s3://data/d2.parquet", "PARQUET", 100_000_000L, 10000L,
        LakehouseFileDescriptor.CONTENT_DATA, 0, null, Map.of("ts", String.valueOf(day2Start)),
        Map.of("ts", String.valueOf(day2Start + 86399999L)), null));
    // Day 3 file
    files.add(new LakehouseFileDescriptor("s3://data/d3.parquet", "PARQUET", 100_000_000L, 10000L,
        LakehouseFileDescriptor.CONTENT_DATA, 0, null, Map.of("ts", String.valueOf(day3Start)),
        Map.of("ts", String.valueOf(day3Start + 86399999L)), null));
    _mockAdapter.setDataFiles(files);

    TabletConfig tabletConfig = new TabletConfig(128, null, "DAY");
    TableConfig tableConfig =
        createLakehouseTableConfigWithTabletAndTimeColumn("prunable_OFFLINE", tabletConfig, "ts");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("prunable_OFFLINE", tableConfig);

    assertEquals(tablets.size(), 3);

    // Prune: query only day 2 -> should return 1 tablet
    List<TabletMetadataEnvelope> prunedDay2 = pruneByTimeRange(tablets, day2Start, day2Start + 86399999L);
    assertEquals(prunedDay2.size(), 1);
    assertEquals(prunedDay2.get(0).getMinTimeMs(), day2Start);

    // Prune: query days 1-2 -> should return 2 tablets
    List<TabletMetadataEnvelope> prunedDay1To2 =
        pruneByTimeRange(tablets, day1Start, day2Start + 86399999L);
    assertEquals(prunedDay1To2.size(), 2);

    // Prune: query all 3 days -> should return all 3 tablets
    List<TabletMetadataEnvelope> prunedAll =
        pruneByTimeRange(tablets, day1Start, day3Start + 86399999L);
    assertEquals(prunedAll.size(), 3);

    // Prune: query far future -> should return 0 tablets
    List<TabletMetadataEnvelope> prunedFuture =
        pruneByTimeRange(tablets, day3Start + 90000000L, day3Start + 180000000L);
    assertEquals(prunedFuture.size(), 0);

    // Prune: null/empty list -> should return empty
    List<TabletMetadataEnvelope> prunedEmpty =
        pruneByTimeRange(Collections.emptyList(), day1Start, day3Start);
    assertTrue(prunedEmpty.isEmpty());
  }

  // ---- Scenario 10: Multiple partitions combined with time buckets ----

  @Test
  public void testPartitionAndTimeBucketCombination() {
    _mockAdapter.setSnapshotId(10001L);
    long day1Start = 1705276800000L;
    long day2Start = 1705363200000L;

    List<LakehouseFileDescriptor> files = new ArrayList<>();
    // Partition US, Day 1: 2 files
    for (int i = 0; i < 2; i++) {
      long ts = day1Start + i * 3600000L;
      files.add(new LakehouseFileDescriptor("s3://us/d1/" + i + ".parquet", "PARQUET", 100_000_000L, 10000L,
          LakehouseFileDescriptor.CONTENT_DATA, 0, Arrays.asList("US"), Map.of("ts", String.valueOf(ts)),
          Map.of("ts", String.valueOf(ts + 3600000L)), null));
    }
    // Partition US, Day 2: 3 files
    for (int i = 0; i < 3; i++) {
      long ts = day2Start + i * 3600000L;
      files.add(new LakehouseFileDescriptor("s3://us/d2/" + i + ".parquet", "PARQUET", 100_000_000L, 10000L,
          LakehouseFileDescriptor.CONTENT_DATA, 0, Arrays.asList("US"), Map.of("ts", String.valueOf(ts)),
          Map.of("ts", String.valueOf(ts + 3600000L)), null));
    }
    // Partition EU, Day 1: 2 files
    for (int i = 0; i < 2; i++) {
      long ts = day1Start + i * 3600000L;
      files.add(new LakehouseFileDescriptor("s3://eu/d1/" + i + ".parquet", "PARQUET", 100_000_000L, 10000L,
          LakehouseFileDescriptor.CONTENT_DATA, 0, Arrays.asList("EU"), Map.of("ts", String.valueOf(ts)),
          Map.of("ts", String.valueOf(ts + 3600000L)), null));
    }
    _mockAdapter.setDataFiles(files);

    TabletConfig tabletConfig = new TabletConfig(128, null, "DAY");
    TableConfig tableConfig =
        createLakehouseTableConfigWithTabletAndTimeColumn("partTime_OFFLINE", tabletConfig, "ts");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("partTime_OFFLINE", tableConfig);

    // 2 partitions * 2 days for US + 1 day for EU = 3 tablets
    // US-Day1(2 files), US-Day2(3 files), EU-Day1(2 files)
    assertEquals(tablets.size(), 3);

    int totalMicrosegments = 0;
    for (TabletMetadataEnvelope envelope : tablets) {
      totalMicrosegments += envelope.getMicrosegmentCount();
    }
    assertEquals(totalMicrosegments, 7);
  }

  // ---- Scenario 11: Empty table produces no tablets ----

  @Test
  public void testEmptyTableNoTablets() {
    _mockAdapter.setSnapshotId(11001L);
    _mockAdapter.setDataFiles(Collections.emptyList());

    TableConfig tableConfig = createLakehouseTableConfig("empty_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("empty_OFFLINE", tableConfig);

    assertNotNull(tablets);
    assertTrue(tablets.isEmpty());
  }

  // --- Helpers ---

  /**
   * Inline time-range pruning that mirrors TabletPruner.pruneByTimeRange logic.
   * This avoids a cross-module dependency on pinot-broker from pinot-controller tests.
   */
  private static List<TabletMetadataEnvelope> pruneByTimeRange(List<TabletMetadataEnvelope> tablets,
      long queryMinTimeMs, long queryMaxTimeMs) {
    if (tablets == null || tablets.isEmpty()) {
      return Collections.emptyList();
    }
    List<TabletMetadataEnvelope> result = new ArrayList<>(tablets.size());
    for (TabletMetadataEnvelope tablet : tablets) {
      if (tablet.getMinTimeMs() <= queryMaxTimeMs && tablet.getMaxTimeMs() >= queryMinTimeMs) {
        result.add(tablet);
      }
    }
    return result;
  }

  private List<LakehouseFileDescriptor> createDataFiles(int count, long sizeEach, long rowCountEach) {
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      files.add(
          new LakehouseFileDescriptor("s3://data/file" + i + ".parquet", "PARQUET", sizeEach, rowCountEach,
              LakehouseFileDescriptor.CONTENT_DATA, 0, null, null, null, null));
    }
    return files;
  }

  private TableConfig createLakehouseTableConfig(String tableName) {
    return createLakehouseTableConfigWithTablet(tableName, null);
  }

  private TableConfig createLakehouseTableConfigWithTablet(String tableName, @Nullable TabletConfig tabletConfig) {
    return createLakehouseTableConfigWithTabletAndTimeColumn(tableName, tabletConfig, null);
  }

  private TableConfig createLakehouseTableConfigWithTabletAndTimeColumn(String tableName,
      @Nullable TabletConfig tabletConfig, @Nullable String timeColumnName) {
    IcebergCatalogConfig catalogConfig =
        new IcebergCatalogConfig(IcebergCatalogConfig.CatalogType.REST, "http://localhost:8181", "s3://warehouse",
            "db.events", null);
    LakehouseReadConfig readConfig =
        new LakehouseReadConfig(LakehouseConfig.VisibilityMode.SNAPSHOT_ONLY, LakehouseConfig.RefType.BRANCH, "main");
    LakehouseConfig lakehouseConfig =
        new LakehouseConfig(true, LakehouseConfig.Mode.ICEBERG_NATIVE, catalogConfig, readConfig, null, tabletConfig,
            null);
    TableConfigBuilder builder = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName.replace("_OFFLINE", ""))
        .setLakehouseConfig(lakehouseConfig);
    if (timeColumnName != null) {
      builder.setTimeColumnName(timeColumnName);
    }
    return builder.build();
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
      return "mock-uuid-integration-test";
    }

    @Override
    public List<LakehouseFieldDescriptor> getSchemaFields() {
      return Arrays.asList(new LakehouseFieldDescriptor(1, "id", "long", true),
          new LakehouseFieldDescriptor(2, "name", "string", false),
          new LakehouseFieldDescriptor(3, "ts", "long", false));
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

    boolean isEmpty() {
      return _store.isEmpty();
    }

    int size() {
      return _store.size();
    }
  }
}
