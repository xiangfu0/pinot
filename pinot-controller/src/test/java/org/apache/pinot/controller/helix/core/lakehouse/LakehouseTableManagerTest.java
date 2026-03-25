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
 * Tests the full LakehouseTableManager pipeline with mock catalog adapter and manifest store.
 * Validates snapshot resolution, file grouping, manifest creation, and envelope generation.
 */
public class LakehouseTableManagerTest {
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

  @Test
  public void testRefreshCreatesTablets() {
    _mockAdapter.setSnapshotId(1001L);
    _mockAdapter.setDataFiles(createDataFiles(10, 100_000_000L));

    TableConfig tableConfig = createLakehouseTableConfig("events_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("events_OFFLINE", tableConfig);

    assertNotNull(tablets);
    assertFalse(tablets.isEmpty());
    assertEquals(tablets.get(0).getSnapshotId(), 1001L);
    assertEquals(tablets.get(0).getTableNameWithType(), "events_OFFLINE");

    // Verify manifests were persisted
    assertFalse(_manifestStore.isEmpty());
  }

  @Test
  public void testRefreshSkipsWhenSnapshotUnchanged() {
    _mockAdapter.setSnapshotId(2001L);
    _mockAdapter.setDataFiles(createDataFiles(5, 50_000_000L));

    TableConfig tableConfig = createLakehouseTableConfig("events_OFFLINE");

    // First refresh
    List<TabletMetadataEnvelope> tablets1 = _manager.refreshTableTablets("events_OFFLINE", tableConfig);
    int manifestCount1 = _manifestStore.size();

    // Second refresh with same snapshot -> should return cached envelopes
    List<TabletMetadataEnvelope> tablets2 = _manager.refreshTableTablets("events_OFFLINE", tableConfig);

    assertEquals(tablets2.size(), tablets1.size());
    // No new manifests should be written
    assertEquals(_manifestStore.size(), manifestCount1);
  }

  @Test
  public void testRefreshDetectsSnapshotChange() {
    _mockAdapter.setSnapshotId(3001L);
    _mockAdapter.setDataFiles(createDataFiles(5, 50_000_000L));

    TableConfig tableConfig = createLakehouseTableConfig("events_OFFLINE");

    List<TabletMetadataEnvelope> tablets1 = _manager.refreshTableTablets("events_OFFLINE", tableConfig);
    assertEquals(tablets1.get(0).getSnapshotId(), 3001L);

    // Change snapshot
    _mockAdapter.setSnapshotId(3002L);
    _mockAdapter.setDataFiles(createDataFiles(8, 80_000_000L));

    List<TabletMetadataEnvelope> tablets2 = _manager.refreshTableTablets("events_OFFLINE", tableConfig);
    assertEquals(tablets2.get(0).getSnapshotId(), 3002L);
  }

  @Test
  public void testTabletGroupingRespectsLimits() {
    _mockAdapter.setSnapshotId(4001L);
    // 20 files with target 5 per tablet -> should create 4 tablets
    _mockAdapter.setDataFiles(createDataFiles(20, 10_000L));

    TabletConfig tabletConfig = new TabletConfig(5, null, "NONE");
    TableConfig tableConfig = createLakehouseTableConfigWithTablet("events_OFFLINE", tabletConfig);

    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("events_OFFLINE", tableConfig);

    assertEquals(tablets.size(), 4);
    // Each tablet should have ~5 microsegments
    for (TabletMetadataEnvelope envelope : tablets) {
      assertTrue(envelope.getMicrosegmentCount() <= 5);
      assertTrue(envelope.getMicrosegmentCount() > 0);
    }
  }

  @Test
  public void testGetTableTablets() {
    _mockAdapter.setSnapshotId(5001L);
    _mockAdapter.setDataFiles(createDataFiles(3, 10_000L));

    TableConfig tableConfig = createLakehouseTableConfig("test_OFFLINE");

    // Before first refresh, should return null or empty
    List<TabletMetadataEnvelope> before = _manager.getTableTablets("test_OFFLINE");
    assertTrue(before == null || before.isEmpty());

    _manager.refreshTableTablets("test_OFFLINE", tableConfig);
    List<TabletMetadataEnvelope> tablets = _manager.getTableTablets("test_OFFLINE");
    assertNotNull(tablets);
    assertFalse(tablets.isEmpty());
  }

  @Test
  public void testRemoveTable() {
    _mockAdapter.setSnapshotId(6001L);
    _mockAdapter.setDataFiles(createDataFiles(3, 10_000L));

    TableConfig tableConfig = createLakehouseTableConfig("drop_OFFLINE");
    _manager.refreshTableTablets("drop_OFFLINE", tableConfig);

    assertNotNull(_manager.getTableTablets("drop_OFFLINE"));

    _manager.removeTable("drop_OFFLINE");
    List<TabletMetadataEnvelope> after = _manager.getTableTablets("drop_OFFLINE");
    assertTrue(after == null || after.isEmpty());
  }

  @Test
  public void testEmptyDataFiles() {
    _mockAdapter.setSnapshotId(7001L);
    _mockAdapter.setDataFiles(Collections.emptyList());

    TableConfig tableConfig = createLakehouseTableConfig("empty_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("empty_OFFLINE", tableConfig);

    assertNotNull(tablets);
    assertTrue(tablets.isEmpty());
  }

  @Test
  public void testEnvelopeMetadataAccuracy() {
    _mockAdapter.setSnapshotId(8001L);
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    files.add(new LakehouseFileDescriptor("s3://data/a.parquet", "PARQUET", 200_000_000L, 1000000L,
        LakehouseFileDescriptor.CONTENT_DATA, 0, null, null, null, null));
    files.add(new LakehouseFileDescriptor("s3://data/b.parquet", "PARQUET", 300_000_000L, 1500000L,
        LakehouseFileDescriptor.CONTENT_DATA, 0, null, null, null, null));
    _mockAdapter.setDataFiles(files);

    TableConfig tableConfig = createLakehouseTableConfig("accurate_OFFLINE");
    List<TabletMetadataEnvelope> tablets = _manager.refreshTableTablets("accurate_OFFLINE", tableConfig);

    // Both files should be in one tablet (default target is 128 files)
    assertEquals(tablets.size(), 1);
    TabletMetadataEnvelope envelope = tablets.get(0);
    assertEquals(envelope.getRowCount(), 2500000L);
    assertEquals(envelope.getSizeBytes(), 500_000_000L);
    assertEquals(envelope.getMicrosegmentCount(), 2);
    assertEquals(envelope.getSnapshotId(), 8001L);
  }

  // --- Helpers ---

  private List<LakehouseFileDescriptor> createDataFiles(int count, long sizeEach) {
    List<LakehouseFileDescriptor> files = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      files.add(new LakehouseFileDescriptor("s3://data/file" + i + ".parquet", "PARQUET", sizeEach, sizeEach / 100,
          LakehouseFileDescriptor.CONTENT_DATA, 0, null, null, null, null));
    }
    return files;
  }

  private TableConfig createLakehouseTableConfig(String tableName) {
    return createLakehouseTableConfigWithTablet(tableName, null);
  }

  private TableConfig createLakehouseTableConfigWithTablet(String tableName, @Nullable TabletConfig tabletConfig) {
    IcebergCatalogConfig catalogConfig =
        new IcebergCatalogConfig(IcebergCatalogConfig.CatalogType.REST, "http://localhost:8181", "s3://warehouse",
            "db.events", null);
    LakehouseReadConfig readConfig =
        new LakehouseReadConfig(LakehouseConfig.VisibilityMode.SNAPSHOT_ONLY, LakehouseConfig.RefType.BRANCH, "main");
    LakehouseConfig lakehouseConfig =
        new LakehouseConfig(true, LakehouseConfig.Mode.ICEBERG_NATIVE, catalogConfig, readConfig, null, tabletConfig,
            null);
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
      return "mock-uuid";
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

    boolean isEmpty() {
      return _store.isEmpty();
    }

    int size() {
      return _store.size();
    }
  }
}
