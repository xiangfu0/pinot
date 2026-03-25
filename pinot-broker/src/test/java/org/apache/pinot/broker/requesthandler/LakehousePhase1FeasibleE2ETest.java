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
package org.apache.pinot.broker.requesthandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.lakehouse.LakehouseCatalogAdapterProvider;
import org.apache.pinot.controller.lakehouse.LakehouseTableRefreshResponse;
import org.apache.pinot.controller.lakehouse.LakehouseTabletPlanner;
import org.apache.pinot.controller.lakehouse.LocalFileSystemLakehouseManifestStore;
import org.apache.pinot.controller.lakehouse.PinotLakehouseTableService;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.LakehouseOfflineTableDataManager;
import org.apache.pinot.core.data.manager.offline.LakehouseTabletSegmentDataManager;
import org.apache.pinot.core.data.manager.provider.DefaultTableDataManagerProvider;
import org.apache.pinot.core.query.executor.SingleTableExecutionInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogConfig;
import org.apache.pinot.spi.config.table.lakehouse.IcebergCatalogType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseReadVisibilityMode;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseTabletConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseWriteMode;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.lakehouse.LakehouseCatalogAdapter;
import org.apache.pinot.spi.lakehouse.LakehouseFileFormat;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotDescriptor;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotRequest;
import org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration-style test for the current feasible Phase 1 path.
 *
 * <p>The test uses the real controller tablet planner/publication path, the real server-side lakehouse table-data
 * manager/provider seam, and the broker's tablet trace helper. It does not exercise a real Parquet reader yet, but it
 * proves that Phase 1 metadata stays tablet-bounded end to end across the three roles.</p>
 */
public class LakehousePhase1FeasibleE2ETest {
  private static final String RAW_TABLE_NAME = "lakehousePhase1E2E";
  private static final String TABLE_NAME_WITH_TYPE = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

  private Path _tempDir;
  private FakePropertyStore _propertyStore;
  private ExecutorService _reloadExecutor;
  private LakehouseOfflineTableDataManager _tableDataManager;

  @BeforeClass
  public void setUpClass() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("LakehousePhase1FeasibleE2ETest");
    _propertyStore = new FakePropertyStore();
    _reloadExecutor = Executors.newSingleThreadExecutor();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    if (_tableDataManager != null) {
      _tableDataManager.shutDown();
      _tableDataManager = null;
    }
    if (_reloadExecutor != null) {
      _reloadExecutor.shutdownNow();
      _reloadExecutor = null;
    }
    if (_tempDir != null) {
      FileUtils.deleteQuietly(_tempDir.toFile());
      _tempDir = null;
    }
  }

  @Test
  public void testControllerRefreshServerLoadAndBrokerTraceRemainTabletBounded()
      throws Exception {
    TableConfig tableConfig = createLakehouseOfflineTable();
    PinotLakehouseTableService lakehouseTableService =
        createLakehouseTableService(tableConfig, _tempDir.resolve("lakehouse-manifests"));

    LakehouseTableRefreshResponse refreshResponse = lakehouseTableService.refreshTable(TABLE_NAME_WITH_TYPE,
        snapshotRequest(333L));

    assertEquals(refreshResponse.getTabletCount(), 2);
    assertEquals(refreshResponse.getTabletIds(), List.of("snapshot-333-tablet-0", "snapshot-333-tablet-1"));
    assertEquals(refreshResponse.getRemovedTabletIds(), List.of());

    List<TabletMetadataEnvelope> tabletMetadataEnvelopes =
        ZKMetadataProvider.getTabletMetadataEnvelopes(_propertyStore, TABLE_NAME_WITH_TYPE);
    assertEquals(tabletMetadataEnvelopes.size(), 2);
    for (String tabletId : refreshResponse.getTabletIds()) {
      SegmentZKMetadata tabletZkMetadata =
          ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, TABLE_NAME_WITH_TYPE, tabletId);
      assertNotNull(tabletZkMetadata);
      assertNotNull(ZKMetadataProvider.getTabletMetadataEnvelope(_propertyStore, TABLE_NAME_WITH_TYPE, tabletId));
    }

    _tableDataManager = createLakehouseTableDataManager(tableConfig);
    for (String tabletId : refreshResponse.getTabletIds()) {
      _tableDataManager.addOnlineSegment(tabletId);
    }

    SegmentDataManager firstTabletSegmentDataManager = _tableDataManager.acquireSegment("snapshot-333-tablet-0");
    assertNotNull(firstTabletSegmentDataManager);
    try {
      assertTrue(firstTabletSegmentDataManager instanceof LakehouseTabletSegmentDataManager);
      assertEquals(firstTabletSegmentDataManager.getSegmentName(), "snapshot-333-tablet-0");
      assertEquals(extractSegmentNames(firstTabletSegmentDataManager.getSegments()),
          List.of("snapshot-333-tablet-0$east-1"));
    } finally {
      _tableDataManager.releaseSegment(firstTabletSegmentDataManager);
    }

    SegmentDataManager secondTabletSegmentDataManager = _tableDataManager.acquireSegment("snapshot-333-tablet-1");
    assertNotNull(secondTabletSegmentDataManager);
    try {
      assertTrue(secondTabletSegmentDataManager instanceof LakehouseTabletSegmentDataManager);
      assertEquals(secondTabletSegmentDataManager.getSegmentName(), "snapshot-333-tablet-1");
      assertEquals(extractSegmentNames(secondTabletSegmentDataManager.getSegments()),
          List.of("snapshot-333-tablet-1$west-1", "snapshot-333-tablet-1$west-2"));
    } finally {
      _tableDataManager.releaseSegment(secondTabletSegmentDataManager);
    }

    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager(TABLE_NAME_WITH_TYPE)).thenReturn(_tableDataManager);
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getQueryOptions()).thenReturn(Map.of());

    SingleTableExecutionInfo executionInfo =
        SingleTableExecutionInfo.create(instanceDataManager, TABLE_NAME_WITH_TYPE, refreshResponse.getTabletIds(), null,
            queryContext);
    try {
      assertTrue(executionInfo.hasTabletBackedSegments());
      assertEquals(executionInfo.getSegmentsToQuery(), refreshResponse.getTabletIds());
      assertEquals(extractSegmentNames(executionInfo.getIndexSegments()),
          List.of("snapshot-333-tablet-0$east-1", "snapshot-333-tablet-1$west-1", "snapshot-333-tablet-1$west-2"));
    } finally {
      executionInfo.releaseSegmentDataManagers();
    }

    ServerInstance serverA = new ServerInstance(new InstanceConfig("serverA_8098"));
    ServerInstance serverB = new ServerInstance(new InstanceConfig("serverB_8098"));
    Map<ServerInstance, SegmentsToQuery> routingTable = Map.of(serverA,
        new SegmentsToQuery(refreshResponse.getTabletIds(), List.of()), serverB,
        new SegmentsToQuery(List.of("snapshot-333-tablet-1"), List.of()));
    Set<String> routedTabletIds = BaseSingleStageBrokerRequestHandler.collectUniqueSegments(routingTable, false);
    Map<String, String> traceInfo = BaseSingleStageBrokerRequestHandler.buildLakehouseTraceInfo(
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.SNAPSHOT_ID, "333"), routedTabletIds, Set.of(), 0,
        BaseSingleStageBrokerRequestHandler.calculateLakehouseTabletPruningPercent(routedTabletIds.size(), 0));

    assertEquals(routedTabletIds, Set.of("snapshot-333-tablet-0", "snapshot-333-tablet-1"));
    assertEquals(traceInfo.get("lakehouseSnapshotSelector"), "snapshotId=333");
    assertEquals(traceInfo.get("lakehouseRoutedTabletCount"), "2");
    assertEquals(traceInfo.get("lakehouseRoutedTabletIds"), "snapshot-333-tablet-0,snapshot-333-tablet-1");
  }

  private PinotLakehouseTableService createLakehouseTableService(TableConfig tableConfig, Path manifestDirectory)
      throws IOException {
    Files.createDirectories(manifestDirectory);
    PinotHelixResourceManager resourceManager = Mockito.mock(PinotHelixResourceManager.class);
    ControllerMetrics controllerMetrics = Mockito.mock(ControllerMetrics.class);
    when(resourceManager.getPropertyStore()).thenReturn(_propertyStore);
    when(resourceManager.getTableConfig(TABLE_NAME_WITH_TYPE)).thenReturn(tableConfig);
    doAnswer(invocation -> ZKMetadataProvider.createSegmentZkMetadata(_propertyStore, TABLE_NAME_WITH_TYPE,
        invocation.getArgument(1))).when(resourceManager).createSegmentZkMetadata(eq(TABLE_NAME_WITH_TYPE), any());
    doAnswer(invocation -> ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, TABLE_NAME_WITH_TYPE,
        invocation.getArgument(1))).when(resourceManager).updateZkMetadata(eq(TABLE_NAME_WITH_TYPE), any());
    doAnswer(invocation -> ZKMetadataProvider.removeSegmentZKMetadata(_propertyStore, TABLE_NAME_WITH_TYPE,
        invocation.getArgument(1))).when(resourceManager).removeSegmentZKMetadata(eq(TABLE_NAME_WITH_TYPE), any());
    doNothing().when(resourceManager).assignSegments(eq(tableConfig), anyMap());
    doNothing().when(resourceManager).removeSegmentsFromIdealState(eq(TABLE_NAME_WITH_TYPE), any());

    return new PinotLakehouseTableService(resourceManager, controllerMetrics,
        new FixedLakehouseCatalogAdapterProvider(new FeasibleE2ELakehouseCatalogAdapter()),
        new LakehouseTabletPlanner(), new LocalFileSystemLakehouseManifestStore(manifestDirectory));
  }

  private LakehouseOfflineTableDataManager createLakehouseTableDataManager(TableConfig tableConfig) {
    DefaultTableDataManagerProvider provider = new DefaultTableDataManagerProvider();
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(_propertyStore);
    provider.init(createInstanceDataManagerConfig(), helixManager, new SegmentLocks(), null,
        new ServerReloadJobStatusCache("server"));
    TableDataManager tableDataManager = provider.getTableDataManager(tableConfig, createSchema(),
        new SegmentReloadSemaphore(1), _reloadExecutor, null, null, () -> true, false,
        new ServerReloadJobStatusCache("server"));
    assertTrue(tableDataManager instanceof LakehouseOfflineTableDataManager);
    return (LakehouseOfflineTableDataManager) tableDataManager;
  }

  private static List<String> extractSegmentNames(List<? extends IndexSegment> indexSegments) {
    return indexSegments.stream().map(IndexSegment::getSegmentName).collect(Collectors.toList());
  }

  private static TableConfig createLakehouseOfflineTable() {
    IcebergCatalogConfig catalogConfig = new IcebergCatalogConfig();
    catalogConfig.setCatalogType(IcebergCatalogType.CUSTOM);
    catalogConfig.setCatalogName("testCatalog");
    catalogConfig.setTableIdentifier("db." + RAW_TABLE_NAME);

    LakehouseTabletConfig tabletConfig = new LakehouseTabletConfig();
    tabletConfig.setTargetFilesPerTablet(2);
    tabletConfig.setTargetBytesPerTablet(1024L);
    tabletConfig.setMaxEnvelopeBytes(4096);

    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    lakehouseConfig.setMode(LakehouseMode.ICEBERG_NATIVE);
    lakehouseConfig.setCatalogConfig(catalogConfig);
    lakehouseConfig.setReadVisibilityMode(LakehouseReadVisibilityMode.SNAPSHOT_ONLY);
    lakehouseConfig.setWriteMode(LakehouseWriteMode.DISABLED);
    lakehouseConfig.setTabletConfig(tabletConfig);

    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName("eventTime")
        .setTimeType("MILLISECONDS").setRetentionTimeUnit("DAYS").setRetentionTimeValue("7")
        .setBrokerTenant("DefaultTenant").setServerTenant("DefaultTenant_OFFLINE").setLakehouseConfig(lakehouseConfig)
        .build();
  }

  private static LakehouseSnapshotRequest snapshotRequest(long snapshotId) {
    LakehouseSnapshotRequest snapshotRequest = new LakehouseSnapshotRequest();
    snapshotRequest.setSnapshotId(snapshotId);
    return snapshotRequest;
  }

  private static Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("region", FieldSpec.DataType.STRING)
        .addSingleValueDimension("userId", FieldSpec.DataType.STRING)
        .addMetric("metric", FieldSpec.DataType.LONG)
        .addDateTime("eventTime", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  private static InstanceDataManagerConfig createInstanceDataManagerConfig() {
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceId()).thenReturn("Server_test_1234");
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(FileUtils.getTempDirectoryPath());
    when(instanceDataManagerConfig.getMaxParallelSegmentBuilds()).thenReturn(0);
    when(instanceDataManagerConfig.getMaxParallelSegmentDownloads()).thenReturn(0);
    when(instanceDataManagerConfig.isStreamSegmentDownloadUntar()).thenReturn(false);
    when(instanceDataManagerConfig.getStreamSegmentDownloadUntarRateLimit()).thenReturn(0L);
    when(instanceDataManagerConfig.getDeletedSegmentsCacheSize()).thenReturn(100);
    when(instanceDataManagerConfig.getDeletedSegmentsCacheTtlMinutes()).thenReturn(1);
    when(instanceDataManagerConfig.getSegmentPeerDownloadScheme()).thenReturn(null);
    when(instanceDataManagerConfig.getAuthConfig()).thenReturn(null);
    return instanceDataManagerConfig;
  }

  private static final class FixedLakehouseCatalogAdapterProvider extends LakehouseCatalogAdapterProvider {
    private final LakehouseCatalogAdapter _catalogAdapter;

    private FixedLakehouseCatalogAdapterProvider(LakehouseCatalogAdapter catalogAdapter) {
      _catalogAdapter = catalogAdapter;
    }

    @Override
    public LakehouseCatalogAdapter getAdapter(LakehouseConfig lakehouseConfig) {
      return _catalogAdapter;
    }
  }

  private static final class FeasibleE2ELakehouseCatalogAdapter implements LakehouseCatalogAdapter {
    @Override
    public LakehouseSnapshotDescriptor resolveSnapshot(LakehouseConfig lakehouseConfig,
        LakehouseSnapshotRequest request)
        throws IOException {
      LakehouseSnapshotDescriptor snapshotDescriptor = new LakehouseSnapshotDescriptor();
      snapshotDescriptor.setTableIdentifier(lakehouseConfig.getCatalogConfig().getTableIdentifier());
      snapshotDescriptor.setSnapshotId(request.getSnapshotId());
      snapshotDescriptor.setSpecId(7);
      snapshotDescriptor.setCommittedAtMillis(1_700_000_000_000L);
      snapshotDescriptor.setManifestListUri("file:/tmp/testing-manifest-list.avro");
      snapshotDescriptor.setSummary(Map.of("branch", "main"));
      return snapshotDescriptor;
    }

    @Override
    public List<MicrosegmentDescriptor> listMicrosegments(LakehouseConfig lakehouseConfig,
        LakehouseSnapshotDescriptor snapshotDescriptor)
        throws IOException {
      return List.of(
          microsegment("west-1", "file:/west-1.parquet", 400L, 10L, 100L, 150L, Map.of("region", "west")),
          microsegment("west-2", "file:/west-2.parquet", 500L, 20L, 151L, 220L, Map.of("region", "west")),
          microsegment("east-1", "file:/east-1.parquet", 300L, 15L, 50L, 120L, Map.of("region", "east")));
    }

    private static MicrosegmentDescriptor microsegment(String microsegmentId, String filePath, long fileSizeBytes,
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
}
