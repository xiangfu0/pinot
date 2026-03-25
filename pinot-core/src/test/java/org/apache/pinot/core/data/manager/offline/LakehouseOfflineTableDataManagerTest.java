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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.lakehouse.MicrosegmentDescriptor;
import org.apache.pinot.spi.lakehouse.TabletManifest;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class LakehouseOfflineTableDataManagerTest {
  private static final String RAW_TABLE_NAME = "lakehouseTable";
  private static final String TABLE_NAME_WITH_TYPE = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

  private File _tempDir;
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
    _tempDir = new File(FileUtils.getTempDirectory(), "LakehouseOfflineTableDataManagerTest");
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);
    _propertyStore = new FakePropertyStore();
    _reloadExecutor = Executors.newSingleThreadExecutor();
    _tableDataManager = createTableDataManager();
  }

  @AfterMethod
  public void tearDown() {
    if (_tableDataManager != null) {
      _tableDataManager.shutDown();
    }
    if (_reloadExecutor != null) {
      _reloadExecutor.shutdownNow();
    }
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testAddOnlineSegmentLoadsTabletBackedSegmentDataManager()
      throws Exception {
    publishTablet("tablet-0", 101L, 11L, List.of(
        microsegment("file-0", 10L, 100L, 1000L, 1999L, 1L),
        microsegment("file-1", 20L, 200L, 2000L, 2999L, 2L)));

    _tableDataManager.addOnlineSegment("tablet-0");

    SegmentDataManager segmentDataManager = _tableDataManager.acquireSegment("tablet-0");
    assertNotNull(segmentDataManager);
    try {
      assertTrue(segmentDataManager instanceof LakehouseTabletSegmentDataManager);
      assertTrue(segmentDataManager.hasMultiSegments());
      assertEquals(segmentDataManager.getSegmentName(), "tablet-0");
      assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(), 30);
      assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getCrc(), "11");
      assertEquals(segmentDataManager.getSegments().size(), 2);
      assertEquals(segmentDataManager.getSegments().get(0).getSegmentName(), "tablet-0$file-0");
      assertEquals(segmentDataManager.getSegments().get(1).getSegmentName(), "tablet-0$file-1");
    } finally {
      _tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  @Test
  public void testReloadSegmentRefreshesTabletManifestVersion()
      throws Exception {
    publishTablet("tablet-0", 101L, 11L, List.of(
        microsegment("file-0", 10L, 100L, 1000L, 1999L, 1L),
        microsegment("file-1", 20L, 200L, 2000L, 2999L, 2L)));
    _tableDataManager.addOnlineSegment("tablet-0");

    publishTablet("tablet-0", 102L, 12L, List.of(
        microsegment("file-0", 15L, 150L, 1000L, 1499L, 3L),
        microsegment("file-1", 25L, 250L, 1500L, 2499L, 4L),
        microsegment("file-2", 5L, 50L, 2500L, 2999L, 5L)));

    _tableDataManager.reloadSegment("tablet-0", false, "job-1");

    SegmentDataManager segmentDataManager = _tableDataManager.acquireSegment("tablet-0");
    assertNotNull(segmentDataManager);
    try {
      assertTrue(segmentDataManager instanceof LakehouseTabletSegmentDataManager);
      assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getCrc(), "12");
      assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(), 45);
      assertEquals(segmentDataManager.getSegments().size(), 3);
      assertEquals(segmentDataManager.getSegments().get(2).getSegmentName(), "tablet-0$file-2");
    } finally {
      _tableDataManager.releaseSegment(segmentDataManager);
    }
  }

  private LakehouseOfflineTableDataManager createTableDataManager() {
    LakehouseOfflineTableDataManager tableDataManager = new LakehouseOfflineTableDataManager();
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(_propertyStore);
    tableDataManager.init(createInstanceDataManagerConfig(), helixManager, new SegmentLocks(),
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
            .setLakehouseConfig(enabledLakehouseConfig()).build(),
        createSchema(), new SegmentReloadSemaphore(1), _reloadExecutor, null, null, null, false,
        new ServerReloadJobStatusCache("server"));
    tableDataManager.start();
    return tableDataManager;
  }

  private void publishTablet(String tabletId, long snapshotId, long manifestVersion,
      List<MicrosegmentDescriptor> microsegments)
      throws Exception {
    Path manifestPath = _tempDir.toPath().resolve(tabletId + "-" + manifestVersion + ".json");
    TabletManifest tabletManifest = new TabletManifest();
    tabletManifest.setTabletId(tabletId);
    tabletManifest.setTableNameWithType(TABLE_NAME_WITH_TYPE);
    tabletManifest.setSnapshotId(snapshotId);
    tabletManifest.setSpecId(7);
    tabletManifest.setMicrosegments(microsegments);
    Files.writeString(manifestPath, JsonUtils.objectToString(tabletManifest));

    TabletMetadataEnvelope tabletMetadataEnvelope = new TabletMetadataEnvelope();
    tabletMetadataEnvelope.setTableNameWithType(TABLE_NAME_WITH_TYPE);
    tabletMetadataEnvelope.setTabletId(tabletId);
    tabletMetadataEnvelope.setSnapshotId(snapshotId);
    tabletMetadataEnvelope.setSpecId(7);
    tabletMetadataEnvelope.setManifestVersion(manifestVersion);
    tabletMetadataEnvelope.setManifestUri(manifestPath.toUri().toString());
    tabletMetadataEnvelope.setApproximateRowCount(microsegments.stream()
        .map(MicrosegmentDescriptor::getRecordCount)
        .filter(value -> value != null)
        .mapToLong(Long::longValue)
        .sum());
    tabletMetadataEnvelope.setApproximateSizeBytes(microsegments.stream()
        .map(MicrosegmentDescriptor::getFileSizeBytes)
        .filter(value -> value != null)
        .mapToLong(Long::longValue)
        .sum());
    tabletMetadataEnvelope.setMinTimeMillis(microsegments.stream()
        .map(MicrosegmentDescriptor::getMinTimeMillis)
        .filter(value -> value != null)
        .mapToLong(Long::longValue)
        .min()
        .orElse(-1L));
    tabletMetadataEnvelope.setMaxTimeMillis(microsegments.stream()
        .map(MicrosegmentDescriptor::getMaxTimeMillis)
        .filter(value -> value != null)
        .mapToLong(Long::longValue)
        .max()
        .orElse(-1L));
    ZKMetadataProvider.setTabletMetadataEnvelope(_propertyStore, tabletMetadataEnvelope);

    SegmentZKMetadata segmentZKMetadata = SegmentZKMetadataUtils.createLakehouseTabletSegmentZKMetadata(
        tabletMetadataEnvelope);
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, TABLE_NAME_WITH_TYPE, segmentZKMetadata);
  }

  private static MicrosegmentDescriptor microsegment(String microsegmentId, long recordCount, long fileSizeBytes,
      long minTimeMillis, long maxTimeMillis, long sequenceNumber) {
    MicrosegmentDescriptor microsegmentDescriptor = new MicrosegmentDescriptor();
    microsegmentDescriptor.setMicrosegmentId(microsegmentId);
    microsegmentDescriptor.setRecordCount(recordCount);
    microsegmentDescriptor.setFileSizeBytes(fileSizeBytes);
    microsegmentDescriptor.setMinTimeMillis(minTimeMillis);
    microsegmentDescriptor.setMaxTimeMillis(maxTimeMillis);
    microsegmentDescriptor.setDataSequenceNumber(sequenceNumber);
    microsegmentDescriptor.setFilePath("file:/tmp/" + microsegmentId + ".parquet");
    return microsegmentDescriptor;
  }

  private InstanceDataManagerConfig createInstanceDataManagerConfig() {
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceId()).thenReturn("Server_test_1234");
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(_tempDir.getAbsolutePath());
    when(instanceDataManagerConfig.getMaxParallelSegmentDownloads()).thenReturn(0);
    when(instanceDataManagerConfig.isStreamSegmentDownloadUntar()).thenReturn(false);
    when(instanceDataManagerConfig.getStreamSegmentDownloadUntarRateLimit()).thenReturn(0L);
    when(instanceDataManagerConfig.getDeletedSegmentsCacheSize()).thenReturn(100);
    when(instanceDataManagerConfig.getDeletedSegmentsCacheTtlMinutes()).thenReturn(1);
    when(instanceDataManagerConfig.getSegmentPeerDownloadScheme()).thenReturn(null);
    when(instanceDataManagerConfig.getAuthConfig()).thenReturn(null);
    return instanceDataManagerConfig;
  }

  private static Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("dim", FieldSpec.DataType.STRING)
        .addMetric("metric", FieldSpec.DataType.LONG)
        .build();
  }

  private static LakehouseConfig enabledLakehouseConfig() {
    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    return lakehouseConfig;
  }
}
