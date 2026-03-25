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
package org.apache.pinot.core.data.manager.provider;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.pinot.core.data.manager.offline.LakehouseOfflineTableDataManager;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.SegmentReloadSemaphore;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;


public class DefaultTableDataManagerProviderTest {
  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @Test
  public void testLakehouseOfflineTablesUseDedicatedManager() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    TableDataManager tableDataManager = buildProvider().getTableDataManager(
        new TableConfigBuilder(TableType.OFFLINE).setTableName("lakehouseTable")
            .setLakehouseConfig(enabledLakehouseConfig()).build(),
        new Schema.SchemaBuilder().setSchemaName("lakehouseTable").build(), new SegmentReloadSemaphore(1),
        executorService, null, null, () -> true, false, new ServerReloadJobStatusCache("server"));

    try {
      assertTrue(tableDataManager instanceof LakehouseOfflineTableDataManager);
    } finally {
      tableDataManager.shutDown();
      executorService.shutdownNow();
    }
  }

  @Test
  public void testLegacyOfflineTablesKeepExistingManager() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    TableDataManager tableDataManager = buildProvider().getTableDataManager(
        new TableConfigBuilder(TableType.OFFLINE).setTableName("legacyTable").build(),
        new Schema.SchemaBuilder().setSchemaName("legacyTable").build(), new SegmentReloadSemaphore(1),
        executorService, null, null, () -> true, false, new ServerReloadJobStatusCache("server"));

    try {
      assertTrue(tableDataManager instanceof OfflineTableDataManager);
      assertTrue(!(tableDataManager instanceof LakehouseOfflineTableDataManager));
    } finally {
      tableDataManager.shutDown();
      executorService.shutdownNow();
    }
  }

  private static DefaultTableDataManagerProvider buildProvider() {
    DefaultTableDataManagerProvider provider = new DefaultTableDataManagerProvider();
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(new FakePropertyStore());
    provider.init(createInstanceDataManagerConfig(), helixManager, new SegmentLocks(), null,
        new ServerReloadJobStatusCache("server"));
    return provider;
  }

  private static InstanceDataManagerConfig createInstanceDataManagerConfig() {
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    File tempDir = new File(FileUtils.getTempDirectory(), "DefaultTableDataManagerProviderTest");
    when(instanceDataManagerConfig.getInstanceId()).thenReturn("Server_test_1234");
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(tempDir.getAbsolutePath());
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

  private static LakehouseConfig enabledLakehouseConfig() {
    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    return lakehouseConfig;
  }
}
