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
package org.apache.pinot.core.query.executor;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests the query-time seam for lakehouse tablets represented as multi-segment segment data managers.
 */
public class SingleTableExecutionInfoTest {
  @Test
  public void testLakehouseTableUsesMultiSegmentsForNonUpsert()
      throws TableNotFoundException {
    IndexSegment primarySegment = mock(IndexSegment.class);
    IndexSegment firstMicrosegment = mock(IndexSegment.class);
    IndexSegment secondMicrosegment = mock(IndexSegment.class);
    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    when(segmentDataManager.getSegment()).thenReturn(primarySegment);
    when(segmentDataManager.hasMultiSegments()).thenReturn(true);
    when(segmentDataManager.getSegments()).thenReturn(List.of(firstMicrosegment, secondMicrosegment));

    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.isUpsertEnabled()).thenReturn(false);
    when(tableDataManager.acquireSegments(anyList(), isNull(), anyList())).thenReturn(List.of(segmentDataManager));
    when(tableDataManager.getCachedTableConfigAndSchema()).thenReturn(Pair.of(new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("lakehouse")
        .setLakehouseConfig(enabledLakehouseConfig())
        .build(), null));

    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager("lakehouse_OFFLINE")).thenReturn(tableDataManager);

    SingleTableExecutionInfo executionInfo = SingleTableExecutionInfo.create(instanceDataManager, "lakehouse_OFFLINE",
        List.of("tablet-0"), null, mock(QueryContext.class));

    assertEquals(executionInfo.getIndexSegments(), List.of(firstMicrosegment, secondMicrosegment));
  }

  @Test
  public void testLakehouseTableReportsTabletBackedSegments()
      throws TableNotFoundException {
    SingleTableExecutionInfo executionInfo = createExecutionInfo(true, true);

    assertTrue(executionInfo.hasTabletBackedSegments());
  }

  @Test
  public void testLakehouseTableKeepsTabletIdsInSegmentsToQuery()
      throws TableNotFoundException {
    IndexSegment primarySegment = mock(IndexSegment.class);
    IndexSegment firstFileSegment = mock(IndexSegment.class);
    IndexSegment secondFileSegment = mock(IndexSegment.class);
    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    when(segmentDataManager.getSegment()).thenReturn(primarySegment);
    when(segmentDataManager.hasMultiSegments()).thenReturn(true);
    when(segmentDataManager.getSegments()).thenReturn(List.of(firstFileSegment, secondFileSegment));

    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.isUpsertEnabled()).thenReturn(false);
    when(tableDataManager.acquireSegments(anyList(), isNull(), anyList())).thenReturn(List.of(segmentDataManager));
    when(tableDataManager.getCachedTableConfigAndSchema()).thenReturn(Pair.of(new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("lakehouse")
        .setLakehouseConfig(enabledLakehouseConfig())
        .build(), null));

    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager("lakehouse_OFFLINE")).thenReturn(tableDataManager);

    List<String> tabletIds = List.of("tablet-0", "tablet-1");
    List<String> optionalTabletIds = List.of("tablet-2");
    when(tableDataManager.acquireSegments(eq(tabletIds), eq(optionalTabletIds), anyList()))
        .thenReturn(List.of(segmentDataManager));
    SingleTableExecutionInfo executionInfo = SingleTableExecutionInfo.create(instanceDataManager, "lakehouse_OFFLINE",
        tabletIds, optionalTabletIds, mock(QueryContext.class));

    assertEquals(executionInfo.getSegmentsToQuery(), tabletIds);
    assertEquals(executionInfo.getOptionalSegments(), optionalTabletIds);
    assertEquals(executionInfo.getIndexSegments(), List.of(firstFileSegment, secondFileSegment));
  }

  @Test
  public void testLegacyTableKeepsPrimarySegmentForNonUpsert()
      throws TableNotFoundException {
    IndexSegment primarySegment = mock(IndexSegment.class);
    IndexSegment microsegment = mock(IndexSegment.class);
    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    when(segmentDataManager.getSegment()).thenReturn(primarySegment);
    when(segmentDataManager.hasMultiSegments()).thenReturn(true);
    when(segmentDataManager.getSegments()).thenReturn(List.of(microsegment));

    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.isUpsertEnabled()).thenReturn(false);
    when(tableDataManager.acquireSegments(anyList(), isNull(), anyList())).thenReturn(List.of(segmentDataManager));
    when(tableDataManager.getCachedTableConfigAndSchema()).thenReturn(Pair.of(new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("legacy")
        .build(), null));

    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager("legacy_OFFLINE")).thenReturn(tableDataManager);

    SingleTableExecutionInfo executionInfo = SingleTableExecutionInfo.create(instanceDataManager, "legacy_OFFLINE",
        List.of("segment-0"), null, mock(QueryContext.class));

    assertEquals(executionInfo.getIndexSegments(), List.of(primarySegment));
  }

  @Test
  public void testLegacyTableDoesNotReportTabletBackedSegments()
      throws TableNotFoundException {
    SingleTableExecutionInfo executionInfo = createExecutionInfo(false, true);

    assertFalse(executionInfo.hasTabletBackedSegments());
  }

  @Test
  public void testTabletBackedFlagPlumbsIntoQueryContext()
      throws TableNotFoundException {
    SingleTableExecutionInfo executionInfo = createExecutionInfo(true, true);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM t");

    assertFalse(queryContext.hasTabletBackedSegments());
    ServerQueryExecutorV1Impl.setTabletBackedSegments(queryContext, executionInfo);
    assertTrue(queryContext.hasTabletBackedSegments());
  }

  private static SingleTableExecutionInfo createExecutionInfo(boolean lakehouseEnabled, boolean hasMultiSegments)
      throws TableNotFoundException {
    IndexSegment primarySegment = mock(IndexSegment.class);
    IndexSegment microsegment = mock(IndexSegment.class);
    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    when(segmentDataManager.getSegment()).thenReturn(primarySegment);
    when(segmentDataManager.hasMultiSegments()).thenReturn(hasMultiSegments);
    when(segmentDataManager.getSegments()).thenReturn(List.of(microsegment));

    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.isUpsertEnabled()).thenReturn(false);
    when(tableDataManager.acquireSegments(anyList(), isNull(), anyList())).thenReturn(List.of(segmentDataManager));
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.OFFLINE).setTableName("tablet");
    if (lakehouseEnabled) {
      tableConfigBuilder.setLakehouseConfig(enabledLakehouseConfig());
    }
    when(tableDataManager.getCachedTableConfigAndSchema()).thenReturn(Pair.of(tableConfigBuilder.build(), null));

    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getTableDataManager("tablet_OFFLINE")).thenReturn(tableDataManager);

    return SingleTableExecutionInfo.create(instanceDataManager, "tablet_OFFLINE", List.of("segment-0"), null,
        mock(QueryContext.class));
  }

  private static LakehouseConfig enabledLakehouseConfig() {
    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    return lakehouseConfig;
  }
}
