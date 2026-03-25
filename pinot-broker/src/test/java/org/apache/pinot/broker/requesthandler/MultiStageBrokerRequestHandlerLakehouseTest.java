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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.SNAPSHOT_TAG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class MultiStageBrokerRequestHandlerLakehouseTest {

  @Test
  public void testRejectsLakehousePhysicalTable() {
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName("lakeTable")).thenReturn("lakeTable_OFFLINE");
    when(tableCache.getTableConfig("lakeTable_OFFLINE")).thenReturn(buildLakehouseTableConfig("lakeTable_OFFLINE"));

    assertLakehouseQueryRejected(tableCache, Set.of("lakeTable"));
  }

  @Test
  public void testRejectsLakehouseLogicalTable() {
    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName("logicalLakeTable")).thenReturn("logicalLakeTable");
    when(tableCache.getLogicalTableConfig("logicalLakeTable")).thenReturn(buildLogicalLakehouseConfig());
    when(tableCache.getTableConfig("lakeTable_OFFLINE")).thenReturn(buildLakehouseTableConfig("lakeTable_OFFLINE"));

    assertLakehouseQueryRejected(tableCache, Set.of("logicalLakeTable"));
  }

  @Test
  public void testBuildExplainExtraFieldsIncludesSnapshotSelector() {
    Map<String, String> extraFields = new HashMap<>();
    extraFields.put("RULE_TIMINGS", "{\"planner\":1}");

    Map<String, String> explainExtraFields =
        MultiStageBrokerRequestHandler.buildExplainExtraFields(Map.of(SNAPSHOT_TAG, "prod"), extraFields);

    assertEquals(explainExtraFields.get("RULE_TIMINGS"), "{\"planner\":1}");
    assertEquals(explainExtraFields.get("LAKEHOUSE_SNAPSHOT_SELECTOR"), "snapshotTag=prod");
    assertNull(extraFields.get("LAKEHOUSE_SNAPSHOT_SELECTOR"));
  }

  private static void assertLakehouseQueryRejected(TableCache tableCache, Set<String> tableNames) {
    try {
      MultiStageBrokerRequestHandler.validateLakehouseTablesDoNotUseMultiStage(tableNames, tableCache);
      Assert.fail("Expected lakehouse multi-stage guard to reject the table set");
    } catch (QueryException e) {
      Assert.assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
      Assert.assertEquals(e.getMessage(),
          "Lakehouse-native tables do not support the multi-stage query engine in Phase 1. "
              + "Use single-stage execution instead.");
    }
  }

  private static TableConfig buildLakehouseTableConfig(String tableNameWithType) {
    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    lakehouseConfig.setMode(LakehouseMode.ICEBERG_NATIVE);
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableNameWithType)
        .setLakehouseConfig(lakehouseConfig)
        .build();
  }

  private static LogicalTableConfig buildLogicalLakehouseConfig() {
    LogicalTableConfig logicalTableConfig = new LogicalTableConfig();
    logicalTableConfig.setTableName("logicalLakeTable");
    logicalTableConfig.setPhysicalTableConfigMap(Map.of("lakeTable_OFFLINE", new PhysicalTableConfig()));
    return logicalTableConfig;
  }
}
