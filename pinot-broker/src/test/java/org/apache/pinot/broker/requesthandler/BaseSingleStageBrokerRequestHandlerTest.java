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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.LoggerConstants;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.slf4j.MDC;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class BaseSingleStageBrokerRequestHandlerTest {

  @AfterMethod
  public void cleanupMdc() {
    MDC.clear();
  }

  @Test
  public void testUpdateColumnNames() {
    String query = "SELECT database.my_table.column_name_1st, column_name_2nd from database.my_table";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Map<String, String> columnNameMap =
        Map.of("column_name_1st", "column_name_1st", "column_name_2nd", "column_name_2nd");
    BaseSingleStageBrokerRequestHandler.updateColumnNames("database.my_table", pinotQuery, false, columnNameMap);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 2);
    for (Expression expression : pinotQuery.getSelectList()) {
      String columnName = expression.getIdentifier().getName();
      if (columnName.endsWith("column_name_1st")) {
        Assert.assertEquals(columnName, "column_name_1st");
      } else if (columnName.endsWith("column_name_2nd")) {
        Assert.assertEquals(columnName, "column_name_2nd");
      } else {
        Assert.fail("rewritten column name should be column_name_1st or column_name_1st, but is " + columnName);
      }
    }
  }

  @Test
  public void testGetActualColumnNameCaseSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable.student_name", columnNameMap,
            false);
    Assert.assertEquals(actualColumnName, "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.mytable", "db1.mytable.student_name",
            columnNameMap, false), "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.mytable", "mytable.student_name", columnNameMap,
            false), "student_name");
    boolean exceptionThrown = false;
    try {
      BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable2.student_name", columnNameMap, false);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    exceptionThrown = false;
    try {
      BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, false);
      Assert.fail("should throw exception if case sensitive and table name different");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable_student_name", columnNameMap,
            false);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable", columnNameMap, false);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testGetActualColumnNameCaseInSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, true);
    Assert.assertEquals(actualColumnName, "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.MYTABLE", "DB1.mytable.student_name",
            columnNameMap, true), "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.mytable", "MYTABLE.student_name", columnNameMap,
            true), "student_name");
    boolean exceptionThrown = false;
    try {
      BaseSingleStageBrokerRequestHandler.getActualColumnName("student", "MYTABLE2.student_name", columnNameMap, true);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE_student_name", columnNameMap, true);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("MYTABLE", "mytable", columnNameMap, true);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testCancelQuery() {
    String tableName = "myTable_OFFLINE";
    // Mock pretty much everything until the query can be submitted.
    TableCache tableCache = mock(TableCache.class);
    TableConfig tableCfg = mock(TableConfig.class);
    when(tableCache.getActualTableName(anyString())).thenReturn(tableName);
    TenantConfig tenant = new TenantConfig("tier_BROKER", "tier_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(tableName)).thenReturn(tableCfg);
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(tableName)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(tableName)).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
        new SegmentsToQuery(List.of("segment01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    long[] testRequestId = {-1};
    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);
    BaseSingleStageBrokerRequestHandler requestHandler =
        new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(), routingManager,
            new AllowAllAccessControlFactory(), queryQuotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext)
              throws Exception {
            testRequestId[0] = requestId;
            latch.await();
            return null;
          }
        };
    CompletableFuture.runAsync(() -> {
      try {
        requestHandler.handleRequest(String.format("select * from %s limit 10", tableName));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    TestUtils.waitForCondition((aVoid) -> requestHandler.getRunningServers(testRequestId[0]).size() == 1, 500, 5000,
        "Failed to submit query");
    Map.Entry<Long, String> entry = requestHandler.getRunningQueries().entrySet().iterator().next();
    Assert.assertEquals(entry.getKey().longValue(), testRequestId[0]);
    Assert.assertTrue(entry.getValue().contains("select * from myTable_OFFLINE limit 10"));
    Set<ServerInstance> servers = requestHandler.getRunningServers(testRequestId[0]);
    Assert.assertEquals(servers.size(), 1);
    Assert.assertEquals(servers.iterator().next().getHostname(), "server01");
    Assert.assertEquals(servers.iterator().next().getPort(), 9000);
    Assert.assertEquals(servers.iterator().next().getInstanceId(), "server01_9000");
    Assert.assertEquals(servers.iterator().next().getAdminEndpoint(), "http://server01:8097");
    latch.countDown();
  }

  @Test
  public void testAddRoutingPolicyInErrMsg() {
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", null, null), "error1");
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", "rt_rp", null),
        "error1, with routing policy: rt_rp [realtime]");
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", null, "off_rp"),
        "error1, with routing policy: off_rp [offline]");
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", "rt_rp", "off_rp"),
        "error1, with routing policy: rt_rp [realtime], off_rp [offline]");
  }

  @Test
  public void testQueryHashRegisteredInMdc() {
    String queryHash = "test_hash_abc123";
    LoggerConstants.QUERY_HASH_KEY.registerInMdc(queryHash);
    String mdcValue = MDC.get(LoggerConstants.QUERY_HASH_KEY.getKey());
    Assert.assertNotNull(mdcValue, "QueryHash should be registered in MDC");
    Assert.assertEquals(mdcValue, queryHash, "MDC should contain the correct queryHash");
  }

  @Test
  public void testQueryHashNotRegisteredWhenNull() {
    String mdcValue = MDC.get(LoggerConstants.QUERY_HASH_KEY.getKey());
    Assert.assertNull(mdcValue, "Null queryHash should not be registered in MDC");
  }

  @Test
  public void testQueryHashAddedToQueryOptions() {
    String query = "SELECT * FROM myTable WHERE col = 100";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    String queryHash = "generated_hash_xyz";
    pinotQuery.putToQueryOptions(
        CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH,
        queryHash);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey(
        CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH),
        "QueryHash should be added to queryOptions");
    Assert.assertEquals(
        pinotQuery.getQueryOptions().get(CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH),
        queryHash,
        "QueryOptions should contain the correct queryHash value");
  }

  @Test
  public void testRejectsConflictingLakehouseSnapshotSelectors() throws Exception {
    TableCache tableCache = mock(TableCache.class);
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);
    BaseSingleStageBrokerRequestHandler requestHandler =
        new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(), routingManager,
            new AllowAllAccessControlFactory(), queryQuotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            Assert.fail("Snapshot selector validation should fail before broker execution");
            return null;
          }
        };

    DefaultRequestContext requestContext = new DefaultRequestContext();
    requestContext.setRequestArrivalTimeMillis(System.currentTimeMillis());
    com.fasterxml.jackson.databind.node.ObjectNode request = JsonUtils.newObjectNode();
    request.put(CommonConstants.Broker.Request.SQL, "SELECT * FROM myTable");
    request.put(CommonConstants.Broker.Request.QUERY_OPTIONS, "snapshotId=1;snapshotTag=prod");

    BrokerResponseNative response =
        (BrokerResponseNative) requestHandler.handleRequest(request, null, null, requestContext, null);
    assertEquals(response.getExceptionsSize(), 1);
    assertEquals(response.getExceptions().get(0).getErrorCode(), QueryErrorCode.QUERY_VALIDATION.getId());
    Assert.assertTrue(response.getExceptions().get(0).getMessage().contains(
        "Only one lakehouse snapshot selector can be set in query options. Found: [snapshotId, snapshotTag]"));
  }

  @Test
  public void testCountUniqueLakehouseTabletsAcrossReplicas() {
    ServerInstance serverA = new ServerInstance(new InstanceConfig("serverA_8098"));
    ServerInstance serverB = new ServerInstance(new InstanceConfig("serverB_8098"));
    Map<ServerInstance, SegmentsToQuery> routingTable = Map.of(serverA,
        new SegmentsToQuery(List.of("tablet-1", "tablet-2"), List.of("tablet-optional")),
        serverB, new SegmentsToQuery(List.of("tablet-2", "tablet-3"), List.of("tablet-optional", "tablet-4")));

    assertEquals(BaseSingleStageBrokerRequestHandler.collectUniqueSegments(routingTable, false),
        Set.of("tablet-1", "tablet-2", "tablet-3"));
    assertEquals(BaseSingleStageBrokerRequestHandler.collectUniqueSegments(routingTable, true),
        Set.of("tablet-4", "tablet-optional"));
    assertEquals(BaseSingleStageBrokerRequestHandler.countUniqueSegments(routingTable, false), 3);
    assertEquals(BaseSingleStageBrokerRequestHandler.countUniqueSegments(routingTable, true), 2);
  }

  @Test
  public void testBuildLakehouseTraceInfo() {
    Map<String, String> traceInfo = BaseSingleStageBrokerRequestHandler.buildLakehouseTraceInfo(
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.SNAPSHOT_BRANCH, "main"),
        Set.of("tablet-1", "tablet-2"), Set.of("tablet-optional-1", "tablet-optional-2"), 3, 30);

    assertEquals(traceInfo.get("lakehouseSnapshotSelector"), "snapshotBranch=main");
    assertEquals(traceInfo.get("lakehouseRoutedTabletCount"), "2");
    assertEquals(traceInfo.get("lakehouseOptionalTabletCount"), "2");
    assertEquals(traceInfo.get("lakehousePrunedTabletCount"), "3");
    assertEquals(traceInfo.get("lakehouseTabletPruningPercent"), "30");
    assertEquals(traceInfo.get("lakehouseRoutedTabletIds"), "tablet-1,tablet-2");
  }

  @Test
  public void testBuildLakehouseTraceInfoTruncatesTabletIds() {
    Map<String, String> traceInfo = BaseSingleStageBrokerRequestHandler.buildLakehouseTraceInfo(
        Map.of(), Set.of("tablet-0", "tablet-1", "tablet-2", "tablet-3", "tablet-4", "tablet-5", "tablet-6",
            "tablet-7", "tablet-8"), Set.of(), 0, 0);

    assertEquals(traceInfo.get("lakehouseRoutedTabletCount"), "9");
    assertEquals(traceInfo.get("lakehouseRoutedTabletIds"),
        "tablet-0,tablet-1,tablet-2,tablet-3,tablet-4,tablet-5,tablet-6,tablet-7,...(1 more)");
  }

  @Test
  public void testCalculateLakehouseTabletPruningPercent() {
    assertEquals(BaseSingleStageBrokerRequestHandler.calculateLakehouseTabletPruningPercent(0, 0), 0);
    assertEquals(BaseSingleStageBrokerRequestHandler.calculateLakehouseTabletPruningPercent(6, 0), 0);
    assertEquals(BaseSingleStageBrokerRequestHandler.calculateLakehouseTabletPruningPercent(6, 2), 25);
  }

  @Test
  public void testLakehouseRoutingStatsAddedToBrokerResponseTraceInfo()
      throws Exception {
    String tableName = "lakehouse_OFFLINE";
    TableCache tableCache = mock(TableCache.class);
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);
    when(tableCache.getActualTableName(anyString())).thenReturn(tableName);
    when(tableCache.getColumnNameMap("lakehouse")).thenReturn(Map.of("col", "col"));
    when(tableCache.getTableConfig(tableName)).thenReturn(buildLakehouseTableConfig(tableName));

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(tableName)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(tableName)).thenReturn(10_000L);
    RoutingTable routingTable = mock(RoutingTable.class);
    ServerInstance serverA = new ServerInstance(new InstanceConfig("serverA_8098"));
    ServerInstance serverB = new ServerInstance(new InstanceConfig("serverB_8098"));
    when(routingTable.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(serverA, new SegmentsToQuery(List.of("tablet-1", "tablet-2"), List.of("tablet-3")), serverB,
            new SegmentsToQuery(List.of("tablet-2"), List.of("tablet-4"))));
    when(routingTable.getUnavailableSegments()).thenReturn(List.of());
    when(routingTable.getNumPrunedSegments()).thenReturn(2);
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(routingTable);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);
    BaseSingleStageBrokerRequestHandler requestHandler =
        new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(), routingManager,
            new AllowAllAccessControlFactory(), queryQuotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return BrokerResponseNative.empty();
          }
        };

    BrokerResponseNative response =
        (BrokerResponseNative) requestHandler.handleRequest("SELECT col FROM lakehouse WHERE col = 1");
    assertEquals(response.getTraceInfo().get("lakehouseRoutedTabletCount"), "2");
    assertEquals(response.getTraceInfo().get("lakehouseOptionalTabletCount"), "2");
    assertEquals(response.getTraceInfo().get("lakehousePrunedTabletCount"), "2");
    assertEquals(response.getTraceInfo().get("lakehouseTabletPruningPercent"), "50");
    assertEquals(response.getTraceInfo().get("lakehouseRoutedTabletIds"), "tablet-1,tablet-2");
  }

  private static TableConfig buildLakehouseTableConfig(String tableNameWithType) {
    LakehouseConfig lakehouseConfig = new LakehouseConfig();
    lakehouseConfig.setEnabled(true);
    lakehouseConfig.setMode(LakehouseMode.ICEBERG_NATIVE);
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableNameWithType).setBrokerTenant("DefaultTenant")
        .setServerTenant("DefaultTenant_OFFLINE").setLakehouseConfig(lakehouseConfig).build();
  }
}
