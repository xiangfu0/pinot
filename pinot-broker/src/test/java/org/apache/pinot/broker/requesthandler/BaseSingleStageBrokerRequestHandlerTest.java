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
import org.apache.pinot.broker.routing.BrokerRoutingManager;
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
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class BaseSingleStageBrokerRequestHandlerTest {

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
    PinotConfiguration config =
        new PinotConfiguration(Map.of(Broker.CONFIG_OF_BROKER_ENABLE_QUERY_CANCELLATION, "true"));
    BrokerQueryEventListenerFactory.init(config);
    BaseSingleStageBrokerRequestHandler requestHandler =
        new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", routingManager,
            new AllowAllAccessControlFactory(), queryQuotaManager, tableCache,
            new Tracing.DefaultThreadResourceUsageAccountant()) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs,
              ServerStats serverStats, RequestContext requestContext)
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
}
