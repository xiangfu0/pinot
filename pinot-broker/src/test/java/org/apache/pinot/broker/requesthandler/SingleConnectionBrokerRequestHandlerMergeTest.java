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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;


/**
 * Regression test for the MV-split DataTable merge contract in
 * {@link SingleConnectionBrokerRequestHandler#mergeDataTablesByIdentity}.
 *
 * <p>Pins the invariant that the same physical server (matching hostname/port/tableType) appearing
 * on BOTH the base sub-query and the MV sub-query contributes two distinct entries to the merged
 * map.  Using a regular HashMap would collapse the two entries (since
 * {@link ServerRoutingInstance#equals} keys on hostname/port/tableType), silently undercounting
 * results during the broker reduce.
 */
public class SingleConnectionBrokerRequestHandlerMergeTest {

  @Test
  public void testSameServerOnBothBranchesProducesTwoEntries() {
    // Two distinct ServerRoutingInstance objects with identical hostname/port/tableType — i.e.
    // hash/equals identical, references different.  This mirrors the production case where the
    // base sub-query and MV sub-query each hit an OFFLINE server hosted on the same host:port.
    ServerRoutingInstance baseInstance = new ServerRoutingInstance("server-1", 9000, TableType.OFFLINE);
    ServerRoutingInstance viewInstance = new ServerRoutingInstance("server-1", 9000, TableType.OFFLINE);
    assertEquals(baseInstance, viewInstance, "Test fixture: instances must be equal()");
    assertEquals(baseInstance.hashCode(), viewInstance.hashCode(),
        "Test fixture: instances must hash equally");
    assertNotSame(baseInstance, viewInstance, "Test fixture: instances must be distinct references");

    DataTable baseDataTable = mock(DataTable.class);
    DataTable viewDataTable = mock(DataTable.class);

    ServerResponse baseResponse = mock(ServerResponse.class);
    when(baseResponse.getDataTable()).thenReturn(baseDataTable);
    when(baseResponse.getResponseSize()).thenReturn(100);
    ServerResponse viewResponse = mock(ServerResponse.class);
    when(viewResponse.getDataTable()).thenReturn(viewDataTable);
    when(viewResponse.getResponseSize()).thenReturn(50);

    Map<ServerRoutingInstance, ServerResponse> baseResponses = new HashMap<>();
    baseResponses.put(baseInstance, baseResponse);
    Map<ServerRoutingInstance, ServerResponse> viewResponses = new HashMap<>();
    viewResponses.put(viewInstance, viewResponse);

    List<ServerRoutingInstance> serversNotResponded = new ArrayList<>();
    long[] totalResponseSizeHolder = {0L};
    Map<ServerRoutingInstance, DataTable> merged = SingleConnectionBrokerRequestHandler
        .mergeDataTablesByIdentity(baseResponses, viewResponses, serversNotResponded,
            totalResponseSizeHolder);

    assertEquals(merged.size(), 2,
        "IdentityHashMap must hold both DataTables when the two sub-queries hit the same physical server. "
            + "A regular HashMap collapses them via ServerRoutingInstance.equals(), silently dropping one. "
            + "Got: " + merged);
    assertTrue(merged.values().contains(baseDataTable), "Base DataTable must be retained");
    assertTrue(merged.values().contains(viewDataTable), "View DataTable must be retained");
    assertEquals(totalResponseSizeHolder[0], 150L);
    assertEquals(serversNotResponded.size(), 0);
  }

  @Test
  public void testNullDataTableRecordedAsServerNotResponded() {
    ServerRoutingInstance instance = new ServerRoutingInstance("server-1", 9000, TableType.OFFLINE);
    ServerResponse response = mock(ServerResponse.class);
    when(response.getDataTable()).thenReturn(null);
    when(response.getResponseSize()).thenReturn(0);

    Map<ServerRoutingInstance, ServerResponse> baseResponses = new HashMap<>();
    baseResponses.put(instance, response);
    List<ServerRoutingInstance> serversNotResponded = new ArrayList<>();
    long[] totalResponseSizeHolder = {0L};
    Map<ServerRoutingInstance, DataTable> merged = SingleConnectionBrokerRequestHandler
        .mergeDataTablesByIdentity(baseResponses, new HashMap<>(), serversNotResponded,
            totalResponseSizeHolder);

    assertEquals(merged.size(), 0);
    assertEquals(serversNotResponded.size(), 1);
    assertEquals(serversNotResponded.get(0), instance);
  }
}
