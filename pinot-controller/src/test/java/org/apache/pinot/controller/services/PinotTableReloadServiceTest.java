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
package org.apache.pinot.controller.services;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.resources.SuccessResponse;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Tests for time range reload behavior in {@link PinotTableReloadService}. Not thread-safe.
 */
public class PinotTableReloadServiceTest {
  @Test
  public void testReloadSegmentsInTimeRangeFiltersSegmentsAndDispatches() throws Exception {
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    PinotTableReloadService service = new PinotTableReloadService(helixResourceManager, new ControllerConf(),
        mock(Executor.class), mock(HttpClientConnectionManager.class));

    String rawTableName = "myTable";
    String tableNameWithType = "myTable_OFFLINE";
    when(helixResourceManager.getExistingTableNamesWithType(rawTableName, TableType.OFFLINE))
        .thenReturn(List.of(tableNameWithType));

    long startTimestamp = 1000L;
    long endTimestamp = 2000L;
    when(helixResourceManager.getSegmentsFor(tableNameWithType, true, startTimestamp, endTimestamp, false))
        .thenReturn(List.of("seg_1", "seg_2"));

    Map<String, List<String>> serverToSegments = new HashMap<>();
    serverToSegments.put("server1", List.of("seg_1", "seg_3"));
    serverToSegments.put("server2", List.of("seg_2"));
    when(helixResourceManager.getServerToSegmentsMap(tableNameWithType, null, false)).thenReturn(serverToSegments);

    Map<String, Pair<Integer, String>> reloadResult = new HashMap<>();
    reloadResult.put("server1", Pair.of(1, "job1"));
    reloadResult.put("server2", Pair.of(1, "job2"));
    when(helixResourceManager.reloadSegments(eq(tableNameWithType), eq(false), anyMap(), anyString()))
        .thenReturn(reloadResult);
    when(helixResourceManager.addNewReloadSegmentJob(eq(tableNameWithType), anyString(), eq(null), anyString(),
        anyLong(), anyInt())).thenReturn(true);

    SuccessResponse response =
        service.reloadSegmentsInTimeRange(rawTableName, "OFFLINE", "1000", "2000", false, false, null, null);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, List<String>>> mapCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);
    verify(helixResourceManager)
        .reloadSegments(eq(tableNameWithType), eq(false), mapCaptor.capture(), jobIdCaptor.capture());
    Map<String, List<String>> capturedMap = mapCaptor.getValue();
    assertEquals(capturedMap.get("server1"), List.of("seg_1"));
    assertEquals(capturedMap.get("server2"), List.of("seg_2"));

    ArgumentCaptor<String> zkJobIdCaptor = ArgumentCaptor.forClass(String.class);
    verify(helixResourceManager)
        .addNewReloadSegmentJob(eq(tableNameWithType), anyString(), eq(null), zkJobIdCaptor.capture(), anyLong(),
            eq(2));
    assertEquals(zkJobIdCaptor.getValue(), jobIdCaptor.getValue());

    Map<String, Map<String, String>> payload =
        JsonUtils.stringToObject(response.getStatus(), new TypeReference<>() {
        });
    assertNotNull(payload);
    assertTrue(payload.containsKey(tableNameWithType));
    Map<String, String> tablePayload = payload.get(tableNameWithType);
    assertEquals(tablePayload.get("numMessagesSent"), "2");
    assertNotNull(tablePayload.get("reloadJobId"));
    assertEquals(tablePayload.get("reloadJobId"), jobIdCaptor.getValue());
  }

  @Test
  public void testReloadSegmentsInTimeRangeForceDownloadDefaultsToOffline() throws Exception {
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    PinotTableReloadService service = new PinotTableReloadService(helixResourceManager, new ControllerConf(),
        mock(Executor.class), mock(HttpClientConnectionManager.class));

    when(helixResourceManager.getExistingTableNamesWithType("rawTable", TableType.OFFLINE))
        .thenReturn(List.of("rawTable_OFFLINE"));
    when(helixResourceManager.getSegmentsFor("rawTable_OFFLINE", true, 0L, 10L, false)).thenReturn(List.of());

    ControllerApplicationException exception = null;
    try {
      service.reloadSegmentsInTimeRange("rawTable", null, "0", "10", false, true, null, null);
      fail("Expected ControllerApplicationException");
    } catch (ControllerApplicationException e) {
      exception = e;
    }
    assertNotNull(exception);
    assertEquals(exception.getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    verify(helixResourceManager).getExistingTableNamesWithType("rawTable", TableType.OFFLINE);
  }

  @Test
  public void testReloadSegmentsInTimeRangeRejectsInvalidTimestamp() {
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    PinotTableReloadService service = new PinotTableReloadService(helixResourceManager, new ControllerConf(),
        mock(Executor.class), mock(HttpClientConnectionManager.class));

    ControllerApplicationException exception = null;
    try {
      service.reloadSegmentsInTimeRange("myTable", "OFFLINE", "abc", "1000", false, false, null, null);
      fail("Expected ControllerApplicationException");
    } catch (ControllerApplicationException e) {
      exception = e;
    }
    assertNotNull(exception);
    assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    verifyNoInteractions(helixResourceManager);
  }
}
