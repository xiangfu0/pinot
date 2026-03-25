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
package org.apache.pinot.common.metadata.lakehouse;

import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class TabletMetadataEnvelopeTest {

  @Test
  public void testCreationAndGetters() {
    List<String> partitionValues = Arrays.asList("2024-01-15", "US");
    TabletMetadataEnvelope envelope =
        new TabletMetadataEnvelope("tablet-001", "events_OFFLINE", 1, 1234567890L, 0, partitionValues,
            1705276800000L, 1705363200000L, 1000000L, 5368709120L, 42,
            "s3://manifests/tablet-001/v1.json");

    assertEquals(envelope.getTabletId(), "tablet-001");
    assertEquals(envelope.getTableNameWithType(), "events_OFFLINE");
    assertEquals(envelope.getVersion(), 1);
    assertEquals(envelope.getSnapshotId(), 1234567890L);
    assertEquals(envelope.getSpecId(), 0);
    assertEquals(envelope.getPartitionValues(), partitionValues);
    assertEquals(envelope.getMinTimeMs(), 1705276800000L);
    assertEquals(envelope.getMaxTimeMs(), 1705363200000L);
    assertEquals(envelope.getRowCount(), 1000000L);
    assertEquals(envelope.getSizeBytes(), 5368709120L);
    assertEquals(envelope.getMicrosegmentCount(), 42);
    assertEquals(envelope.getManifestPointerUri(), "s3://manifests/tablet-001/v1.json");
  }

  @Test
  public void testJsonRoundTrip() {
    TabletMetadataEnvelope original =
        new TabletMetadataEnvelope("tablet-002", "events_OFFLINE", 1, 9876543210L, 1,
            Arrays.asList("2024-03-01"), 1709251200000L, 1709337600000L, 500000L, 2684354560L, 20,
            "s3://manifests/tablet-002/v1.json");

    String json = original.toJsonString();
    assertNotNull(json);

    TabletMetadataEnvelope deserialized = TabletMetadataEnvelope.fromJsonString(json);
    assertEquals(deserialized.getTabletId(), "tablet-002");
    assertEquals(deserialized.getSnapshotId(), 9876543210L);
    assertEquals(deserialized.getSpecId(), 1);
    assertEquals(deserialized.getPartitionValues(), Arrays.asList("2024-03-01"));
    assertEquals(deserialized.getRowCount(), 500000L);
  }

  @Test
  public void testDefaultVersion() {
    TabletMetadataEnvelope envelope =
        new TabletMetadataEnvelope("tablet-003", "events_OFFLINE", 0, 100L, 0, null,
            0L, 0L, 0L, 0L, 0, "s3://manifests/tablet-003/v1.json");
    assertEquals(envelope.getVersion(), TabletMetadataEnvelope.CURRENT_VERSION);
  }

  @Test
  public void testForwardCompatibility()
      throws Exception {
    // Simulate a future version with unknown fields
    String futureJson = "{\"tabletId\":\"tablet-future\",\"tableNameWithType\":\"t_OFFLINE\","
        + "\"version\":2,\"snapshotId\":999,\"specId\":0,"
        + "\"minTimeMs\":0,\"maxTimeMs\":0,\"rowCount\":0,\"sizeBytes\":0,"
        + "\"microsegmentCount\":0,\"manifestPointerUri\":\"s3://m/v2.json\","
        + "\"newFutureField\":\"someValue\",\"anotherFuture\":42}";
    TabletMetadataEnvelope envelope = TabletMetadataEnvelope.fromJsonString(futureJson);
    assertEquals(envelope.getTabletId(), "tablet-future");
    assertEquals(envelope.getVersion(), 2);
  }
}
