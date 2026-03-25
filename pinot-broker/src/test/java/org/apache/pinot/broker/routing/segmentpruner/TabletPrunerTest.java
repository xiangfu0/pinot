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
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link TabletPruner} time-range pruning utility.
 */
public class TabletPrunerTest {

  private static final String TABLE_NAME = "testTable_OFFLINE";

  private static TabletMetadataEnvelope createTablet(String tabletId, long minTimeMs, long maxTimeMs) {
    return new TabletMetadataEnvelope(
        tabletId,
        TABLE_NAME,
        TabletMetadataEnvelope.CURRENT_VERSION,
        /* snapshotId= */ 1L,
        /* specId= */ 0,
        /* partitionValues= */ null,
        minTimeMs,
        maxTimeMs,
        /* rowCount= */ 100L,
        /* sizeBytes= */ 4096L,
        /* microsegmentCount= */ 2,
        "s3://bucket/manifest/" + tabletId + ".json"
    );
  }

  @Test
  public void testAllTabletsMatch() {
    // All tablets overlap with the query range
    List<TabletMetadataEnvelope> tablets = Arrays.asList(
        createTablet("t1", 100, 200),
        createTablet("t2", 150, 300),
        createTablet("t3", 250, 400)
    );

    List<TabletMetadataEnvelope> result = TabletPruner.pruneByTimeRange(tablets, 100, 400);
    assertEquals(result.size(), 3);
    assertEquals(result.get(0).getTabletId(), "t1");
    assertEquals(result.get(1).getTabletId(), "t2");
    assertEquals(result.get(2).getTabletId(), "t3");
  }

  @Test
  public void testSomeTabletsPruned() {
    // Only t2 and t3 overlap with the query range [200, 350]
    List<TabletMetadataEnvelope> tablets = Arrays.asList(
        createTablet("t1", 100, 199),   // entirely before query range
        createTablet("t2", 150, 300),    // overlaps
        createTablet("t3", 250, 400),    // overlaps
        createTablet("t4", 351, 500)     // entirely after query range
    );

    List<TabletMetadataEnvelope> result = TabletPruner.pruneByTimeRange(tablets, 200, 350);
    assertEquals(result.size(), 2);
    assertEquals(result.get(0).getTabletId(), "t2");
    assertEquals(result.get(1).getTabletId(), "t3");
  }

  @Test
  public void testAllTabletsPruned() {
    // Query range is entirely outside all tablet ranges
    List<TabletMetadataEnvelope> tablets = Arrays.asList(
        createTablet("t1", 100, 200),
        createTablet("t2", 300, 400)
    );

    List<TabletMetadataEnvelope> result = TabletPruner.pruneByTimeRange(tablets, 500, 600);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testEmptyTabletList() {
    List<TabletMetadataEnvelope> result = TabletPruner.pruneByTimeRange(Collections.emptyList(), 100, 200);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testNullTabletList() {
    List<TabletMetadataEnvelope> result = TabletPruner.pruneByTimeRange(null, 100, 200);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testOverlappingTabletTimeRanges() {
    // Tablets with overlapping time ranges; query should match all overlapping ones
    List<TabletMetadataEnvelope> tablets = Arrays.asList(
        createTablet("t1", 100, 300),
        createTablet("t2", 200, 400),
        createTablet("t3", 350, 500),
        createTablet("t4", 600, 700)
    );

    List<TabletMetadataEnvelope> result = TabletPruner.pruneByTimeRange(tablets, 250, 380);
    assertEquals(result.size(), 3);
    assertEquals(result.get(0).getTabletId(), "t1");
    assertEquals(result.get(1).getTabletId(), "t2");
    assertEquals(result.get(2).getTabletId(), "t3");
  }

  @Test
  public void testExactBoundaryMatch() {
    // Tablet boundary exactly matches query boundary
    List<TabletMetadataEnvelope> tablets = Arrays.asList(
        createTablet("t1", 100, 200),
        createTablet("t2", 200, 300),
        createTablet("t3", 300, 400)
    );

    // Query [200, 300] should match all three because of inclusive boundaries
    List<TabletMetadataEnvelope> result = TabletPruner.pruneByTimeRange(tablets, 200, 300);
    assertEquals(result.size(), 3);
  }

  @Test
  public void testSinglePointQueryRange() {
    // Query range is a single point
    List<TabletMetadataEnvelope> tablets = Arrays.asList(
        createTablet("t1", 100, 200),
        createTablet("t2", 300, 400)
    );

    // Point query at 200 should match only t1
    List<TabletMetadataEnvelope> result = TabletPruner.pruneByTimeRange(tablets, 200, 200);
    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getTabletId(), "t1");
  }
}
