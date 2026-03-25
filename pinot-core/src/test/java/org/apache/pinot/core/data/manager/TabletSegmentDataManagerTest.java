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
package org.apache.pinot.core.data.manager;

import java.util.List;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TabletSegmentDataManagerTest {
  @Test
  public void testFlattensChildSegmentsForQueryExecution() {
    SegmentDataManager child1 = mockChildSegmentDataManager("tablet-1-child-1");
    SegmentDataManager child2 = mockChildSegmentDataManager("tablet-1-child-2");

    TabletSegmentDataManager tabletSegmentDataManager =
        new TabletSegmentDataManager("tablet-1", List.of(child1, child2));

    assertTrue(tabletSegmentDataManager.hasMultiSegments());
    assertEquals(tabletSegmentDataManager.getSegmentName(), "tablet-1");
    assertEquals(tabletSegmentDataManager.getSegments(), List.of(child1.getSegment(), child2.getSegment()));
  }

  @Test
  public void testSingleChildStillActsAsTabletContainer() {
    SegmentDataManager child = mockChildSegmentDataManager("tablet-2-child-1");

    TabletSegmentDataManager tabletSegmentDataManager = new TabletSegmentDataManager("tablet-2", List.of(child));

    assertTrue(tabletSegmentDataManager.hasMultiSegments());
    assertEquals(tabletSegmentDataManager.getSegments(), List.of(child.getSegment()));
    assertEquals(tabletSegmentDataManager.getSegment(), child.getSegment());
  }

  private static SegmentDataManager mockChildSegmentDataManager(String segmentName) {
    SegmentDataManager segmentDataManager = mock(SegmentDataManager.class);
    when(segmentDataManager.getReferenceCount()).thenReturn(1);
    when(segmentDataManager.getLoadTimeMs()).thenReturn(123L);
    when(segmentDataManager.getSegmentName()).thenReturn(segmentName);
    ImmutableSegment segment = mock(ImmutableSegment.class);
    when(segment.getSegmentName()).thenReturn(segmentName);
    when(segmentDataManager.getSegment()).thenReturn(segment);
    return segmentDataManager;
  }
}
