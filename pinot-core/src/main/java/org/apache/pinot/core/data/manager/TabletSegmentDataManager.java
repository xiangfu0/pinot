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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Tablet-backed segment container that preserves the tablet ID as the routing key while exposing any child segments
 * to query-time flattening.
 *
 * <p>The current Phase 1 server slice only materializes a single placeholder child segment, but this wrapper keeps the
 * shape open for real tablet-backed execution where a tablet may expand into multiple internal child segments.</p>
 */
public class TabletSegmentDataManager extends SegmentDataManager {
  private final String _tabletId;
  private final List<SegmentDataManager> _segmentDataManagers;

  public TabletSegmentDataManager(String tabletId, List<SegmentDataManager> segmentDataManagers) {
    Preconditions.checkArgument(!segmentDataManagers.isEmpty(), "Tablet segment must have at least one child segment");
    _tabletId = tabletId;
    _segmentDataManagers = List.copyOf(segmentDataManagers);
  }

  @Override
  public long getLoadTimeMs() {
    return _segmentDataManagers.get(0).getLoadTimeMs();
  }

  @Override
  public synchronized int getReferenceCount() {
    return _segmentDataManagers.get(0).getReferenceCount();
  }

  @Override
  public String getSegmentName() {
    return _tabletId;
  }

  @Override
  public IndexSegment getSegment() {
    return _segmentDataManagers.get(0).getSegment();
  }

  @Override
  public synchronized boolean increaseReferenceCount() {
    boolean any = false;
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.increaseReferenceCount()) {
        any = true;
      }
    }
    return any;
  }

  @Override
  public synchronized boolean decreaseReferenceCount() {
    boolean any = false;
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.decreaseReferenceCount()) {
        any = true;
      }
    }
    return any;
  }

  @Override
  public boolean hasMultiSegments() {
    return true;
  }

  @Override
  public List<IndexSegment> getSegments() {
    List<IndexSegment> segments = new ArrayList<>(_segmentDataManagers.size());
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.getReferenceCount() > 0) {
        segments.add(segmentDataManager.getSegment());
      }
    }
    return segments;
  }

  @Override
  public void doOffload() {
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.getReferenceCount() == 0) {
        segmentDataManager.offload();
      }
    }
  }

  @Override
  protected void doDestroy() {
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.getReferenceCount() == 0) {
        segmentDataManager.destroy();
      }
    }
  }
}
