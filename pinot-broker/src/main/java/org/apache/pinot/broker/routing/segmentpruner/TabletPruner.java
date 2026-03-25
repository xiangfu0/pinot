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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;


/**
 * Utility for pruning tablet envelopes based on query predicates.
 *
 * <p>Phase 1 supports time-range pruning only. A tablet is pruned (excluded) when its
 * {@code [minTimeMs, maxTimeMs]} range does not overlap with the query's time range
 * {@code [queryMinTimeMs, queryMaxTimeMs]}.</p>
 *
 * <p>Two intervals {@code [a, b]} and {@code [c, d]} overlap if and only if
 * {@code a <= d && c <= b}. A tablet is therefore pruned when
 * {@code tablet.minTimeMs > queryMaxTimeMs || tablet.maxTimeMs < queryMinTimeMs}.</p>
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class TabletPruner {

  private TabletPruner() {
  }

  /**
   * Prunes tablets whose time range does not overlap with the given query time range.
   *
   * <p>If the input list is empty, an empty list is returned. If all tablets overlap with
   * the query range, the full list is returned (as a new list instance).</p>
   *
   * @param tablets the candidate tablet envelopes to evaluate
   * @param queryMinTimeMs the inclusive lower bound of the query time range (epoch ms)
   * @param queryMaxTimeMs the inclusive upper bound of the query time range (epoch ms)
   * @return a new list containing only the tablets whose time ranges overlap with the query range
   */
  public static List<TabletMetadataEnvelope> pruneByTimeRange(List<TabletMetadataEnvelope> tablets,
      long queryMinTimeMs, long queryMaxTimeMs) {
    if (tablets == null || tablets.isEmpty()) {
      return Collections.emptyList();
    }

    List<TabletMetadataEnvelope> result = new ArrayList<>(tablets.size());
    for (TabletMetadataEnvelope tablet : tablets) {
      // Keep the tablet if its time range overlaps with the query time range.
      // Two intervals [a, b] and [c, d] overlap iff a <= d && c <= b.
      if (tablet.getMinTimeMs() <= queryMaxTimeMs && tablet.getMaxTimeMs() >= queryMinTimeMs) {
        result.add(tablet);
      }
    }
    return result;
  }
}
