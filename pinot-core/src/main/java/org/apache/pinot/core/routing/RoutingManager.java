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
package org.apache.pinot.core.routing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/**
 * The {@code RouteManager} provides the routing information for a query that requests access to a Pinot table.
 *
 * The implementation of this interface should ensure the routing and server information are up-to-date at the
 * time when the routing request was made.
 *
 * set by the user. This needs to be added to support features like segment pruning.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RoutingManager {

  /**
   * Get all enabled server instances in the cluster.
   *
   * @return all currently enabled server instances.
   */
  Map<String, ServerInstance> getEnabledServerInstanceMap();

  /**
   * Returns whether the given table is enabled
   * @param tableNameWithType Table name with type
   * @return Whether the given table is enabled
   */
  default boolean isTableDisabled(String tableNameWithType) {
    return false;
  }

  /**
   * Get the {@link RoutingTable} for a specific broker request.
   *
   * @param brokerRequest the broker request constructed from a query.
   * @return the route table.
   */
  @Nullable
  RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId);

  /**
   * Get the {@link RoutingTable} for a specific broker request.
   * @param brokerRequest the broker request constructed from a query.
   * @param tableNameWithType the name of the table.
   * @param requestId the request id.
   * @return the route table.
   */
  @Nullable
  RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId);

  /**
   * Returns the segments that are relevant for the given broker request. Returns {@code null} if the table does not
   * exist.
   */
  @Nullable
  List<String> getSegments(BrokerRequest brokerRequest);

  /**
   * Validate routing exist for a table
   *
   * @param tableNameWithType the name of the table.
   * @return true if the route table exists.
   */
  boolean routingExists(String tableNameWithType);

  /**
   * Acquire the time boundary info. Useful for hybrid logical table queries that needs to split between
   * realtime and offline.
   * @param offlineTableName offline table name
   * @return time boundary info.
   */
  @Nullable
  TimeBoundaryInfo getTimeBoundaryInfo(String offlineTableName);

  /**
   * Returns the {@link TablePartitionInfo} for a given table.
   */
  @Nullable
  TablePartitionInfo getTablePartitionInfo(String tableNameWithType);

  /**
   * Returns the {@link TablePartitionReplicatedServersInfo} for a given table.
   */
  @Nullable
  TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType);

  /**
   * Returns the enabled server instances currently serving the given table.
   */
  @Nullable
  Set<String> getServingInstances(String tableNameWithType);
}
