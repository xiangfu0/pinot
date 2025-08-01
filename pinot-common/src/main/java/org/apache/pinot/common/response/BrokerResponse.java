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
package org.apache.pinot.common.response;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Interface for broker response.
 */
public interface BrokerResponse {
  static final Logger LOGGER = LoggerFactory.getLogger(BrokerResponse.class);
  /**
   * Convert the broker response to JSON String.
   */
  default String toJsonString()
      throws IOException {
    return JsonUtils.objectToString(this);
  }

  /**
   * Write the object JSON to the output stream.
   */
  default void toOutputStream(OutputStream outputStream)
      throws IOException {
    JsonUtils.objectToOutputStream(this, outputStream);
  }

  /**
   * Convert the broker response metadata to JSON String.
   */
  default String toMetadataJsonString()
      throws IOException {
    ObjectNode objectNode = (ObjectNode) JsonUtils.objectToJsonNode(this);
    objectNode.remove("resultTable");
    return JsonUtils.objectToString(objectNode);
  }

  /**
   * Emits metrics for the BrokerResponse. Currently only emits metrics for exceptions.
   * If a broker response has multiple exceptions, we will emit metrics for all of them.
   * Thus, the sum total of all exceptions is >= total number of queries impacted.
   * Additionally, some parts of code might already be emitting metrics for individual error codes.
   * But that list isn't accurate with a many-to-many relationship (or no metrics) between error codes and metrics.
   * This method ensures we emit metrics for all queries that have exceptions with a one-to-one mapping.
   */
  default void emitBrokerResponseMetrics(BrokerMetrics brokerMetrics) {
    for (QueryProcessingException exception : this.getExceptions()) {
      QueryErrorCode queryErrorCode;
      try {
        queryErrorCode = QueryErrorCode.fromErrorCode(exception.getErrorCode());
      } catch (IllegalArgumentException e) {
        LOGGER.warn("Invalid error code: " + exception.getErrorCode(), e);
        queryErrorCode = QueryErrorCode.UNKNOWN;
      }
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.getQueryErrorMeter(queryErrorCode), 1);
    }
  }

  /**
   * Returns the result table.
   */
  @Nullable
  ResultTable getResultTable();

  /**
   * Sets the result table. We expose this method to allow modifying the results on the client side, e.g. hiding the
   * results and only showing the stats.
   */
  void setResultTable(@Nullable ResultTable resultTable);

  /**
   * Returns the number of rows in the result table.
   */
  int getNumRowsResultSet();

  /**
   * Returns whether the query doesn't guarantee to have the complete result due to exceptions or limits.
   */
  boolean isPartialResult();

  /**
   * Returns the processing exceptions encountered during the query execution.
   */
  List<QueryProcessingException> getExceptions();

  @Deprecated
  @JsonIgnore
  default List<QueryProcessingException> getProcessingExceptions() {
    return getExceptions();
  }

  @JsonIgnore
  default int getExceptionsSize() {
    return getExceptions().size();
  }

  /**
   * Returns whether groups were trimmed (reduced in size after sorting).
   */
  boolean isGroupsTrimmed();

  /**
   * Returns whether the number of groups limit has been reached.
   */
  boolean isNumGroupsLimitReached();

  /**
   * Returns whether the number of groups warning limit has been reached.
   */
  boolean isNumGroupsWarningLimitReached();

  /**
   * Returns whether the limit for max rows in join has been reached.
   */
  boolean isMaxRowsInJoinReached();

  /**
   * Returns whether the limit for max rows in window has been reached.
   */
  boolean isMaxRowsInWindowReached();

  /**
   * Returns the total time used for query execution in milliseconds.
   */
  long getTimeUsedMs();

  /**
   * Returns the request ID of the query.
   */
  String getRequestId();

  /**
   * Sets the request ID of the query.
   */
  void setRequestId(String requestId);

  /**
   * Returns the client request IF of the query (if any).
   */
  String getClientRequestId();

  /**
   * Sets the (optional) client requestID of the query;
   */
  void setClientRequestId(String clientRequestId);

  /**
   * Returns the broker ID that handled the query.
   */
  String getBrokerId();

  /**
   * Sets the broker ID that handled the query.
   */
  void setBrokerId(String brokerId);

  /**
   * Returns the number of documents selected (matching the filter) for the query.
   */
  long getNumDocsScanned();

  /**
   * Returns the total number of documents within the table(s) hit.
   */
  long getTotalDocs();

  /**
   * Returns the number of entries scanned in filter phase while processing the query.
   */
  long getNumEntriesScannedInFilter();

  /**
   * Returns the number of entries scanned post filter phase while processing the query.
   */
  long getNumEntriesScannedPostFilter();

  /**
   * Returns the number of servers queried.
   */
  int getNumServersQueried();

  /**
   * Returns the number of servers responded.
   */
  int getNumServersResponded();

  /**
   * Returns the number of segments queried by the broker after broker side pruning.
   */
  long getNumSegmentsQueried();

  /**
   * Returns the number of segments processed by server after server side pruning.
   */
  long getNumSegmentsProcessed();

  /**
   * Returns the number of segments that had at least one matching document.
   */
  long getNumSegmentsMatched();

  /**
   * Returns the number of consuming segments queried by the broker after broker side pruning.
   */
  long getNumConsumingSegmentsQueried();

  /**
   * Returns the number of consuming segments processed by server after server side pruning.
   */
  long getNumConsumingSegmentsProcessed();

  /**
   * Returns the number of consuming segments that had at least one matching document.
   */
  long getNumConsumingSegmentsMatched();

  /**
   * Returns the minimum freshness timestamp across consuming segments that were queried.
   *
   * The freshness timestamp for a segment is the largest event ingestion timestamp if provided by the stream, or the
   * index timestamp of the last message.
   */
  long getMinConsumingFreshnessTimeMs();

  /**
   * Returns the number of segments pruned on the broker side.
   */
  long getNumSegmentsPrunedByBroker();

  /**
   * Returns the number of segments pruned on the server side.
   */
  long getNumSegmentsPrunedByServer();

  /**
   * Returns the number of segments pruned due to invalid data or schema.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  long getNumSegmentsPrunedInvalid();

  /**
   * Returns the number of segments pruned by applying the limit optimization.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  long getNumSegmentsPrunedByLimit();

  /**
   * Returns the number of segments pruned applying value optimizations, like bloom filters.
   *
   * This value is always lower or equal than {@link #getNumSegmentsPrunedByServer()}
   */
  long getNumSegmentsPrunedByValue();

  /**
   * Returns the time used to reduce the server responses into the final response in milliseconds.
   */
  long getBrokerReduceTimeMs();

  /**
   * Returns the thread cpu time used for query execution against offline table in nanoseconds.
   */
  long getOfflineThreadCpuTimeNs();

  /**
   * Returns the thread cpu time used for query execution against real-time table in nanoseconds.
   */
  long getRealtimeThreadCpuTimeNs();

  /**
   * Returns the cpu time used for system activities against offline table in nanoseconds.
   */
  long getOfflineSystemActivitiesCpuTimeNs();

  /**
   * Returns the cpu time used for system activities against real-time table in nanoseconds.
   */
  long getRealtimeSystemActivitiesCpuTimeNs();

  /**
   * Returns the cpu time used for response serialization against offline table in nanoseconds.
   */
  long getOfflineResponseSerializationCpuTimeNs();

  /**
   * Returns the cpu time used for response serialization against real-time table in nanoseconds.
   */
  long getRealtimeResponseSerializationCpuTimeNs();

  /**
   * Returns the total cpu time (query execution + system activities + response serialization) used against offline
   * table in nanoseconds.
   */
  default long getOfflineTotalCpuTimeNs() {
    return getOfflineThreadCpuTimeNs() + getOfflineSystemActivitiesCpuTimeNs()
        + getOfflineResponseSerializationCpuTimeNs();
  }

  /**
   * Returns the total cpu time (query execution + system activities + response serialization) used against real-time
   * table in nanoseconds.
   */
  default long getRealtimeTotalCpuTimeNs() {
    return getRealtimeThreadCpuTimeNs() + getRealtimeSystemActivitiesCpuTimeNs()
        + getRealtimeResponseSerializationCpuTimeNs();
  }

  /**
   * Returns the thread memory bytes allocated for query execution against offline table.
   */
  long getOfflineThreadMemAllocatedBytes();

  /**
   * Returns the thread memory bytes allocated for query execution against realtime table.
   */
  long getRealtimeThreadMemAllocatedBytes();

  /**
   * Returns the memory bytes allocated  for response serialization against offline table.
   */
  long getOfflineResponseSerMemAllocatedBytes();

  /**
   * Returns the memory bytes allocated  for response serialization against realtime table.
   */
  long getRealtimeResponseSerMemAllocatedBytes();

  /**
   * Returns the total memory bytes allocated for query execution and response serialization against offline table.
   */
  default long getOfflineTotalMemAllocatedBytes() {
    return getOfflineThreadMemAllocatedBytes() + getOfflineResponseSerMemAllocatedBytes();
  }

  /**
   * Returns the total memory bytes allocated for query execution and response serialization against offline table.
   */
  default long getRealtimeTotalMemAllocatedBytes() {
    return getRealtimeThreadMemAllocatedBytes() + getRealtimeResponseSerMemAllocatedBytes();
  }


  /**
   * Returns the total number of segments with an EmptyFilterOperator when Explain Plan is called.
   */
  long getExplainPlanNumEmptyFilterSegments();

  /**
   * Returns the total number of segments with a MatchAllFilterOperator when Explain Plan is called.
   */
  long getExplainPlanNumMatchAllFilterSegments();

  /**
   * Returns the trace info for the query execution when tracing is enabled, empty map otherwise.
   */
  Map<String, String> getTraceInfo();

  /**
   * Set the tables queried in the request
   * @param tablesQueried Set of tables queried
   */
  void setTablesQueried(Set<String> tablesQueried);

  /**
   * Get the tables queried in the request
   * @return Set of tables queried
   */
  Set<String> getTablesQueried();

  /**
   * Set the pools queried in the request
   * @param pools
   */
  void setPools(Set<Integer> pools);

  /**
   * Get the pools queried in the request
   * @return
   */
  Set<Integer> getPools();

  /**
   * Set whether RLS (row level security) filters were applied to the query.
   * @param rlsFiltersApplied true if RLS filters were applied, false otherwise
   */
  void setRLSFiltersApplied(boolean rlsFiltersApplied);

  /**
   * Get whether RLS (row level security) filters were applied to the query.
   * @return true if RLS filters were applied, false otherwise
   */
  boolean getRLSFiltersApplied();
}
