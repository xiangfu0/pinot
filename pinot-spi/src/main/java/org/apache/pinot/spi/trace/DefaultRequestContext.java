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
package org.apache.pinot.spi.trace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.exception.QueryErrorCode;


/**
 * A class to hold the details regarding a request and the statistics.
 * This object can be used to publish the query processing statistics to a stream for
 * post-processing at a finer level than metrics.
 */
public class DefaultRequestContext implements RequestScope {

  private static final String DEFAULT_TABLE_NAME = "NotYetParsed";

  private int _errorCode = 0;
  private String _query;
  private List<String> _tableNames = new ArrayList<>();
  private long _processingTimeMillis = -1;
  private long _totalDocs;
  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;
  private long _numSegmentsQueried;
  private long _numSegmentsProcessed;
  private long _numSegmentsMatched;
  private long _offlineThreadCpuTimeNs;
  private long _realtimeThreadCpuTimeNs;
  private long _offlineSystemActivitiesCpuTimeNs;
  private long _realtimeSystemActivitiesCpuTimeNs;
  private long _offlineResponseSerializationCpuTimeNs;
  private long _realtimeResponseSerializationCpuTimeNs;
  private long _offlineTotalCpuTimeNs;
  private long _realtimeTotalCpuTimeNs;
  private long _offlineThreadMemAllocatedBytes;
  private long _realtimeThreadMemAllocatedBytes;
  private long _offlineResponseSerMemAllocatedBytes;
  private long _realtimeResponseSerMemAllocatedBytes;
  private long _offlineTotalMemAllocatedBytes;
  private long _realtimeTotalMemAllocatedBytes;
  private int _numServersQueried;
  private int _numServersResponded;
  private boolean _groupsTrimmed;
  private boolean _isNumGroupsLimitReached;
  private int _numExceptions;
  private String _brokerId;
  private String _offlineServerTenant;
  private String _realtimeServerTenant;
  private long _requestId;
  private int _numRowsResultSet;
  private long _requestArrivalTimeMillis;
  private long _reduceTimeMillis;

  private FanoutType _fanoutType;
  private int _numUnavailableSegments;
  private long _numConsumingSegmentsQueried;
  private long _numConsumingSegmentsProcessed;
  private long _numConsumingSegmentsMatched;
  private long _minConsumingFreshnessTimeMs;
  private long _numSegmentsPrunedByBroker;
  private long _numSegmentsPrunedByServer;
  private long _numSegmentsPrunedInvalid;
  private long _numSegmentsPrunedByLimit;
  private long _numSegmentsPrunedByValue;
  private long _explainPlanNumEmptyFilterSegments;
  private long _explainPlanNumMatchAllFilterSegments;
  private Map<String, String> _traceInfo = new HashMap<>();
  private List<String> _processingExceptions = new ArrayList<>();
  private Map<String, List<String>> _requestHttpHeaders = new HashMap<>();

  public DefaultRequestContext() {
  }

  @Override
  public long getOfflineSystemActivitiesCpuTimeNs() {
    return _offlineSystemActivitiesCpuTimeNs;
  }

  @Override
  public void setOfflineSystemActivitiesCpuTimeNs(long offlineSystemActivitiesCpuTimeNs) {
    _offlineSystemActivitiesCpuTimeNs = offlineSystemActivitiesCpuTimeNs;
  }

  @Override
  public long getRealtimeSystemActivitiesCpuTimeNs() {
    return _realtimeSystemActivitiesCpuTimeNs;
  }

  @Override
  public void setRealtimeSystemActivitiesCpuTimeNs(long realtimeSystemActivitiesCpuTimeNs) {
    _realtimeSystemActivitiesCpuTimeNs = realtimeSystemActivitiesCpuTimeNs;
  }

  @Override
  public long getOfflineResponseSerializationCpuTimeNs() {
    return _offlineResponseSerializationCpuTimeNs;
  }

  @Override
  public void setOfflineResponseSerializationCpuTimeNs(long offlineResponseSerializationCpuTimeNs) {
    _offlineResponseSerializationCpuTimeNs = offlineResponseSerializationCpuTimeNs;
  }

  @Override
  public long getOfflineTotalCpuTimeNs() {
    return _offlineTotalCpuTimeNs;
  }

  @Override
  public void setOfflineTotalCpuTimeNs(long offlineTotalCpuTimeNs) {
    _offlineTotalCpuTimeNs = offlineTotalCpuTimeNs;
  }

  @Override
  public long getRealtimeResponseSerializationCpuTimeNs() {
    return _realtimeResponseSerializationCpuTimeNs;
  }

  @Override
  public void setRealtimeResponseSerializationCpuTimeNs(long realtimeResponseSerializationCpuTimeNs) {
    _realtimeResponseSerializationCpuTimeNs = realtimeResponseSerializationCpuTimeNs;
  }

  @Override
  public long getRealtimeTotalCpuTimeNs() {
    return _realtimeTotalCpuTimeNs;
  }

  @Override
  public void setRealtimeTotalCpuTimeNs(long realtimeTotalCpuTimeNs) {
    _realtimeTotalCpuTimeNs = realtimeTotalCpuTimeNs;
  }

  @Override
  public String getBrokerId() {
    return _brokerId;
  }

  @Override
  public String getOfflineServerTenant() {
    return _offlineServerTenant;
  }

  @Override
  public String getRealtimeServerTenant() {
    return _realtimeServerTenant;
  }

  @Override
  public long getRequestId() {
    return _requestId;
  }

  @Override
  public long getRequestArrivalTimeMillis() {
    return _requestArrivalTimeMillis;
  }

  @Override
  public long getReduceTimeMillis() {
    return _reduceTimeMillis;
  }

  @Override
  public void setErrorCode(QueryErrorCode errorCode) {
    _errorCode = errorCode.getId();
  }

  @Override
  public void setQuery(String query) {
    _query = query;
  }

  @Override
  public void setTableName(String tableName) {
    _tableNames.add(tableName);
  }

  @Override
  public void setTableNames(List<String> tableNames) {
    _tableNames.addAll(tableNames);
  }

  @Override
  public void setQueryProcessingTime(long processingTimeMillis) {
    _processingTimeMillis = processingTimeMillis;
  }

  @Override
  public void setBrokerId(String brokerId) {
    _brokerId = brokerId;
  }

  @Override
  public void setOfflineServerTenant(String offlineServerTenant) {
    _offlineServerTenant = offlineServerTenant;
  }

  @Override
  public void setRealtimeServerTenant(String realtimeServerTenant) {
    _realtimeServerTenant = realtimeServerTenant;
  }

  @Override
  public void setRequestId(long requestId) {
    _requestId = requestId;
  }

  @Override
  public void setRequestArrivalTimeMillis(long requestArrivalTimeMillis) {
    _requestArrivalTimeMillis = requestArrivalTimeMillis;
  }

  @Override
  public void setReduceTimeNanos(long reduceTimeNanos) {
    _reduceTimeMillis = TimeUnit.MILLISECONDS.convert(reduceTimeNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void setFanoutType(FanoutType fanoutType) {
    _fanoutType = fanoutType;
  }

  @Override
  public FanoutType getFanoutType() {
    return _fanoutType;
  }

  @Override
  public void setNumUnavailableSegments(int numUnavailableSegments) {
    _numUnavailableSegments = numUnavailableSegments;
  }

  @Override
  public int getNumUnavailableSegments() {
    return _numUnavailableSegments;
  }

  @Override
  public int getErrorCode() {
    return _errorCode;
  }

  @Override
  public String getQuery() {
    return _query;
  }

  @Override
  public String getTableName() {
    if (_tableNames.size() == 0) {
      return DEFAULT_TABLE_NAME;
    }
    return _tableNames.get(0);
  }

  @Override
  public List<String> getTableNames() {
    return _tableNames;
  }

  @Override
  public long getProcessingTimeMillis() {
    return _processingTimeMillis;
  }

  @Override
  public long getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  @Override
  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }

  @Override
  public long getNumSegmentsQueried() {
    return _numSegmentsQueried;
  }

  @Override
  public long getNumSegmentsProcessed() {
    return _numSegmentsProcessed;
  }

  @Override
  public long getNumSegmentsMatched() {
    return _numSegmentsMatched;
  }

  @Override
  public int getNumServersQueried() {
    return _numServersQueried;
  }

  @Override
  public int getNumServersResponded() {
    return _numServersResponded;
  }

  @Override
  public long getOfflineThreadCpuTimeNs() {
    return _offlineThreadCpuTimeNs;
  }

  @Override
  public long getRealtimeThreadCpuTimeNs() {
    return _realtimeThreadCpuTimeNs;
  }

  @Override
  public boolean isGroupsTrimmed() {
    return _groupsTrimmed;
  }

  @Override
  public boolean isNumGroupsLimitReached() {
    return _isNumGroupsLimitReached;
  }

  @Override
  public int getNumExceptions() {
    return _numExceptions;
  }

  @Override
  public boolean hasValidTableName() {
    return !_tableNames.isEmpty();
  }

  @Override
  public int getNumRowsResultSet() {
    return _numRowsResultSet;
  }

  @Override
  public void setProcessingTimeMillis(long processingTimeMillis) {
    _processingTimeMillis = processingTimeMillis;
  }

  @Override
  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
  }

  @Override
  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  @Override
  public void setNumEntriesScannedInFilter(long numEntriesScannedInFilter) {
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
  }

  @Override
  public void setNumEntriesScannedPostFilter(long numEntriesScannedPostFilter) {
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
  }

  @Override
  public void setNumSegmentsQueried(long numSegmentsQueried) {
    _numSegmentsQueried = numSegmentsQueried;
  }

  @Override
  public void setNumSegmentsProcessed(long numSegmentsProcessed) {
    _numSegmentsProcessed = numSegmentsProcessed;
  }

  @Override
  public void setNumSegmentsMatched(long numSegmentsMatched) {
    _numSegmentsMatched = numSegmentsMatched;
  }

  @Override
  public void setOfflineThreadCpuTimeNs(long offlineThreadCpuTimeNs) {
    _offlineThreadCpuTimeNs = offlineThreadCpuTimeNs;
  }

  @Override
  public void setRealtimeThreadCpuTimeNs(long realtimeThreadCpuTimeNs) {
    _realtimeThreadCpuTimeNs = realtimeThreadCpuTimeNs;
  }

  @Override
  public long getOfflineThreadMemAllocatedBytes() {
    return _offlineThreadMemAllocatedBytes;
  }

  @Override
  public void setOfflineThreadMemAllocatedBytes(long offlineThreadMemAllocatedBytes) {
    _offlineThreadMemAllocatedBytes = offlineThreadMemAllocatedBytes;
  }

  @Override
  public long getRealtimeThreadMemAllocatedBytes() {
    return _realtimeThreadMemAllocatedBytes;
  }

  @Override
  public void setRealtimeThreadMemAllocatedBytes(long realtimeThreadMemAllocatedBytes) {
    _realtimeThreadMemAllocatedBytes = realtimeThreadMemAllocatedBytes;
  }

  @Override
  public long getOfflineResponseSerMemAllocatedBytes() {
    return _offlineResponseSerMemAllocatedBytes;
  }

  @Override
  public void setOfflineResponseSerMemAllocatedBytes(long offlineResponseSerMemAllocatedBytes) {
    _offlineResponseSerMemAllocatedBytes = offlineResponseSerMemAllocatedBytes;
  }

  @Override
  public long getRealtimeResponseSerMemAllocatedBytes() {
    return _realtimeResponseSerMemAllocatedBytes;
  }

  @Override
  public void setRealtimeResponseSerMemAllocatedBytes(long realtimeResponseSerMemAllocatedBytes) {
    _realtimeResponseSerMemAllocatedBytes = realtimeResponseSerMemAllocatedBytes;
  }

  @Override
  public long getOfflineTotalMemAllocatedBytes() {
    return _offlineTotalMemAllocatedBytes;
  }

  @Override
  public void setOfflineTotalMemAllocatedBytes(long offlineTotalMemAllocatedBytes) {
    _offlineTotalMemAllocatedBytes = offlineTotalMemAllocatedBytes;
  }

  @Override
  public long getRealtimeTotalMemAllocatedBytes() {
    return _realtimeTotalMemAllocatedBytes;
  }

  @Override
  public void setRealtimeTotalMemAllocatedBytes(long realtimeTotalMemAllocatedBytes) {
    _realtimeTotalMemAllocatedBytes = realtimeTotalMemAllocatedBytes;
  }

  @Override
  public void setNumServersQueried(int numServersQueried) {
    _numServersQueried = numServersQueried;
  }

  @Override
  public void setNumServersResponded(int numServersResponded) {
    _numServersResponded = numServersResponded;
  }

  @Override
  public void setGroupsTrimmed(boolean groupsTrimmed) {
    _groupsTrimmed = groupsTrimmed;
  }

  @Override
  public void setNumGroupsLimitReached(boolean numGroupsLimitReached) {
    _isNumGroupsLimitReached = numGroupsLimitReached;
  }

  @Override
  public void setNumExceptions(int numExceptions) {
    _numExceptions = numExceptions;
  }

  @Override
  public void setNumRowsResultSet(int numRowsResultSet) {
    _numRowsResultSet = numRowsResultSet;
  }

  @Override
  public void setReduceTimeMillis(long reduceTimeMillis) {
    _reduceTimeMillis = reduceTimeMillis;
  }

  @Override
  public long getNumConsumingSegmentsQueried() {
    return _numConsumingSegmentsQueried;
  }

  @Override
  public void setNumConsumingSegmentsQueried(long numConsumingSegmentsQueried) {
    _numConsumingSegmentsQueried = numConsumingSegmentsQueried;
  }

  @Override
  public long getNumConsumingSegmentsProcessed() {
    return _numConsumingSegmentsProcessed;
  }

  @Override
  public void setNumConsumingSegmentsProcessed(long numConsumingSegmentsProcessed) {
    _numConsumingSegmentsProcessed = numConsumingSegmentsProcessed;
  }

  @Override
  public long getNumConsumingSegmentsMatched() {
    return _numConsumingSegmentsMatched;
  }

  @Override
  public void setNumConsumingSegmentsMatched(long numConsumingSegmentsMatched) {
    _numConsumingSegmentsMatched = numConsumingSegmentsMatched;
  }

  @Override
  public long getMinConsumingFreshnessTimeMs() {
    return _minConsumingFreshnessTimeMs;
  }

  @Override
  public void setMinConsumingFreshnessTimeMs(long minConsumingFreshnessTimeMs) {
    _minConsumingFreshnessTimeMs = minConsumingFreshnessTimeMs;
  }

  @Override
  public long getNumSegmentsPrunedByBroker() {
    return _numSegmentsPrunedByBroker;
  }

  @Override
  public void setNumSegmentsPrunedByBroker(long numSegmentsPrunedByBroker) {
    _numSegmentsPrunedByBroker = numSegmentsPrunedByBroker;
  }

  @Override
  public long getNumSegmentsPrunedByServer() {
    return _numSegmentsPrunedByServer;
  }

  @Override
  public void setNumSegmentsPrunedByServer(long numSegmentsPrunedByServer) {
    _numSegmentsPrunedByServer = numSegmentsPrunedByServer;
  }

  @Override
  public long getNumSegmentsPrunedInvalid() {
    return _numSegmentsPrunedInvalid;
  }

  @Override
  public void setNumSegmentsPrunedInvalid(long numSegmentsPrunedInvalid) {
    _numSegmentsPrunedInvalid = numSegmentsPrunedInvalid;
  }

  @Override
  public long getNumSegmentsPrunedByLimit() {
    return _numSegmentsPrunedByLimit;
  }

  @Override
  public void setNumSegmentsPrunedByLimit(long numSegmentsPrunedByLimit) {
    _numSegmentsPrunedByLimit = numSegmentsPrunedByLimit;
  }

  @Override
  public long getNumSegmentsPrunedByValue() {
    return _numSegmentsPrunedByValue;
  }

  @Override
  public void setNumSegmentsPrunedByValue(long numSegmentsPrunedByValue) {
    _numSegmentsPrunedByValue = numSegmentsPrunedByValue;
  }

  @Override
  public long getExplainPlanNumEmptyFilterSegments() {
    return _explainPlanNumEmptyFilterSegments;
  }

  @Override
  public void setExplainPlanNumEmptyFilterSegments(long explainPlanNumEmptyFilterSegments) {
    _explainPlanNumEmptyFilterSegments = explainPlanNumEmptyFilterSegments;
  }

  @Override
  public long getExplainPlanNumMatchAllFilterSegments() {
    return _explainPlanNumMatchAllFilterSegments;
  }

  @Override
  public void setExplainPlanNumMatchAllFilterSegments(long explainPlanNumMatchAllFilterSegments) {
    _explainPlanNumMatchAllFilterSegments = explainPlanNumMatchAllFilterSegments;
  }

  @Override
  public Map<String, String> getTraceInfo() {
    return _traceInfo;
  }

  @Override
  public void setTraceInfo(Map<String, String> traceInfo) {
    _traceInfo.putAll(traceInfo);
  }

  @Override
  public List<String> getProcessingExceptions() {
    return _processingExceptions;
  }

  @Override
  public void setProcessingExceptions(List<String> processingExceptions) {
    _processingExceptions.addAll(processingExceptions);
  }

  @Override
  public Map<String, List<String>> getRequestHttpHeaders() {
    return _requestHttpHeaders;
  }

  @Override
  public void setRequestHttpHeaders(Map<String, List<String>> requestHttpHeaders) {
    _requestHttpHeaders.putAll(requestHttpHeaders);
  }

  @Override
  public void close() {
  }
}
