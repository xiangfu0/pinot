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
package org.apache.pinot.query.runtime.plan.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.request.TableSegmentsInfo;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.LogicalTableContext;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PlanNodeToOpChain;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.rewriter.NonAggregationGroupByToDistinctQueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.PredicateComparisonRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.apache.pinot.sql.parsers.rewriter.RlsFiltersRewriter;


public class ServerPlanRequestUtils {
  private ServerPlanRequestUtils() {
  }

  private static final int DEFAULT_LEAF_NODE_LIMIT = Integer.MAX_VALUE;
  private static final List<String> QUERY_REWRITERS_CLASS_NAMES =
      ImmutableList.of(PredicateComparisonRewriter.class.getName(),
          NonAggregationGroupByToDistinctQueryRewriter.class.getName(), RlsFiltersRewriter.class.getName());
  private static final List<QueryRewriter> QUERY_REWRITERS =
      new ArrayList<>(QueryRewriterFactory.getQueryRewriters(QUERY_REWRITERS_CLASS_NAMES));
  private static final QueryOptimizer QUERY_OPTIMIZER = new QueryOptimizer();

  public static OpChain compileLeafStage(OpChainExecutionContext executionContext, StagePlan stagePlan,
      QueryExecutor leafQueryExecutor, ExecutorService executorService, Map<String, String> rowFilters) {
    return compileLeafStage(executionContext, stagePlan, leafQueryExecutor, executorService,
        (planNode, multiStageOperator) -> {
        }, false, rowFilters);
  }

  public static OpChain compileLeafStage(
      OpChainExecutionContext executionContext,
      StagePlan stagePlan,
      QueryExecutor leafQueryExecutor,
      ExecutorService executorService) {
    return compileLeafStage(executionContext, stagePlan, leafQueryExecutor, executorService,
        (planNode, multiStageOperator) -> {
        }, false, null);
  }

  /**
   * main entry point for compiling leaf-stage {@link StagePlan}.
   *
   * @param executionContext the execution context used by the leaf-stage execution engine.
   * @param stagePlan the distribute stage plan on the leaf.
   * @return an opChain that executes the leaf-stage, with the leaf-stage execution encapsulated within.
   */
  public static OpChain compileLeafStage(
      OpChainExecutionContext executionContext,
      StagePlan stagePlan,
      QueryExecutor leafQueryExecutor,
      ExecutorService executorService,
      BiConsumer<PlanNode, MultiStageOperator> relationConsumer,
      boolean explain, @Nullable Map<String, String> rowFilters) {
    long queryArrivalTimeMs = System.currentTimeMillis();

    ServerPlanRequestContext serverContext = new ServerPlanRequestContext(stagePlan, leafQueryExecutor, executorService,
        executionContext.getPipelineBreakerResult());
    // 1. Compile the PinotQuery
    constructPinotQueryPlan(serverContext, executionContext.getOpChainMetadata());
    // 2. Convert PinotQuery into InstanceRequest list (one for each physical table)
    PinotQuery pinotQuery = serverContext.getPinotQuery();
    pinotQuery.setExplain(explain);

    if (MapUtils.isNotEmpty(rowFilters)) {
      pinotQuery.setQueryOptions(rowFilters);
    }

    List<InstanceRequest> instanceRequests;
    if (executionContext.getWorkerMetadata().getLogicalTableSegmentsMap() != null) {
      instanceRequests = constructLogicalTableServerQueryRequests(executionContext, pinotQuery,
          leafQueryExecutor.getInstanceDataManager());
    } else {
      instanceRequests =
          constructServerQueryRequests(executionContext, pinotQuery, leafQueryExecutor.getInstanceDataManager());
    }
    int numRequests = instanceRequests.size();
    List<ServerQueryRequest> serverQueryRequests = new ArrayList<>(numRequests);
    for (InstanceRequest instanceRequest : instanceRequests) {
      serverQueryRequests.add(new ServerQueryRequest(instanceRequest, ServerMetrics.get(), queryArrivalTimeMs, true));
    }
    serverContext.setServerQueryRequests(serverQueryRequests);
    // 3. Compile the OpChain
    executionContext.setLeafStageContext(serverContext);
    return PlanNodeToOpChain.convert(stagePlan.getRootNode(), executionContext, relationConsumer);
  }

  /**
   * First step of Server physical plan - construct {@link PinotQuery} and determine the leaf-stage boundary
   * {@link PlanNode}.
   *
   * It constructs the content for {@link ServerPlanRequestContext#getPinotQuery()} and set the boundary via:
   *   {@link ServerPlanRequestContext#setLeafStageBoundaryNode(PlanNode)}.
   */
  private static void constructPinotQueryPlan(ServerPlanRequestContext serverContext,
      Map<String, String> requestMetadata) {
    StagePlan stagePlan = serverContext.getStagePlan();
    PinotQuery pinotQuery = serverContext.getPinotQuery();
    // attach leaf node limit it not set
    Integer leafNodeLimit = QueryOptionsUtils.getMultiStageLeafLimit(requestMetadata);
    pinotQuery.setLimit(leafNodeLimit != null ? leafNodeLimit : DEFAULT_LEAF_NODE_LIMIT);
    // visit the plan and create PinotQuery and determine the leaf stage boundary PlanNode.
    ServerPlanRequestVisitor.walkPlanNode(stagePlan.getRootNode(), serverContext);
  }

  /**
   * Entry point to construct a list of {@link InstanceRequest}s for executing leaf-stage v1 runner.
   */
  public static List<InstanceRequest> constructServerQueryRequests(OpChainExecutionContext executionContext,
      PinotQuery pinotQuery, InstanceDataManager instanceDataManager) {
    StageMetadata stageMetadata = executionContext.getStageMetadata();
    String rawTableName = TableNameBuilder.extractRawTableName(stageMetadata.getTableName());
    Map<String, List<String>> tableSegmentsMap = executionContext.getWorkerMetadata().getTableSegmentsMap();
    assert tableSegmentsMap != null;
    TimeBoundaryInfo timeBoundary = stageMetadata.getTimeBoundary();
    int numRequests = tableSegmentsMap.size();
    if (numRequests == 1) {
      Map.Entry<String, List<String>> entry = tableSegmentsMap.entrySet().iterator().next();
      String tableType = entry.getKey();
      List<String> segments = entry.getValue();
      if (tableType.equals(TableType.OFFLINE.name())) {
        String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName);
        TableDataManager tableDataManager = instanceDataManager.getTableDataManager(offlineTableName);
        Preconditions.checkState(tableDataManager != null, "Failed to find data manager for table: %s",
            offlineTableName);
        Pair<TableConfig, Schema> tableConfigAndSchema = tableDataManager.getCachedTableConfigAndSchema();
        return List.of(compileInstanceRequest(executionContext, pinotQuery, timeBoundary, TableType.OFFLINE,
            tableDataManager.getTableName(), tableConfigAndSchema.getLeft(), tableConfigAndSchema.getRight(), segments,
            null));
      } else {
        assert tableType.equals(TableType.REALTIME.name());
        String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName);
        TableDataManager tableDataManager = instanceDataManager.getTableDataManager(realtimeTableName);
        Preconditions.checkState(tableDataManager != null, "Failed to find data manager for table: %s",
            realtimeTableName);
        Pair<TableConfig, Schema> tableConfigAndSchema = tableDataManager.getCachedTableConfigAndSchema();
        return List.of(compileInstanceRequest(executionContext, pinotQuery, timeBoundary, TableType.REALTIME,
            tableDataManager.getTableName(), tableConfigAndSchema.getLeft(), tableConfigAndSchema.getRight(), segments,
            null));
      }
    } else {
      assert numRequests == 2;
      List<String> offlineSegments = tableSegmentsMap.get(TableType.OFFLINE.name());
      List<String> realtimeSegments = tableSegmentsMap.get(TableType.REALTIME.name());
      assert offlineSegments != null && realtimeSegments != null;
      String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName);
      TableDataManager offlineTableDataManager = instanceDataManager.getTableDataManager(offlineTableName);
      Preconditions.checkState(offlineTableDataManager != null, "Failed to find data manager for table: %s",
          offlineTableName);
      Pair<TableConfig, Schema> offlineTableConfigAndSchema = offlineTableDataManager.getCachedTableConfigAndSchema();
      String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName);
      TableDataManager realtimeTableDataManager = instanceDataManager.getTableDataManager(realtimeTableName);
      Preconditions.checkState(realtimeTableDataManager != null, "Failed to find data manager for table: %s",
          realtimeTableName);
      Pair<TableConfig, Schema> realtimeTableConfigAndSchema =
          realtimeTableDataManager.getCachedTableConfigAndSchema();
      // NOTE: Make a deep copy of PinotQuery for OFFLINE request.
      return List.of(
          compileInstanceRequest(executionContext, new PinotQuery(pinotQuery), timeBoundary, TableType.OFFLINE,
              offlineTableDataManager.getTableName(), offlineTableConfigAndSchema.getLeft(),
              offlineTableConfigAndSchema.getRight(), offlineSegments, null),
          compileInstanceRequest(executionContext, pinotQuery, timeBoundary, TableType.REALTIME,
              realtimeTableDataManager.getTableName(), realtimeTableConfigAndSchema.getLeft(),
              realtimeTableConfigAndSchema.getRight(), realtimeSegments, null));
    }
  }

  /**
   * Convert {@link PinotQuery} into an {@link InstanceRequest}.
   */
  private static InstanceRequest compileInstanceRequest(OpChainExecutionContext executionContext, PinotQuery pinotQuery,
      @Nullable TimeBoundaryInfo timeBoundaryInfo, TableType tableType,
      String tableNameWithType, TableConfig tableConfig, Schema schema, @Nullable List<String> segmentList,
      @Nullable List<TableSegmentsInfo> tableRouteInfoList) {
    Preconditions.checkArgument(segmentList == null || tableRouteInfoList == null,
        "Either segmentList OR tableRouteInfoList should be set");

    // Making a unique requestId for leaf stages otherwise it causes problem on stats/metrics/tracing.
    long requestId = (executionContext.getRequestId() << 16) + ((long) executionContext.getStageId() << 8) + (
        tableType == TableType.REALTIME ? 1 : 0);
    // 1. Modify the PinotQuery
    pinotQuery.getDataSource().setTableName(tableNameWithType);
    if (timeBoundaryInfo != null) {
      attachTimeBoundary(pinotQuery, timeBoundaryInfo, tableType == TableType.OFFLINE);
    }
    for (QueryRewriter queryRewriter : QUERY_REWRITERS) {
      pinotQuery = queryRewriter.rewrite(pinotQuery);
    }
    QUERY_OPTIMIZER.optimize(pinotQuery, tableConfig, schema);

    // 2. Update query options according to requestMetadataMap
    updateQueryOptions(pinotQuery, executionContext);

    // 3. Wrap PinotQuery into BrokerRequest
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(pinotQuery);
    QuerySource querySource = new QuerySource();
    querySource.setTableName(tableNameWithType);
    brokerRequest.setQuerySource(querySource);

    // 4. Create InstanceRequest with segmentList
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
    instanceRequest.setCid(QueryThreadContext.getCid());
    instanceRequest.setBrokerId("unknown");
    instanceRequest.setEnableTrace(executionContext.isTraceEnabled());
    /*
     * If segmentList is not null, it means that the query is for a single table and we can directly set the segments.
     * If segmentList is null, it means that the query is for a logical table and we need to set TableSegmentInfoList
     *
     * Either one of segmentList or tableRouteInfoList has to be set, but not both.
     */
    if (segmentList != null) {
      instanceRequest.setSearchSegments(segmentList);
    } else {
      instanceRequest.setTableSegmentsInfoList(tableRouteInfoList);
    }
    instanceRequest.setQuery(brokerRequest);

    return instanceRequest;
  }

  /**
   * Helper method to update query options.
   */
  private static void updateQueryOptions(PinotQuery pinotQuery, OpChainExecutionContext executionContext) {
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    if (queryOptions != null) {
      queryOptions.putAll(executionContext.getOpChainMetadata());
    } else {
      queryOptions = new HashMap<>(executionContext.getOpChainMetadata());
      pinotQuery.setQueryOptions(queryOptions);
    }
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS,
        Long.toString(executionContext.getActiveDeadlineMs() - System.currentTimeMillis()));
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.EXTRA_PASSIVE_TIMEOUT_MS,
        Long.toString(executionContext.getPassiveDeadlineMs() - executionContext.getActiveDeadlineMs()));
  }

  /**
   * Helper method to attach the time boundary to the given PinotQuery.
   */
  private static void attachTimeBoundary(PinotQuery pinotQuery, TimeBoundaryInfo timeBoundaryInfo,
      boolean isOfflineRequest) {
    String timeColumn = timeBoundaryInfo.getTimeColumn();
    String timeValue = timeBoundaryInfo.getTimeValue();
    Expression timeFilterExpression = RequestUtils.getFunctionExpression(
        isOfflineRequest ? FilterKind.LESS_THAN_OR_EQUAL.name() : FilterKind.GREATER_THAN.name(),
        RequestUtils.getIdentifierExpression(timeColumn), RequestUtils.getLiteralExpression(timeValue));

    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      Expression andFilterExpression =
          RequestUtils.getFunctionExpression(FilterKind.AND.name(), filterExpression, timeFilterExpression);
      pinotQuery.setFilterExpression(andFilterExpression);
    } else {
      pinotQuery.setFilterExpression(timeFilterExpression);
    }
  }

  /**
   * attach the dynamic filter to the given PinotQuery.
   */
  static void attachDynamicFilter(PinotQuery pinotQuery, List<Integer> leftKeys, List<Integer> rightKeys,
      List<Object[]> dataContainer, DataSchema dataSchema) {
    List<Expression> expressions = new ArrayList<>();
    for (int i = 0; i < leftKeys.size(); i++) {
      Expression leftExpr = pinotQuery.getSelectList().get(leftKeys.get(i));
      if (dataContainer.isEmpty()) {
        // put a constant false expression
        expressions.add(RequestUtils.getLiteralExpression(false));
      } else {
        int rightIdx = rightKeys.get(i);
        List<Expression> operands = new ArrayList<>(dataContainer.size() + 1);
        operands.add(leftExpr);
        operands.addAll(computeInOperands(dataContainer, dataSchema, rightIdx));
        expressions.add(RequestUtils.getFunctionExpression(FilterKind.IN.name(), operands));
      }
    }
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      expressions.add(filterExpression);
    }
    if (expressions.size() > 1) {
      pinotQuery.setFilterExpression(RequestUtils.getFunctionExpression(FilterKind.AND.name(), expressions));
    } else {
      pinotQuery.setFilterExpression(expressions.get(0));
    }
  }

  private static List<Expression> computeInOperands(List<Object[]> dataContainer, DataSchema dataSchema, int colIdx) {
    final DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(colIdx);
    final FieldSpec.DataType storedType = columnDataType.getStoredType().toDataType();
    final int numRows = dataContainer.size();
    List<Expression> expressions = new ArrayList<>();
    switch (storedType) {
      case INT:
        int[] arrInt = new int[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrInt[rowIdx] = (int) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrInt);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrInt[rowIdx]));
        }
        break;
      case LONG:
        long[] arrLong = new long[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrLong[rowIdx] = (long) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrLong);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrLong[rowIdx]));
        }
        break;
      case FLOAT:
        float[] arrFloat = new float[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrFloat[rowIdx] = (float) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrFloat);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrFloat[rowIdx]));
        }
        break;
      case DOUBLE:
        double[] arrDouble = new double[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrDouble[rowIdx] = (double) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrDouble);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrDouble[rowIdx]));
        }
        break;
      case STRING:
        String[] arrString = new String[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrString[rowIdx] = (String) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrString);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrString[rowIdx]));
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] arrBigDecimal = new BigDecimal[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrBigDecimal[rowIdx] = (BigDecimal) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrBigDecimal);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrBigDecimal[rowIdx]));
        }
        break;
      case BYTES:
        ByteArray[] arrBytes = new ByteArray[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrBytes[rowIdx] = (ByteArray) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrBytes);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrBytes[rowIdx].getBytes()));
        }
        break;
      default:
        throw new IllegalStateException("Illegal SV data type for IN filter: " + storedType);
    }
    return expressions;
  }

  private static List<InstanceRequest> constructLogicalTableServerQueryRequests(
      OpChainExecutionContext executionContext, PinotQuery pinotQuery, InstanceDataManager instanceDataManager) {
    StageMetadata stageMetadata = executionContext.getStageMetadata();
    String logicalTableName = stageMetadata.getTableName();
    LogicalTableContext logicalTableContext = instanceDataManager.getLogicalTableContext(logicalTableName);
    Preconditions.checkNotNull(logicalTableContext,
        String.format("LogicalTableContext not found for logical table name: %s, query context id: %s",
            logicalTableName, QueryThreadContext.getCid()));

    Map<String, List<String>> logicalTableSegmentsMap =
        executionContext.getWorkerMetadata().getLogicalTableSegmentsMap();
    List<TableSegmentsInfo> offlineTableRouteInfoList = new ArrayList<>();
    List<TableSegmentsInfo> realtimeTableRouteInfoList = new ArrayList<>();

    Preconditions.checkNotNull(logicalTableSegmentsMap);
    for (Map.Entry<String, List<String>> entry : logicalTableSegmentsMap.entrySet()) {
      String physicalTableName = entry.getKey();
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(physicalTableName);
      TableSegmentsInfo tableSegmentsInfo = new TableSegmentsInfo();
      tableSegmentsInfo.setTableName(physicalTableName);
      tableSegmentsInfo.setSegments(entry.getValue());
      if (tableType == TableType.REALTIME) {
        realtimeTableRouteInfoList.add(tableSegmentsInfo);
      } else {
        offlineTableRouteInfoList.add(tableSegmentsInfo);
      }
    }

    TimeBoundaryInfo timeBoundaryInfo = stageMetadata.getTimeBoundary();

    if (offlineTableRouteInfoList.isEmpty() || realtimeTableRouteInfoList.isEmpty()) {
      List<TableSegmentsInfo> routeInfoList =
          offlineTableRouteInfoList.isEmpty() ? realtimeTableRouteInfoList : offlineTableRouteInfoList;
      String tableType = offlineTableRouteInfoList.isEmpty() ? TableType.REALTIME.name() : TableType.OFFLINE.name();
      if (tableType.equals(TableType.OFFLINE.name())) {
        Preconditions.checkNotNull(logicalTableContext.getRefOfflineTableConfig());
        String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(logicalTableName);
        return List.of(
            compileInstanceRequest(executionContext, pinotQuery, timeBoundaryInfo, TableType.OFFLINE, offlineTableName,
                logicalTableContext.getRefOfflineTableConfig(), logicalTableContext.getLogicalTableSchema(), null,
                routeInfoList));
      } else {
        Preconditions.checkNotNull(logicalTableContext.getRefRealtimeTableConfig());
        String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(logicalTableName);
        return List.of(
            compileInstanceRequest(executionContext, pinotQuery, timeBoundaryInfo, TableType.REALTIME,
                realtimeTableName, logicalTableContext.getRefRealtimeTableConfig(),
                logicalTableContext.getLogicalTableSchema(), null, routeInfoList));
      }
    } else {
      Preconditions.checkNotNull(logicalTableContext.getRefOfflineTableConfig());
      Preconditions.checkNotNull(logicalTableContext.getRefRealtimeTableConfig());
      String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(logicalTableName);
      String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(logicalTableName);
      PinotQuery offlinePinotQuery = pinotQuery.deepCopy();
      PinotQuery realtimePinotQuery = pinotQuery.deepCopy();
      return List.of(
          compileInstanceRequest(executionContext, offlinePinotQuery, timeBoundaryInfo, TableType.OFFLINE,
              offlineTableName, logicalTableContext.getRefOfflineTableConfig(),
              logicalTableContext.getLogicalTableSchema(), null, offlineTableRouteInfoList),
          compileInstanceRequest(executionContext, realtimePinotQuery, timeBoundaryInfo, TableType.REALTIME,
              realtimeTableName, logicalTableContext.getRefRealtimeTableConfig(),
              logicalTableContext.getLogicalTableSchema(), null, realtimeTableRouteInfoList));
    }
  }
}
