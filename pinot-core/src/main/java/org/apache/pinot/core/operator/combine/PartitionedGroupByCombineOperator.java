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
package org.apache.pinot.core.operator.combine;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for group-by queries.
 * TODO: Use CombineOperatorUtils.getNumThreadsForQuery() to get the parallelism of the query instead of using
 *       all threads
 */
@SuppressWarnings("rawtypes")
public class PartitionedGroupByCombineOperator extends GroupByCombineOperator {
  public static final String ALGORITHM = "PARTITIONED";

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedGroupByCombineOperator.class);
  private static final String EXPLAIN_NAME = "PARTITIONED_COMBINE_GROUP_BY";

  protected final IndexedTable[] _indexedTables;
  private final int _partitionMask;

  public PartitionedGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(operators, queryContext, executorService);
    int numGroupByPartitions = Math.max(1, _queryContext.getNumGroupByPartitions());
    _indexedTables = new IndexedTable[numGroupByPartitions];
    _partitionMask = Integer.bitCount(numGroupByPartitions) == 1 ? numGroupByPartitions - 1 : -1;
    LOGGER.info("Using {} for group-by combine, with {} partitions and {} numTasks", EXPLAIN_NAME, numGroupByPartitions,
        _numTasks);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /**
   * Executes query on one segment in a worker thread and merges the results into the indexed table.
   */
  @Override
  protected void processSegments() {
    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        GroupByResultsBlock resultsBlock = (GroupByResultsBlock) operator.nextBlock();
        if (_indexedTables[0] == null) {
          synchronized (this) {
            if (_indexedTables[0] == null) {
              for (int i = 0; i < _indexedTables.length; i++) {
                _indexedTables[i] = createIndexedTable(resultsBlock, _numTasks);
              }
            }
          }
        }

        updateCombineResultsStats(resultsBlock);
        Collection<IntermediateRecord> intermediateRecords = resultsBlock.getIntermediateRecords();
        int mergedKeys = 0;
        if (intermediateRecords == null) {
          AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
          if (aggregationGroupByResult != null) {
            try {
              Iterator<GroupKeyGenerator.GroupKey> dicGroupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
              while (dicGroupKeyIterator.hasNext()) {
                QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
                GroupKeyGenerator.GroupKey groupKey = dicGroupKeyIterator.next();
                Object[] keys = groupKey._keys;
                Object[] values = Arrays.copyOf(keys, _numColumns);
                int groupId = groupKey._groupId;
                for (int i = 0; i < _numAggregationFunctions; i++) {
                  values[_numGroupByExpressions + i] = aggregationGroupByResult.getResultForGroupId(i, groupId);
                }
                Key key = new Key(keys);
                _indexedTables[getPartitionId(key)].upsert(key, new Record(values));
              }
            } finally {
              aggregationGroupByResult.closeGroupKeyGenerator();
            }
          }
        } else {
          for (IntermediateRecord intermediateResult : intermediateRecords) {
            QueryThreadContext.checkTerminationAndSampleUsagePeriodically(mergedKeys++, EXPLAIN_NAME);
            _indexedTables[getPartitionId(intermediateResult._key)].upsert(intermediateResult._key,
                intermediateResult._record);
          }
        }
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Combines intermediate selection result blocks from underlying operators and returns a merged one.
   * <ul>
   *   <li>
   *     Merges multiple intermediate selection result blocks as a merged one.
   *   </li>
   *   <li>
   *     Set all exceptions encountered during execution into the merged result block
   *   </li>
   * </ul>
   */
  @Override
  public BaseResultsBlock mergeResults()
      throws Exception {
    long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
    boolean opCompleted = _operatorLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    if (!opCompleted) {
      return getTimeoutResultsBlock(timeoutMs);
    }

    Throwable processingException = _processingException.get();
    if (processingException != null) {
      return getExceptionResultsBlock(processingException);
    }

    IndexedTable indexedTable = _indexedTables[0];
    for (int i = 1; i < _indexedTables.length; i++) {
      if (_indexedTables[i].size() > indexedTable.size()) {
        indexedTable.mergePartitionTable(_indexedTables[i]);
      } else {
        _indexedTables[i].mergePartitionTable(indexedTable);
        indexedTable = _indexedTables[i];
      }
    }
    return getMergedResultsBlock(indexedTable);
  }

  private int getPartitionId(Key key) {
    int hashCode = key.hashCode();
    return _partitionMask >= 0 ? (hashCode & _partitionMask) : Math.floorMod(hashCode, _indexedTables.length);
  }
}
