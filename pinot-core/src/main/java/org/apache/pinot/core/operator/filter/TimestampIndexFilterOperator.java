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
package org.apache.pinot.core.operator.filter;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.function.DateTimeUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.TimestampIndexGranularity;
import org.apache.pinot.segment.spi.index.reader.TimestampIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A filter operator that uses Timestamp index
 */
public class TimestampIndexFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "TimestampIndexFilterOperator";
  private static final String EXPLAIN_NAME = "FILTER_TIMESTAMP_INDEX";
  private final IndexSegment _segment;
  private final String _columnName;
  private final Predicate _predicate;
  private final int _numDocs;

  private final String _granularity;
  private final TimestampIndexReader _timestampIndexReader;
  private final long _lowerBound;
  private final long _upperBound;

  public TimestampIndexFilterOperator(IndexSegment segment, Predicate predicate, int numDocs) {
    _segment = segment;
    _predicate = predicate;
    _numDocs = numDocs;
    FunctionContext functionContext = _predicate.getLhs().getFunction();
    _columnName = DateTimeUtils.getColumnNameFromFunctionContext(functionContext);
    _granularity = DateTimeUtils.getTimestampIndexGranularityFromFunctionContext(functionContext).toString();
    _timestampIndexReader = segment.getDataSource(_columnName).getTimestampIndex(_granularity);
    assert _timestampIndexReader != null;

    RangePredicate rangePredicate = (RangePredicate) _predicate;
    if (!rangePredicate.getLowerBound().equals(RangePredicate.UNBOUNDED)) {
      _lowerBound = Long.parseLong(rangePredicate.getLowerBound());
    } else {
      _lowerBound = Long.MIN_VALUE;
    }
    if (!rangePredicate.getUpperBound().equals(RangePredicate.UNBOUNDED)) {
      _upperBound = Long.parseLong(rangePredicate.getUpperBound());
    } else {
      _upperBound = Long.MAX_VALUE;
    }
  }

  @Override
  protected FilterBlock getNextBlock() {

    ImmutableRoaringBitmap matches;
    // if the implementation cannot match the entire query exactly, it will
    // yield partial matches, which need to be verified by scanning. If it
    // can answer the query exactly, this will be null.
    ImmutableRoaringBitmap partialMatches;
    int firstRangeId;
    int lastRangeId;
    matches = (ImmutableRoaringBitmap) _timestampIndexReader.getMatchingDocIds(_lowerBound, _upperBound);
    partialMatches =
        (ImmutableRoaringBitmap) _timestampIndexReader.getPartiallyMatchingDocIds(_lowerBound, _upperBound);
    // this branch is likely until _timestampIndexReader reimplemented and enabled by default
    if (partialMatches != null) {
      // Need to scan the first and last range as they might be partially matched
      DataSource dataSource = _segment.getDataSource(_columnName);
      ScanBasedFilterOperator scanBasedFilterOperator =
          new ScanBasedFilterOperator(PredicateEvaluatorProvider.getPredicateEvaluator(_predicate, dataSource.getDictionary(),
              dataSource.getDataSourceMetadata().getDataType()), dataSource, _numDocs);
      FilterBlockDocIdSet scanBasedDocIdSet = scanBasedFilterOperator.getNextBlock().getBlockDocIdSet();
      MutableRoaringBitmap docIds = ((ScanBasedDocIdIterator) scanBasedDocIdSet.iterator()).applyAnd(partialMatches);
      if (matches != null) {
        docIds.or(matches);
      }
      return new FilterBlock(new BitmapDocIdSet(docIds, _numDocs) {
        // Override this method to reflect the entries scanned
        @Override
        public long getNumEntriesScannedInFilter() {
          return scanBasedDocIdSet.getNumEntriesScannedInFilter();
        }
      });
    } else {
      return new FilterBlock(new BitmapDocIdSet(matches == null ? new MutableRoaringBitmap() : matches, _numDocs));
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:timestamp_index");
    stringBuilder.append(",operator:").append(_predicate.getType());
    stringBuilder.append(",predicate:").append(_predicate.toString());
    return stringBuilder.append(')').toString();
  }
}
