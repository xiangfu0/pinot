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
package org.apache.pinot.core.operator.filter.predicate;

import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;


public class PredicateEvaluatorProvider {
  private PredicateEvaluatorProvider() {
  }

  /// Returns a [PredicateEvaluator] for the given predicate against a column with the supplied dictionary and data
  /// type. When `dictionary` is non-null the dictionary-based factory is used; otherwise the raw-value-based
  /// factory is used. Callers are responsible for deciding which dictionary (if any) to pass — the
  /// [DataSource]-aware overload [#getPredicateEvaluator(Predicate, DataSource, QueryContext)] handles the
  /// "RAW forward + standalone dictionary" decision and is the preferred entry point for any caller that has a
  /// data source at hand.
  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, @Nullable Dictionary dictionary,
      DataType dataType, @Nullable QueryContext queryContext) {
    try {
      if (dictionary != null) {
        // dictionary based predicate evaluators
        switch (predicate.getType()) {
          case EQ:
            return EqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator((EqPredicate) predicate, dictionary,
                dataType);
          case NOT_EQ:
            return NotEqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator((NotEqPredicate) predicate,
                dictionary, dataType);
          case IN:
            return InPredicateEvaluatorFactory.newDictionaryBasedEvaluator((InPredicate) predicate, dictionary,
                dataType, queryContext);
          case NOT_IN:
            return NotInPredicateEvaluatorFactory.newDictionaryBasedEvaluator((NotInPredicate) predicate, dictionary,
                dataType, queryContext);
          case RANGE:
            return RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator((RangePredicate) predicate, dictionary,
                dataType);
          case REGEXP_LIKE:
            return RegexpLikePredicateEvaluatorFactory.newDictionaryBasedEvaluator((RegexpLikePredicate) predicate,
                dictionary, dataType, queryContext);
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      } else {
        // raw value based predicate evaluators
        switch (predicate.getType()) {
          case EQ:
            return EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((EqPredicate) predicate, dataType);
          case NOT_EQ:
            return NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((NotEqPredicate) predicate, dataType);
          case IN:
            return InPredicateEvaluatorFactory.newRawValueBasedEvaluator((InPredicate) predicate, dataType);
          case NOT_IN:
            return NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator((NotInPredicate) predicate, dataType);
          case RANGE:
            return RangePredicateEvaluatorFactory.newRawValueBasedEvaluator((RangePredicate) predicate, dataType);
          case REGEXP_LIKE:
            return RegexpLikePredicateEvaluatorFactory
                .newRawValueBasedEvaluator((RegexpLikePredicate) predicate, dataType);
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      }
    } catch (Exception e) {
      // Exception here is caused by mismatch between the column data type and the predicate value in the query
      throw new BadQueryRequestException(e);
    }
  }

  /// Returns a [PredicateEvaluator] for the given predicate against the given data source. Computes the effective
  /// dictionary to drive predicate evaluation based on the column's forward-index encoding and the set of usable
  /// dict-backed secondary indexes (sorted / inverted / FST / IFST), then delegates to the per-column overload.
  ///
  /// For columns whose forward index is DICTIONARY-encoded, the dictionary is always used — values can only be
  /// materialized via the dictionary, so there is no choice.
  ///
  /// For columns with a RAW forward index AND a standalone dictionary (e.g. the dictionary exists purely to
  /// support a secondary index), the dictionary is dropped — and a raw-value-based predicate evaluator is used —
  /// when no dict-using filter operator is expected to fire for this predicate. The decision is per-predicate-type:
  ///
  ///   - EQ / NOT_EQ / IN / NOT_IN — keep the dictionary if a sorted or inverted index can fire.
  ///   - RANGE — keep the dictionary if a sorted index can fire. Range index works on raw or dict values, so its
  ///     presence alone does not justify dictionary-driven evaluation.
  ///   - REGEXP_LIKE — keep the dictionary if a sorted, inverted, FST, or IFST index can fire.
  ///   - Other types — keep the dictionary (conservative default).
  ///
  /// The `skipIndexes` query option (if set in `queryContext`) is honored: an index is considered usable only
  /// when [QueryContext#isIndexUseAllowed] returns `true` for it.
  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, DataSource dataSource,
      QueryContext queryContext) {
    Dictionary dictionary = dataSource.getDictionary();
    Dictionary effectiveDictionary = dictionary;
    if (dictionary != null) {
      ForwardIndexReader<?> forwardIndex = dataSource.getForwardIndex();
      // Transform/expression sources without a forward index are treated as DICTIONARY-encoded (the dictionary,
      // if any, is the only way to materialize values).
      boolean dictEncoded = forwardIndex == null || forwardIndex.isDictionaryEncoded();
      if (!dictEncoded) {
        // RAW forward index + standalone dictionary: keep the dictionary only when a dict-using secondary index
        // is expected to fire for this predicate type. Also honor the `skipIndexes` query option.
        boolean sortedUsable = dataSource.getDataSourceMetadata().isSorted()
            && queryContext.isIndexUseAllowed(dataSource, FieldConfig.IndexType.SORTED);
        boolean invertedUsable = dataSource.getInvertedIndex() != null
            && queryContext.isIndexUseAllowed(dataSource, FieldConfig.IndexType.INVERTED);
        boolean useDict;
        switch (predicate.getType()) {
          case EQ:
          case NOT_EQ:
          case IN:
          case NOT_IN:
            useDict = sortedUsable || invertedUsable;
            break;
          case RANGE:
            // Range index works on raw values too — only the sorted index path strictly requires the dictionary.
            useDict = sortedUsable;
            break;
          case REGEXP_LIKE:
            useDict = sortedUsable || invertedUsable
                || (dataSource.getFSTIndex() != null
                    && queryContext.isIndexUseAllowed(dataSource, FieldConfig.IndexType.FST))
                || (dataSource.getIFSTIndex() != null
                    && queryContext.isIndexUseAllowed(dataSource, FieldConfig.IndexType.IFST));
            break;
          default:
            useDict = true;
            break;
        }
        if (!useDict) {
          effectiveDictionary = null;
        }
      }
    }
    return getPredicateEvaluator(predicate, effectiveDictionary,
        dataSource.getDataSourceMetadata().getDataType(), queryContext);
  }
}
