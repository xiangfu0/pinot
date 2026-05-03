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

import java.util.Arrays;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage of the dict-vs-raw decision baked into
/// [PredicateEvaluatorProvider#getPredicateEvaluator(Predicate, DataSource, QueryContext)] for the new
/// "RAW forward index + standalone dictionary" column shape. Whereas the helper-style tests would have asserted on
/// an internal "should we use the dictionary" boolean, these tests assert on the *kind* of [PredicateEvaluator]
/// the provider returns — which is the user-visible behavior driving filter-operator selection downstream:
///
///   - A [BaseDictionaryBasedPredicateEvaluator] indicates the dict-based factory was used (so a sorted-index,
///     inverted-index, or FST/IFST filter operator can fire on the dict IDs).
///   - A [BaseRawValueBasedPredicateEvaluator] indicates the raw-value factory was used (so the operator falls
///     through to scan / range over raw values).
///
/// The test exercises mixed predicates on the same column shape — including the equivalence-question scenario
/// where a `=` predicate should pick the dict path (when an inverted index is available) while a `REGEXP_LIKE`
/// on the same column with no FST/IFST/inverted/sorted should fall back to raw.
public class PredicateEvaluatorProviderMixedPredicateTest {

  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("col");
  private static final String COLUMN_NAME = "col";

  private static DataSource buildDataSource(boolean dictPresent, boolean forwardDictEncoded, boolean sorted,
      boolean inverted, boolean fst, boolean ifst) {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getColumnName()).thenReturn(COLUMN_NAME);

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.isSorted()).thenReturn(sorted);
    when(metadata.getDataType()).thenReturn(FieldSpec.DataType.STRING);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);

    if (dictPresent) {
      Dictionary dictionary = mock(Dictionary.class);
      // Make dict-based factory construction succeed for every predicate type:
      //   - indexOf returns a valid dict id (EQ / NOT_EQ / IN / NOT_IN factories).
      //   - length is non-trivial so factories don't short-circuit on cardinality.
      //   - getStringValue returns a placeholder for every dict id (dict-based REGEXP_LIKE iterates the full
      //     dictionary at construction).
      //   - isSorted=true + insertionIndexOf=0 routes RANGE to the simpler SortedDictionaryBased… path so we
      //     don't have to stub IntSet behavior for the unsorted path.
      when(dictionary.indexOf(any(String.class))).thenReturn(0);
      when(dictionary.length()).thenReturn(8);
      when(dictionary.getStringValue(anyInt())).thenReturn("placeholder");
      when(dictionary.isSorted()).thenReturn(true);
      when(dictionary.insertionIndexOf(any(String.class))).thenReturn(0);
      when(dataSource.getDictionary()).thenReturn(dictionary);
    } else {
      when(dataSource.getDictionary()).thenReturn(null);
    }

    ForwardIndexReader<?> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(forwardDictEncoded);
    when(dataSource.getForwardIndex()).thenReturn((ForwardIndexReader) forwardIndex);

    when(dataSource.getInvertedIndex()).thenReturn(inverted ? mock(InvertedIndexReader.class) : null);
    when(dataSource.getFSTIndex()).thenReturn(fst ? mock(TextIndexReader.class) : null);
    when(dataSource.getIFSTIndex()).thenReturn(ifst ? mock(TextIndexReader.class) : null);
    return dataSource;
  }

  private static QueryContext queryContextAllowAll() {
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.isIndexUseAllowed(any(DataSource.class), any(FieldConfig.IndexType.class))).thenReturn(true);
    when(queryContext.isIndexUseAllowed(any(String.class), any(FieldConfig.IndexType.class))).thenReturn(true);
    return queryContext;
  }

  private static EqPredicate eq() {
    return new EqPredicate(COLUMN, "v");
  }

  private static NotEqPredicate notEq() {
    return new NotEqPredicate(COLUMN, "v");
  }

  private static InPredicate in() {
    return new InPredicate(COLUMN, Arrays.asList("v1", "v2"));
  }

  private static NotInPredicate notIn() {
    return new NotInPredicate(COLUMN, Arrays.asList("v1", "v2"));
  }

  private static RangePredicate range() {
    return new RangePredicate(COLUMN, false, "0", false, "100", FieldSpec.DataType.INT);
  }

  private static RegexpLikePredicate regexp() {
    return new RegexpLikePredicate(COLUMN, "abc.*");
  }

  // -------------------------------------------------------------------------
  // Trivial baselines: no dictionary => raw evaluator; dict-encoded forward => dict evaluator.
  // -------------------------------------------------------------------------

  @Test
  public void testNoDictionaryProducesRawEvaluator() {
    DataSource dataSource = buildDataSource(false, false, false, false, false, false);
    PredicateEvaluator evaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(eq(), dataSource, queryContextAllowAll());
    assertTrue(evaluator instanceof BaseRawValueBasedPredicateEvaluator,
        "no dictionary => raw-value-based evaluator");
  }

  @Test
  public void testDictEncodedForwardAlwaysProducesDictEvaluator() {
    // Dict-encoded forward index: every predicate type must use the dictionary regardless of secondary indexes.
    DataSource dataSource = buildDataSource(true, true, false, false, false, false);
    QueryContext queryContext = queryContextAllowAll();
    for (Predicate p : new Predicate[] {eq(), notEq(), in(), notIn(), range(), regexp()}) {
      PredicateEvaluator evaluator = PredicateEvaluatorProvider.getPredicateEvaluator(p, dataSource, queryContext);
      assertTrue(evaluator instanceof BaseDictionaryBasedPredicateEvaluator,
          "predicate=" + p.getType() + " on dict-encoded forward must produce dict-based evaluator (got "
              + evaluator.getClass().getSimpleName() + ")");
    }
  }

  // -------------------------------------------------------------------------
  // RAW forward + standalone dict — mixed-predicate equivalence:
  // same column, different predicate types pick different evaluator types.
  // -------------------------------------------------------------------------

  /// Mixed-predicate scenario on a single RAW + dict column with an inverted index:
  ///
  ///   - `col = 'v'`  → dict-based (inverted index serves dict IDs)
  ///   - `col REGEXP_LIKE 'abc.*'` (no FST/IFST/sorted) → still dict-based here because the inverted-index path
  ///     can also fire for REGEXP via [BaseDictIdBasedRegexpLikePredicateEvaluator].
  ///
  /// This confirms the per-predicate-type decision matrix produces consistent dict-based evaluators when the
  /// dict-using inverted index is available.
  @Test
  public void testRawForwardWithInvertedIndexEqAndRegexpBothPickDictEvaluator() {
    DataSource dataSource = buildDataSource(true, false, false, true, false, false);
    QueryContext queryContext = queryContextAllowAll();

    PredicateEvaluator eqEvaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(eq(), dataSource, queryContext);
    assertTrue(eqEvaluator instanceof BaseDictionaryBasedPredicateEvaluator,
        "EQ with inverted index must use dict evaluator (got " + eqEvaluator.getClass().getSimpleName() + ")");

    PredicateEvaluator regexpEvaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(regexp(), dataSource, queryContext);
    assertTrue(regexpEvaluator instanceof BaseDictionaryBasedPredicateEvaluator,
        "REGEXP_LIKE with inverted index must use dict evaluator (got "
            + regexpEvaluator.getClass().getSimpleName() + ")");
  }

  /// The harder mixed-predicate scenario: same RAW + dict column has an inverted index ONLY (no FST/IFST/sorted),
  /// and the user runs `col IN (...) AND col > 'c'`.
  ///
  ///   - `col IN (...)` → dict-based (inverted serves IN via dict-id list)
  ///   - `col > 'c'` (RANGE)  → raw-value-based (range index alone doesn't justify keeping dict; sorted index is
  ///     the only one that strictly requires dict for RANGE, and we don't have it).
  ///
  /// This is the canonical mixed-predicate equivalence question — does the same column produce different
  /// evaluator types for different predicates on the same query?
  @Test
  public void testRawForwardWithInvertedIndexInAndRangePickDifferentEvaluators() {
    DataSource dataSource = buildDataSource(true, false, false, true, false, false);
    QueryContext queryContext = queryContextAllowAll();

    PredicateEvaluator inEvaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(in(), dataSource, queryContext);
    assertTrue(inEvaluator instanceof BaseDictionaryBasedPredicateEvaluator,
        "IN with inverted index must use dict evaluator (got " + inEvaluator.getClass().getSimpleName() + ")");

    PredicateEvaluator rangeEvaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(range(), dataSource, queryContext);
    assertTrue(rangeEvaluator instanceof BaseRawValueBasedPredicateEvaluator,
        "RANGE without sorted index must use raw evaluator even when inverted is present (got "
            + rangeEvaluator.getClass().getSimpleName() + ")");
  }

  // -------------------------------------------------------------------------
  // Scan-forced predicate: RAW forward + dict with NO usable secondary index.
  // -------------------------------------------------------------------------

  /// RAW forward + standalone dictionary column with no secondary index can serve the predicate. Any predicate
  /// type must produce a raw-value-based evaluator so the downstream filter operator falls through to scan
  /// (reading from the raw forward index, NOT from the dictionary).
  @Test
  public void testRawForwardScanOnlyAllPredicatesProduceRawEvaluator() {
    DataSource dataSource = buildDataSource(true, false, false, false, false, false);
    QueryContext queryContext = queryContextAllowAll();
    for (Predicate p : new Predicate[] {eq(), notEq(), in(), notIn(), range(), regexp()}) {
      PredicateEvaluator evaluator = PredicateEvaluatorProvider.getPredicateEvaluator(p, dataSource, queryContext);
      assertTrue(evaluator instanceof BaseRawValueBasedPredicateEvaluator,
          "predicate=" + p.getType() + " on RAW + dict scan-only column must produce raw-value-based evaluator"
              + " (got " + evaluator.getClass().getSimpleName() + ")");
    }
  }

  /// RAW forward + standalone dictionary column where every dict-using secondary index is configured but
  /// disallowed via `skipIndexes`. Behavior must match the no-secondary-index case: every predicate falls
  /// through to a raw-value-based evaluator. Pins down the `skipIndexes` honoring inside the decision.
  @Test
  public void testRawForwardSkipIndexesForcesRawEvaluator() {
    DataSource dataSource = buildDataSource(true, false, true, true, true, true);
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.isIndexUseAllowed(any(DataSource.class), any(FieldConfig.IndexType.class))).thenReturn(false);
    when(queryContext.isIndexUseAllowed(any(String.class), any(FieldConfig.IndexType.class))).thenReturn(false);
    for (Predicate p : new Predicate[] {eq(), notEq(), in(), notIn(), range(), regexp()}) {
      PredicateEvaluator evaluator = PredicateEvaluatorProvider.getPredicateEvaluator(p, dataSource, queryContext);
      assertTrue(evaluator instanceof BaseRawValueBasedPredicateEvaluator,
          "predicate=" + p.getType() + " with all dict-using indexes skipped must produce raw evaluator (got "
              + evaluator.getClass().getSimpleName() + ")");
    }
  }

  /// REGEXP_LIKE on a RAW + dict column with an FST index produces a dict-based evaluator (so the FST path can
  /// fire downstream); the same column with the FST skipped via `skipIndexes` falls back to raw. Verifies the
  /// per-predicate-type FST/IFST coverage in the decision matrix.
  @Test
  public void testRawForwardRegexpFstSwitchesEvaluatorOnSkipIndexes() {
    DataSource dataSource = buildDataSource(true, false, false, false, true, false);

    QueryContext allowFst = queryContextAllowAll();
    PredicateEvaluator withFst =
        PredicateEvaluatorProvider.getPredicateEvaluator(regexp(), dataSource, allowFst);
    assertTrue(withFst instanceof BaseDictionaryBasedPredicateEvaluator,
        "REGEXP_LIKE with FST available must use dict evaluator (got " + withFst.getClass().getSimpleName() + ")");

    QueryContext skipFst = mock(QueryContext.class);
    when(skipFst.isIndexUseAllowed(any(DataSource.class), any(FieldConfig.IndexType.class)))
        .thenAnswer(invocation -> invocation.getArgument(1) != FieldConfig.IndexType.FST);
    when(skipFst.isIndexUseAllowed(any(String.class), any(FieldConfig.IndexType.class)))
        .thenAnswer(invocation -> invocation.getArgument(1) != FieldConfig.IndexType.FST);
    PredicateEvaluator withoutFst =
        PredicateEvaluatorProvider.getPredicateEvaluator(regexp(), dataSource, skipFst);
    assertTrue(withoutFst instanceof BaseRawValueBasedPredicateEvaluator,
        "REGEXP_LIKE with FST skipped must fall back to raw evaluator (got "
            + withoutFst.getClass().getSimpleName() + ")");
  }

  /// RANGE on a RAW + dict column: only the sorted index keeps the dictionary; an inverted index alone does not.
  @Test
  public void testRawForwardRangeOnlySortedKeepsDictEvaluator() {
    QueryContext queryContext = queryContextAllowAll();

    DataSource invertedOnly = buildDataSource(true, false, false, true, false, false);
    PredicateEvaluator invertedEvaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(range(), invertedOnly, queryContext);
    assertTrue(invertedEvaluator instanceof BaseRawValueBasedPredicateEvaluator,
        "RANGE on RAW + dict with inverted only must use raw evaluator (got "
            + invertedEvaluator.getClass().getSimpleName() + ")");

    DataSource sortedOnly = buildDataSource(true, false, true, false, false, false);
    PredicateEvaluator sortedEvaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(range(), sortedOnly, queryContext);
    assertTrue(sortedEvaluator instanceof BaseDictionaryBasedPredicateEvaluator,
        "RANGE on RAW + dict with sorted index must use dict evaluator (got "
            + sortedEvaluator.getClass().getSimpleName() + ")");
  }
}
