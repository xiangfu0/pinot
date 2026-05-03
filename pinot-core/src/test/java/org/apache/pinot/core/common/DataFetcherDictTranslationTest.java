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
package org.apache.pinot.core.common;

import java.math.BigDecimal;
import java.util.Collections;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Unit tests for [DataFetcher]'s raw-forward-with-dictionary support — the `_useDictionary` branching and the
/// `readDictIdsFromRawValues` (SV + MV) translation paths added so that columns with a RAW forward index but a
/// standalone dictionary still answer `fetchDictIds` correctly.
///
/// Uses mocks for [ForwardIndexReader] and [Dictionary]; complements the existing [DataFetcherTest] which exercises
/// real segments.
public class DataFetcherDictTranslationTest {

  private static final String COLUMN = "col";

  private static DataSource buildDataSource(ForwardIndexReader<?> forwardIndexReader, Dictionary dictionary,
      DataType storedType, boolean singleValue, int maxNumValuesPerMVEntry) {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getForwardIndex()).thenReturn((ForwardIndexReader) forwardIndexReader);
    when(dataSource.getDictionary()).thenReturn(dictionary);
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.isSingleValue()).thenReturn(singleValue);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(maxNumValuesPerMVEntry);
    when(metadata.getDataType()).thenReturn(storedType);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    return dataSource;
  }

  /// RAW forward index + standalone dictionary: `fetchDictIds` should NOT call [ForwardIndexReader#readDictIds]; it
  /// should read raw values and translate via [Dictionary#indexOf].
  @Test
  public void testFetchDictIdsForRawForwardWithDictionarySV() {
    ForwardIndexReader<ForwardIndexReaderContext> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getStoredType()).thenReturn(DataType.STRING);
    when(forwardIndex.createContext()).thenReturn(null);
    when(forwardIndex.getString(0, null)).thenReturn("alpha");
    when(forwardIndex.getString(1, null)).thenReturn("beta");
    when(forwardIndex.getString(2, null)).thenReturn("gamma");

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.indexOf("alpha")).thenReturn(7);
    when(dictionary.indexOf("beta")).thenReturn(3);
    when(dictionary.indexOf("gamma")).thenReturn(11);

    DataSource dataSource = buildDataSource(forwardIndex, dictionary, DataType.STRING, true, 0);
    DataFetcher fetcher = new DataFetcher(Collections.singletonMap(COLUMN, dataSource), Collections.emptyMap());

    int[] docIds = {0, 1, 2};
    int[] outDictIds = new int[3];
    fetcher.fetchDictIds(COLUMN, docIds, 3, outDictIds);

    assertEquals(outDictIds, new int[] {7, 3, 11});
    // The dict-encoded fast path must NOT have been taken — the forward index does not store dict IDs.
    verify(forwardIndex, never()).readDictIds(any(int[].class), anyInt(), any(int[].class), any());
  }

  /// Dict-encoded forward index: `fetchDictIds` should call [ForwardIndexReader#readDictIds] directly, not go
  /// through the raw-value translation path.
  @Test
  public void testFetchDictIdsForDictEncodedForwardSV() {
    ForwardIndexReader<ForwardIndexReaderContext> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(true);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getStoredType()).thenReturn(DataType.INT);
    when(forwardIndex.createContext()).thenReturn(null);

    Dictionary dictionary = mock(Dictionary.class);
    DataSource dataSource = buildDataSource(forwardIndex, dictionary, DataType.STRING, true, 0);
    DataFetcher fetcher = new DataFetcher(Collections.singletonMap(COLUMN, dataSource), Collections.emptyMap());

    int[] docIds = {0, 1};
    int[] outDictIds = new int[2];
    fetcher.fetchDictIds(COLUMN, docIds, 2, outDictIds);

    verify(forwardIndex).readDictIds(docIds, 2, outDictIds, null);
    // We must not have called the raw-value translation helpers.
    verify(forwardIndex, never()).getString(anyInt(), any());
  }

  /// RAW forward index + standalone dictionary on a multi-valued column: each row's raw values are read from the
  /// forward index and each is translated via [Dictionary#indexOf].
  @Test
  public void testFetchDictIdsForRawForwardWithDictionaryMV() {
    ForwardIndexReader<ForwardIndexReaderContext> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getStoredType()).thenReturn(DataType.INT);
    when(forwardIndex.createContext()).thenReturn(null);
    when(forwardIndex.getIntMV(0, null)).thenReturn(new int[] {10, 20});
    when(forwardIndex.getIntMV(1, null)).thenReturn(new int[] {20, 30, 40});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.indexOf(10)).thenReturn(0);
    when(dictionary.indexOf(20)).thenReturn(1);
    when(dictionary.indexOf(30)).thenReturn(2);
    when(dictionary.indexOf(40)).thenReturn(3);

    DataSource dataSource = buildDataSource(forwardIndex, dictionary, DataType.INT, false, 8);
    DataFetcher fetcher = new DataFetcher(Collections.singletonMap(COLUMN, dataSource), Collections.emptyMap());

    int[] docIds = {0, 1};
    int[][] outDictIds = new int[2][];
    fetcher.fetchDictIds(COLUMN, docIds, 2, outDictIds);

    assertEquals(outDictIds[0], new int[] {0, 1});
    assertEquals(outDictIds[1], new int[] {1, 2, 3});
    // Dict-encoded fast path must not have been taken.
    verify(forwardIndex, never()).getDictIdMV(anyInt(), any(int[].class), any());
  }

  /// MV BIG_DECIMAL is supported on the SV path; the MV switch must not throw "Unsupported stored type" for it.
  /// This test pins down the BIG_DECIMAL case in `readDictIdsFromRawValuesMV`.
  @Test
  public void testFetchDictIdsForRawForwardWithDictionaryMVBigDecimal() {
    ForwardIndexReader<ForwardIndexReaderContext> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getStoredType()).thenReturn(DataType.BIG_DECIMAL);
    when(forwardIndex.createContext()).thenReturn(null);
    BigDecimal v1 = new BigDecimal("1.50");
    BigDecimal v2 = new BigDecimal("2.50");
    when(forwardIndex.getBigDecimalMV(0, null)).thenReturn(new BigDecimal[] {v1, v2});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.indexOf(v1)).thenReturn(5);
    when(dictionary.indexOf(v2)).thenReturn(6);

    DataSource dataSource = buildDataSource(forwardIndex, dictionary, DataType.BIG_DECIMAL, false, 4);
    DataFetcher fetcher = new DataFetcher(Collections.singletonMap(COLUMN, dataSource), Collections.emptyMap());

    int[] docIds = {0};
    int[][] outDictIds = new int[1][];
    fetcher.fetchDictIds(COLUMN, docIds, 1, outDictIds);

    assertEquals(outDictIds[0], new int[] {5, 6});
  }

  /// When the standalone dictionary doesn't contain a value found in the RAW forward index, `indexOf` returns
  /// `Dictionary.NULL_VALUE_INDEX` (`-1`). Writing `-1` into the dictId buffer would silently corrupt downstream
  /// dict-id-keyed group-by / distinct buckets, so the translation path fails fast with an `IllegalStateException`.
  @Test(expectedExceptions = AssertionError.class,
      expectedExceptionsMessageRegExp = ".*Dictionary lookup miss.*")
  public void testFetchDictIdsThrowsWhenDictionaryMissesValueSV() {
    ForwardIndexReader<ForwardIndexReaderContext> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getStoredType()).thenReturn(DataType.STRING);
    when(forwardIndex.createContext()).thenReturn(null);
    when(forwardIndex.getString(0, null)).thenReturn("missing");

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.indexOf("missing")).thenReturn(Dictionary.NULL_VALUE_INDEX);

    DataSource dataSource = buildDataSource(forwardIndex, dictionary, DataType.STRING, true, 0);
    DataFetcher fetcher = new DataFetcher(Collections.singletonMap(COLUMN, dataSource), Collections.emptyMap());

    fetcher.fetchDictIds(COLUMN, new int[] {0}, 1, new int[1]);
  }

  /// MV variant of the dictionary-miss safety check — same contract, must throw rather than silently propagate -1.
  @Test(expectedExceptions = AssertionError.class,
      expectedExceptionsMessageRegExp = ".*Dictionary lookup miss.*")
  public void testFetchDictIdsThrowsWhenDictionaryMissesValueMV() {
    ForwardIndexReader<ForwardIndexReaderContext> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getStoredType()).thenReturn(DataType.INT);
    when(forwardIndex.createContext()).thenReturn(null);
    when(forwardIndex.getIntMV(0, null)).thenReturn(new int[] {99});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.indexOf(99)).thenReturn(Dictionary.NULL_VALUE_INDEX);

    DataSource dataSource = buildDataSource(forwardIndex, dictionary, DataType.INT, false, 4);
    DataFetcher fetcher = new DataFetcher(Collections.singletonMap(COLUMN, dataSource), Collections.emptyMap());

    fetcher.fetchDictIds(COLUMN, new int[] {0}, 1, new int[1][]);
  }

  /// Cross-encoding equivalence: the same MV column data is presented twice — once via a DICTIONARY-encoded
  /// forward index (the historical baseline) and once via a RAW forward index + standalone dictionary (the new
  /// shape). For every doc, [DataFetcher#fetchDictIds] for the MV column must return the same dict IDs in both
  /// cases. This pins the [DataFetcher#readDictIdsFromRawValuesMV] translation against the dict-encoded fast
  /// path — any divergence would surface as different group-by buckets / distinct counts in production.
  @Test
  public void testFetchDictIdsForRawForwardWithDictionaryMVMatchesDictEncodedBaseline() {
    int[][] mvValues = {
        {10, 20},
        {20, 30, 40},
        {40},
        {10, 30, 40, 20}
    };

    // Dictionary in sort order — all callers use the same one. indexOf is the inverse of get(int).
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.indexOf(10)).thenReturn(0);
    when(dictionary.indexOf(20)).thenReturn(1);
    when(dictionary.indexOf(30)).thenReturn(2);
    when(dictionary.indexOf(40)).thenReturn(3);

    // -- Dict-encoded baseline: forward index reports isDictionaryEncoded() == true and serves dict IDs directly
    // via getDictIdMV. ColumnValueReader.readDictIdsMV walks docs and copies the dict IDs out of the reusable
    // buffer, so the mock fills the buffer with the canonical dict IDs for each doc.
    ForwardIndexReader<ForwardIndexReaderContext> dictEncodedFwd = mock(ForwardIndexReader.class);
    when(dictEncodedFwd.isDictionaryEncoded()).thenReturn(true);
    when(dictEncodedFwd.isSingleValue()).thenReturn(false);
    when(dictEncodedFwd.getStoredType()).thenReturn(DataType.INT);
    when(dictEncodedFwd.createContext()).thenReturn(null);
    // Use any(int[].class) for the buffer arg: ColumnValueReader passes its own internal _reusableMVDictIds
    // buffer, which is not accessible from the test.
    for (int docId = 0; docId < mvValues.length; docId++) {
      int[] expectedDictIds = new int[mvValues[docId].length];
      for (int j = 0; j < mvValues[docId].length; j++) {
        expectedDictIds[j] = mvValues[docId][j] / 10 - 1; // 10→0, 20→1, 30→2, 40→3
      }
      when(dictEncodedFwd.getDictIdMV(eq(docId), any(int[].class), eq(null))).thenAnswer(invocation -> {
        int[] buffer = invocation.getArgument(1);
        System.arraycopy(expectedDictIds, 0, buffer, 0, expectedDictIds.length);
        return expectedDictIds.length;
      });
    }
    DataSource dictEncodedSource = buildDataSource(dictEncodedFwd, dictionary, DataType.INT, false, 8);
    DataFetcher dictFetcher =
        new DataFetcher(Collections.singletonMap(COLUMN, dictEncodedSource), Collections.emptyMap());

    // -- RAW + standalone dict: forward index reports isDictionaryEncoded() == false and serves raw values via
    // getIntMV; readDictIdsFromRawValuesMV translates each value through dictionary.indexOf.
    ForwardIndexReader<ForwardIndexReaderContext> rawFwd = mock(ForwardIndexReader.class);
    when(rawFwd.isDictionaryEncoded()).thenReturn(false);
    when(rawFwd.isSingleValue()).thenReturn(false);
    when(rawFwd.getStoredType()).thenReturn(DataType.INT);
    when(rawFwd.createContext()).thenReturn(null);
    for (int docId = 0; docId < mvValues.length; docId++) {
      when(rawFwd.getIntMV(docId, null)).thenReturn(mvValues[docId]);
    }
    DataSource rawSource = buildDataSource(rawFwd, dictionary, DataType.INT, false, 8);
    DataFetcher rawFetcher = new DataFetcher(Collections.singletonMap(COLUMN, rawSource), Collections.emptyMap());

    // -- Compare row by row: every doc's dict IDs must match.
    int[] docIds = {0, 1, 2, 3};
    int[][] dictOut = new int[docIds.length][];
    int[][] rawOut = new int[docIds.length][];
    dictFetcher.fetchDictIds(COLUMN, docIds, docIds.length, dictOut);
    rawFetcher.fetchDictIds(COLUMN, docIds, docIds.length, rawOut);

    for (int i = 0; i < docIds.length; i++) {
      assertEquals(rawOut[i], dictOut[i],
          "MV dict-id translation diverges for doc " + i + ": expected " + java.util.Arrays.toString(dictOut[i])
              + " (dict-encoded baseline) but got " + java.util.Arrays.toString(rawOut[i]) + " (RAW + dict)");
    }
  }
}
