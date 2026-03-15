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
package org.apache.pinot.perf;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Benchmark comparing two approaches for checking bitmap intersection in the inverted index distinct path:
 *
 * <p>1. <b>fullAnd</b> (current code): converts ImmutableRoaringBitmap to RoaringBitmap, computes full
 *    {@code RoaringBitmap.and()}, then checks {@code isEmpty()}.
 *
 * <p>2. <b>intersects</b> (optimized): uses {@code RoaringBitmap.intersects()} which short-circuits
 *    on the first common element.
 *
 * <p>Usage: {@code java -jar pinot-perf/target/benchmarks.jar BenchmarkBitmapIntersectionVsAnd}
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class BenchmarkBitmapIntersectionVsAnd {

  @Param({"1000000"})
  int _numDocs;

  @Param({"100", "1000", "10000", "100000"})
  int _dictionaryCardinality;

  @Param({"0.01", "0.1", "0.5", "1.0"})
  double _filterSelectivity;

  // Inverted index as ImmutableRoaringBitmap (matches actual operator: InvertedIndexReader returns
  // ImmutableRoaringBitmap)
  private ImmutableRoaringBitmap[] _invertedIndexImmutable;

  // Same bitmaps as RoaringBitmap for the intersects() path
  private RoaringBitmap[] _invertedIndexMutable;

  // Filter bitmap
  private RoaringBitmap _filterBitmap;

  @Setup(Level.Trial)
  public void setup() {
    Random random = new Random(42);

    // Build forward index and inverted index
    int[] forwardIndex = new int[_numDocs];
    for (int docId = 0; docId < _numDocs; docId++) {
      forwardIndex[docId] = random.nextInt(_dictionaryCardinality);
    }

    RoaringBitmap[] invertedIndex = new RoaringBitmap[_dictionaryCardinality];
    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      invertedIndex[dictId] = new RoaringBitmap();
    }
    for (int docId = 0; docId < _numDocs; docId++) {
      invertedIndex[forwardIndex[docId]].add(docId);
    }

    // Build filter bitmap
    int filterCardinality = Math.max(1, (int) (_numDocs * _filterSelectivity));
    _filterBitmap = new RoaringBitmap();
    if (_filterSelectivity >= 1.0) {
      _filterBitmap.add(0L, _numDocs);
    } else {
      int[] docIds = new int[_numDocs];
      for (int i = 0; i < _numDocs; i++) {
        docIds[i] = i;
      }
      for (int i = 0; i < filterCardinality; i++) {
        int j = i + random.nextInt(_numDocs - i);
        int tmp = docIds[i];
        docIds[i] = docIds[j];
        docIds[j] = tmp;
        _filterBitmap.add(docIds[i]);
      }
    }
    _filterBitmap.runOptimize();

    // Convert inverted index to ImmutableRoaringBitmap (simulates what InvertedIndexReader returns)
    _invertedIndexImmutable = new ImmutableRoaringBitmap[_dictionaryCardinality];
    _invertedIndexMutable = new RoaringBitmap[_dictionaryCardinality];
    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      invertedIndex[dictId].runOptimize();
      // Convert to ImmutableRoaringBitmap via serialization (simulates reading from index)
      _invertedIndexImmutable[dictId] = invertedIndex[dictId].toMutableRoaringBitmap();
      _invertedIndexMutable[dictId] = invertedIndex[dictId];
    }
  }

  /**
   * Current operator code path: convert ImmutableRoaringBitmap -> MutableRoaringBitmap -> RoaringBitmap,
   * compute full RoaringBitmap.and(), check isEmpty().
   */
  @Benchmark
  public int fullAndPath(Blackhole bh) {
    int matchCount = 0;
    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      ImmutableRoaringBitmap docIds = _invertedIndexImmutable[dictId];
      // This is what InvertedIndexDistinctOperator currently does:
      RoaringBitmap docIdsRoaring = docIds.toMutableRoaringBitmap().toRoaringBitmap();
      RoaringBitmap intersection = RoaringBitmap.and(docIdsRoaring, _filterBitmap);
      if (!intersection.isEmpty()) {
        matchCount++;
      }
    }
    bh.consume(matchCount);
    return matchCount;
  }

  /**
   * Optimized path: use RoaringBitmap.intersects() which short-circuits on first common element.
   * No bitmap allocation, no conversion.
   */
  @Benchmark
  public int intersectsPath(Blackhole bh) {
    int matchCount = 0;
    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      RoaringBitmap docIds = _invertedIndexMutable[dictId];
      if (RoaringBitmap.intersects(docIds, _filterBitmap)) {
        matchCount++;
      }
    }
    bh.consume(matchCount);
    return matchCount;
  }

  /**
   * Optimized path using ImmutableRoaringBitmap directly (no conversion at all).
   * Uses MutableRoaringBitmap.intersects() since MutableRoaringBitmap extends ImmutableRoaringBitmap.
   */
  @Benchmark
  public int intersectsImmutablePath(Blackhole bh) {
    int matchCount = 0;
    MutableRoaringBitmap filterMutable = _filterBitmap.toMutableRoaringBitmap();
    for (int dictId = 0; dictId < _dictionaryCardinality; dictId++) {
      ImmutableRoaringBitmap docIds = _invertedIndexImmutable[dictId];
      if (MutableRoaringBitmap.intersects(docIds, filterMutable)) {
        matchCount++;
      }
    }
    bh.consume(matchCount);
    return matchCount;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkBitmapIntersectionVsAnd.class.getSimpleName())
        .build())
        .run();
  }
}
