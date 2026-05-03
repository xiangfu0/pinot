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
package org.apache.pinot.segment.spi.index.mutable.provider;

import java.io.File;
import java.util.Objects;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;


public class MutableIndexContext {
  private final int _capacity;
  private final FieldSpec _fieldSpec;
  private final int _fixedLengthBytes;
  private final boolean _hasDictionary;
  private final FieldConfig.EncodingType _forwardIndexEncoding;
  private final boolean _offHeap;
  private final int _estimatedColSize;
  private final int _estimatedCardinality;
  private final int _avgNumMultiValues;
  private final String _segmentName;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final File _consumerDir;

  public MutableIndexContext(FieldSpec fieldSpec, int fixedLengthBytes, boolean hasDictionary,
      FieldConfig.EncodingType forwardIndexEncoding, String segmentName, PinotDataBufferMemoryManager memoryManager,
      int capacity, boolean offHeap, int estimatedColSize, int estimatedCardinality, int avgNumMultiValues,
      File consumerDir) {
    _fieldSpec = fieldSpec;
    _fixedLengthBytes = fixedLengthBytes;
    _hasDictionary = hasDictionary;
    _forwardIndexEncoding = Objects.requireNonNull(forwardIndexEncoding);
    _segmentName = segmentName;
    _memoryManager = memoryManager;
    _capacity = capacity;
    _offHeap = offHeap;
    _estimatedColSize = estimatedColSize;
    _estimatedCardinality = estimatedCardinality;
    _avgNumMultiValues = avgNumMultiValues;
    _consumerDir = consumerDir;
  }

  public PinotDataBufferMemoryManager getMemoryManager() {
    return _memoryManager;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  public int getFixedLengthBytes() {
    return _fixedLengthBytes;
  }

  public boolean hasDictionary() {
    return _hasDictionary;
  }

  /// Returns the forward-index encoding for this column. [FieldConfig.EncodingType#DICTIONARY] means the forward
  /// index stores dictionary IDs; [FieldConfig.EncodingType#RAW] means the forward index stores raw values, even
  /// if the column also carries a dictionary (e.g. for a secondary index).
  ///
  /// Currently always [FieldConfig.EncodingType#DICTIONARY] for in-tree mutable index providers (mutable segments
  /// without a dictionary use the raw multi-value implementations directly without consulting this field). The
  /// signal exists so the upcoming shared-dictionary mutable-segment work can produce a `RAW` mutable forward
  /// index alongside a `MutableDictionary` from a single context, mirroring the immutable contract introduced in
  /// apache/pinot#18364.
  public FieldConfig.EncodingType getForwardIndexEncoding() {
    return _forwardIndexEncoding;
  }

  public int getCapacity() {
    return _capacity;
  }

  public boolean isOffHeap() {
    return _offHeap;
  }

  public int getEstimatedColSize() {
    return _estimatedColSize;
  }

  public int getEstimatedCardinality() {
    return _estimatedCardinality;
  }

  public int getAvgNumMultiValues() {
    return _avgNumMultiValues;
  }

  public File getConsumerDir() {
    return _consumerDir;
  }

  /// Constructs a builder for the given forward-index encoding. The encoding is required at construction time to
  /// avoid implicit defaults that would silently misclassify shared-dictionary columns; callers should typically
  /// pass `forwardIndexConfig.getEncodingType()` (which itself defaults to `FieldConfig.getEncodingType()`).
  public static Builder builder(FieldConfig.EncodingType forwardIndexEncoding) {
    return new Builder(forwardIndexEncoding);
  }

  public static class Builder {
    private final FieldConfig.EncodingType _forwardIndexEncoding;
    private FieldSpec _fieldSpec;
    private int _fixedLengthBytes;
    private String _segmentName;
    private boolean _hasDictionary = true;
    private boolean _offHeap = true;
    private int _capacity;
    private PinotDataBufferMemoryManager _memoryManager;
    private int _estimatedColSize;
    private int _estimatedCardinality;
    private int _avgNumMultiValues;
    private File _consumerDir;

    public Builder(FieldConfig.EncodingType forwardIndexEncoding) {
      _forwardIndexEncoding = Objects.requireNonNull(forwardIndexEncoding);
    }

    public Builder withMemoryManager(PinotDataBufferMemoryManager memoryManager) {
      _memoryManager = memoryManager;
      return this;
    }

    public Builder withFieldSpec(FieldSpec fieldSpec) {
      _fieldSpec = fieldSpec;
      return this;
    }

    public Builder withSegmentName(String segmentName) {
      _segmentName = segmentName;
      return this;
    }

    public Builder withDictionary(boolean hasDictionary) {
      _hasDictionary = hasDictionary;
      return this;
    }

    public Builder offHeap(boolean offHeap) {
      _offHeap = offHeap;
      return this;
    }

    public Builder withCapacity(int capacity) {
      _capacity = capacity;
      return this;
    }

    public Builder withEstimatedColSize(int estimatedColSize) {
      _estimatedColSize = estimatedColSize;
      return this;
    }

    public Builder withEstimatedCardinality(int estimatedCardinality) {
      _estimatedCardinality = estimatedCardinality;
      return this;
    }

    public Builder withAvgNumMultiValues(int avgNumMultiValues) {
      _avgNumMultiValues = avgNumMultiValues;
      return this;
    }

    public Builder withConsumerDir(File consumerDir) {
      _consumerDir = consumerDir;
      return this;
    }

    public Builder withFixedLengthBytes(int fixedLengthBytes) {
      _fixedLengthBytes = fixedLengthBytes;
      return this;
    }

    public MutableIndexContext build() {
      return new MutableIndexContext(Objects.requireNonNull(_fieldSpec), _fixedLengthBytes, _hasDictionary,
          _forwardIndexEncoding, Objects.requireNonNull(_segmentName), Objects.requireNonNull(_memoryManager),
          _capacity, _offHeap, _estimatedColSize, _estimatedCardinality, _avgNumMultiValues, _consumerDir);
    }
  }
}
