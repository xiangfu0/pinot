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
package org.apache.pinot.core.query.aggregation.function;

import com.dynatrace.hash4j.hashing.HashValue128;
import it.unimi.dsi.fastutil.HashCommon;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.distinct.BaseOffHeapDistinctSet;
import org.apache.pinot.core.query.aggregation.function.distinct.OffHeap128BitDistinctSet;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountParallelMergeAggregationFunction extends DistinctCountSequentialMergeAggregationFunction {
  private final boolean _sharedAcrossOperators;

  // When the function is shared across operators without group-by, merge results into this share value set.
  private ConcurrentHashMap.KeySetView _sharedValueSet;

  public DistinctCountParallelMergeAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled,
      boolean sharedAcrossOperators) {
    super(arguments, nullHandlingEnabled);
    _sharedAcrossOperators = sharedAcrossOperators;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTPARALLELMERGE;
  }

  @Override
  public BaseOffHeapDistinctSet extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    if (!_sharedAcrossOperators) {
      return super.extractAggregationResult(aggregationResultHolder);
    }

    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return EMPTY_PLACEHOLDER;
    }
    if (result instanceof DictIdsWrapper) {
      addToSharedValueSet((DictIdsWrapper) result);
    } else {
      addToSharedValueSet((BaseOffHeapDistinctSet) result);
    }
    return EMPTY_PLACEHOLDER;
  }

  private void addToSharedValueSet(DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    BitSet bitSet = dictIdsWrapper._bitSet;
    synchronized (this) {
      if (_sharedValueSet == null) {
        int initialCapacity = _initialCapacity > 0 ? _initialCapacity : bitSet.cardinality();
        _sharedValueSet = ConcurrentHashMap.newKeySet(initialCapacity);
      }
    }
    ConcurrentHashMap.KeySetView valueSet = _sharedValueSet;
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          valueSet.add(HashCommon.mix(dictionary.getIntValue(i)));
        }
        break;
      case LONG:
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          valueSet.add(HashCommon.mix(dictionary.getLongValue(i)));
        }
        break;
      case FLOAT:
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          valueSet.add(HashCommon.mix(Float.floatToRawIntBits(dictionary.getFloatValue(i))));
        }
        break;
      case DOUBLE:
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          valueSet.add(HashCommon.mix(Double.doubleToRawLongBits(dictionary.getLongValue(i))));
        }
        break;
      default:
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          HashValue128 hashValue = dictionary.get128BitsMurmur3HashValue(i);
          valueSet.add(new OffHeap128BitDistinctSet.Value(HashCommon.mix(hashValue.getMostSignificantBits()),
              HashCommon.mix(hashValue.getLeastSignificantBits())));
        }
    }
  }

  private void addToSharedValueSet(BaseOffHeapDistinctSet set) {
    synchronized (this) {
      if (_sharedValueSet == null) {
        int initialCapacity = _initialCapacity > 0 ? _initialCapacity : set.size();
        _sharedValueSet = ConcurrentHashMap.newKeySet(initialCapacity);
      }
    }
    ConcurrentHashMap.KeySetView valueSet = _sharedValueSet;
    Iterator<?> iterator = set.iterator();
    while (iterator.hasNext()) {
      valueSet.add(iterator.next());
    }
    set.close();
  }

  @Override
  public BaseOffHeapDistinctSet merge(BaseOffHeapDistinctSet intermediateResult1,
      BaseOffHeapDistinctSet intermediateResult2) {
    return _sharedValueSet != null ? intermediateResult1 : super.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(BaseOffHeapDistinctSet set) {
    if (_sharedValueSet == null) {
      return super.serializeIntermediateResult(set);
    }

    ConcurrentHashMap.KeySetView valueSet = _sharedValueSet;
    int size = valueSet.size();
    if (size == 0) {
      return new SerializedIntermediateResult(0, new byte[Integer.BYTES]);
    }
    Object sampleValue = valueSet.iterator().next();
    if (sampleValue instanceof Integer) {
      byte[] bytes = new byte[Integer.BYTES + size << 2];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      for (Object v : valueSet) {
        byteBuffer.putInt((int) v);
      }
      return new SerializedIntermediateResult(0, bytes);
    } else if (sampleValue instanceof Long) {
      byte[] bytes = new byte[Integer.BYTES + size << 3];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      for (Object v : valueSet) {
        byteBuffer.putLong((long) v);
      }
      return new SerializedIntermediateResult(1, bytes);
    } else if (sampleValue instanceof OffHeap128BitDistinctSet.Value) {
      byte[] bytes = new byte[Integer.BYTES + size << 4];
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      byteBuffer.putInt(size);
      for (Object v : valueSet) {
        OffHeap128BitDistinctSet.Value value = (OffHeap128BitDistinctSet.Value) v;
        byteBuffer.putLong(value.getHigh());
        byteBuffer.putLong(value.getLow());
      }
      return new SerializedIntermediateResult(2, bytes);
    } else {
      throw new IllegalStateException();
    }
  }

  @Override
  public Integer extractFinalResult(BaseOffHeapDistinctSet set) {
    if (_sharedValueSet != null) {
      return _sharedValueSet.size();
    } else {
      assert set != null;
      return set.size();
    }
  }
}