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
import com.google.common.base.Preconditions;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.distinct.BaseOffHeapDistinctSet;
import org.apache.pinot.core.query.aggregation.function.distinct.OffHeap128BitDistinctSet;
import org.apache.pinot.core.query.aggregation.function.distinct.OffHeap32BitDistinctSet;
import org.apache.pinot.core.query.aggregation.function.distinct.OffHeap64BitDistinctSet;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class DistinctCountSequentialMergeAggregationFunction
    extends NullableSingleInputAggregationFunction<BaseOffHeapDistinctSet, Integer> {
  protected static final int DEFAULT_INITIAL_CAPACITY = 10_000;
  // Use empty OffHeap32BitDistinctSet as a placeholder for empty result
  // NOTE: It is okay to close it (multiple times) since we are never adding values into it
  protected static final OffHeap32BitDistinctSet EMPTY_PLACEHOLDER = new OffHeap32BitDistinctSet(0);

  protected final int _initialCapacity;

  public DistinctCountSequentialMergeAggregationFunction(List<ExpressionContext> arguments,
      boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);
    if (arguments.size() > 1) {
      _initialCapacity = arguments.get(1).getLiteral().getIntValue();
      Preconditions.checkArgument(_initialCapacity > 0, "Initial capacity must be > 0, got: %s", _initialCapacity);
    } else {
      _initialCapacity = DEFAULT_INITIAL_CAPACITY;
    }
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTSEQUENTIALMERGE;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    throw new UnsupportedOperationException("Use DISTINCT_COUNT instead");
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      // For dictionary-encoded expression, store dictionary ids into the bitmap
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      BitSet dictIdBitSet = getDictIdBitSet(aggregationResultHolder, dictionary);
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          dictIdBitSet.set(dictIds[i]);
        }
      });
    } else {
      // For non-dictionary-encoded expression, add values into the value set
      BaseOffHeapDistinctSet valueSet = aggregationResultHolder.getResult();
      if (valueSet == null) {
        valueSet = createValueSet(blockValSet.getValueType().getStoredType());
        aggregationResultHolder.setValue(valueSet);
      }
      addToValueSet(length, blockValSet, valueSet);
    }
  }

  private static BitSet getDictIdBitSet(AggregationResultHolder aggregationResultHolder, Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._bitSet;
  }

  private BaseOffHeapDistinctSet createValueSet(DataType storedType) {
    switch (storedType) {
      case INT:
      case FLOAT:
        return new OffHeap32BitDistinctSet(_initialCapacity);
      case LONG:
      case DOUBLE:
        return new OffHeap64BitDistinctSet(_initialCapacity);
      default:
        return new OffHeap128BitDistinctSet(_initialCapacity);
    }
  }

  private void addToValueSet(int length, BlockValSet blockValSet, BaseOffHeapDistinctSet valueSet) {
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        OffHeap32BitDistinctSet intSet = (OffHeap32BitDistinctSet) valueSet;
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            intSet.add(intValues[i]);
          }
        });
        break;
      case LONG:
        OffHeap64BitDistinctSet longSet = (OffHeap64BitDistinctSet) valueSet;
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            longSet.add(longValues[i]);
          }
        });
        break;
      case FLOAT:
        OffHeap32BitDistinctSet floatSet = (OffHeap32BitDistinctSet) valueSet;
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            floatSet.add(Float.floatToRawIntBits(floatValues[i]));
          }
        });
        break;
      case DOUBLE:
        OffHeap64BitDistinctSet doubleSet = (OffHeap64BitDistinctSet) valueSet;
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            doubleSet.add(Double.doubleToRawLongBits(doubleValues[i]));
          }
        });
        break;
      default:
        OffHeap128BitDistinctSet hashSet = (OffHeap128BitDistinctSet) valueSet;
        HashValue128[] hashValues = blockValSet.get128BitsMurmur3HashValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            hashSet.add(hashValues[i].getMostSignificantBits(), hashValues[i].getLeastSignificantBits());
          }
        });
        break;
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BaseOffHeapDistinctSet extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return EMPTY_PLACEHOLDER;
    }
    if (result instanceof DictIdsWrapper) {
      return extractAggregationResult((DictIdsWrapper) result);
    } else {
      return (BaseOffHeapDistinctSet) result;
    }
  }

  private BaseOffHeapDistinctSet extractAggregationResult(DictIdsWrapper dictIdsWrapper) {
    BitSet bitSet = dictIdsWrapper._bitSet;
    int length = bitSet.cardinality();
    Dictionary dictionary = dictIdsWrapper._dictionary;
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        OffHeap32BitDistinctSet intSet = new OffHeap32BitDistinctSet(length);
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          intSet.add(dictionary.getIntValue(i));
        }
        return intSet;
      case LONG:
        OffHeap64BitDistinctSet longSet = new OffHeap64BitDistinctSet(length);
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          longSet.add(dictionary.getLongValue(i));
        }
        return longSet;
      case FLOAT:
        OffHeap32BitDistinctSet floatSet = new OffHeap32BitDistinctSet(length);
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          floatSet.add(Float.floatToRawIntBits(dictionary.getFloatValue(i)));
        }
        return floatSet;
      case DOUBLE:
        OffHeap64BitDistinctSet doubleSet = new OffHeap64BitDistinctSet(length);
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          doubleSet.add(Double.doubleToRawLongBits(dictionary.getDoubleValue(i)));
        }
        return doubleSet;
      default:
        OffHeap128BitDistinctSet hashSet = new OffHeap128BitDistinctSet(length);
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          HashValue128 hashValue = dictionary.get128BitsMurmur3HashValue(i);
          hashSet.add(hashValue.getMostSignificantBits(), hashValue.getLeastSignificantBits());
        }
        return hashSet;
    }
  }

  /**
   * Extracts the value set from the dictionary.
   */
  public BaseOffHeapDistinctSet extractAggregationResult(Dictionary dictionary) {
    int length = dictionary.length();
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        OffHeap32BitDistinctSet intSet = new OffHeap32BitDistinctSet(length);
        for (int i = 0; i < length; i++) {
          intSet.add(dictionary.getIntValue(i));
        }
        return intSet;
      case LONG:
        OffHeap64BitDistinctSet longSet = new OffHeap64BitDistinctSet(length);
        for (int i = 0; i < length; i++) {
          longSet.add(dictionary.getLongValue(i));
        }
        return longSet;
      case FLOAT:
        OffHeap32BitDistinctSet floatSet = new OffHeap32BitDistinctSet(length);
        for (int i = 0; i < length; i++) {
          floatSet.add(Float.floatToRawIntBits(dictionary.getFloatValue(i)));
        }
        return floatSet;
      case DOUBLE:
        OffHeap64BitDistinctSet doubleSet = new OffHeap64BitDistinctSet(length);
        for (int i = 0; i < length; i++) {
          doubleSet.add(Double.doubleToRawLongBits(dictionary.getDoubleValue(i)));
        }
        return doubleSet;
      default:
        OffHeap128BitDistinctSet hashSet = new OffHeap128BitDistinctSet(length);
        for (int i = 0; i < length; i++) {
          HashValue128 hashValue = dictionary.get128BitsMurmur3HashValue(i);
          hashSet.add(hashValue.getMostSignificantBits(), hashValue.getLeastSignificantBits());
        }
        return hashSet;
    }
  }

  @Override
  public BaseOffHeapDistinctSet extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BaseOffHeapDistinctSet merge(BaseOffHeapDistinctSet intermediateResult1,
      BaseOffHeapDistinctSet intermediateResult2) {
    assert intermediateResult1 != null && intermediateResult2 != null;
    if (intermediateResult1.isEmpty()) {
      intermediateResult1.close();
      return intermediateResult2;
    }
    intermediateResult1.merge(intermediateResult2);
    intermediateResult2.close();
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(BaseOffHeapDistinctSet set) {
    int type;
    if (set instanceof OffHeap32BitDistinctSet) {
      type = 0;
    } else if (set instanceof OffHeap64BitDistinctSet) {
      type = 1;
    } else if (set instanceof OffHeap128BitDistinctSet) {
      type = 2;
    } else {
      throw new IllegalStateException();
    }
    byte[] bytes = set.serialize();
    set.close();
    return new SerializedIntermediateResult(type, bytes);
  }

  @Override
  public BaseOffHeapDistinctSet deserializeIntermediateResult(CustomObject customObject) {
    switch (customObject.getType()) {
      case 0:
        return OffHeap32BitDistinctSet.deserialize(customObject.getBuffer());
      case 1:
        return OffHeap64BitDistinctSet.deserialize(customObject.getBuffer());
      case 2:
        return OffHeap128BitDistinctSet.deserialize(customObject.getBuffer());
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(BaseOffHeapDistinctSet set) {
    assert set != null;
    int size = set.size();
    set.close();
    return size;
  }

  @Override
  public Integer mergeFinalResult(Integer finalResult1, Integer finalResult2) {
    return finalResult1 + finalResult2;
  }

  // Different from the BaseDistinctAggregateAggregationFunction.DictIdsWrapper, here we use a pre-allocated BitSet
  // instead of RoaringBitmap for better performance on high cardinality distinct count.
  protected static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final BitSet _bitSet;

    DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _bitSet = new BitSet(dictionary.length());
    }
  }
}
