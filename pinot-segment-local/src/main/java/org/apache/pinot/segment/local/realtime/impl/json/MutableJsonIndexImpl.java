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
package org.apache.pinot.segment.local.realtime.impl.json;

import com.google.common.base.Preconditions;
import com.google.common.base.Utf8;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.common.utils.regex.Matcher;
import org.apache.pinot.common.utils.regex.Pattern;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableJsonIndex;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Json index for mutable segment.
 */
public class MutableJsonIndexImpl implements MutableJsonIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableJsonIndexImpl.class);
  private final JsonIndexConfig _jsonIndexConfig;
  private final String _segmentName;
  private final String _columnName;
  private final TreeMap<String, RoaringBitmap> _postingListMap;
  private final IntList _docIdMapping;
  private final long _maxBytesSize;
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;
  private final ServerMetrics _serverMetrics;

  private int _nextDocId;
  private int _nextFlattenedDocId;
  private long _bytesSize;

  public MutableJsonIndexImpl(JsonIndexConfig jsonIndexConfig, String segmentName, String columnName) {
    _jsonIndexConfig = jsonIndexConfig;
    _segmentName = segmentName;
    _columnName = columnName;
    _postingListMap = new TreeMap<>();
    _docIdMapping = new IntArrayList();
    _maxBytesSize = jsonIndexConfig.getMaxBytesSize() == null ? Long.MAX_VALUE : jsonIndexConfig.getMaxBytesSize();

    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
    _serverMetrics = ServerMetrics.get();
  }

  /**
   * Adds the next json value.
   */
  @Override
  public void add(String jsonString)
      throws IOException {
    try {
      List<Map<String, String>> flattenedRecords = JsonUtils.flatten(jsonString, _jsonIndexConfig);
      _writeLock.lock();
      try {
        addFlattenedRecords(flattenedRecords);
      } finally {
        _writeLock.unlock();
      }
    } finally {
      _nextDocId++;
    }
  }

  /**
   * Adds the flattened records for the next document.
   */
  private void addFlattenedRecords(List<Map<String, String>> records) {
    int numRecords = records.size();
    Preconditions.checkState(_nextFlattenedDocId + numRecords >= 0, "Got more than %s flattened records",
        Integer.MAX_VALUE);
    for (int i = 0; i < numRecords; i++) {
      _docIdMapping.add(_nextDocId);
    }
    // TODO: Consider storing tuples as the key of the posting list so that the strings can be reused, and the hashcode
    //       can be cached.
    for (Map<String, String> record : records) {
      for (Map.Entry<String, String> entry : record.entrySet()) {
        // Put both key and key-value into the posting list. Key is useful for checking if a key exists in the json.
        String key = entry.getKey();
        _postingListMap.computeIfAbsent(key, k -> {
          _bytesSize += Utf8.encodedLength(key);
          return new RoaringBitmap();
        }).add(_nextFlattenedDocId);
        String keyValue = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + entry.getValue();
        _postingListMap.computeIfAbsent(keyValue, k -> {
          _bytesSize += Utf8.encodedLength(keyValue);
          return new RoaringBitmap();
        }).add(_nextFlattenedDocId);
      }
      _nextFlattenedDocId++;
    }
  }

  @Override
  public MutableRoaringBitmap getMatchingDocIds(String filterString) {
    FilterContext filter;
    try {
      filter = RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(filterString));
      Preconditions.checkArgument(!filter.isConstant());
    } catch (Exception e) {
      throw new BadQueryRequestException("Invalid json match filter: " + filterString);
    }
    return getMatchingDocIds(filter);
  }

  @Override
  public MutableRoaringBitmap getMatchingDocIds(Object filterObj) {
    if (!(filterObj instanceof FilterContext)) {
      throw new BadQueryRequestException("Invalid json match filter: " + filterObj);
    }
    return getMatchingDocIds((FilterContext) filterObj);
  }

  private MutableRoaringBitmap getMatchingDocIds(FilterContext filter) {
    _readLock.lock();
    try {
      Predicate predicate = filter.getPredicate();
      if (predicate != null && isExclusive(predicate.getType())) {
        // Handle exclusive predicate separately because the flip can only be applied to the unflattened doc ids in
        // order to get the correct result, and it cannot be nested
        LazyBitmap flattenedDocIds = getMatchingFlattenedDocIds(predicate);
        MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
        flattenedDocIds.forEach(flattenedDocId -> matchingDocIds.add(_docIdMapping.getInt(flattenedDocId)));
        matchingDocIds.flip(0, (long) _nextDocId);
        return matchingDocIds;
      } else {
        LazyBitmap flattenedDocIds = getMatchingFlattenedDocIds(filter);
        MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
        flattenedDocIds.forEach(flattenedDocId -> matchingDocIds.add(_docIdMapping.getInt(flattenedDocId)));
        return matchingDocIds;
      }
    } finally {
      _readLock.unlock();
    }
  }

  /**
   * Returns {@code true} if the given predicate type is exclusive for json_match calculation, {@code false} otherwise.
   */
  private boolean isExclusive(Predicate.Type predicateType) {
    return predicateType == Predicate.Type.IS_NULL;
  }

  /** This class allows delaying of cloning posting list bitmap for as long as possible
   * It stores either a bitmap from posting list that must be cloned before mutating (readOnly=true)
   * or an already  cloned bitmap.
   */
  private static class LazyBitmap {
    final static LazyBitmap EMPTY_BITMAP = createImmutable(new RoaringBitmap());

    // value should be null only for EMPTY
    final RoaringBitmap _value;

    // if readOnly then bitmap needs to be cloned before applying mutating operations
    final boolean _mutable;

    LazyBitmap(RoaringBitmap bitmap, boolean mutable) {
      _value = bitmap;
      _mutable = mutable;
    }

    static LazyBitmap createMutable(RoaringBitmap bitmap) {
      return new LazyBitmap(bitmap, true);
    }

    static LazyBitmap createImmutable(RoaringBitmap bitmap) {
      return new LazyBitmap(bitmap, false);
    }

    boolean isMutable() {
      return _mutable;
    }

    LazyBitmap toMutable() {
      if (_mutable) {
        return this;
      }
      return createMutable(_value.clone());
    }

    LazyBitmap and(LazyBitmap other) {
      if (isEmpty() || other.isEmpty()) {
        return LazyBitmap.EMPTY_BITMAP;
      }
      if (isMutable()) {
        _value.and(other._value);
        return this;
      }
      if (other.isMutable()) {
        other._value.and(_value);
        return other;
      }
      return createMutable(RoaringBitmap.and(_value, other._value));
    }

    LazyBitmap and(RoaringBitmap bitmap) {
      if (isEmpty() || bitmap.isEmpty()) {
        return EMPTY_BITMAP;
      }
      if (isMutable()) {
        _value.and(bitmap);
        return this;
      }
      return createMutable(RoaringBitmap.and(_value, bitmap));
    }

    LazyBitmap or(LazyBitmap other) {
      if (isEmpty()) {
        return other;
      }
      if (other.isEmpty()) {
        return this;
      }
      if (isMutable()) {
        _value.or(other._value);
        return this;
      }
      if (other.isMutable()) {
        other._value.or(_value);
        return other;
      }
      return createMutable(RoaringBitmap.or(_value, other._value));
    }

    LazyBitmap or(RoaringBitmap bitmap) {
      if (isEmpty()) {
        return createImmutable(bitmap);
      }
      if (bitmap.isEmpty()) {
        return this;
      }
      if (isMutable()) {
        _value.or(bitmap);
        return this;
      }
      return createMutable(RoaringBitmap.or(_value, bitmap));
    }

    LazyBitmap andNot(LazyBitmap other) {
      if (isEmpty()) {
        return EMPTY_BITMAP;
      }
      if (other.isEmpty()) {
        return this;
      }
      if (isMutable()) {
        _value.andNot(other._value);
        return this;
      }
      return createMutable(RoaringBitmap.andNot(_value, other._value));
    }

    LazyBitmap andNot(RoaringBitmap bitmap) {
      if (isEmpty()) {
        return EMPTY_BITMAP;
      }
      if (bitmap.isEmpty()) {
        return this;
      }
      if (isMutable()) {
        _value.andNot(bitmap);
        return this;
      }
      return createMutable(RoaringBitmap.andNot(_value, bitmap));
    }

    boolean isEmpty() {
      return _value.isEmpty();
    }

    void forEach(IntConsumer ic) {
      _value.forEach(ic);
    }

    LazyBitmap flip(long rangeStart, long rangeEnd) {
      LazyBitmap result = toMutable();
      result._value.flip(rangeStart, rangeEnd);
      return result;
    }

    RoaringBitmap getValue() {
      return _value;
    }
  }

  /**
   * Returns the matching flattened doc ids for the given filter.
   */
  private LazyBitmap getMatchingFlattenedDocIds(FilterContext filter) {
    switch (filter.getType()) {
      case AND: {
        List<FilterContext> filters = filter.getChildren();
        LazyBitmap matchingDocIds = getMatchingFlattenedDocIds(filters.get(0));
        for (int i = 1, numFilters = filters.size(); i < numFilters; i++) {
          if (matchingDocIds.isEmpty()) {
            return LazyBitmap.EMPTY_BITMAP;
          }
          LazyBitmap filterDocIds = getMatchingFlattenedDocIds(filters.get(i));
          matchingDocIds = matchingDocIds.and(filterDocIds);
        }
        return matchingDocIds;
      }
      case OR: {
        List<FilterContext> filters = filter.getChildren();
        LazyBitmap matchingDocIds = getMatchingFlattenedDocIds(filters.get(0));
        for (int i = 1, numFilters = filters.size(); i < numFilters; i++) {
          LazyBitmap filterDocIds = getMatchingFlattenedDocIds(filters.get(i));
          matchingDocIds = matchingDocIds.or(filterDocIds);
        }
        return matchingDocIds;
      }
      case PREDICATE: {
        Predicate predicate = filter.getPredicate();
        Preconditions.checkArgument(!isExclusive(predicate.getType()), "Exclusive predicate: %s cannot be nested",
            predicate);
        return getMatchingFlattenedDocIds(predicate);
      }
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Returns the matching flattened doc ids for the given predicate.
   * <p>Exclusive predicate is handled as the inclusive predicate, and the caller should flip the unflattened doc ids in
   * order to get the correct exclusive predicate result.
   */
  private LazyBitmap getMatchingFlattenedDocIds(Predicate predicate) {
    ExpressionContext lhs = predicate.getLhs();
    Preconditions.checkArgument(lhs.getType() == ExpressionContext.Type.IDENTIFIER,
        "Left-hand side of the predicate must be an identifier, got: %s (%s). Put double quotes around the identifier"
            + " if needed.", lhs, lhs.getType());

    // Support 2 formats:
    // - JSONPath format (e.g. "$.a[1].b"='abc', "$[0]"=1, "$"='abc')
    // - Legacy format (e.g. "a[1].b"='abc')
    String key = lhs.getIdentifier();
    if (key.charAt(0) == '$') {
      key = key.substring(1);
    } else {
      key = JsonUtils.KEY_SEPARATOR + key;
    }

    Pair<String, LazyBitmap> pair = getKeyAndFlattenedDocIds(key);
    key = pair.getLeft();
    LazyBitmap matchingDocIdsForKey = pair.getRight();
    if (matchingDocIdsForKey != null && matchingDocIdsForKey.isEmpty()) {
      return LazyBitmap.EMPTY_BITMAP;
    }
    LazyBitmap matchingDocIdsForKeyValue = getMatchingFlattenedDocIdsForKeyValue(predicate, key);
    if (matchingDocIdsForKey == null) {
      return matchingDocIdsForKeyValue;
    } else {
      return matchingDocIdsForKeyValue.and(matchingDocIdsForKey);
    }
  }

  private LazyBitmap getMatchingFlattenedDocIdsForKeyValue(Predicate predicate, String key) {
    Predicate.Type predicateType = predicate.getType();
    switch (predicateType) {
      case EQ: {
        String value = ((EqPredicate) predicate).getValue();
        RoaringBitmap docIds = _postingListMap.get(key + JsonIndexCreator.KEY_VALUE_SEPARATOR + value);
        return docIds != null ? LazyBitmap.createImmutable(docIds) : LazyBitmap.EMPTY_BITMAP;
      }

      case NOT_EQ: {
        RoaringBitmap allDocIds = _postingListMap.get(key);
        if (allDocIds == null) {
          return LazyBitmap.EMPTY_BITMAP;
        }
        LazyBitmap result = LazyBitmap.createImmutable(allDocIds);
        String value = ((NotEqPredicate) predicate).getValue();
        RoaringBitmap docIds = _postingListMap.get(key + JsonIndexCreator.KEY_VALUE_SEPARATOR + value);
        if (docIds != null) {
          return result.andNot(docIds);
        } else {
          return result;
        }
      }

      case IN: {
        StringBuilder buffer = new StringBuilder(key);
        buffer.append(JsonIndexCreator.KEY_VALUE_SEPARATOR);
        int pos = buffer.length();
        LazyBitmap result = LazyBitmap.EMPTY_BITMAP;
        List<String> values = ((InPredicate) predicate).getValues();
        for (String value : values) {
          buffer.setLength(pos);
          buffer.append(value);
          RoaringBitmap docIds = _postingListMap.get(buffer.toString());
          if (docIds != null) {
            result = result.or(docIds);
          }
        }
        return result;
      }

      case NOT_IN: {
        RoaringBitmap allDocIds = _postingListMap.get(key);
        if (allDocIds == null) {
          return LazyBitmap.EMPTY_BITMAP;
        }
        StringBuilder buffer = new StringBuilder(key);
        buffer.append(JsonIndexCreator.KEY_VALUE_SEPARATOR);
        int pos = buffer.length();
        LazyBitmap result = LazyBitmap.createImmutable(allDocIds);
        List<String> values = ((NotInPredicate) predicate).getValues();
        for (String value : values) {
          if (result.isEmpty()) {
            return LazyBitmap.EMPTY_BITMAP;
          }
          buffer.setLength(pos);
          buffer.append(value);
          RoaringBitmap docIds = _postingListMap.get(buffer.toString());
          if (docIds != null) {
            result = result.andNot(docIds);
          }
        }
        return result;
      }

      case IS_NOT_NULL:
      case IS_NULL: {
        RoaringBitmap docIds = _postingListMap.get(key);
        return docIds != null ? LazyBitmap.createImmutable(docIds) : LazyBitmap.EMPTY_BITMAP;
      }

      case REGEXP_LIKE: {
        Map<String, RoaringBitmap> subMap = getMatchingKeysMap(key);
        if (subMap.isEmpty()) {
          return LazyBitmap.EMPTY_BITMAP;
        }
        Pattern pattern = ((RegexpLikePredicate) predicate).getPattern();
        Matcher matcher = pattern.matcher("");
        LazyBitmap result = LazyBitmap.EMPTY_BITMAP;
        StringBuilder value = new StringBuilder();
        int valueStart = key.length() + 1;
        for (Map.Entry<String, RoaringBitmap> entry : subMap.entrySet()) {
          String keyValue = entry.getKey();
          value.setLength(0);
          value.append(keyValue, valueStart, keyValue.length());
          if (matcher.reset(value).matches()) {
            result = result.or(entry.getValue());
          }
        }
        return result;
      }

      case RANGE: {
        Map<String, RoaringBitmap> subMap = getMatchingKeysMap(key);
        if (subMap.isEmpty()) {
          return LazyBitmap.EMPTY_BITMAP;
        }
        RangePredicate rangePredicate = (RangePredicate) predicate;
        FieldSpec.DataType rangeDataType = rangePredicate.getRangeDataType();
        // Simplify to only support numeric and string types
        if (rangeDataType.isNumeric()) {
          rangeDataType = FieldSpec.DataType.DOUBLE;
        } else {
          rangeDataType = FieldSpec.DataType.STRING;
        }
        boolean lowerUnbounded = rangePredicate.getLowerBound().equals(RangePredicate.UNBOUNDED);
        boolean upperUnbounded = rangePredicate.getUpperBound().equals(RangePredicate.UNBOUNDED);
        boolean lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
        boolean upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();
        Object lowerBound = lowerUnbounded ? null : rangeDataType.convert(rangePredicate.getLowerBound());
        Object upperBound = upperUnbounded ? null : rangeDataType.convert(rangePredicate.getUpperBound());
        LazyBitmap result = LazyBitmap.EMPTY_BITMAP;
        int valueStart = key.length() + 1;
        for (Map.Entry<String, RoaringBitmap> entry : subMap.entrySet()) {
          Object valueObj = rangeDataType.convert(entry.getKey().substring(valueStart));
          boolean lowerCompareResult =
              lowerUnbounded || (lowerInclusive ? rangeDataType.compare(valueObj, lowerBound) >= 0
                  : rangeDataType.compare(valueObj, lowerBound) > 0);
          boolean upperCompareResult =
              upperUnbounded || (upperInclusive ? rangeDataType.compare(valueObj, upperBound) <= 0
                  : rangeDataType.compare(valueObj, upperBound) < 0);
          if (lowerCompareResult && upperCompareResult) {
            result = result.or(entry.getValue());
          }
        }
        return result;
      }

      default:
        throw new IllegalStateException("Unsupported json_match predicate type: " + predicate);
    }
  }

  public void convertFlattenedDocIdsToDocIds(Map<String, RoaringBitmap> valueToFlattenedDocIds) {
    _readLock.lock();
    try {
      valueToFlattenedDocIds.replaceAll((key, value) -> {
        RoaringBitmap docIds = new RoaringBitmap();
        value.forEach((IntConsumer) flattenedDocId -> docIds.add(_docIdMapping.getInt(flattenedDocId)));
        return docIds;
      });
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public Map<String, RoaringBitmap> getMatchingFlattenedDocsMap(String jsonPathKey, @Nullable String filterString) {
    Map<String, RoaringBitmap> resultMap = new HashMap<>();
    _readLock.lock();
    try {
      LazyBitmap filteredDocIds = null;
      FilterContext filter;
      if (filterString != null) {
        filter = RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(filterString));
        Preconditions.checkArgument(!filter.isConstant(), "Invalid json match filter: " + filterString);

        if (filter.getType() == FilterContext.Type.PREDICATE && isExclusive(filter.getPredicate().getType())) {
          // Handle exclusive predicate separately because the flip can only be applied to the
          // un-flattened doc ids in order to get the correct result, and it cannot be nested
          filteredDocIds = getMatchingFlattenedDocIds(filter.getPredicate());
          filteredDocIds = filteredDocIds.flip(0, _nextFlattenedDocId);
        } else {
          filteredDocIds = getMatchingFlattenedDocIds(filter);
        }
      }
      // Support 2 formats:
      // - JSONPath format (e.g. "$.a[1].b"='abc', "$[0]"=1, "$"='abc')
      // - Legacy format (e.g. "a[1].b"='abc')
      if (jsonPathKey.startsWith("$")) {
        jsonPathKey = jsonPathKey.substring(1);
      } else {
        jsonPathKey = JsonUtils.KEY_SEPARATOR + jsonPathKey;
      }
      Pair<String, LazyBitmap> result = getKeyAndFlattenedDocIds(jsonPathKey);
      jsonPathKey = result.getLeft();
      LazyBitmap arrayIndexDocIds = result.getRight();
      if (arrayIndexDocIds != null && arrayIndexDocIds.isEmpty()) {
        return resultMap;
      }

      RoaringBitmap filteredBitmap = filteredDocIds != null ? filteredDocIds.getValue() : null;
      RoaringBitmap arrayIndexBitmap = arrayIndexDocIds != null ? arrayIndexDocIds.getValue() : null;

      Map<String, RoaringBitmap> subMap = getMatchingKeysMap(jsonPathKey);
      for (Map.Entry<String, RoaringBitmap> entry : subMap.entrySet()) {
        // there is no point using lazy bitmap here because filteredDocIds and arrayIndexDocIds
        // are shared and can't be modified
        RoaringBitmap docIds = entry.getValue();
        if (docIds == null || docIds.isEmpty()) {
          continue;
        }
        docIds = docIds.clone();
        if (filteredDocIds != null) {
          docIds.and(filteredBitmap);
        }
        if (arrayIndexDocIds != null) {
          docIds.and(arrayIndexBitmap);
        }

        if (!docIds.isEmpty()) {
          String value = entry.getKey().substring(jsonPathKey.length() + 1);
          resultMap.put(value, docIds);
          Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(resultMap.size());
        }
      }

      return resultMap;
    } finally {
      _readLock.unlock();
    }
  }

  /**
   *  If key doesn't contain the array index, return <original key, null bitmap>
   *  Elif the key, i.e. the json path provided by user doesn't match any data, return <null, empty bitmap>
   *  Else, return the json path that is generated by replacing array index with . on the original key
   *  and the associated flattenDocId bitmap
   */
  private Pair<String, LazyBitmap> getKeyAndFlattenedDocIds(String key) {
    // Process the array index within the key if exists
    // E.g. "[*]"=1 -> "."='1'
    // E.g. "[0]"=1 -> ".$index"='0' && "."='1'
    // E.g. "[0][1]"=1 -> ".$index"='0' && "..$index"='1' && ".."='1'
    // E.g. ".foo[*].bar[*].foobar"='abc' -> ".foo..bar..foobar"='abc'
    // E.g. ".foo[0].bar[1].foobar"='abc' -> ".foo.$index"='0' && ".foo..bar.$index"='1' && ".foo..bar..foobar"='abc'
    // E.g. ".foo[0][1].bar"='abc' -> ".foo.$index"='0' && ".foo..$index"='1' && ".foo...bar"='abc'
    LazyBitmap matchingDocIds = null;
    int leftBracketIndex;
    while ((leftBracketIndex = key.indexOf('[')) >= 0) {
      int rightBracketIndex = key.indexOf(']', leftBracketIndex + 2);
      Preconditions.checkArgument(rightBracketIndex > 0, "Missing right bracket in key: %s", key);

      String leftPart = key.substring(0, leftBracketIndex);
      String arrayIndex = key.substring(leftBracketIndex + 1, rightBracketIndex);
      String rightPart = key.substring(rightBracketIndex + 1);

      if (!arrayIndex.equals(JsonUtils.WILDCARD)) {
        // "[0]"=1 -> ".$index"='0' && "."='1'
        // ".foo[1].bar"='abc' -> ".foo.$index"=1 && ".foo..bar"='abc'
        String searchKey = leftPart + JsonUtils.ARRAY_INDEX_KEY + JsonIndexCreator.KEY_VALUE_SEPARATOR + arrayIndex;
        RoaringBitmap docIds = _postingListMap.get(searchKey);

        if (docIds != null) {
          if (matchingDocIds == null) {
            matchingDocIds = LazyBitmap.createImmutable(docIds);
          } else {
            matchingDocIds = matchingDocIds.and(docIds);
          }
        } else {
          return Pair.of(null, LazyBitmap.EMPTY_BITMAP);
        }
      }

      key = leftPart + JsonUtils.KEY_SEPARATOR + rightPart;
    }
    return Pair.of(key, matchingDocIds);
  }

  private Map<String, RoaringBitmap> getMatchingKeysMap(String key) {
    return _postingListMap.subMap(key + JsonIndexCreator.KEY_VALUE_SEPARATOR, false,
        key + JsonIndexCreator.KEY_VALUE_SEPARATOR_NEXT_CHAR, false);
  }

  @Override
  public String[][] getValuesMV(int[] docIds, int length, Map<String, RoaringBitmap> valueToMatchingFlattenedDocs) {
    String[][] result = new String[length][];
    List<PriorityQueue<Pair<String, Integer>>> docIdToFlattenedDocIdsAndValues = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      // Sort based on flattened doc id
      docIdToFlattenedDocIdsAndValues.add(new PriorityQueue<>(Comparator.comparingInt(Pair::getRight)));
    }
    Map<Integer, Integer> docIdToPos = new HashMap<>();
    for (int i = 0; i < length; i++) {
      docIdToPos.put(docIds[i], i);
    }

    _readLock.lock();
    try {
      for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingFlattenedDocs.entrySet()) {
        String value = entry.getKey();
        RoaringBitmap matchingFlattenedDocIds = entry.getValue();
        matchingFlattenedDocIds.forEach((IntConsumer) flattenedDocId -> {
          int docId = _docIdMapping.getInt(flattenedDocId);
          if (docIdToPos.containsKey(docId)) {
            docIdToFlattenedDocIdsAndValues.get(docIdToPos.get(docId)).add(Pair.of(value, flattenedDocId));
          }
        });
      }
    } finally {
      _readLock.unlock();
    }

    for (int i = 0; i < length; i++) {
      PriorityQueue<Pair<String, Integer>> pq = docIdToFlattenedDocIdsAndValues.get(i);
      result[i] = new String[pq.size()];
      int j = 0;
      while (!pq.isEmpty()) {
        result[i][j++] = pq.poll().getLeft();
      }
    }

    return result;
  }

  @Override
  public String[] getValuesSV(int[] docIds, int length, Map<String, RoaringBitmap> valueToMatchingFlattenedDocs,
      boolean isFlattenedDocIds) {
    Int2ObjectOpenHashMap<String> docIdToValues = new Int2ObjectOpenHashMap<>(length);
    RoaringBitmap docIdMask = RoaringBitmap.bitmapOf(Arrays.copyOfRange(docIds, 0, length));
    _readLock.lock();
    try {
      for (Map.Entry<String, RoaringBitmap> entry : valueToMatchingFlattenedDocs.entrySet()) {
        String value = entry.getKey();
        RoaringBitmap matchingDocIds = entry.getValue();

        if (isFlattenedDocIds) {
          matchingDocIds.forEach((IntConsumer) flattenedDocId -> {
            int docId = _docIdMapping.getInt(flattenedDocId);
            if (docIdMask.contains(docId)) {
              docIdToValues.put(docId, value);
            }
          });
        } else {
          RoaringBitmap intersection = RoaringBitmap.and(entry.getValue(), docIdMask);
          if (intersection.isEmpty()) {
            continue;
          }
          for (int docId : intersection) {
            docIdToValues.put(docId, entry.getKey());
          }
        }
      }
    } finally {
      _readLock.unlock();
    }

    String[] values = new String[length];
    for (int i = 0; i < length; i++) {
      values[i] = docIdToValues.get(docIds[i]);
    }
    return values;
  }

  @Override
  public boolean canAddMore() {
    return _bytesSize < _maxBytesSize;
  }

  @Override
  public void close() {
    try {
      String tableName = SegmentUtils.getTableNameFromSegmentName(_segmentName);
      _serverMetrics.addMeteredTableValue(tableName, _columnName, ServerMeter.MUTABLE_JSON_INDEX_MEMORY_USAGE,
          _bytesSize);
    } catch (Exception e) {
      LOGGER.warn(
          "Caught exception while updating mutable json index memory usage for segment: {}, column: {}, value: {}",
          _segmentName, _columnName, _bytesSize, e);
    }
  }
}
