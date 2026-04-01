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
package org.apache.pinot.query.runtime.operator.groupby;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMap;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMapFactory;
import org.apache.pinot.spi.utils.FixedIntArray;


/**
 * {@link GroupIdGenerator} for queries with three or more group-by keys of arbitrary types.
 *
 * <p>Each distinct combination of key values is mapped to a compact integer group ID. Internally the generator
 * maintains per-column {@link ValueToIdMap}s (one int per key column per row) and stores the packed ID-tuple as a
 * {@link FixedIntArray} key in a fastutil {@code Object2IntOpenHashMap}.
 *
 * <p><b>Zero-allocation steady-state</b>: a single reusable {@link FixedIntArray} ({@code _probeKey}) backed by a
 * pre-allocated {@code int[]} array is used for every look-up. A fresh {@link FixedIntArray} is allocated only when a
 * genuinely new group is inserted, so the common case (group already seen) is allocation-free.
 */
public class MultiKeysGroupIdGenerator implements GroupIdGenerator {
  private final Object2IntOpenHashMap<FixedIntArray> _groupIdMap;
  private final ValueToIdMap[] _keyToIdMaps;
  private final int _numGroupsLimit;
  private final int _numKeyColumns;
  /// Reusable probe key: backing array is overwritten on every getGroupId call; cloned only on new-group insertion.
  private final int[] _probeKeyArray;
  private final FixedIntArray _probeKey;
  private int _numGroups = 0;

  public MultiKeysGroupIdGenerator(ColumnDataType[] keyTypes, int numKeyColumns,
      int numGroupsLimit, int initialCapacity) {
    _groupIdMap = new Object2IntOpenHashMap<>(initialCapacity);
    _groupIdMap.defaultReturnValue(INVALID_ID);
    _numGroupsLimit = numGroupsLimit;
    _numKeyColumns = numKeyColumns;
    _probeKeyArray = new int[numKeyColumns];
    _probeKey = new FixedIntArray(_probeKeyArray);
    _keyToIdMaps = new ValueToIdMap[numKeyColumns];
    for (int i = 0; i < numKeyColumns; i++) {
      _keyToIdMaps[i] = ValueToIdMapFactory.get(keyTypes[i].toDataType());
    }
  }

  @Override
  public int getGroupId(Object key) {
    Object[] keyValues = (Object[]) key;
    if (_numGroups < _numGroupsLimit) {
      // Below group limit: resolve (or insert) each key's integer ID, then look up the group.
      for (int i = 0; i < _numKeyColumns; i++) {
        Object keyValue = keyValues[i];
        _probeKeyArray[i] = keyValue != null ? _keyToIdMaps[i].put(keyValue) : NULL_ID;
      }
      int groupId = _groupIdMap.getInt(_probeKey);
      if (groupId == INVALID_ID) {
        // New group: clone the probe array so the map key is stable, then insert.
        groupId = _numGroups++;
        _groupIdMap.put(_probeKey.clone(), groupId);
      }
      return groupId;
    } else {
      // Above group limit: look up only, return INVALID_ID for any unknown key.
      for (int i = 0; i < _numKeyColumns; i++) {
        Object keyValue = keyValues[i];
        if (keyValue == null) {
          _probeKeyArray[i] = NULL_ID;
        } else {
          int keyId = _keyToIdMaps[i].getId(keyValue);
          if (keyId == INVALID_ID) {
            return INVALID_ID;
          }
          _probeKeyArray[i] = keyId;
        }
      }
      return _groupIdMap.getInt(_probeKey);
    }
  }

  @Override
  public int getNumGroups() {
    return _numGroups;
  }

  @Override
  public Iterator<GroupKey> getGroupKeyIterator(int numColumns) {
    return new Iterator<GroupKey>() {
      final ObjectIterator<Object2IntOpenHashMap.Entry<FixedIntArray>> _entryIterator =
          _groupIdMap.object2IntEntrySet().fastIterator();

      @Override
      public boolean hasNext() {
        return _entryIterator.hasNext();
      }

      @Override
      public GroupKey next() {
        Object2IntOpenHashMap.Entry<FixedIntArray> entry = _entryIterator.next();
        int[] keyIds = entry.getKey().elements();
        Object[] row = new Object[numColumns];
        for (int i = 0; i < _numKeyColumns; i++) {
          int keyId = keyIds[i];
          if (keyId != NULL_ID) {
            row[i] = _keyToIdMaps[i].get(keyId);
          }
        }
        return new GroupKey(entry.getIntValue(), row);
      }
    };
  }
}
