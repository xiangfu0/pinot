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
package org.apache.pinot.segment.local.segment.index;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GenericHeader {
  private final List<SubHeader> _subHeaders;

  // Empty constructor (Supports Builder Pattern)
  public GenericHeader() {
    _subHeaders = new ArrayList<>();
  }

  /**
   * Adds a new SubHeader to the Header.
   */
  public GenericHeader addSubHeader(SubHeader subHeader) {
    _subHeaders.add(subHeader);
    return this;
  }

  public List<SubHeader> getSubHeaders() {
    return _subHeaders;
  }

  /**
   * Serializes the Header object into a ByteBuffer.
   */
  public byte[] toBuffer() {
    int estimatedSize = getEstimatedSize();
    byte[] bytes = new byte[estimatedSize];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putInt(_subHeaders.size()); // Write number of SubHeaders

    for (SubHeader subHeader : _subHeaders) {
      subHeader.writeToBuffer(buffer);
    }
    return bytes;
  }

  public int getEstimatedSize() {
    int estimatedSize = 4; // Number of subHeaders (4 bytes)

    for (SubHeader subHeader : _subHeaders) {
      estimatedSize += subHeader.estimateSize();
    }
    return estimatedSize;
  }

  /**
   * Deserializes a ByteBuffer into a Header object.
   */
  public static GenericHeader fromBuffer(ByteBuffer buffer) {
    GenericHeader header = new GenericHeader();
    int numSubHeaders = buffer.getInt();

    for (int i = 0; i < numSubHeaders; i++) {
      header.addSubHeader(SubHeader.readFromBuffer(buffer));
    }

    return header;
  }

  /**
   * SubHeader class with a Map<String, String> and Map<String, Long>.
   */
  public static class SubHeader {
    private final Map<String, String> _stringMap;
    private final Map<String, Long> _longMap;

    public SubHeader() {
      _stringMap = new HashMap<>();
      _longMap = new HashMap<>();
    }

    public SubHeader addString(String key, String value) {
      _stringMap.put(key, value);
      return this;
    }

    public SubHeader addLong(String key, long value) {
      _longMap.put(key, value);
      return this;
    }

    public String getString(String key) {
      return _stringMap.get(key);
    }

    public long getLong(String key) {
      return _longMap.get(key);
    }

    /**
     * Estimates the byte size of this SubHeader.
     */
    public int estimateSize() {
      int size = 4; // Number of entries in stringMap
      for (Map.Entry<String, String> entry : _stringMap.entrySet()) {
        size += 4 + entry.getKey().getBytes(StandardCharsets.UTF_8).length;
        size += 4 + entry.getValue().getBytes(StandardCharsets.UTF_8).length;
      }

      size += 4; // Number of entries in longMap
      for (Map.Entry<String, Long> entry : _longMap.entrySet()) {
        size += 4 + entry.getKey().getBytes(StandardCharsets.UTF_8).length;
        size += 8; // Long value (8 bytes)
      }

      return size;
    }

    /**
     * Serializes this SubHeader into a ByteBuffer.
     */
    public void writeToBuffer(ByteBuffer buffer) {
      // Serialize stringMap
      buffer.putInt(_stringMap.size());
      for (Map.Entry<String, String> entry : _stringMap.entrySet()) {
        byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);

        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
      }

      // Serialize longMap
      buffer.putInt(_longMap.size());
      for (Map.Entry<String, Long> entry : _longMap.entrySet()) {
        byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putLong(entry.getValue());
      }
    }

    /**
     * Deserializes a SubHeader from a ByteBuffer.
     */
    public static SubHeader readFromBuffer(ByteBuffer buffer) {
      SubHeader subHeader = new SubHeader();

      // Read stringMap
      int stringMapSize = buffer.getInt();
      for (int i = 0; i < stringMapSize; i++) {
        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        int valueLength = buffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        buffer.get(valueBytes);
        String value = new String(valueBytes, StandardCharsets.UTF_8);

        subHeader.addString(key, value);
      }

      // Read longMap
      int longMapSize = buffer.getInt();
      for (int i = 0; i < longMapSize; i++) {
        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        long value = buffer.getLong();
        subHeader.addLong(key, value);
      }

      return subHeader;
    }
  }
}
