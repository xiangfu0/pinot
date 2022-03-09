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
package org.apache.pinot.segment.local.segment.index.readers;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.TimestampIndexCreator;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.reader.TimestampIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImmutableTimestampIndexReader implements TimestampIndexReader<ImmutableRoaringBitmap> {
  public static final Logger LOGGER = LoggerFactory.getLogger(ImmutableTimestampIndexReader.class);

  private final Map<String, RangeIndexReaderImpl> _rangeIndexMap = new HashMap<>();

  public ImmutableTimestampIndexReader(PinotDataBuffer dataBuffer) {
    long bytesRead = 0;
    int version = dataBuffer.getInt(0);
    bytesRead += Integer.BYTES;
    Preconditions.checkArgument(version == TimestampIndexCreator.VERSION, "Unsupported Timestamp index version: %s",
        version);
    int numGranularities = dataBuffer.getInt(bytesRead);
    bytesRead += Integer.BYTES;
    List<TimestampIndexGranularity> timestampIndexGranularities = new ArrayList<>();
    for (int i = 0; i < numGranularities; i++) {
      timestampIndexGranularities.add(TimestampIndexGranularity.getFromIndex(dataBuffer.getInt(bytesRead)));
      bytesRead += Integer.BYTES;
    }
    long rangeIndexOffset = bytesRead + numGranularities * Long.BYTES;
    for (int i = 0; i < numGranularities; i++) {
      long indexLength = dataBuffer.getLong(bytesRead);
      bytesRead += Long.BYTES;
      PinotDataBuffer rangeIndexBuffer =
          dataBuffer.view(rangeIndexOffset, rangeIndexOffset + indexLength, ByteOrder.BIG_ENDIAN);
      _rangeIndexMap.put(timestampIndexGranularities.get(i).toString(), new RangeIndexReaderImpl(rangeIndexBuffer));
      rangeIndexOffset += indexLength;
    }
  }

  @Override
  public void close()
      throws IOException {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
    for (RangeIndexReader reader : _rangeIndexMap.values()) {
      reader.close();
    }
  }

  @Nullable
  public ImmutableRoaringBitmap getMatchingDocIds(String granularity, long min, long max) {
    return _rangeIndexMap.get(granularity).getMatchingDocIds(min, max);
  }

  @Nullable
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(String granularity, long min, long max) {
    return _rangeIndexMap.get(granularity).getPartiallyMatchingDocIds(min, max);
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getMatchingDocIds(long min, long max) {
    throw new RuntimeException();
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getPartiallyMatchingDocIds(long min, long max) {
    throw new RuntimeException();
  }

  @Override
  public TimestampIndexReader get(String granularity) {
    return new SingleTimestampIndexReader(this, granularity);
  }

  private class SingleTimestampIndexReader implements TimestampIndexReader<ImmutableRoaringBitmap> {
    private final String _granularity;
    private final ImmutableTimestampIndexReader _timestampIndexReaderRef;

    public SingleTimestampIndexReader(ImmutableTimestampIndexReader timestampIndexReader, String granularity) {
      _timestampIndexReaderRef = timestampIndexReader;
      _granularity = granularity;
    }

    @Nullable
    @Override
    public ImmutableRoaringBitmap getMatchingDocIds(long min, long max) {
      return _timestampIndexReaderRef.getMatchingDocIds(_granularity, min, max);
    }

    @Nullable
    @Override
    public ImmutableRoaringBitmap getPartiallyMatchingDocIds(long min, long max) {
      return _timestampIndexReaderRef.getPartiallyMatchingDocIds(_granularity, min, max);
    }

    @Override
    public TimestampIndexReader get(String granularity) {
      throw new RuntimeException();
    }

    @Override
    public void close()
        throws IOException {
    }
  }
}
