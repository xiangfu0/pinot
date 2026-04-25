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
package org.apache.pinot.segment.local.io.codec;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.segment.spi.codec.CodecOptions;


/**
 * Transform codec that computes delta-of-delta values between successive values before the
 * compression stage (if any). Useful for data where the differences between consecutive values
 * are approximately constant (e.g. timestamps with regular intervals).
 *
 * <p>DSL form: {@code DELTADELTA} (no arguments)
 *
 * <p>Supported stored types: {@code INT}, {@code LONG}.
 *
 * <p>Wire format:
 * <pre>
 *   [1 byte : type flag — 0=INT, 1=LONG]
 *   [4 bytes: count (number of values)]
 *   [element_size bytes: first value verbatim]
 *   [element_size bytes: first delta (second - first), if count &gt; 1]
 *   [(count-2) * element_size bytes: delta-of-deltas, if count &gt; 2]
 * </pre>
 */
public final class DeltaDeltaCodecDefinition
    extends AbstractDeltaCodecDefinition<DeltaDeltaCodecDefinition.Options> {

  /**
   * On-disk permanent name stored verbatim in segment file headers.
   * This string is a frozen on-disk API contract and must never be changed.
   */
  public static final String NAME = "DELTADELTA";

  public static final DeltaDeltaCodecDefinition INSTANCE = new DeltaDeltaCodecDefinition();

  private DeltaDeltaCodecDefinition() {
  }

  /** Singleton options object — DELTADELTA has no configurable parameters. */
  public static final class Options implements CodecOptions {
    public static final Options INSTANCE = new Options();

    private Options() {
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Options parseOptions(List<String> args) {
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("DELTADELTA codec takes no arguments but got: " + args);
    }
    return Options.INSTANCE;
  }

  @Override
  public String canonicalize(Options options) {
    return NAME;
  }

  @Override
  protected ByteBuffer encodeInt(ByteBuffer src, int remaining) {
    int count = remaining / Integer.BYTES;
    ByteBuffer out = ByteBuffer.allocateDirect(1 + Integer.BYTES + remaining);
    out.put((byte) 0); // INT flag
    out.putInt(count);
    if (count == 0) {
      out.flip();
      return out;
    }
    int prev = src.getInt();
    out.putInt(prev);
    if (count == 1) {
      out.flip();
      return out;
    }
    int prevDelta = src.getInt() - prev;
    out.putInt(prevDelta);
    prev += prevDelta;
    for (int i = 2; i < count; i++) {
      int cur = src.getInt();
      int curDelta = cur - prev;
      out.putInt(curDelta - prevDelta);
      prev = cur;
      prevDelta = curDelta;
    }
    out.flip();
    return out;
  }

  @Override
  protected ByteBuffer encodeLong(ByteBuffer src, int remaining) {
    int count = remaining / Long.BYTES;
    ByteBuffer out = ByteBuffer.allocateDirect(1 + Integer.BYTES + remaining);
    out.put((byte) 1); // LONG flag
    out.putInt(count);
    if (count == 0) {
      out.flip();
      return out;
    }
    long prev = src.getLong();
    out.putLong(prev);
    if (count == 1) {
      out.flip();
      return out;
    }
    long prevDelta = src.getLong() - prev;
    out.putLong(prevDelta);
    prev += prevDelta;
    for (int i = 2; i < count; i++) {
      long cur = src.getLong();
      long curDelta = cur - prev;
      out.putLong(curDelta - prevDelta);
      prev = cur;
      prevDelta = curDelta;
    }
    out.flip();
    return out;
  }

  @Override
  protected ByteBuffer decodeInt(ByteBuffer src, int count) {
    ByteBuffer out = ByteBuffer.allocateDirect(count * Integer.BYTES);
    int prev = src.getInt();
    out.putInt(prev);
    if (count > 1) {
      int prevDelta = src.getInt();
      prev += prevDelta;
      out.putInt(prev);
      for (int i = 2; i < count; i++) {
        int dod = src.getInt();
        prevDelta += dod;
        prev += prevDelta;
        out.putInt(prev);
      }
    }
    out.flip();
    return out;
  }

  @Override
  protected ByteBuffer decodeLong(ByteBuffer src, int count) {
    ByteBuffer out = ByteBuffer.allocateDirect(count * Long.BYTES);
    long prev = src.getLong();
    out.putLong(prev);
    if (count > 1) {
      long prevDelta = src.getLong();
      prev += prevDelta;
      out.putLong(prev);
      for (int i = 2; i < count; i++) {
        long dod = src.getLong();
        prevDelta += dod;
        prev += prevDelta;
        out.putLong(prev);
      }
    }
    out.flip();
    return out;
  }

  @Override
  protected void decodeIntInto(ByteBuffer src, int count, ByteBuffer dst) {
    int prev = src.getInt();
    dst.putInt(prev);
    if (count > 1) {
      int prevDelta = src.getInt();
      prev += prevDelta;
      dst.putInt(prev);
      for (int i = 2; i < count; i++) {
        int dod = src.getInt();
        prevDelta += dod;
        prev += prevDelta;
        dst.putInt(prev);
      }
    }
  }

  @Override
  protected void decodeLongInto(ByteBuffer src, int count, ByteBuffer dst) {
    long prev = src.getLong();
    dst.putLong(prev);
    if (count > 1) {
      long prevDelta = src.getLong();
      prev += prevDelta;
      dst.putLong(prev);
      for (int i = 2; i < count; i++) {
        long dod = src.getLong();
        prevDelta += dod;
        prev += prevDelta;
        dst.putLong(prev);
      }
    }
  }
}
