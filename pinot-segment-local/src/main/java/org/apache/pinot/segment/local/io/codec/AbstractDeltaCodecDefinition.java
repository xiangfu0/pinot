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
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Shared structure for {@link DeltaCodecDefinition} and {@link DeltaDeltaCodecDefinition}.
 *
 * <p>Both codecs: accept no arguments, operate only on INT/LONG, produce the same header format
 * ({@code [flag:byte][count:int]…}), and have the same {@code maxEncodedSize}. Only the
 * encode/decode arithmetic differs and is delegated to the four abstract helper methods.
 *
 * <p><b>Two's-complement wrap-around is intentional.</b> Deltas are computed using ordinary Java
 * {@code -} on int/long, which silently wraps when the mathematical delta does not fit in the
 * value's width (e.g. {@code Integer.MAX_VALUE - Integer.MIN_VALUE}). Decode applies the symmetric
 * {@code +} so the original sequence is recovered bit-for-bit. Boundary tests in
 * {@code CodecPipelineForwardIndexTest} lock in this property — do not switch the encode/decode
 * helpers to checked arithmetic ({@code Math.subtractExact}, {@code Math.addExact}) without also
 * changing the on-disk format.
 */
abstract class AbstractDeltaCodecDefinition<O extends CodecOptions> implements ChunkCodecHandler<O> {

  @Override
  public final CodecKind kind() {
    return CodecKind.TRANSFORM;
  }

  @Override
  public final void validateContext(O options, CodecContext ctx) {
    DataType dt = ctx.getDataType();
    if (dt != DataType.INT && dt != DataType.LONG) {
      throw new IllegalArgumentException(
          name() + " codec only supports INT and LONG columns, but column has type: " + dt);
    }
  }

  @Override
  public final int maxEncodedSize(O options, int inputSize) {
    // header: 1 flag byte + 4 count bytes; payload is at most the same size as input
    return 1 + Integer.BYTES + inputSize;
  }

  @Override
  public final boolean requiresDirectDstBuffer() {
    return false;
  }

  /** Subclasses must reject non-empty {@code args}. */
  @Override
  public abstract O parseOptions(List<String> args);

  @Override
  public final ByteBuffer encode(O options, CodecContext ctx, ByteBuffer src) {
    int remaining = src.remaining();
    DataType dt = ctx.getDataType();
    if (dt != DataType.INT && dt != DataType.LONG) {
      throw new IllegalArgumentException(name() + " does not support stored type: " + dt);
    }
    int elementSize = dt.size();
    if (remaining % elementSize != 0) {
      throw new IllegalArgumentException(
          name() + ": input buffer size (" + remaining + ") is not a multiple of element size (" + elementSize
              + ") for stored type " + dt);
    }
    return dt == DataType.LONG ? encodeLong(src, remaining) : encodeInt(src, remaining);
  }

  @Override
  public final ByteBuffer decode(O options, CodecContext ctx, ByteBuffer src) {
    byte flag = src.get();
    int count = src.getInt();
    if (count < 0) {
      throw new IllegalStateException(
          name() + ": invalid count in header: " + count + ". Segment may be corrupt.");
    }
    if (count == 0) {
      return ByteBuffer.allocateDirect(0);
    }
    int elementSize;
    if (flag == 0) {
      elementSize = Integer.BYTES;
    } else if (flag == 1) {
      elementSize = Long.BYTES;
    } else {
      throw new IllegalStateException(
          "Unknown " + name() + " type flag: " + flag + ". Expected 0 (INT) or 1 (LONG). Segment may be corrupt.");
    }
    long requiredInputBytes = (long) count * elementSize;
    if (requiredInputBytes != src.remaining()) {
      // Frame size must match exactly: codec frames are tightly packed by the writer. A mismatch
      // is either corruption or a caller passing a buffer with extra bytes — both are bugs.
      throw new IllegalStateException(
          name() + ": header claims " + count + " values (" + requiredInputBytes + " bytes) but src has "
              + src.remaining() + " bytes remaining. Segment may be corrupt.");
    }
    return flag == 0 ? decodeInt(src, count) : decodeLong(src, count);
  }

  @Override
  public final void decodeInto(O options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) {
    dst.clear();
    byte flag = src.get();
    int count = src.getInt();
    if (count < 0) {
      throw new IllegalStateException(
          name() + ": invalid count in header: " + count + ". Segment may be corrupt.");
    }
    if (count == 0) {
      dst.flip();
      return;
    }
    int elementSize;
    if (flag == 0) {
      elementSize = Integer.BYTES;
    } else if (flag == 1) {
      elementSize = Long.BYTES;
    } else {
      throw new IllegalStateException(
          "Unknown " + name() + " type flag: " + flag + ". Expected 0 (INT) or 1 (LONG). Segment may be corrupt.");
    }
    long requiredCapacity = (long) count * elementSize;
    if (requiredCapacity > dst.capacity()) {
      throw new IllegalArgumentException(
          name() + ": decoded size " + requiredCapacity + " exceeds dst capacity " + dst.capacity());
    }
    if (requiredCapacity != src.remaining()) {
      // Frame size mismatch: the codec frame is tightly packed; a mismatch is corruption or a
      // caller passing a buffer with extra bytes.
      throw new IllegalStateException(
          name() + ": header claims " + count + " values (" + requiredCapacity + " bytes) but src has "
              + src.remaining() + " bytes remaining. Segment may be corrupt.");
    }
    if (flag == 0) {
      decodeIntInto(src, count, dst);
    } else {
      decodeLongInto(src, count, dst);
    }
    dst.flip();
  }

  // -------------------------------------------------------------------------
  // Arithmetic helpers — subclasses provide the encoding/decoding logic
  // -------------------------------------------------------------------------

  protected abstract ByteBuffer encodeInt(ByteBuffer src, int remaining);

  protected abstract ByteBuffer encodeLong(ByteBuffer src, int remaining);

  protected abstract ByteBuffer decodeInt(ByteBuffer src, int count);

  protected abstract ByteBuffer decodeLong(ByteBuffer src, int count);

  protected abstract void decodeIntInto(ByteBuffer src, int count, ByteBuffer dst);

  protected abstract void decodeLongInto(ByteBuffer src, int count, ByteBuffer dst);
}
