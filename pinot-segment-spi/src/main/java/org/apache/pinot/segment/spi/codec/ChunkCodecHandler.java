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
package org.apache.pinot.segment.spi.codec;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Extension of {@link CodecDefinition} that adds the encode/decode operations needed to execute a
 * codec pipeline over forward-index chunks.
 *
 * <p>All implementations are expected to be stateless and thread-safe.
 *
 * <p>Buffer contract for all methods:
 * <ul>
 *   <li>{@link #encode} and {@link #decode}: {@code src} is ready for read (position=0);
 *       the returned buffer is ready for read and owned by the caller.</li>
 *   <li>{@link #decodeInto}: implementations must treat {@code dst} as freshly cleared
 *       (calling {@code dst.clear()} internally is recommended as a defensive first step);
 *       {@code dst} is flipped (position=0, limit=decoded bytes) on return.</li>
 * </ul>
 *
 * @param <O> typed {@link CodecOptions} for this codec
 */
public interface ChunkCodecHandler<O extends CodecOptions> extends CodecDefinition<O> {

  /**
   * Encodes {@code src} and returns the encoded bytes ready for read.
   *
   * <p><b>Position contract:</b> implementations may consume {@code src} (advance its position).
   * Callers that need to re-read {@code src} after this call must pass {@code src.duplicate()}.
   *
   * @param options parsed options for this codec invocation
   * @param ctx     column context (data type, etc.)
   * @param src     unencoded data, ready for read
   * @return encoded buffer ready for read; caller owns this buffer
   */
  ByteBuffer encode(O options, CodecContext ctx, ByteBuffer src) throws IOException;

  /**
   * Decodes {@code src} and returns the decoded bytes ready for read.
   *
   * <p><b>Position contract:</b> implementations may consume {@code src} (advance its position).
   * Callers that need to re-read {@code src} after this call must pass {@code src.duplicate()}.
   *
   * @param options parsed options for this codec invocation
   * @param ctx     column context
   * @param src     encoded data, ready for read
   * @return decoded buffer ready for read; caller owns this buffer
   */
  ByteBuffer decode(O options, CodecContext ctx, ByteBuffer src) throws IOException;

  /**
   * Decodes {@code src} directly into {@code dst}, avoiding an extra allocation.
   * Implementations must treat {@code dst} as freshly cleared and flip it before returning.
   *
   * <p>Callers must ensure {@code dst} is a direct {@link ByteBuffer} when
   * {@link #requiresDirectDstBuffer()} returns {@code true}.
   *
   * @param options parsed options for this codec invocation
   * @param ctx     column context
   * @param src     encoded data, ready for read
   * @param dst     output buffer; must be direct when required; must have sufficient capacity
   */
  void decodeInto(O options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException;

  /**
   * Returns an upper bound on the encoded byte count for an input of {@code inputSize} bytes.
   */
  int maxEncodedSize(O options, int inputSize);

  /**
   * Returns {@code true} if {@link #decodeInto} requires {@code dst} to be a direct
   * {@link ByteBuffer} (e.g. codecs that delegate to JNI libraries with direct-buffer-only APIs).
   */
  boolean requiresDirectDstBuffer();
}
