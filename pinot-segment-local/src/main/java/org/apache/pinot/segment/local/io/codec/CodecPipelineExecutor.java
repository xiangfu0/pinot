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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecInvocation;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;
import org.apache.pinot.segment.spi.codec.CodecPipeline;
import org.apache.pinot.segment.spi.codec.CodecSpecParser;


/**
 * Executes a parsed and validated {@link CodecPipeline} for a single forward-index chunk.
 *
 * <p>Write path: values → transforms (in order) → compression → bytes stored on disk.
 * Read path: bytes from disk → decompress → reverse transforms (in reverse order) → values.
 *
 * <p>The executor is constructed once per column from the canonical {@code codecSpec} string
 * stored in the file header and is thread-safe for concurrent read calls (it holds no mutable
 * per-call state).
 *
 * <p>The executor is codec-agnostic: it holds an ordered list of {@link BoundStage} objects,
 * each pairing a {@link ChunkCodecHandler} with its parsed {@link CodecOptions}. Codec logic
 * lives entirely in the handler implementations; this class only drives the pipeline loop.
 *
 * <h3>Buffer contract</h3>
 * <ul>
 *   <li>{@link #compress}: {@code src} is ready for read (position=0); returns a new
 *       {@link ByteBuffer} ready for read containing the encoded bytes.</li>
 *   <li>{@link #decompress}: {@code src} is ready for read; returns a new
 *       {@link ByteBuffer} ready for read containing the decoded bytes.</li>
 *   <li>{@link #maxCompressedSize}: returns an upper bound on encoded size.</li>
 * </ul>
 */
public final class CodecPipelineExecutor {

  /** A codec handler bound to the options parsed from a specific pipeline invocation. */
  private static final class BoundStage<O extends CodecOptions> {
    final ChunkCodecHandler<O> _handler;
    final O _options;
    final CodecContext _ctx;

    BoundStage(ChunkCodecHandler<O> handler, O options, CodecContext ctx) {
      _handler = handler;
      _options = options;
      _ctx = ctx;
    }

    ByteBuffer encode(ByteBuffer src) throws IOException {
      return _handler.encode(_options, _ctx, src);
    }

    ByteBuffer decode(ByteBuffer src) throws IOException {
      return _handler.decode(_options, _ctx, src);
    }

    void decodeInto(ByteBuffer src, ByteBuffer dst) throws IOException {
      _handler.decodeInto(_options, _ctx, src, dst);
    }

    int maxEncodedSize(int inputSize) {
      return _handler.maxEncodedSize(_options, inputSize);
    }

    boolean requiresDirectDstBuffer() {
      return _handler.requiresDirectDstBuffer();
    }

    boolean isCompression() {
      return _handler.kind() == CodecKind.COMPRESSION;
    }

    String canonicalize() {
      return _handler.canonicalize(_options);
    }
  }

  private final List<BoundStage<?>> _stages;
  private final String _canonicalSpec;
  private final boolean _hasCompression;
  private final boolean _requiresDirectDstBuffer;

  /**
   * Creates an executor by parsing and validating the given spec.
   *
   * @param spec     the codec DSL string (e.g. {@code "CODEC(DELTA,ZSTD(3))"})
   * @param ctx      column context used for validation
   * @param registry codec registry
   */
  public static CodecPipelineExecutor create(String spec, CodecContext ctx, CodecRegistry registry) {
    CodecPipeline pipeline = CodecSpecParser.parse(spec);
    CodecPipelineValidator.validate(pipeline, registry, ctx);
    return new CodecPipelineExecutor(pipeline, registry, ctx);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private CodecPipelineExecutor(CodecPipeline pipeline, CodecRegistry registry, CodecContext ctx) {
    List<CodecInvocation> invocations = pipeline.stages();
    if (invocations.isEmpty()) {
      throw new IllegalArgumentException("Codec pipeline must contain at least one stage");
    }
    List<BoundStage<?>> stages = new ArrayList<>(invocations.size());

    for (CodecInvocation inv : invocations) {
      ChunkCodecHandler handler = (ChunkCodecHandler) registry.getOrThrow(inv.name());
      CodecOptions opts = handler.parseOptions(inv.args());
      stages.add(new BoundStage<>(handler, opts, ctx));
    }
    _stages = stages;
    _canonicalSpec = buildCanonical(stages);
    _hasCompression = stages.stream().anyMatch(BoundStage::isCompression);
    _requiresDirectDstBuffer = stages.stream().anyMatch(BoundStage::requiresDirectDstBuffer);
  }

  /** Returns the canonical spec string derived from the parsed pipeline. */
  public String getCanonicalSpec() {
    return _canonicalSpec;
  }

  /**
   * Returns an upper bound on the number of bytes that {@link #compress} may produce for
   * an uncompressed chunk of the given byte length.
   */
  public int maxCompressedSize(int uncompressedSize) {
    int size = uncompressedSize;
    for (BoundStage<?> stage : _stages) {
      size = stage.maxEncodedSize(size);
    }
    return size;
  }

  /**
   * Encodes {@code src} through the pipeline and returns the encoded bytes ready for read.
   *
   * <p><b>Position contract:</b> stages may consume {@code src} (advance its position). Callers
   * that need to re-read {@code src} after this call must pass {@code src.duplicate()}.
   *
   * @param src uncompressed chunk data, ready for read (position=0, limit=dataSize)
   * @return encoded buffer ready for read; the caller owns this buffer
   */
  public ByteBuffer compress(ByteBuffer src) throws IOException {
    ByteBuffer current = src;
    for (BoundStage<?> stage : _stages) {
      current = stage.encode(current);
    }
    return current;
  }

  /**
   * Decodes {@code src} through the reversed pipeline and returns the original bytes.
   *
   * @param src encoded chunk data, ready for read
   * @return decoded buffer ready for read; the caller owns this buffer
   */
  public ByteBuffer decompress(ByteBuffer src) throws IOException {
    ByteBuffer current = src;
    for (int i = _stages.size() - 1; i >= 0; i--) {
      current = _stages.get(i).decode(current);
    }
    return current;
  }

  /**
   * Decodes {@code src} through the reversed pipeline, writing the result directly into
   * {@code dst}.  On return {@code dst} is flipped and ready for read (position=0,
   * limit=decompressed size).
   *
   * <p>For single-stage pipelines (transform-only or compression-only), the decoded bytes are
   * written directly into {@code dst}, avoiding an intermediate allocation.  For multi-stage
   * pipelines an intermediate buffer is allocated for any stage that is not the last in the
   * reversed pipeline; the final stage writes directly into {@code dst}.
   *
   * @param src encoded chunk data, ready for read
   * @param dst caller-supplied output buffer; must be a <em>direct</em> {@link ByteBuffer} when
   *            the pipeline requires it (see {@link ChunkCodecHandler#requiresDirectDstBuffer()});
   *            must have sufficient {@link ByteBuffer#capacity()} for the decoded data; its
   *            position and limit are overwritten before returning
   * @throws IOException              if decompression fails
   * @throws IllegalArgumentException if {@code dst} is not direct when required, or does not have
   *                                  enough capacity
   */
  public void decompress(ByteBuffer src, ByteBuffer dst) throws IOException {
    Preconditions.checkArgument(!_requiresDirectDstBuffer || dst.isDirect(),
        "decompress(src, dst) requires a direct ByteBuffer for pipeline: %s", _canonicalSpec);

    int stageCount = _stages.size();
    if (stageCount == 1) {
      _stages.get(0).decodeInto(src, dst);
      return;
    }

    // Multi-stage: decode all stages except the last into intermediate buffers, then write into dst
    ByteBuffer current = src;
    for (int i = stageCount - 1; i > 0; i--) {
      current = _stages.get(i).decode(current);
    }
    _stages.get(0).decodeInto(current, dst);
  }

  /** Returns true if the pipeline has at least one compression stage. */
  public boolean isCompressed() {
    return _hasCompression;
  }

  // -------------------------------------------------------------------------
  // Canonical spec builder
  // -------------------------------------------------------------------------

  private static String buildCanonical(List<BoundStage<?>> stages) {
    if (stages.size() == 1) {
      return stages.get(0).canonicalize();
    }
    StringBuilder sb = new StringBuilder("CODEC(");
    for (int i = 0; i < stages.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(stages.get(i).canonicalize());
    }
    sb.append(')');
    return sb.toString();
  }
}
