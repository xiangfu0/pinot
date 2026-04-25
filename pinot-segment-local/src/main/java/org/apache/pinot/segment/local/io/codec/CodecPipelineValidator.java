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

import java.util.List;
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecInvocation;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;
import org.apache.pinot.segment.spi.codec.CodecPipeline;


/**
 * Validates a {@link CodecPipeline} against structural rules and per-codec context.
 *
 * <p>Rules enforced:
 * <ol>
 *   <li>All codec names must be registered in the supplied {@link CodecRegistry}.</li>
 *   <li>At most one {@link CodecKind#COMPRESSION} stage is allowed.</li>
 *   <li>The compression stage, if present, must be last.</li>
 *   <li>All {@link CodecKind#TRANSFORM} stages must precede the compression stage.</li>
 *   <li>Each codec's {@link ChunkCodecHandler#validateContext} must pass.</li>
 * </ol>
 */
public final class CodecPipelineValidator {

  private CodecPipelineValidator() {
  }

  /**
   * Validates the pipeline.
   *
   * @param pipeline pipeline AST to validate
   * @param registry registry used to resolve codec names
   * @param ctx      column context for type validation
   * @throws IllegalArgumentException if any validation rule is violated
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void validate(CodecPipeline pipeline, CodecRegistry registry, CodecContext ctx) {
    List<CodecInvocation> stages = pipeline.stages();

    int transformCount = 0;
    int compressionCount = 0;
    int compressionIndex = -1;

    for (int i = 0; i < stages.size(); i++) {
      CodecInvocation invocation = stages.get(i);
      ChunkCodecHandler codec = registry.getOrThrow(invocation.name());
      CodecOptions options = codec.parseOptions(invocation.args());
      codec.validateContext(options, ctx);

      if (codec.kind() == CodecKind.TRANSFORM) {
        transformCount++;
      } else if (codec.kind() == CodecKind.COMPRESSION) {
        compressionCount++;
        compressionIndex = i;
      }
    }

    if (transformCount > 1) {
      throw new IllegalArgumentException(
          "A codec pipeline may contain at most one transform stage, but found " + transformCount
              + " in: " + pipeline.toDslString());
    }

    if (compressionCount > 1) {
      throw new IllegalArgumentException(
          "A codec pipeline may contain at most one compression stage, but found " + compressionCount
              + " in: " + pipeline.toDslString());
    }

    if (compressionIndex >= 0 && compressionIndex != stages.size() - 1) {
      throw new IllegalArgumentException(
          "The compression stage must be last in the pipeline, but it is at position " + compressionIndex
              + " in: " + pipeline.toDslString());
    }
  }
}
