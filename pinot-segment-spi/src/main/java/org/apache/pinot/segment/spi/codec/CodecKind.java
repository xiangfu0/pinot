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

/**
 * Classification of a codec within a pipeline.
 *
 * <p>A pipeline may contain at most one {@link #COMPRESSION} stage, which must be last.
 * At most one {@link #TRANSFORM} stage is currently supported and must precede the compression stage.
 */
public enum CodecKind {
  /** Reversible transformation that does not change data size significantly (e.g. DELTA). */
  TRANSFORM,
  /** Byte-level compression (e.g. ZSTD). Must be the last stage in a pipeline. */
  COMPRESSION
}
