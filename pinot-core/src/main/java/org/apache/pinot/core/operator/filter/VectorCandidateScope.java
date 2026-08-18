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
package org.apache.pinot.core.operator.filter;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Immutable query-scoped constraint that must be applied before vector candidate selection.
///
/// The factories own detached copies of their source bitmaps. Operators can safely share this scope and only allocate
/// another bitmap when they need to intersect it with an independent optional filter.
public final class VectorCandidateScope {
  @Nullable
  private final ImmutableRoaringBitmap _upsertDocIds;
  @Nullable
  private final ImmutableRoaringBitmap _publishedDocIds;
  private final ImmutableRoaringBitmap _requiredDocIds;
  private final String _fallbackReason;

  private VectorCandidateScope(@Nullable ImmutableRoaringBitmap upsertDocIds,
      @Nullable ImmutableRoaringBitmap publishedDocIds, String fallbackReason) {
    Preconditions.checkArgument(upsertDocIds != null || publishedDocIds != null,
        "At least one required candidate source must be provided");
    _upsertDocIds = immutableCopy(upsertDocIds);
    _publishedDocIds = immutableCopy(publishedDocIds);
    if (_upsertDocIds != null && _publishedDocIds != null) {
      MutableRoaringBitmap requiredDocIds = _upsertDocIds.toMutableRoaringBitmap();
      requiredDocIds.and(_publishedDocIds);
      _requiredDocIds = immutableCopy(requiredDocIds);
    } else {
      _requiredDocIds = _upsertDocIds != null ? _upsertDocIds : _publishedDocIds;
    }
    _fallbackReason = Preconditions.checkNotNull(fallbackReason, "Fallback reason must not be null");
  }

  /// Creates a mandatory candidate scope from a detached FULL-upsert valid-document snapshot.
  public static VectorCandidateScope forUpsertSnapshot(ImmutableRoaringBitmap requiredDocIds,
      String fallbackReason) {
    return new VectorCandidateScope(
        Preconditions.checkNotNull(requiredDocIds, "Required doc IDs must not be null"), null, fallbackReason);
  }

  /// Creates a mandatory scope for a mutable segment, intersecting an optional upsert snapshot with the range of rows
  /// that the segment has published to queries.
  public static VectorCandidateScope forMutableSegment(@Nullable ImmutableRoaringBitmap upsertDocIds,
      int numPublishedDocs, String fallbackReason) {
    Preconditions.checkArgument(numPublishedDocs >= 0, "Number of published documents must not be negative");
    MutableRoaringBitmap publishedDocIds = new MutableRoaringBitmap();
    publishedDocIds.add(0L, numPublishedDocs);
    return new VectorCandidateScope(upsertDocIds, publishedDocIds, fallbackReason);
  }

  public ImmutableRoaringBitmap getRequiredDocIds() {
    return _requiredDocIds;
  }

  @Nullable
  public ImmutableRoaringBitmap getUpsertDocIds() {
    return _upsertDocIds;
  }

  @Nullable
  public ImmutableRoaringBitmap getPublishedDocIds() {
    return _publishedDocIds;
  }

  public String getFallbackReason() {
    return _fallbackReason;
  }

  @Nullable
  private static ImmutableRoaringBitmap immutableCopy(@Nullable ImmutableRoaringBitmap docIds) {
    if (docIds == null) {
      return null;
    }
    byte[] serializedDocIds = new byte[docIds.serializedSizeInBytes()];
    docIds.serialize(ByteBuffer.wrap(serializedDocIds));
    return new ImmutableRoaringBitmap(ByteBuffer.wrap(serializedDocIds).asReadOnlyBuffer());
  }
}
