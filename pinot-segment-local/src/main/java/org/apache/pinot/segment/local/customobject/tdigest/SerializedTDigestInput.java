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
package org.apache.pinot.segment.local.customobject.tdigest;

import java.nio.ByteBuffer;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestCodec.SerializedTDigestMetadata;

/// Reusable, invocation-local view of one serialized TDigest for group-by-MV fanout.
///
/// [#reset(byte\[\])] validates the header once per input row. The first decode combines numerical validation
/// with filling reusable centroid arrays, so all groups for a row merge the same primitive input without sharing
/// mutable accumulator state. A pending accumulator requests an immutable byte snapshot, created at most once per
/// reset
/// and shared by the row's group fanout. The input must remain thread-confined and must not outlive the aggregation
/// call that owns it. Pending-only inputs inspect their bytes without allocating centroid arrays; their first
/// later materialization reads the already validated values without another numerical validation pass.
public final class SerializedTDigestInput {
  byte[] _bytes;
  byte[] _retainedBytes;
  int _numCentroids;
  double[] _means;
  double[] _weights;
  private boolean _decoded;
  private boolean _metadataInspected;

  SerializedTDigestMetadata _metadata;

  public void reset(byte[] bytes) {
    reset(bytes, TDigestCodec.readSerializedHeader(ByteBuffer.wrap(bytes)));
  }

  /// Owns one snapshot of the encoded digest and advances the source buffer past it.
  public void reset(ByteBuffer bytes) {
    reset(bytes, true);
  }

  /// Reads a source digest, optionally enforcing the historical decoder's declared centroid capacity.
  public void reset(ByteBuffer bytes, boolean checkCapacity) {
    SerializedTDigestMetadata metadata = TDigestCodec.readSerializedHeader(bytes, checkCapacity);
    byte[] snapshot = new byte[metadata.encodedLength()];
    bytes.get(snapshot);
    reset(snapshot, metadata);
    _retainedBytes = snapshot;
  }

  void reset(byte[] bytes, SerializedTDigestMetadata metadata) {
    _metadata = metadata;
    _bytes = bytes;
    _retainedBytes = null;
    _numCentroids = metadata.centroidCount();
    _decoded = false;
    _metadataInspected = !Double.isNaN(metadata.totalWeight());
  }

  public double getCompression() {
    return _metadata.compression();
  }

  /// Returns validated metadata without allocating centroid arrays for a digest that will remain pending.
  public SerializedTDigestMetadata getMetadata() {
    inspectMetadata();
    return _metadata;
  }

  void inspectMetadata() {
    if (!_metadataInspected) {
      _metadata = TDigestCodec.inspectSerialized(ByteBuffer.wrap(_bytes), _metadata, null, null);
      _metadataInspected = true;
    }
  }

  /// Propagates endpoint provenance cached during the one numerical inspection of this serialized distribution.
  /// The encoded endpoint tuple remains valid when a previous fanout decode has split boundary centroids.
  boolean recordHistoricalFractionalBoundaries(PercentileTDigestAccumulator target) {
    inspectMetadata();
    if (!_metadata.fractionalWeights() || _metadata.needsLegacyFallback()) {
      return false;
    }
    target.requireMutable();
    double minMean = _metadata.historicalFractionalMinMean();
    double maxMean = _metadata.historicalFractionalMaxMean();
    target.inheritHistoricalFractionalBoundaries(minMean, maxMean);
    return !Double.isNaN(minMean) || !Double.isNaN(maxMean);
  }

  byte[] retainBytes() {
    if (_retainedBytes == null) {
      _retainedBytes = _bytes.clone();
    }
    return _retainedBytes;
  }

  void decode() {
    if (_decoded) {
      return;
    }
    int encodedCentroidCount = _numCentroids;
    ensureCapacity(Math.addExact(encodedCentroidCount, 2));
    if (!_metadataInspected) {
      _metadata = TDigestCodec.inspectSerialized(ByteBuffer.wrap(_bytes), _metadata, _means, _weights);
      _metadataInspected = true;
    } else {
      // Pending state was already inspected before retaining bytes. Its later first read only materializes
      // those validated centroids; regular merged inputs compute flags and weights in the decode pass above.
      TDigestCodec.decodeSerializedCentroids(ByteBuffer.wrap(_bytes), _metadata, _means, _weights);
    }
    if (!_metadata.needsLegacyFallback()) {
      if (_metadata.hasZeroWeightCentroids()) {
        int nonZeroCount = 0;
        for (int i = 0; i < encodedCentroidCount; i++) {
          if (_weights[i] != 0.0) {
            _means[nonZeroCount] = _means[i];
            _weights[nonZeroCount++] = _weights[i];
          }
        }
        encodedCentroidCount = nonZeroCount;
      }
      if (_metadata.unorderedMeans()) {
        PercentileTDigestAccumulator.sortCentroids(_means, _weights, encodedCentroidCount);
      }
      _numCentroids = PercentileTDigestAccumulator.normalizeSerializedBoundaries(_means, _weights, encodedCentroidCount,
            _metadata.min(), _metadata.max());
    }
    _decoded = true;
  }

  private void ensureCapacity(int capacity) {
    if (capacity > 0 && (_means == null || capacity > _means.length)) {
      int newCapacity = _means == null ? capacity : Math.max(capacity, Math.multiplyExact(_means.length, 2));
      _means = new double[newCapacity];
      _weights = new double[newCapacity];
    }
  }
}
