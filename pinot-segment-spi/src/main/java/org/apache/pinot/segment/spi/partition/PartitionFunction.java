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
package org.apache.pinot.segment.spi.partition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * Interface for partition function.
 *
 * <p>Implementations of this interface are assumed not to be stateful.
 * That is, two invocations of {@code PartitionFunction.getPartition(value)}
 * with the same value are expected to produce the same result. Implementations must also be safe for concurrent
 * invocation by multiple threads.
 *
 * <p><b>Expression-mode partition functions:</b> When {@link #getFunctionExpr()} returns non-null, the implementation
 * is operating in expression mode (e.g. {@code PartitionPipelineFunction}). In that case
 * {@link #getPartitionIdNormalizer()} and {@link #getPartitionColumn()} also typically return non-null. Existing
 * legacy partition functions ({@code Murmur}, {@code Modulo}, {@code HashCode}, etc.) return {@code null} from these
 * accessors and continue to operate as before. Framework callers (segment writers, broker pruners, staleness checks)
 * use the non-null/null distinction on {@code getFunctionExpr} to dispatch between the two modes — plugins that
 * want to be treated as expression-mode must override the relevant accessors.
 */
public interface PartitionFunction extends Serializable {

  /**
   * Method to compute and return partition id for the given value.
   * NOTE: The value is expected to be a string representation of the actual value.
   *
   * <p><b>Return-value contract:</b> implementations must return a non-negative partition id in
   * {@code [0, getNumPartitions())}. The value {@code -1} is reserved as a framework-internal sentinel for
   * "expression evaluated to null" (see
   * {@link org.apache.pinot.segment.spi.partition.pipeline.PartitionPipelineFunction#NULL_RESULT_PARTITION_ID}) —
   * custom plugin implementations must not return {@code -1} as a real partition id. Internal callers
   * (broker pruner, stats collector, segment processing partitioner) treat {@code -1} as "skip / no partition".
   *
   * @param value Value for which to determine the partition id.
   * @return partition id for the value (non-negative for real partitions; never {@code -1}).
   */
  int getPartition(String value);

  /**
   * Returns the partition id for a raw byte array value.
   *
   * <p>The default implementation converts the bytes to a hex string and delegates to {@link #getPartition(String)},
   * matching the historical behaviour for BYTES columns. Expression-mode pipelines that were compiled with
   * {@link org.apache.pinot.segment.spi.partition.pipeline.PartitionValueType#BYTES} input type override this method
   * to hash the raw bytes directly without the hex-encoding round-trip.
   *
   * @param bytes Raw byte array value.
   * @return partition id for the value.
   */
  default int getPartition(byte[] bytes) {
    return getPartition(BytesUtils.toHexString(bytes));
  }

  /**
   * Returns the name of the partition function.
   * @return Name of the partition function.
   */
  String getName();

  /**
   * Returns the total number of possible partitions.
   * @return Number of possible partitions.
   */
  int getNumPartitions();

  @Nullable
  default Map<String, String> getFunctionConfig() {
    return null;
  }

  @JsonIgnore
  @Nullable
  default String getPartitionColumn() {
    return null;
  }

  @JsonIgnore
  @Nullable
  default String getFunctionExpr() {
    return null;
  }

  @JsonIgnore
  @Nullable
  default String getPartitionIdNormalizer() {
    return null;
  }
}
