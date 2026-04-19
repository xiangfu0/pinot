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
package org.apache.pinot.segment.spi.partition.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * {@link PartitionFunction} adapter for expression-mode partition pipelines.
 *
 * <p><b>Note on {@code Serializable}:</b> {@link PartitionFunction} extends {@link java.io.Serializable} for
 * historical reasons, but partition functions are never Java-serialized in Pinot's runtime. This class holds a
 * {@link PartitionPipeline} that extends {@link org.apache.pinot.segment.spi.function.ExecutableFunctionEvaluator},
 * which contains {@link ThreadLocal} fields that are not serializable. Do not rely on Java serialization for this
 * class.
 */
@SuppressWarnings("serial")
public class PartitionPipelineFunction implements PartitionFunction, FunctionEvaluator {
  public static final String NAME = "FunctionExpr";

  private final PartitionPipeline _pipeline;
  private final int _numPartitions;

  public PartitionPipelineFunction(PartitionPipeline pipeline, int numPartitions) {
    Preconditions.checkNotNull(pipeline, "Partition pipeline must be configured");
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    Preconditions.checkArgument(pipeline.getOutputType().isIntegral(),
        "Partition pipeline must produce INT or LONG output, got: %s", pipeline.getOutputType());
    _pipeline = pipeline;
    _numPartitions = numPartitions;
  }

  public PartitionPipeline getPartitionPipeline() {
    return _pipeline;
  }

  @Override
  public int getPartition(String value) {
    // BYTES-input pipelines expect raw bytes. When the caller provides a string (e.g. a hex-encoded predicate value
    // from broker routing), convert the hex string back to raw bytes so the partition computation matches ingestion.
    if (_pipeline.getInputType() == PartitionValueType.BYTES) {
      return getPartition(BytesUtils.toBytes(value));
    }
    PartitionValue partitionValue = _pipeline.evaluate(value);
    PartitionIntNormalizer intNormalizer = _pipeline.getIntNormalizer();
    Preconditions.checkState(intNormalizer != null, "Integral-output partition pipeline must have an INT normalizer");
    if (partitionValue.getType() == PartitionValueType.INT) {
      return intNormalizer.getPartitionId(partitionValue.getIntValue(), _numPartitions);
    }
    Preconditions.checkState(partitionValue.getType() == PartitionValueType.LONG,
        "Expected INT or LONG partition value but got: %s", partitionValue.getType());
    return intNormalizer.getPartitionId(partitionValue.getLongValue(), _numPartitions);
  }

  /**
   * Overrides the default bytes partition to pass raw bytes directly through the pipeline when this pipeline was
   * compiled with {@link PartitionValueType#BYTES} input type, avoiding the hex-encoding round-trip.
   */
  @Override
  public int getPartition(byte[] bytes) {
    if (_pipeline.getInputType() != PartitionValueType.BYTES) {
      return getPartition(BytesUtils.toHexString(bytes));
    }
    PartitionValue partitionValue = _pipeline.evaluate(PartitionValue.bytesValue(bytes));
    PartitionIntNormalizer intNormalizer = _pipeline.getIntNormalizer();
    Preconditions.checkState(intNormalizer != null, "Integral-output partition pipeline must have an INT normalizer");
    if (partitionValue.getType() == PartitionValueType.INT) {
      return intNormalizer.getPartitionId(partitionValue.getIntValue(), _numPartitions);
    }
    Preconditions.checkState(partitionValue.getType() == PartitionValueType.LONG,
        "Expected INT or LONG partition value but got: %s", partitionValue.getType());
    return intNormalizer.getPartitionId(partitionValue.getLongValue(), _numPartitions);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  @JsonIgnore
  public String getPartitionColumn() {
    return _pipeline.getRawColumn();
  }

  @Override
  @JsonIgnore(false)
  @JsonProperty("functionExpr")
  public String getFunctionExpr() {
    return _pipeline.getCanonicalFunctionExpr();
  }

  @Override
  @JsonIgnore(false)
  @JsonProperty("partitionIdNormalizer")
  public String getPartitionIdNormalizer() {
    PartitionIntNormalizer intNormalizer = _pipeline.getIntNormalizer();
    return intNormalizer != null ? intNormalizer.name() : null;
  }

  // FunctionEvaluator implementation

  @Override
  public List<String> getArguments() {
    return Collections.singletonList(_pipeline.getRawColumn());
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    Object inputValue = genericRow.getValue(_pipeline.getRawColumn());
    if (inputValue == null) {
      return null;
    }
    if (inputValue instanceof byte[] && _pipeline.getInputType() == PartitionValueType.BYTES) {
      return getPartition((byte[]) inputValue);
    }
    return getPartition(FieldSpec.getStringValue(inputValue));
  }

  @Override
  public Object evaluate(Object[] values) {
    Preconditions.checkArgument(values.length == 1,
        "Partition pipeline function for column '%s' expects exactly 1 positional argument, got: %s",
        _pipeline.getRawColumn(), values.length);
    Object inputValue = values[0];
    if (inputValue == null) {
      return null;
    }
    if (inputValue instanceof byte[] && _pipeline.getInputType() == PartitionValueType.BYTES) {
      return getPartition((byte[]) inputValue);
    }
    return getPartition(FieldSpec.getStringValue(inputValue));
  }

  @Override
  public String toString() {
    return NAME;
  }
}
