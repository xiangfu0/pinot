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
import java.math.BigInteger;
import java.util.List;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link PartitionFunction} adapter for expression-mode partition pipelines.
 *
 * <p><b>Note on {@code Serializable}:</b> {@link PartitionFunction} extends {@link java.io.Serializable} for
 * historical reasons, but partition functions are never Java-serialized in Pinot's runtime.
 */
@SuppressWarnings("serial")
public class PartitionPipelineFunction implements PartitionFunction, FunctionEvaluator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionPipelineFunction.class);
  public static final String NAME = "FunctionExpr";
  // Sentinel returned when the partition expression evaluates to null for a given value (e.g. null input).
  // Callers that update per-segment partition sets must skip this value.
  public static final int NULL_RESULT_PARTITION_ID = -1;

  private final PartitionPipeline _pipeline;
  private final int _numPartitions;

  public PartitionPipelineFunction(PartitionPipeline pipeline, int numPartitions) {
    Preconditions.checkNotNull(pipeline, "Partition pipeline must be configured");
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
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
    if (_pipeline.isBytesInput()) {
      return getPartition(BytesUtils.toBytes(value));
    }
    return normalizeResult(_pipeline.evaluate(new Object[]{value}));
  }

  /**
   * Overrides the default bytes partition to pass raw bytes directly through the pipeline when this pipeline was
   * compiled with {@link PartitionValueType#BYTES} input type, avoiding the hex-encoding round-trip.
   */
  @Override
  public int getPartition(byte[] bytes) {
    if (!_pipeline.isBytesInput()) {
      return getPartition(BytesUtils.toHexString(bytes));
    }
    return normalizeResult(_pipeline.evaluate(new Object[]{bytes}));
  }

  private int normalizeResult(Object result) {
    if (result == null) {
      LOGGER.debug("Partition expression for column '{}' returned null; skipping partition assignment for this value",
          _pipeline.getRawColumn());
      return NULL_RESULT_PARTITION_ID;
    }
    Preconditions.checkState(result instanceof Number,
        "Partition expression for column '%s' must return a numeric value, got: %s",
        _pipeline.getRawColumn(), result.getClass().getSimpleName());
    Number num = (Number) result;
    Preconditions.checkState(!(num instanceof Float) && !(num instanceof Double),
        "Partition expression for column '%s' must return an integral value (int/long), got: %s (%s)",
        _pipeline.getRawColumn(), result, result.getClass().getSimpleName());
    PartitionIntNormalizer intNormalizer = _pipeline.getIntNormalizer();
    Preconditions.checkState(intNormalizer != null,
        "Integral-output partition pipeline for column '%s' must have an INT normalizer",
        _pipeline.getRawColumn());
    if (num instanceof Long || num instanceof BigInteger) {
      return intNormalizer.getPartitionId(num.longValue(), _numPartitions);
    }
    return intNormalizer.getPartitionId(num.intValue(), _numPartitions);
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
    return _pipeline.getArguments();
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    Object inputValue = genericRow.getValue(_pipeline.getRawColumn());
    if (inputValue == null) {
      return null;
    }
    if (inputValue instanceof byte[] && _pipeline.isBytesInput()) {
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
    if (inputValue instanceof byte[] && _pipeline.isBytesInput()) {
      return getPartition((byte[]) inputValue);
    }
    return getPartition(FieldSpec.getStringValue(inputValue));
  }

  @Override
  public String toString() {
    return NAME;
  }
}
