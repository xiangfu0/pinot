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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionFunctionExprCompiler;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionValueType;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;


/**
 * Factory to build instances of {@link PartitionFunction}.
 */
public class PartitionFunctionFactory {
  // Enum for various partition functions to be added.
  public enum PartitionFunctionType {
    Modulo, Murmur, Murmur2, Murmur3, Fnv, ByteArray, HashCode, BoundedColumnValue;
    // Add more functions here.

    private static final Map<String, PartitionFunctionType> VALUE_MAP = new HashMap<>();

    static {
      for (PartitionFunctionType functionType : PartitionFunctionType.values()) {
        VALUE_MAP.put(functionType.name().toLowerCase(), functionType);
      }
    }

    public static PartitionFunctionType fromString(String name) {
      PartitionFunctionType functionType = VALUE_MAP.get(name.toLowerCase());

      if (functionType == null) {
        throw new IllegalArgumentException("No enum constant for: " + name);
      }
      return functionType;
    }
  }

  /**
   * Private constructor so that the class cannot be instantiated.
   */
  private PartitionFunctionFactory() {
  }

  /**
   * This method generates and returns a partition function based on the provided string.
   *
   * @param functionName Name of partition function
   * @param numPartitions Number of partitions.
   * @param functionConfig The function configuration for given function.
   * @return Partition function
   */
  // TODO: introduce a way to inject custom partition function
  // a custom partition function could be used in the realtime stream partitioning or offline segment partitioning.
  // The PartitionFunctionFactory should be able to support these default implementations, as well as instantiate
  // based on config
  public static PartitionFunction getPartitionFunction(String functionName, int numPartitions,
      @Nullable Map<String, String> functionConfig) {
    PartitionFunctionType function = PartitionFunctionType.fromString(functionName);
    switch (function) {
      case Modulo:
        return new ModuloPartitionFunction(numPartitions);

      case Murmur:
      case Murmur2:
        return new MurmurPartitionFunction(numPartitions, functionConfig);

      case Murmur3:
        return new Murmur3PartitionFunction(numPartitions, functionConfig);

      case Fnv:
        return new FnvPartitionFunction(numPartitions, functionConfig);

      case ByteArray:
        return new ByteArrayPartitionFunction(numPartitions);

      case HashCode:
        return new HashCodePartitionFunction(numPartitions);

      case BoundedColumnValue:
        return new BoundedColumnValuePartitionFunction(numPartitions, functionConfig);

      default:
        throw new IllegalArgumentException("Illegal partition function name: " + functionName);
    }
  }

  /**
   * Returns the legacy (name-mode) partition function for the given config.
   *
   * @deprecated Expression-mode configs require a column name to compile the pipeline. This overload throws on
   *     expression-mode configs; prefer {@link #getPartitionFunction(String, ColumnPartitionConfig)} or
   *     {@link #getPartitionFunction(String, ColumnPartitionConfig, FieldSpec)} which support both modes.
   *     TODO: remove after release 1.7.0.
   * @throws IllegalArgumentException if {@code config.getFunctionExpr()} is non-null.
   */
  @Deprecated
  public static PartitionFunction getPartitionFunction(ColumnPartitionConfig config) {
    Preconditions.checkNotNull(config, "Column partition config must be configured");
    Preconditions.checkArgument(config.getFunctionExpr() == null,
        "Expression-mode config requires a column name; use getPartitionFunction(String, ColumnPartitionConfig)");
    return getPartitionFunction(config.getFunctionName(), config.getNumPartitions(), config.getFunctionConfig());
  }

  /**
   * Returns the legacy (name-mode) partition function for the given segment metadata.
   *
   * @deprecated Expression-mode metadata requires a column name to compile the pipeline. This overload throws on
   *     expression-mode metadata; prefer {@link #getPartitionFunction(String, ColumnPartitionMetadata)}.
   *     TODO: remove after release 1.7.0.
   * @throws IllegalArgumentException if {@code metadata.getFunctionExpr()} is non-null.
   */
  @Deprecated
  public static PartitionFunction getPartitionFunction(ColumnPartitionMetadata metadata) {
    Preconditions.checkNotNull(metadata, "Column partition metadata must be configured");
    Preconditions.checkArgument(metadata.getFunctionExpr() == null,
        "Expression-mode metadata requires a column name; use getPartitionFunction(String, ColumnPartitionMetadata)");
    return getPartitionFunction(metadata.getFunctionName(), metadata.getNumPartitions(), metadata.getFunctionConfig());
  }

  public static PartitionFunction getPartitionFunction(String columnName, ColumnPartitionMetadata metadata) {
    Preconditions.checkNotNull(metadata, "Column partition metadata must be configured");
    if (metadata.getFunctionExpr() != null && metadata.getInputType() != null) {
      PartitionValueType inputType = PartitionValueType.valueOf(metadata.getInputType());
      return PartitionFunctionExprCompiler.compilePartitionFunction(columnName, inputType, metadata.getFunctionExpr(),
          metadata.getNumPartitions(), metadata.getPartitionIdNormalizer());
    }
    return getPartitionFunction(columnName, metadata.getFunctionName(), metadata.getNumPartitions(),
        metadata.getFunctionConfig(), metadata.getFunctionExpr(), metadata.getPartitionIdNormalizer());
  }

  public static PartitionFunction getPartitionFunction(String columnName, ColumnPartitionConfig columnPartitionConfig) {
    return getPartitionFunction(columnName, columnPartitionConfig, columnPartitionConfig.getNumPartitions());
  }

  /**
   * Builds a partition function for the given column with an explicit numPartitions override.
   *
   * @deprecated For BYTES-typed partition columns this overload always compiles the expression pipeline with STRING
   * input, producing partition ids that disagree with ingestion. Prefer
   * {@link #getPartitionFunction(String, ColumnPartitionConfig, int, FieldSpec)} (the FieldSpec-aware 4-arg form).
   * TODO: remove after release 1.7.0.
   */
  @Deprecated
  public static PartitionFunction getPartitionFunction(String columnName, ColumnPartitionConfig columnPartitionConfig,
      int numPartitions) {
    Preconditions.checkNotNull(columnPartitionConfig, "Column partition config must be configured");
    return getPartitionFunction(columnName, columnPartitionConfig.getFunctionName(), numPartitions,
        columnPartitionConfig.getFunctionConfig(), columnPartitionConfig.getFunctionExpr(),
        columnPartitionConfig.getPartitionIdNormalizer());
  }

  public static PartitionFunction getPartitionFunction(String columnName, @Nullable String functionName,
      int numPartitions, @Nullable Map<String, String> functionConfig, @Nullable String functionExpr) {
    return getPartitionFunction(columnName, functionName, numPartitions, functionConfig, functionExpr, null);
  }

  /**
   * Builds a partition function for expression mode using an explicit input type.
   *
   * <p>Use {@link PartitionValueType#BYTES} when the partition column stores raw byte arrays so that functions in the
   * expression receive the original bytes directly rather than a hex-encoded string representation.
   */
  public static PartitionFunction getPartitionFunction(String columnName, ColumnPartitionConfig config,
      PartitionValueType inputType) {
    Preconditions.checkNotNull(config, "Column partition config must be configured");
    Preconditions.checkArgument(config.getFunctionExpr() != null,
        "inputType overload is only valid for expression-mode configs (functionExpr must be set)");
    return PartitionFunctionExprCompiler.compilePartitionFunction(columnName, inputType, config.getFunctionExpr(),
        config.getNumPartitions(), config.getPartitionIdNormalizer());
  }

  /**
   * Builds a partition function using the schema field spec to determine the correct input type for expression-mode
   * partition functions on BYTES-typed columns.
   *
   * <p>When {@code fieldSpec} is non-null and the stored type is {@link FieldSpec.DataType#BYTES}, the expression is
   * compiled with {@link PartitionValueType#BYTES} input so that scalar functions receive raw byte arrays rather than
   * hex-encoded strings.  For all other cases the default STRING input type is used.
   */
  public static PartitionFunction getPartitionFunction(String columnName, ColumnPartitionConfig config,
      @Nullable FieldSpec fieldSpec) {
    return getPartitionFunction(columnName, config, config.getNumPartitions(), fieldSpec);
  }

  /**
   * Builds a partition function with an explicit {@code numPartitions} override and uses {@code fieldSpec} to
   * determine the correct input type for expression-mode partition functions on BYTES-typed columns.
   *
   * <p>Use this overload when the live partition count (e.g. the stream partition count) may differ from the value
   * stored in the table config, so that the built function uses the authoritative count while still receiving the
   * correct input type for BYTES columns.
   */
  public static PartitionFunction getPartitionFunction(String columnName, ColumnPartitionConfig config,
      int numPartitions, @Nullable FieldSpec fieldSpec) {
    if (config.getFunctionExpr() != null && fieldSpec != null
        && fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.BYTES) {
      return PartitionFunctionExprCompiler.compilePartitionFunction(columnName, PartitionValueType.BYTES,
          config.getFunctionExpr(), numPartitions, config.getPartitionIdNormalizer());
    }
    return getPartitionFunction(columnName, config.getFunctionName(), numPartitions, config.getFunctionConfig(),
        config.getFunctionExpr(), config.getPartitionIdNormalizer());
  }

  public static PartitionFunction getPartitionFunction(String columnName, @Nullable String functionName,
      int numPartitions, @Nullable Map<String, String> functionConfig, @Nullable String functionExpr,
      @Nullable String partitionIdNormalizer) {
    if (functionExpr != null) {
      return PartitionFunctionExprCompiler
          .compilePartitionFunction(columnName, functionExpr, numPartitions, partitionIdNormalizer);
    }
    Preconditions.checkArgument(functionName != null, "Partition function name must be configured");
    PartitionFunction partitionFunction = getPartitionFunction(functionName, numPartitions, functionConfig);
    if (partitionIdNormalizer != null) {
      String effectivePartitionIdNormalizer = partitionFunction.getPartitionIdNormalizer();
      Preconditions.checkArgument(effectivePartitionIdNormalizer != null
              && effectivePartitionIdNormalizer.equalsIgnoreCase(partitionIdNormalizer),
          "'partitionIdNormalizer'=%s is incompatible with legacy partition function '%s'; expected '%s'",
          partitionIdNormalizer, functionName, effectivePartitionIdNormalizer);
    }
    return new ColumnBoundPartitionFunction(columnName, partitionFunction);
  }

  // PartitionFunction extends Serializable for historical reasons; partition functions are never
  // Java-serialized in Pinot's runtime. Suppress the warning to avoid noise.
  @SuppressWarnings("serial")
  private static final class ColumnBoundPartitionFunction implements PartitionFunction, FunctionEvaluator {
    private final String _columnName;
    private final PartitionFunction _delegate;

    private ColumnBoundPartitionFunction(String columnName, PartitionFunction delegate) {
      _columnName = Preconditions.checkNotNull(columnName, "Partition column must be configured");
      _delegate = Preconditions.checkNotNull(delegate, "Delegate partition function must be configured");
    }

    @Override
    public int getPartition(String value) {
      return _delegate.getPartition(value);
    }

    @Override
    public String getName() {
      return _delegate.getName();
    }

    @Override
    public int getNumPartitions() {
      return _delegate.getNumPartitions();
    }

    @Override
    public Map<String, String> getFunctionConfig() {
      return _delegate.getFunctionConfig();
    }

    @Override
    public String getPartitionColumn() {
      return _columnName;
    }

    @Override
    public String getFunctionExpr() {
      return _delegate.getFunctionExpr();
    }

    @Override
    public String getPartitionIdNormalizer() {
      return _delegate.getPartitionIdNormalizer();
    }

    @Override
    public List<String> getArguments() {
      return Collections.singletonList(_columnName);
    }

    @Override
    public Object evaluate(GenericRow genericRow) {
      Object value = genericRow.getValue(_columnName);
      return value != null ? getPartition(FieldSpec.getStringValue(value)) : null;
    }

    @Override
    public Object evaluate(Object[] values) {
      Preconditions.checkArgument(values.length == 1,
          "Partition function for column '%s' expects exactly 1 positional argument, got: %s", _columnName,
          values.length);
      return values[0] != null ? getPartition(FieldSpec.getStringValue(values[0])) : null;
    }

    @Override
    public String toString() {
      return _delegate.toString();
    }
  }
}
