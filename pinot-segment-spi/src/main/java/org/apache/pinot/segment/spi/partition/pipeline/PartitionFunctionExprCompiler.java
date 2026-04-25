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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.function.FunctionEvaluator;


/**
 * Compiles a partition-function expression into a {@link PartitionPipeline} backed by a
 * {@link org.apache.pinot.spi.function.FunctionEvaluator} provided by {@link PartitionEvaluatorFactory}.
 */
public final class PartitionFunctionExprCompiler {
  // Cap on user-supplied expression length, sized to fit comfortably within ZK / segment metadata size budgets while
  // allowing realistic chained expressions (e.g. fnv1a_32(md5(lower(trim(col))))).
  private static final int MAX_EXPRESSION_LENGTH = 256;
  // Pre-compiled patterns for canonicalize(): avoids recompiling the regex on every compile() invocation.
  private static final Pattern OPEN_PAREN_WS = Pattern.compile("\\s*\\(\\s*");
  private static final Pattern CLOSE_PAREN_WS = Pattern.compile("\\s*\\)\\s*");
  private static final Pattern COMMA_WS = Pattern.compile("\\s*,\\s*");

  private PartitionFunctionExprCompiler() {
  }

  // Lazy holder: initialized on first use so that class-load of this utility does not fail when
  // pinot-common (which provides InbuiltPartitionEvaluatorFactory) is not yet on the classpath.
  // The JVM class-initialization guarantee makes this inherently thread-safe and single-init.
  private static final class EvaluatorFactoryHolder {
    static final PartitionEvaluatorFactory INSTANCE = loadEvaluatorFactory();

    private static PartitionEvaluatorFactory loadEvaluatorFactory() {
      List<PartitionEvaluatorFactory> factories = new ArrayList<>();
      for (PartitionEvaluatorFactory f : ServiceLoader.load(PartitionEvaluatorFactory.class)) {
        factories.add(f);
      }
      Preconditions.checkState(!factories.isEmpty(),
          "No PartitionEvaluatorFactory implementation found on the classpath");
      Preconditions.checkState(factories.size() == 1,
          "Expected exactly 1 PartitionEvaluatorFactory implementation but found %s: %s", factories.size(), factories);
      return factories.get(0);
    }
  }

  public static PartitionPipeline compile(String rawColumn, String functionExpr) {
    return compile(rawColumn, PartitionValueType.STRING, functionExpr, null);
  }

  public static PartitionPipeline compile(String rawColumn, String functionExpr,
      @Nullable PartitionIntNormalizer partitionIdNormalizer) {
    return compile(rawColumn, PartitionValueType.STRING, functionExpr, partitionIdNormalizer);
  }

  /**
   * Compiles a partition pipeline with an explicit input type.
   *
   * <p>Use {@link PartitionValueType#BYTES} when the partition column stores raw byte arrays so that functions in the
   * expression receive the original bytes directly rather than a hex-encoded string representation.
   */
  public static PartitionPipeline compile(String rawColumn, PartitionValueType inputType, String functionExpr,
      @Nullable PartitionIntNormalizer partitionIdNormalizer) {
    Preconditions.checkArgument(rawColumn != null && !rawColumn.trim().isEmpty(), "Raw column must be configured");
    Preconditions.checkArgument(inputType != null, "Input type must be configured");
    Preconditions.checkArgument(functionExpr != null && !functionExpr.trim().isEmpty(),
        "'functionExpr' must be configured");
    Preconditions.checkArgument(functionExpr.length() <= MAX_EXPRESSION_LENGTH,
        "'functionExpr' must be <= %s characters", MAX_EXPRESSION_LENGTH);

    String canonicalExpr = canonicalize(functionExpr);
    boolean isBytesInput = inputType == PartitionValueType.BYTES;
    FunctionEvaluator evaluator = EvaluatorFactoryHolder.INSTANCE.compile(rawColumn, canonicalExpr);
    return new PartitionPipeline(rawColumn, isBytesInput, canonicalExpr, partitionIdNormalizer, evaluator);
  }

  public static PartitionPipelineFunction compilePartitionFunction(String rawColumn, String functionExpr,
      int numPartitions) {
    return compilePartitionFunction(rawColumn, PartitionValueType.STRING, functionExpr, numPartitions, null);
  }

  public static PartitionPipelineFunction compilePartitionFunction(String rawColumn, String functionExpr,
      int numPartitions, @Nullable String partitionIdNormalizer) {
    return compilePartitionFunction(rawColumn, PartitionValueType.STRING, functionExpr, numPartitions,
        partitionIdNormalizer);
  }

  /**
   * Compiles a partition pipeline function with an explicit input type.
   *
   * <p>Use {@link PartitionValueType#BYTES} when the partition column stores raw byte arrays so that functions in the
   * expression receive the original bytes directly rather than a hex-encoded string representation.
   */
  public static PartitionPipelineFunction compilePartitionFunction(String rawColumn, PartitionValueType inputType,
      String functionExpr, int numPartitions, @Nullable String partitionIdNormalizer) {
    PartitionIntNormalizer normalizer = partitionIdNormalizer != null
        ? PartitionIntNormalizer.fromConfigString(partitionIdNormalizer)
        : PartitionIntNormalizer.fromConfigString(ColumnPartitionConfig.PARTITION_ID_NORMALIZER_POSITIVE_MODULO);
    PartitionPipeline pipeline = compile(rawColumn, inputType, functionExpr, normalizer);
    return new PartitionPipelineFunction(pipeline, numPartitions);
  }

  /**
   * Returns a canonical form of the expression: trimmed, lowercased, with spaces removed around
   * {@code (}, {@code )}, and {@code ,}.
   */
  static String canonicalize(String expression) {
    String lowered = expression.trim().toLowerCase(Locale.ROOT);
    String afterOpen = OPEN_PAREN_WS.matcher(lowered).replaceAll("(");
    String afterClose = CLOSE_PAREN_WS.matcher(afterOpen).replaceAll(")");
    return COMMA_WS.matcher(afterClose).replaceAll(", ");
  }
}
