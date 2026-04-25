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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
  // Collapses any run of internal whitespace (e.g. "a  +  b") to a single space so that segment-vs-config
  // comparisons of canonicalized expressions don't disagree on whitespace alone.
  private static final Pattern INTERNAL_WS = Pattern.compile("\\s+");

  // Cache compiled pipelines so a broker/server with thousands of segments sharing the same partition expression
  // only Calcite-parses the expression once. PartitionPipeline is stateless except for per-thread scratch arrays in
  // its inner ExecutableNode tree, so the cached instance is safe to share across multiple PartitionPipelineFunction
  // wrappers (each wrapper applies its own numPartitions at normalization time).
  //
  // Bounded by maximumSize so that long-lived processes with churning table configs (creates/drops/expression edits)
  // don't accumulate compiled pipelines indefinitely. The cap is sized for the realistic case of a handful of
  // distinct (column, expression) tuples per table × hundreds of tables.
  // expireAfterAccess provides eventual invalidation when a table is dropped or a partition expression is edited
  // (no caller currently emits explicit invalidation events). 1 hour is long enough that the cache helps, short
  // enough that stale entries from dropped tables don't pin memory indefinitely.
  private static final long PIPELINE_CACHE_MAX_SIZE = 10_000L;
  private static final long PIPELINE_CACHE_EXPIRE_HOURS = 1L;
  private static final Cache<PipelineCacheKey, PartitionPipeline> PIPELINE_CACHE = CacheBuilder.newBuilder()
      .maximumSize(PIPELINE_CACHE_MAX_SIZE)
      .expireAfterAccess(PIPELINE_CACHE_EXPIRE_HOURS, TimeUnit.HOURS)
      .build();

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
      // Tolerate multiple implementations (common in shaded-jar / test classpaths where pinot-common appears via more
      // than one route). Prefer the built-in InbuiltPartitionEvaluatorFactory when present; otherwise pick the first
      // and log the choice. Hard-failing here would abort every segment load that reaches the compiler.
      if (factories.size() == 1) {
        return factories.get(0);
      }
      for (PartitionEvaluatorFactory f : factories) {
        if (f.getClass().getSimpleName().equals("InbuiltPartitionEvaluatorFactory")) {
          return f;
        }
      }
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
    PipelineCacheKey cacheKey = new PipelineCacheKey(rawColumn, isBytesInput, canonicalExpr, partitionIdNormalizer);
    try {
      return PIPELINE_CACHE.get(cacheKey, () -> {
        FunctionEvaluator evaluator = EvaluatorFactoryHolder.INSTANCE.compile(rawColumn, canonicalExpr);
        return new PartitionPipeline(rawColumn, isBytesInput, canonicalExpr, partitionIdNormalizer, evaluator);
      });
    } catch (UncheckedExecutionException e) {
      // Loader threw a RuntimeException (e.g. invalid expression). Unwrap so callers see the original failure.
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IllegalStateException("Failed to compile partition pipeline for column '" + rawColumn + "'", cause);
    } catch (ExecutionException e) {
      // Loader threw a checked exception. Unwrap to surface the original cause.
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IllegalStateException("Failed to compile partition pipeline for column '" + rawColumn + "'", cause);
    }
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
    String afterComma = COMMA_WS.matcher(afterClose).replaceAll(", ");
    return INTERNAL_WS.matcher(afterComma).replaceAll(" ");
  }

  private static final class PipelineCacheKey {
    private final String _rawColumn;
    private final boolean _isBytesInput;
    private final String _canonicalExpr;
    private final String _normalizerName;

    PipelineCacheKey(String rawColumn, boolean isBytesInput, String canonicalExpr,
        @Nullable PartitionIntNormalizer normalizer) {
      _rawColumn = rawColumn;
      _isBytesInput = isBytesInput;
      _canonicalExpr = canonicalExpr;
      _normalizerName = normalizer != null ? normalizer.name() : null;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof PipelineCacheKey)) {
        return false;
      }
      PipelineCacheKey other = (PipelineCacheKey) obj;
      return _isBytesInput == other._isBytesInput
          && _rawColumn.equals(other._rawColumn)
          && _canonicalExpr.equals(other._canonicalExpr)
          && Objects.equals(_normalizerName, other._normalizerName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_rawColumn, _isBytesInput, _canonicalExpr, _normalizerName);
    }
  }
}
