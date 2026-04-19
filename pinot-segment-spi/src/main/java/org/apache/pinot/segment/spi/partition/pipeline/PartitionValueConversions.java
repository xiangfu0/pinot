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
import java.util.EnumSet;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Static type-conversion utilities shared by all implementations of {@link PartitionScalarFunctionResolver}.
 *
 * <p>These methods encode which source/target type pairs are allowed during partition-expression binding, what the
 * conversion costs are (lower = preferred during overload resolution), and how to perform the actual Java-type
 * conversion at binding time.
 */
public final class PartitionValueConversions {
  // Pre-computed widening sets used during overload resolution. Kept as constants to avoid per-call allocation.
  private static final Set<PartitionValueType> INT_WIDENING_TARGETS =
      EnumSet.of(PartitionValueType.LONG, PartitionValueType.FLOAT, PartitionValueType.DOUBLE);
  private static final Set<PartitionValueType> LONG_WIDENING_TARGETS = EnumSet.of(PartitionValueType.DOUBLE);
  private static final Set<PartitionValueType> FLOAT_WIDENING_TARGETS = EnumSet.of(PartitionValueType.DOUBLE);

  private PartitionValueConversions() {
  }

  /**
   * Returns the overload-resolution cost for converting a dynamic pipeline value of {@code sourceType} to
   * {@code targetType}, or {@code -1} if the conversion is not allowed.
   */
  public static int getDynamicConversionCost(PartitionValueType sourceType, Class<?> targetType) {
    PartitionValueType targetValueType = PartitionValueType.fromJavaType(targetType);
    if (sourceType == targetValueType) {
      return 0;
    }
    switch (sourceType) {
      case STRING:
        if (targetValueType == PartitionValueType.BYTES || targetValueType.isNumeric()) {
          return 1;
        }
        return -1;
      case BYTES:
        return -1;
      case INT:
        return wideningNumericCost(targetValueType, INT_WIDENING_TARGETS);
      case LONG:
        return wideningNumericCost(targetValueType, LONG_WIDENING_TARGETS);
      case FLOAT:
        return wideningNumericCost(targetValueType, FLOAT_WIDENING_TARGETS);
      case DOUBLE:
      default:
        return -1;
    }
  }

  /**
   * Returns the overload-resolution cost for passing a compile-time constant {@code value} as {@code targetType}, or
   * {@code -1} if the conversion is not allowed.
   */
  public static int getConstantConversionCost(PartitionValue value, Class<?> targetType) {
    PartitionValueType targetValueType = PartitionValueType.fromJavaType(targetType);
    PartitionValueType sourceType = value.getType();
    if (sourceType == targetValueType) {
      return 0;
    }
    switch (sourceType) {
      case STRING:
        if (targetValueType == PartitionValueType.BYTES || targetValueType.isNumeric()) {
          return 1;
        }
        return -1;
      case LONG:
        if (targetValueType == PartitionValueType.INT) {
          long longValue = value.getLongValue();
          return longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE ? 2 : -1;
        }
        return getDynamicConversionCost(sourceType, targetType);
      case DOUBLE:
        if (targetValueType == PartitionValueType.INT) {
          double doubleValue = value.getDoubleValue();
          return doubleValue >= Integer.MIN_VALUE && doubleValue <= Integer.MAX_VALUE
              && doubleValue == Math.rint(doubleValue) ? 2 : -1;
        }
        if (targetValueType == PartitionValueType.LONG) {
          double doubleValue = value.getDoubleValue();
          return doubleValue >= Long.MIN_VALUE && doubleValue <= Long.MAX_VALUE
              && doubleValue == Math.rint(doubleValue) ? 2 : -1;
        }
        return getDynamicConversionCost(sourceType, targetType);
      default:
        return getDynamicConversionCost(sourceType, targetType);
    }
  }

  /**
   * Converts {@code value} to the Java type required by {@code targetType}.
   *
   * @param constant {@code true} when converting a compile-time literal; allows narrowing conversions (e.g. LONG →
   *                 INT) that are rejected for dynamic runtime values.
   */
  public static Object convertValue(PartitionValue value, Class<?> targetType, boolean constant) {
    PartitionValueType targetValueType = PartitionValueType.fromJavaType(targetType);
    switch (targetValueType) {
      case STRING:
        Preconditions.checkArgument(value.getType() == PartitionValueType.STRING,
            "Expected STRING argument but got %s", value.getType());
        return value.getStringValue();
      case BYTES:
        if (value.getType() == PartitionValueType.BYTES) {
          return value.getBytesValue();
        }
        Preconditions.checkArgument(value.getType() == PartitionValueType.STRING,
            "Expected STRING or BYTES argument for BYTES parameter but got %s", value.getType());
        return value.getStringValue().getBytes(UTF_8);
      case INT:
        switch (value.getType()) {
          case INT:
            return value.getIntValue();
          case LONG:
            Preconditions.checkArgument(constant, "Cannot narrow LONG pipeline value to INT dynamically");
            return Math.toIntExact(value.getLongValue());
          case DOUBLE:
            Preconditions.checkArgument(constant, "Cannot narrow DOUBLE pipeline value to INT dynamically");
            return (int) value.getDoubleValue();
          case STRING:
            return Integer.parseInt(value.getStringValue());
          default:
            throw new IllegalArgumentException(
                String.format("Cannot convert %s partition value to INT", value.getType()));
        }
      case LONG:
        switch (value.getType()) {
          case INT:
            return (long) value.getIntValue();
          case LONG:
            return value.getLongValue();
          case DOUBLE:
            Preconditions.checkArgument(constant, "Cannot narrow DOUBLE pipeline value to LONG dynamically");
            return (long) value.getDoubleValue();
          case STRING:
            return Long.parseLong(value.getStringValue());
          default:
            throw new IllegalArgumentException(
                String.format("Cannot convert %s partition value to LONG", value.getType()));
        }
      case FLOAT:
        switch (value.getType()) {
          case INT:
            return (float) value.getIntValue();
          case LONG:
            return (float) value.getLongValue();
          case FLOAT:
            return value.getFloatValue();
          case STRING:
            return Float.parseFloat(value.getStringValue());
          default:
            throw new IllegalArgumentException(
                String.format("Cannot convert %s partition value to FLOAT", value.getType()));
        }
      case DOUBLE:
        switch (value.getType()) {
          case INT:
            return (double) value.getIntValue();
          case LONG:
            return (double) value.getLongValue();
          case FLOAT:
            return (double) value.getFloatValue();
          case DOUBLE:
            return value.getDoubleValue();
          case STRING:
            return Double.parseDouble(value.getStringValue());
          default:
            throw new IllegalArgumentException(
                String.format("Cannot convert %s partition value to DOUBLE", value.getType()));
        }
      default:
        throw new IllegalStateException("Unsupported partition parameter type: " + targetType);
    }
  }

  private static int wideningNumericCost(PartitionValueType targetType, Set<PartitionValueType> supportedTypes) {
    return supportedTypes.contains(targetType) ? 1 : -1;
  }
}
