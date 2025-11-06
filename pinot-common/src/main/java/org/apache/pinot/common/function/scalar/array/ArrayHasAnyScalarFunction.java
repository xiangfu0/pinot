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
package org.apache.pinot.common.function.scalar.array;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.annotations.ScalarFunction;


@ScalarFunction(names = {"ARRAYHASANY"})
public class ArrayHasAnyScalarFunction implements PinotScalarFunction {

  private static final Map<DataSchema.ColumnDataType, FunctionInfo>
      TYPE_FUNCTION_INFO_MAP = new EnumMap<>(DataSchema.ColumnDataType.class);

  private static final float HASH_SET_LOAD_FACTOR = 0.75f;

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.INT_ARRAY,
          new FunctionInfo(ArrayHasAnyScalarFunction.class.getMethod("arrayHasAny", int[].class, int[].class),
              ArrayHasAnyScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.LONG_ARRAY,
          new FunctionInfo(ArrayHasAnyScalarFunction.class.getMethod("arrayHasAny", long[].class, long[].class),
              ArrayHasAnyScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.FLOAT_ARRAY,
          new FunctionInfo(ArrayHasAnyScalarFunction.class.getMethod("arrayHasAny", float[].class, float[].class),
              ArrayHasAnyScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.DOUBLE_ARRAY,
          new FunctionInfo(ArrayHasAnyScalarFunction.class.getMethod("arrayHasAny", double[].class, double[].class),
              ArrayHasAnyScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.STRING_ARRAY,
          new FunctionInfo(ArrayHasAnyScalarFunction.class.getMethod("arrayHasAny", String[].class, String[].class),
              ArrayHasAnyScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "ARRAYHASANY";
  }

  @Override
  public Set<String> getNames() {
    return Set.of("ARRAYHASANY");
  }

  @Nullable
  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    // Should already be registered in PinotOperatorTable by the transform function implementation
    return null;
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(DataSchema.ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 2) {
      return null;
    }
    if (argumentTypes[0] != argumentTypes[1]) {
      return null;
    }
    return TYPE_FUNCTION_INFO_MAP.get(argumentTypes[0]);
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    if (numArguments != 2) {
      return null;
    }
    // Fall back to string
    return getFunctionInfo(new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING_ARRAY,
        DataSchema.ColumnDataType.STRING_ARRAY
    });
  }

  private static boolean hasOverlap(int lenA, int lenB, Runnable buildFromA, BooleanSupplier checkB,
      Runnable buildFromB, BooleanSupplier checkA) {
    if (lenA <= lenB) {
      buildFromA.run();
      return checkB.getAsBoolean();
    } else {
      buildFromB.run();
      return checkA.getAsBoolean();
    }
  }

  private static int capacityForSize(int size) {
    // Avoid HashSet rehashing under default load factor
    return (int) ((size / HASH_SET_LOAD_FACTOR) + 1);
  }

  public static boolean arrayHasAny(int[] array1, int[] array2) {
    Set<Integer> elements = new HashSet<>(capacityForSize(Math.min(array1.length, array2.length)));
    return hasOverlap(array1.length, array2.length,
        () -> {
          for (int v : array1) {
            elements.add(v);
          }
        },
        () -> {
          for (int v : array2) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        },
        () -> {
          for (int v : array2) {
            elements.add(v);
          }
        },
        () -> {
          for (int v : array1) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        });
  }

  public static boolean arrayHasAny(long[] array1, long[] array2) {
    Set<Long> elements = new HashSet<>(capacityForSize(Math.min(array1.length, array2.length)));
    return hasOverlap(array1.length, array2.length,
        () -> {
          for (long v : array1) {
            elements.add(v);
          }
        },
        () -> {
          for (long v : array2) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        },
        () -> {
          for (long v : array2) {
            elements.add(v);
          }
        },
        () -> {
          for (long v : array1) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        });
  }

  public static boolean arrayHasAny(float[] array1, float[] array2) {
    Set<Float> elements = new HashSet<>(capacityForSize(Math.min(array1.length, array2.length)));
    return hasOverlap(array1.length, array2.length,
        () -> {
          for (float v : array1) {
            elements.add(v);
          }
        },
        () -> {
          for (float v : array2) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        },
        () -> {
          for (float v : array2) {
            elements.add(v);
          }
        },
        () -> {
          for (float v : array1) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        });
  }

  public static boolean arrayHasAny(double[] array1, double[] array2) {
    Set<Double> elements = new HashSet<>(capacityForSize(Math.min(array1.length, array2.length)));
    return hasOverlap(array1.length, array2.length,
        () -> {
          for (double v : array1) {
            elements.add(v);
          }
        },
        () -> {
          for (double v : array2) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        },
        () -> {
          for (double v : array2) {
            elements.add(v);
          }
        },
        () -> {
          for (double v : array1) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        });
  }

  public static boolean arrayHasAny(String[] array1, String[] array2) {
    Set<String> elements = new HashSet<>(capacityForSize(Math.min(array1.length, array2.length)));
    return hasOverlap(array1.length, array2.length,
        () -> Collections.addAll(elements, array1),
        () -> {
          for (String v : array2) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        },
        () -> Collections.addAll(elements, array2),
        () -> {
          for (String v : array1) {
            if (elements.contains(v)) {
              return true;
            }
          }
          return false;
        });
  }
}
