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
package org.apache.pinot.common.function;

import com.google.common.base.Preconditions;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.common.function.scalar.InternalFunctions;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionScalarFunctionResolver;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionValue;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionValueConversions;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionValueType;


/**
 * Common-backed partition scalar-function resolver that reuses {@link FunctionRegistry} for lookup and
 * {@link FunctionInvoker} for invocation.
 */
public class CommonPartitionScalarFunctionResolver implements PartitionScalarFunctionResolver {
  private static final int MAX_CLASS_FUNCTION_TYPE_COMBINATIONS = 128;
  private static final Object DYNAMIC_ARGUMENT = new Object();

  public CommonPartitionScalarFunctionResolver() {
    FunctionRegistry.init();
  }

  @Override
  public ResolvedFunction resolve(String functionName, List<Argument> arguments) {
    String canonicalName = FunctionRegistry.canonicalize(functionName);
    if (!FunctionRegistry.contains(canonicalName)) {
      throw new IllegalArgumentException(String.format("Unsupported partition scalar function: %s", functionName));
    }

    List<List<ColumnDataType>> candidateArgumentTypes = new ArrayList<>(arguments.size());
    int typeCombinations = 1;
    for (Argument argument : arguments) {
      List<ColumnDataType> candidateTypes = getCandidateColumnDataTypes(argument);
      candidateArgumentTypes.add(candidateTypes);
      typeCombinations *= candidateTypes.size();
      if (typeCombinations > MAX_CLASS_FUNCTION_TYPE_COMBINATIONS) {
        break;
      }
    }

    ResolutionState resolutionState = new ResolutionState();
    Set<Method> visitedMethods = new HashSet<>();
    LinkedHashSet<String> supportedSignatures = new LinkedHashSet<>();
    if (typeCombinations <= MAX_CLASS_FUNCTION_TYPE_COMBINATIONS) {
      bindByTypes(canonicalName, functionName, arguments, candidateArgumentTypes, new ColumnDataType[arguments.size()],
          0, visitedMethods, supportedSignatures, resolutionState);
    }
    bindFromFunctionInfo(FunctionRegistry.lookupFunctionInfo(canonicalName, arguments.size()), functionName,
        arguments, visitedMethods, supportedSignatures, resolutionState);

    if (resolutionState._bestMatch != null) {
      return resolutionState._bestMatch;
    }
    if (resolutionState._sawNonDeterministicCandidate && !resolutionState._sawDeterministicCandidate) {
      throw new IllegalArgumentException(String.format(
          "Partition scalar function '%s' is not allowed because it is non-deterministic", functionName));
    }

    String supportedSignatureList =
        supportedSignatures.isEmpty() ? functionName + "(...)" : String.join(", ", supportedSignatures);
    throw new IllegalArgumentException(String.format(
        "Function '%s' does not accept argument types (%s). Supported signatures: %s", functionName,
        formatArgumentTypes(arguments), supportedSignatureList));
  }

  private void bindByTypes(String canonicalName, String functionName, List<Argument> arguments,
      List<List<ColumnDataType>> candidateArgumentTypes, ColumnDataType[] argumentTypes, int index,
      Set<Method> visitedMethods, LinkedHashSet<String> supportedSignatures, ResolutionState resolutionState) {
    if (index == arguments.size()) {
      bindFromFunctionInfo(FunctionRegistry.lookupFunctionInfo(canonicalName, argumentTypes), functionName, arguments,
          visitedMethods, supportedSignatures, resolutionState);
      return;
    }

    for (ColumnDataType candidateType : candidateArgumentTypes.get(index)) {
      argumentTypes[index] = candidateType;
      bindByTypes(canonicalName, functionName, arguments, candidateArgumentTypes, argumentTypes, index + 1,
          visitedMethods, supportedSignatures, resolutionState);
    }
  }

  private void bindFromFunctionInfo(@Nullable FunctionInfo functionInfo, String functionName, List<Argument> arguments,
      Set<Method> visitedMethods, LinkedHashSet<String> supportedSignatures, ResolutionState resolutionState) {
    if (functionInfo == null) {
      return;
    }

    Method method = functionInfo.getMethod();
    if (!visitedMethods.add(method) || !isSupportedMethod(method)) {
      return;
    }

    supportedSignatures.add(buildSignature(method));
    if (!functionInfo.isDeterministic() || !isAllowedForPartitioning(method)) {
      resolutionState._sawNonDeterministicCandidate = true;
      return;
    }
    resolutionState._sawDeterministicCandidate = true;

    BoundFunction candidate = new ScalarFunctionMethod(functionInfo).bind(arguments);
    if (candidate == null) {
      return;
    }
    if (resolutionState._bestMatch == null || candidate.getCost() < resolutionState._bestMatch.getCost()) {
      resolutionState._bestMatch = candidate;
      return;
    }
    if (candidate.getCost() == resolutionState._bestMatch.getCost()) {
      throw new IllegalArgumentException(String.format(
          "Ambiguous partition scalar function '%s' for argument types (%s). Matching signatures: %s", functionName,
          formatArgumentTypes(arguments), String.join(", ", supportedSignatures)));
    }
  }

  private static List<ColumnDataType> getCandidateColumnDataTypes(Argument argument) {
    PartitionValueType sourceType = argument.getType();
    PartitionValue constantValue = argument.getConstantValue();
    List<ColumnDataType> candidateTypes = new ArrayList<>();
    switch (sourceType) {
      case STRING:
        candidateTypes.add(ColumnDataType.STRING);
        candidateTypes.add(ColumnDataType.BYTES);
        candidateTypes.add(ColumnDataType.INT);
        candidateTypes.add(ColumnDataType.LONG);
        candidateTypes.add(ColumnDataType.FLOAT);
        candidateTypes.add(ColumnDataType.DOUBLE);
        break;
      case BYTES:
        candidateTypes.add(ColumnDataType.BYTES);
        break;
      case INT:
        candidateTypes.add(ColumnDataType.INT);
        candidateTypes.add(ColumnDataType.LONG);
        candidateTypes.add(ColumnDataType.FLOAT);
        candidateTypes.add(ColumnDataType.DOUBLE);
        break;
      case LONG:
        candidateTypes.add(ColumnDataType.LONG);
        if (constantValue != null) {
          long longValue = constantValue.getLongValue();
          if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
            candidateTypes.add(ColumnDataType.INT);
          }
        }
        candidateTypes.add(ColumnDataType.DOUBLE);
        break;
      case FLOAT:
        candidateTypes.add(ColumnDataType.FLOAT);
        candidateTypes.add(ColumnDataType.DOUBLE);
        break;
      case DOUBLE:
        candidateTypes.add(ColumnDataType.DOUBLE);
        if (constantValue != null) {
          double doubleValue = constantValue.getDoubleValue();
          if (doubleValue == Math.rint(doubleValue)) {
            if (doubleValue >= Long.MIN_VALUE && doubleValue <= Long.MAX_VALUE) {
              candidateTypes.add(ColumnDataType.LONG);
            }
            if (doubleValue >= Integer.MIN_VALUE && doubleValue <= Integer.MAX_VALUE) {
              candidateTypes.add(ColumnDataType.INT);
            }
          }
        }
        break;
      default:
        throw new IllegalStateException("Unsupported partition value type: " + sourceType);
    }
    return candidateTypes;
  }

  private static boolean isSupportedMethod(Method method) {
    try {
      PartitionValueType.fromJavaType(method.getReturnType());
    } catch (IllegalArgumentException e) {
      return false;
    }

    Class<?>[] parameterTypes = method.getParameterTypes();
    int lastIndex = parameterTypes.length - 1;
    for (int i = 0; i < parameterTypes.length; i++) {
      Class<?> parameterType = parameterTypes[i];
      if (method.isVarArgs() && i == lastIndex) {
        if (!parameterType.isArray() || parameterType == byte[].class) {
          return false;
        }
        try {
          PartitionValueType.fromJavaType(parameterType.getComponentType());
        } catch (IllegalArgumentException e) {
          return false;
        }
      } else {
        try {
          PartitionValueType.fromJavaType(parameterType);
        } catch (IllegalArgumentException e) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Partition expressions must stay stable for ingestion and query pruning. Functions that read query-thread context
   * or intentionally block are not safe even if the broader SQL engine exposes them as regular scalar functions.
   */
  private static boolean isAllowedForPartitioning(Method method) {
    Class<?> declaringClass = method.getDeclaringClass();
    if (declaringClass == InternalFunctions.class) {
      return false;
    }
    if (declaringClass == DateTimeFunctions.class) {
      // Block functions that read wall-clock time: the same functionExpr compiled on different nodes or at different
      // times would produce different partition assignments, which would silently corrupt routing and pruning.
      // sleep() is also blocked here (it has a side effect). Note: these functions are still deterministic in the SQL
      // query engine sense (they are constant-folded once at query-parse time), so isDeterministic stays true on their
      // @ScalarFunction annotation; we enforce partition-safety here instead.
      String name = method.getName();
      return !name.equals("sleep") && !name.equals("now") && !name.equals("ago") && !name.equals("agoMV");
    }
    return true;
  }

  private static String formatArgumentTypes(List<Argument> arguments) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < arguments.size(); i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(arguments.get(i).getType());
    }
    return builder.toString();
  }

  private static String buildSignature(Method method) {
    StringBuilder builder = new StringBuilder(method.getName()).append('(');
    Class<?>[] parameterTypes = method.getParameterTypes();
    for (int i = 0; i < parameterTypes.length; i++) {
      if (i > 0) {
        builder.append(", ");
      }
      Class<?> parameterType = parameterTypes[i];
      if (method.isVarArgs() && i == parameterTypes.length - 1) {
        builder.append(parameterType.getComponentType().getSimpleName()).append("...");
      } else {
        builder.append(parameterType.getSimpleName());
      }
    }
    return builder.append(')').toString();
  }

  private static final class ResolutionState {
    @Nullable
    private BoundFunction _bestMatch;
    private boolean _sawNonDeterministicCandidate;
    private boolean _sawDeterministicCandidate;
  }

  private static final class ScalarFunctionMethod {
    private final Method _method;
    private final Class<?>[] _parameterTypes;
    private final boolean _varArgs;
    private final boolean _staticMethod;
    private final PartitionValueType _outputType;
    @Nullable
    private final FunctionInvoker _sharedInvoker;
    @Nullable
    private final ThreadLocal<FunctionInvoker> _threadLocalInvoker;

    private ScalarFunctionMethod(FunctionInfo functionInfo) {
      _method = functionInfo.getMethod();
      _parameterTypes = _method.getParameterTypes();
      _varArgs = _method.isVarArgs();
      _staticMethod = Modifier.isStatic(_method.getModifiers());
      _outputType = PartitionValueType.fromJavaType(_method.getReturnType());
      _sharedInvoker = _staticMethod ? new FunctionInvoker(functionInfo) : null;
      _threadLocalInvoker = _staticMethod ? null : ThreadLocal.withInitial(() -> new FunctionInvoker(functionInfo));
    }

    @Nullable
    public BoundFunction bind(List<Argument> arguments) {
      int parameterCount = _parameterTypes.length;
      int fixedParameterCount = _varArgs ? parameterCount - 1 : parameterCount;
      if ((!_varArgs && arguments.size() != parameterCount) || (_varArgs && arguments.size() < fixedParameterCount)) {
        return null;
      }

      Object[] constantArguments = new Object[arguments.size()];
      int totalCost = 0;
      int dynamicIndex = -1;
      PartitionValueType inputType = PartitionValueType.STRING;
      Class<?> dynamicParameterType = String.class;
      for (int i = 0; i < arguments.size(); i++) {
        Argument argument = arguments.get(i);
        Class<?> parameterType = getParameterType(i);
        if (argument.isDynamic()) {
          if (dynamicIndex >= 0) {
            return null;
          }
          int cost = PartitionValueConversions.getDynamicConversionCost(argument.getType(), parameterType);
          if (cost < 0) {
            return null;
          }
          totalCost += cost;
          dynamicIndex = i;
          inputType = argument.getType();
          dynamicParameterType = parameterType;
          constantArguments[i] = DYNAMIC_ARGUMENT;
        } else {
          PartitionValue constantValue =
              Preconditions.checkNotNull(argument.getConstantValue(), "Constant argument must be configured");
          int cost = PartitionValueConversions.getConstantConversionCost(constantValue, parameterType);
          if (cost < 0) {
            return null;
          }
          totalCost += cost;
          constantArguments[i] = PartitionValueConversions.convertValue(constantValue, parameterType, true);
        }
      }
      return new BoundFunction(this, inputType, dynamicParameterType, dynamicIndex, constantArguments, totalCost);
    }

    public PartitionValueType getOutputType() {
      return _outputType;
    }

    private Class<?> getParameterType(int index) {
      if (_varArgs && index >= _parameterTypes.length - 1) {
        return _parameterTypes[_parameterTypes.length - 1].getComponentType();
      }
      return _parameterTypes[index];
    }

    private Object invoke(Object[] expressionArguments) {
      FunctionInvoker functionInvoker =
          _staticMethod ? Preconditions.checkNotNull(_sharedInvoker) : Preconditions.checkNotNull(_threadLocalInvoker)
              .get();
      return functionInvoker.invoke(adaptArguments(expressionArguments));
    }

    private Object[] adaptArguments(Object[] expressionArguments) {
      if (!_varArgs) {
        return expressionArguments;
      }

      int fixedParameterCount = _parameterTypes.length - 1;
      Object[] methodArguments = new Object[_parameterTypes.length];
      for (int i = 0; i < fixedParameterCount; i++) {
        methodArguments[i] = expressionArguments[i];
      }
      Class<?> componentType = _parameterTypes[_parameterTypes.length - 1].getComponentType();
      int varArgCount = expressionArguments.length - fixedParameterCount;
      Object varArgArray = Array.newInstance(componentType, varArgCount);
      for (int i = 0; i < varArgCount; i++) {
        Array.set(varArgArray, i, expressionArguments[fixedParameterCount + i]);
      }
      methodArguments[_parameterTypes.length - 1] = varArgArray;
      return methodArguments;
    }
  }

  private static final class BoundFunction implements ResolvedFunction {
    private final ScalarFunctionMethod _method;
    private final PartitionValueType _inputType;
    private final Class<?> _dynamicParameterType;
    private final int _dynamicIndex;
    private final Object[] _constantArguments;
    @Nullable
    private final ThreadLocal<Object[]> _threadLocalArguments;
    private final int _cost;

    private BoundFunction(ScalarFunctionMethod method, PartitionValueType inputType, Class<?> dynamicParameterType,
        int dynamicIndex, Object[] constantArguments, int cost) {
      _method = method;
      _inputType = inputType;
      _dynamicParameterType = dynamicParameterType;
      _dynamicIndex = dynamicIndex;
      _constantArguments = constantArguments;
      _threadLocalArguments = dynamicIndex >= 0 ? ThreadLocal.withInitial(_constantArguments::clone) : null;
      _cost = cost;
    }

    public int getCost() {
      return _cost;
    }

    @Override
    public boolean isDynamic() {
      return _dynamicIndex >= 0;
    }

    @Override
    public PartitionValueType getOutputType() {
      return _method.getOutputType();
    }

    @Override
    public PartitionValue invoke(@Nullable PartitionValue dynamicInput) {
      Object[] expressionArguments = _dynamicIndex >= 0 ? Preconditions.checkNotNull(_threadLocalArguments).get()
          : _constantArguments;
      if (_dynamicIndex >= 0) {
        Preconditions.checkNotNull(dynamicInput, "Dynamic partition step input must be configured");
        Preconditions.checkArgument(dynamicInput.getType() == _inputType,
            "Expected %s dynamic input but got %s", _inputType, dynamicInput.getType());
        expressionArguments[_dynamicIndex] = PartitionValueConversions.convertValue(dynamicInput, _dynamicParameterType,
            false);
      }
      return PartitionValue.fromObject(_method.invoke(expressionArguments));
    }
  }
}
