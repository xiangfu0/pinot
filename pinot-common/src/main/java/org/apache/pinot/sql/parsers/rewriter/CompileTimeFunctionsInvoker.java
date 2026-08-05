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
package org.apache.pinot.sql.parsers.rewriter;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.QueryFunctionInvoker;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.sql.parsers.SqlCompilationException;


public class CompileTimeFunctionsInvoker implements QueryRewriter {

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    for (int i = 0; i < pinotQuery.getSelectListSize(); i++) {
      Expression expression = invokeCompileTimeFunctionExpression(pinotQuery.getSelectList().get(i));
      pinotQuery.getSelectList().set(i, expression);
    }
    for (int i = 0; i < pinotQuery.getGroupByListSize(); i++) {
      Expression expression = invokeCompileTimeFunctionExpression(pinotQuery.getGroupByList().get(i));
      pinotQuery.getGroupByList().set(i, expression);
    }
    for (int i = 0; i < pinotQuery.getOrderByListSize(); i++) {
      Expression expression = invokeCompileTimeFunctionExpression(pinotQuery.getOrderByList().get(i));
      pinotQuery.getOrderByList().set(i, expression);
    }
    Expression filterExpression = invokeCompileTimeFunctionExpression(pinotQuery.getFilterExpression());
    pinotQuery.setFilterExpression(filterExpression);
    Expression havingExpression = invokeCompileTimeFunctionExpression(pinotQuery.getHavingExpression());
    pinotQuery.setHavingExpression(havingExpression);
    return pinotQuery;
  }

  @VisibleForTesting
  public static Expression invokeCompileTimeFunctionExpression(@Nullable Expression expression) {
    if (expression == null || expression.getFunctionCall() == null) {
      return expression;
    }
    Function function = expression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    int numOperands = operands.size();
    boolean compilable = true;
    ColumnDataType[] argumentTypes = new ColumnDataType[numOperands];
    Object[] arguments = new Object[numOperands];
    for (int i = 0; i < numOperands; i++) {
      Expression originalOperand = operands.get(i);
      Expression operand = invokeCompileTimeFunctionExpression(originalOperand);
      operands.set(i, operand);
      Literal literal = operand.getLiteral();
      if (compilable && literal != null) {
        Pair<ColumnDataType, Object> typeAndValue = RequestUtils.getLiteralTypeAndValue(literal);
        Object value = typeAndValue.getRight();
        if (value != null && isUuidCast(originalOperand)) {
          argumentTypes[i] = ColumnDataType.UUID;
          arguments[i] = UuidUtils.toUUID(value);
        } else {
          argumentTypes[i] = typeAndValue.getLeft();
          arguments[i] = value;
        }
      } else {
        // NOTE: Do not directly 'return expression;' here because we want to compile all operands even if the current
        //       expression is not compilable.
        compilable = false;
      }
    }
    if (!compilable) {
      return expression;
    }
    String canonicalName = FunctionRegistry.canonicalize(function.getOperator());
    FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(canonicalName, argumentTypes);
    if (functionInfo == null || !functionInfo.isDeterministic()) {
      return expression;
    }
    try {
      QueryFunctionInvoker invoker = new QueryFunctionInvoker(functionInfo);
      Object result;
      if (invoker.getMethod().isVarArgs()) {
        result = invoker.invoke(new Object[]{arguments});
      } else {
        invoker.convertTypes(arguments);
        result = invoker.invoke(arguments);
      }
      return RequestUtils.getLiteralExpression(result);
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while invoking method: " + functionInfo.getMethod().getName() + " with arguments: "
              + Arrays.toString(arguments) + ": " + e.getMessage(), e);
    }
  }

  private static boolean isUuidCast(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null || !"cast".equals(FunctionRegistry.canonicalize(function.getOperator()))) {
      return false;
    }
    List<Expression> operands = function.getOperands();
    return operands.size() == 2 && operands.get(1).isSetLiteral() && operands.get(1).getLiteral().isSetStringValue()
        && "UUID".equalsIgnoreCase(operands.get(1).getLiteral().getStringValue());
  }
}
