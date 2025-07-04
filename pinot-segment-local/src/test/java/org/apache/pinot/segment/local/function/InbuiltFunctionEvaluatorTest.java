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
package org.apache.pinot.segment.local.function;

import java.util.Collections;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class InbuiltFunctionEvaluatorTest {

  @Test
  public void booleanLiteralTest() {
    checkBooleanLiteralExpression("true", 1);
    checkBooleanLiteralExpression("false", 0);
    checkBooleanLiteralExpression("True", 1);
    checkBooleanLiteralExpression("False", 0);
    checkBooleanLiteralExpression("1", 1);
    checkBooleanLiteralExpression("0", 0);
  }

  @Test
  public void testOrWithNulls() {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator("or(null, false, true)");
    Object output = evaluator.evaluate(new GenericRow());
    assertEquals(output, true);

    evaluator = new InbuiltFunctionEvaluator("or(null, false, null)");
    output = evaluator.evaluate(new GenericRow());
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("or(null, null, null)");
    output = evaluator.evaluate(new Object[]{});
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("or(null, true, null)");
    output = evaluator.evaluate(new Object[]{});
    assertEquals(output, true);

    evaluator = new InbuiltFunctionEvaluator("or(true, false)");
    output = evaluator.evaluate(new GenericRow());
    assertEquals(output, true);
  }

  @Test
  public void testAndWithNulls() {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator("and(null, false, true)");
    Object output = evaluator.evaluate(new GenericRow());
    assertEquals(output, false);

    evaluator = new InbuiltFunctionEvaluator("and(null, false, null)");
    output = evaluator.evaluate(new GenericRow());
    assertEquals(output, false);

    evaluator = new InbuiltFunctionEvaluator("and(null, null, null)");
    output = evaluator.evaluate(new Object[]{});
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("and(null, true, null)");
    output = evaluator.evaluate(new Object[]{});
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("and(true, false)");
    output = evaluator.evaluate(new GenericRow());
    assertEquals(output, false);
  }

  @Test
  public void testNotWithNulls() {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator("not(null)");
    Object output = evaluator.evaluate(new GenericRow());
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("not(false)");
    output = evaluator.evaluate(new Object[]{});
    assertEquals(output, true);

    evaluator = new InbuiltFunctionEvaluator("not(true)");
    output = evaluator.evaluate(new GenericRow());
    assertEquals(output, false);
  }

  private void checkBooleanLiteralExpression(String expression, int value) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    Object output = evaluator.evaluate(new GenericRow());
    Class<?> outputValueClass = output.getClass();
    PinotDataType outputType = FunctionUtils.getArgumentType(outputValueClass);
    assertNotNull(outputType);
    // as INT is the stored type for BOOLEAN
    assertEquals(outputType.toInt(output), value);
  }

  @Test
  public void testColumnExpression() {
    String expression = "testColumn";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      assertEquals(evaluator.evaluate(row), value);
    }
  }

  @Test
  public void testLiteralExpression() {
    String expression = "'testValue'";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      assertEquals(evaluator.evaluate(row), "testValue");
    }
  }

  @Test
  public void testScalarWrapperWithReservedKeywordExpression() {
    String expression = "dateTrunc('MONTH', \"date\")";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), Collections.singletonList("date"));
    GenericRow row = new GenericRow();
    for (int i = 1; i < 9; i++) {
      DateTime dt = new DateTime(String.format("2020-0%d-15T12:00:00", i));
      long millis = dt.getMillis();
      DateTime truncDt = dt.withZone(DateTimeZone.UTC).withDayOfMonth(1).withHourOfDay(0).withMillisOfDay(0);
      row.putValue("date", millis);
      assertEquals(evaluator.evaluate(row), truncDt.getMillis());
    }
  }

  @Test
  public void testScalarWrapperNameWithOverrides() {
    String expr = String.format("regexp_extract(testColumn, '%s')", "(.*)([\\d]+)");
    String exprWithGroup = String.format("regexp_extract(testColumn, '%s', 2)", "(.*)([\\d]+)");
    String exprWithGroupAndDefault = String.format("regexp_extract(testColumn, '%s', 3, 'null')", "(.*)([\\d]+)");
    GenericRow row = new GenericRow();
    row.putValue("testColumn", "testValue0");
    InbuiltFunctionEvaluator evaluator;
    evaluator = new InbuiltFunctionEvaluator(expr);
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
    assertEquals(evaluator.evaluate(row), "testValue0");
    evaluator = new InbuiltFunctionEvaluator(exprWithGroup);
    assertEquals(evaluator.evaluate(row), "0");
    evaluator = new InbuiltFunctionEvaluator(exprWithGroupAndDefault);
    assertEquals(evaluator.evaluate(row), "null");
  }

  @Test
  public void testFunctionWithColumn() {
    String expression = "reverse(testColumn)";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      assertEquals(evaluator.evaluate(row), new StringBuilder(value).reverse().toString());
    }
  }

  @Test
  public void testFunctionWithLiteral() {
    String expression = "reverse(12345)";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    assertEquals(evaluator.evaluate(row), "54321");
  }

  @Test
  public void testNestedFunction() {
    String expression = "reverse(reverse(testColumn))";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      assertEquals(evaluator.evaluate(row), value);
    }
  }

  @Test
  public void testStateSharedBetweenRowsForExecution() {
    // This function is auto registered with @ScalarFunction annotation under MyFunc class
    String expression = "appendToStringAndReturn('test ')";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    assertEquals(evaluator.evaluate(row), "test ");
    assertEquals(evaluator.evaluate(row), "test test ");
    assertEquals(evaluator.evaluate(row), "test test test ");
  }

  @Test
  public void testNullReturnedByInbuiltFunctionEvaluatorThatCannotTakeNull() {
    String[] expressions = {
        "fromDateTime(\"NULL\", 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''')",
        "fromDateTime(\"invalid_identifier\", 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''')",
        "toDateTime(1648010797, \"invalid_identifier\", \"invalid_identifier\")",
        "toDateTime(\"invalid_identifier\", \"invalid_identifier\", \"invalid_identifier\")",
        "toDateTime(\"NULL\", \"invalid_identifier\", \"invalid_identifier\")",
        "toDateTime(\"invalid_identifier\", \"NULL\", \"invalid_identifier\")"
    };
    for (String expression : expressions) {
      InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
      GenericRow row = new GenericRow();
      assertNull(evaluator.evaluate(row));
    }
  }

  @SuppressWarnings("unused")
  public static class MyFunc {
    String _baseString = "";

    @ScalarFunction
    public String appendToStringAndReturn(String addedString) {
      _baseString += addedString;
      return _baseString;
    }
  }
}
