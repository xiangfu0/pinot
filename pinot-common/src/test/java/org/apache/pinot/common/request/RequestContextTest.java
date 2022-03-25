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
package org.apache.pinot.common.request;

import com.google.common.collect.ImmutableList;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RequestContextTest {

  @Test
  public void testExpressionContextHashcode() {
    ExpressionContext expressionContext1 = ExpressionContext.forIdentifier("abc");
    ExpressionContext expressionContext2 = ExpressionContext.forIdentifier("abc");
    Assert.assertEquals(expressionContext1, expressionContext2);
    Assert.assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forIdentifier("abcd");
    Assert.assertNotEquals(expressionContext1, expressionContext2);
    Assert.assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forIdentifier("");
    Assert.assertNotEquals(expressionContext1, expressionContext2);
    Assert.assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forLiteral("abc");
    expressionContext2 = ExpressionContext.forLiteral("abc");
    Assert.assertEquals(expressionContext1, expressionContext2);
    Assert.assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forLiteral("abcd");
    Assert.assertNotEquals(expressionContext1, expressionContext2);
    Assert.assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
    expressionContext2 = ExpressionContext.forLiteral("");
    Assert.assertNotEquals(expressionContext1, expressionContext2);
    Assert.assertNotEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "func1",
        ImmutableList.of(ExpressionContext.forIdentifier("abc"), ExpressionContext.forLiteral("abc"))));
    expressionContext2 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "func1",
        ImmutableList.of(ExpressionContext.forIdentifier("abc"), ExpressionContext.forLiteral("abc"))));
    Assert.assertEquals(expressionContext1, expressionContext2);
    Assert.assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());

    expressionContext1 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "datetrunc",
        ImmutableList.of(ExpressionContext.forLiteral("DAY"), ExpressionContext.forLiteral("event_time_ts"))));
    expressionContext2 = ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "datetrunc",
        ImmutableList.of(ExpressionContext.forLiteral("DAY"), ExpressionContext.forLiteral("event_time_ts"))));
    Assert.assertEquals(expressionContext1, expressionContext2);
    Assert.assertEquals(expressionContext1.hashCode(), expressionContext2.hashCode());
  }
}
