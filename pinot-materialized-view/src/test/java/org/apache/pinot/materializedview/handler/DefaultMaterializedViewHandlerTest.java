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
package org.apache.pinot.materializedview.handler;

import java.util.Locale;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Unit tests for [DefaultMaterializedViewHandler#attachFilter], the helper that places the
/// per-branch time-boundary filter on the base (`>= boundary`) and MV (`< boundary`) sides of a
/// split execution.
public class DefaultMaterializedViewHandlerTest {

  @Test
  public void testAttachFilterLessThanOnEmptyFilter() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT carrier, SUM(delay) FROM materializedViewTable GROUP BY carrier");
    Assert.assertNull(query.getFilterExpression(), "precondition: no WHERE clause");

    DefaultMaterializedViewHandler.attachFilter(query, new TimeBoundaryInfo("materializedViewDay", "20000"),
        FilterKind.LESS_THAN);

    Expression filter = query.getFilterExpression();
    Assert.assertNotNull(filter, "MV branch must receive the upper-bound filter");
    Function fn = filter.getFunctionCall();
    Assert.assertNotNull(fn);
    Assert.assertEquals(fn.getOperator().toUpperCase(Locale.ROOT), "LESS_THAN",
        "MV upper-bound filter must use LESS_THAN (exclusive), symmetric to the base branch's >=");
    Assert.assertEquals(fn.getOperands().get(0).getIdentifier().getName(), "materializedViewDay");
  }

  @Test
  public void testAttachFilterLessThanWithExistingFilter() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT carrier, SUM(delay) FROM materializedViewTable WHERE carrier = 'AA' GROUP BY carrier");
    Assert.assertNotNull(query.getFilterExpression(), "precondition: existing WHERE clause");

    DefaultMaterializedViewHandler.attachFilter(query, new TimeBoundaryInfo("materializedViewDay", "20000"),
        FilterKind.LESS_THAN);

    Expression filter = query.getFilterExpression();
    Function root = filter.getFunctionCall();
    Assert.assertNotNull(root);
    Assert.assertEquals(root.getOperator().toUpperCase(Locale.ROOT), "AND",
        "Upper-bound filter must be AND-ed with the user's WHERE, not replace it");
    Assert.assertEquals(root.getOperands().size(), 2);

    // Second operand is the injected LESS_THAN(materializedViewDay, 20000).
    Function injected = root.getOperands().get(1).getFunctionCall();
    Assert.assertNotNull(injected);
    Assert.assertEquals(injected.getOperator().toUpperCase(Locale.ROOT), "LESS_THAN");
    Assert.assertEquals(injected.getOperands().get(0).getIdentifier().getName(), "materializedViewDay");
  }

  @Test
  public void testAttachFilterGreaterThanOrEqualOnBaseSide() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT col FROM baseTable");
    DefaultMaterializedViewHandler.attachFilter(query, new TimeBoundaryInfo("ts", "100"),
        FilterKind.GREATER_THAN_OR_EQUAL);

    Function fn = query.getFilterExpression().getFunctionCall();
    Assert.assertEquals(fn.getOperator().toUpperCase(Locale.ROOT), "GREATER_THAN_OR_EQUAL",
        "Base branch filter must be inclusive >= since the MV uses exclusive <");
    Assert.assertEquals(fn.getOperands().get(0).getIdentifier().getName(), "ts");
  }
}
