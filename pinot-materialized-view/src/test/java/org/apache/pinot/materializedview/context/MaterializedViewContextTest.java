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
package org.apache.pinot.materializedview.context;

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.rewrite.ExecutionMode;
import org.apache.pinot.materializedview.rewrite.MatchType;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Pins the accessor contracts on `MaterializedViewContext` that callers in the broker rely on
/// for ACL / quota / RLS / `tablesQueried` derivation across a FULL_REWRITE swap.
public class MaterializedViewContextTest {

  @Test
  public void testEmptyContextReturnsDefaults() {
    MaterializedViewContext ctx = MaterializedViewContext.empty();
    Assert.assertFalse(ctx.isFullRewrite());
    Assert.assertFalse(ctx.isSplitRewrite());
    Assert.assertNull(ctx.getPlan());
    Assert.assertNull(ctx.getMaterializedViewQueriedName());

    PinotQuery defaultQuery = new PinotQuery();
    Assert.assertSame(ctx.getPreRewriteServerPinotQueryOrDefault(defaultQuery), defaultQuery,
        "Empty context must return the supplied default server query");
    Assert.assertEquals(ctx.getPreRewriteTableNameWithTypeOrDefault("default_OFFLINE"), "default_OFFLINE",
        "Empty context must return the supplied default with-type table name");
    Assert.assertEquals(ctx.getUserRawTableNameOrDefault("defaultRaw"), "defaultRaw",
        "Empty context must return the supplied default raw table name");
  }

  @Test
  public void testFullRewriteContextPreservesPreRewriteState() {
    PinotQuery preRewriteQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM baseTable GROUP BY ts");
    PinotQuery materializedViewQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts");
    MaterializedViewRewritePlan plan = new MaterializedViewRewritePlan(
        "mv_baseTable_OFFLINE", MatchType.EXACT, ExecutionMode.FULL_REWRITE, materializedViewQuery, 1.0);

    MaterializedViewContext ctx = MaterializedViewContext.forFullRewrite(
        plan, preRewriteQuery, "db.baseTable_OFFLINE");

    Assert.assertTrue(ctx.isFullRewrite());
    Assert.assertFalse(ctx.isSplitRewrite());
    Assert.assertSame(ctx.getPlan(), plan);
    Assert.assertEquals(ctx.getMaterializedViewQueriedName(), "mv_baseTable_OFFLINE");

    // The accessor must return the pre-rewrite (base-table) query, NOT the supplied default.
    PinotQuery wrongDefault = new PinotQuery();
    Assert.assertSame(ctx.getPreRewriteServerPinotQueryOrDefault(wrongDefault), preRewriteQuery,
        "FULL_REWRITE context must return the captured pre-rewrite server query — the broker uses "
            + "this for ACL/quota authorization against the base table");

    // The with-type accessor must return the with-type form for use by
    // `DatabaseUtils.extractDatabaseFromFullyQualifiedTableName` and quota lookups.
    Assert.assertEquals(ctx.getPreRewriteTableNameWithTypeOrDefault("ignored"), "db.baseTable_OFFLINE",
        "FULL_REWRITE context must return the with-type table name — load-bearing for "
            + "database-quota and per-table-quota charging");

    // The user-facing raw accessor must strip the `_OFFLINE` suffix.
    Assert.assertEquals(ctx.getUserRawTableNameOrDefault("ignored"), "db.baseTable",
        "FULL_REWRITE context must expose the raw base-table name — load-bearing for "
            + "`tablesQueried` response field and rewrite-exception metrics");
  }

  @Test
  public void testSplitRewriteContextDoesNotExposeFullRewriteState() {
    PinotQuery materializedViewQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts");
    MaterializedViewRewritePlan plan = new MaterializedViewRewritePlan(
        "mv_baseTable_OFFLINE", MatchType.EXACT, ExecutionMode.SPLIT_REWRITE, materializedViewQuery, 1.0);
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName("mv_baseTable")
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();

    MaterializedViewContext ctx = MaterializedViewContext.forSplitRewrite(
        plan, materializedViewQuery, "mv_baseTable_OFFLINE", mvSchema);

    Assert.assertTrue(ctx.isSplitRewrite());
    Assert.assertFalse(ctx.isFullRewrite());

    // Split rewrite preserves the user's original base-table query/table — the *_OrDefault
    // accessors return the caller-supplied default, not any captured pre-rewrite state.
    PinotQuery defaultQuery = new PinotQuery();
    Assert.assertSame(ctx.getPreRewriteServerPinotQueryOrDefault(defaultQuery), defaultQuery);
    Assert.assertEquals(ctx.getPreRewriteTableNameWithTypeOrDefault("baseTable_OFFLINE"), "baseTable_OFFLINE");
    Assert.assertEquals(ctx.getUserRawTableNameOrDefault("baseTable"), "baseTable");
  }
}
