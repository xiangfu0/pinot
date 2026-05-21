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

import javax.annotation.Nullable;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;
import org.apache.pinot.spi.data.Schema;


/// Broker-facing materialized-view state produced during query compilation.
///
/// The generic broker compile result should only need to hold this object, while
/// MV-specific extensions can add data here without expanding broker-owned state.
/// This class is immutable and thread-safe when the contained query/schema objects
/// are not mutated after construction.
public final class MaterializedViewContext {
  private static final MaterializedViewContext EMPTY = new MaterializedViewContext(null, null, null);

  /// Non-null exactly when a swap was committed (either `forFullRewrite` or `forSplitRewrite`).
  /// `empty()` carries a null plan — `annotateResponse` keys on the matching
  /// [#isFullRewrite()] / [#isSplitRewrite()] flag rather than plan-presence so the response
  /// field is stamped only when the broker actually swapped to an MV.
  @Nullable
  private final MaterializedViewRewritePlan _plan;
  @Nullable
  private final SplitRewriteContext _splitRewriteContext;
  @Nullable
  private final FullRewriteContext _fullRewriteContext;

  private MaterializedViewContext(@Nullable MaterializedViewRewritePlan plan,
      @Nullable SplitRewriteContext splitRewriteContext,
      @Nullable FullRewriteContext fullRewriteContext) {
    _plan = plan;
    _splitRewriteContext = splitRewriteContext;
    _fullRewriteContext = fullRewriteContext;
  }

  public static MaterializedViewContext empty() {
    return EMPTY;
  }

  public static MaterializedViewContext forSplitRewrite(MaterializedViewRewritePlan plan,
      PinotQuery viewServerPinotQuery, String viewTableNameWithType, Schema viewSchema) {
    SplitRewriteContext splitRewriteContext =
        new SplitRewriteContext(viewServerPinotQuery, viewTableNameWithType, viewSchema);
    return new MaterializedViewContext(plan, splitRewriteContext, null);
  }

  public static MaterializedViewContext forFullRewrite(MaterializedViewRewritePlan plan,
      PinotQuery preRewriteServerPinotQuery, String preRewriteTableName) {
    FullRewriteContext fullRewriteContext =
        new FullRewriteContext(preRewriteServerPinotQuery, preRewriteTableName);
    return new MaterializedViewContext(plan, null, fullRewriteContext);
  }

  public boolean isSplitRewrite() {
    return _splitRewriteContext != null;
  }

  public boolean isFullRewrite() {
    return _fullRewriteContext != null;
  }

  @Nullable
  public String getMaterializedViewQueriedName() {
    return _plan != null ? _plan.getMaterializedViewTableNameWithType() : null;
  }

  @Nullable
  public MaterializedViewRewritePlan getPlan() {
    return _plan;
  }

  @Nullable
  public SplitRewriteContext getSplitRewriteContext() {
    return _splitRewriteContext;
  }

  public PinotQuery getPreRewriteServerPinotQueryOrDefault(PinotQuery defaultPinotQuery) {
    return _fullRewriteContext != null ? _fullRewriteContext.getPreRewriteServerPinotQuery() : defaultPinotQuery;
  }

  public String getPreRewriteTableNameOrDefault(String defaultTableName) {
    return _fullRewriteContext != null ? _fullRewriteContext.getPreRewriteTableName() : defaultTableName;
  }

  /// MV branch state for split execution.  Callers that need the raw table name derive it
  /// via [TableNameBuilder#extractRawTableName(String)] from
  /// [#getMaterializedViewTableNameWithType()]; storing a separate raw-name field would just
  /// duplicate that single string operation.
  public static final class SplitRewriteContext {
    private final PinotQuery _materializedViewServerPinotQuery;
    private final String _materializedViewTableNameWithType;
    private final Schema _materializedViewSchema;

    private SplitRewriteContext(PinotQuery viewServerPinotQuery, String viewTableNameWithType, Schema viewSchema) {
      _materializedViewServerPinotQuery = viewServerPinotQuery;
      _materializedViewTableNameWithType = viewTableNameWithType;
      _materializedViewSchema = viewSchema;
    }

    public PinotQuery getMaterializedViewServerPinotQuery() {
      return _materializedViewServerPinotQuery;
    }

    public String getMaterializedViewTableNameWithType() {
      return _materializedViewTableNameWithType;
    }

    public Schema getMaterializedViewSchema() {
      return _materializedViewSchema;
    }
  }

  /// Base-table state preserved when full rewrite replaces the server query with an MV query.
  public static final class FullRewriteContext {
    private final PinotQuery _preRewriteServerPinotQuery;
    private final String _preRewriteTableName;

    private FullRewriteContext(PinotQuery preRewriteServerPinotQuery, String preRewriteTableName) {
      _preRewriteServerPinotQuery = preRewriteServerPinotQuery;
      _preRewriteTableName = preRewriteTableName;
    }

    public PinotQuery getPreRewriteServerPinotQuery() {
      return _preRewriteServerPinotQuery;
    }

    public String getPreRewriteTableName() {
      return _preRewriteTableName;
    }
  }
}
