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
package org.apache.pinot.core.plan;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.transform.PassThroughTransformOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;


/**
 * The <code>TransformPlanNode</code> class provides the execution plan for transforms on a single segment.
 */
public class TransformPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final Collection<ExpressionContext> _expressions;
  private final int _maxDocsPerCall;
  private final BaseFilterOperator _filterOperator;

  public TransformPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      Collection<ExpressionContext> expressions, int maxDocsPerCall) {
    this(indexSegment, queryContext, expressions, maxDocsPerCall, null);
  }

  public TransformPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      Collection<ExpressionContext> expressions, int maxDocsPerCall, @Nullable BaseFilterOperator filterOperator) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _expressions = expressions;
    _maxDocsPerCall = maxDocsPerCall;
    _filterOperator = filterOperator;
  }

  @Override
  public TransformOperator run() {
    Set<String> projectionColumns = new HashSet<>();
    boolean hasNonIdentifierExpression = false;
    for (ExpressionContext expression : _expressions) {
      expression.getColumns(projectionColumns);
      if (expression.getType() != ExpressionContext.Type.IDENTIFIER) {
        hasNonIdentifierExpression = true;
      }
      pushdownFunctions(_indexSegment, expression, projectionColumns);
    }
    ProjectionOperator projectionOperator =
        new ProjectionPlanNode(_indexSegment, _queryContext, projectionColumns, _maxDocsPerCall, _filterOperator).run();
    if (hasNonIdentifierExpression) {
      return new TransformOperator(_queryContext, projectionOperator, _expressions);
    } else {
      return new PassThroughTransformOperator(projectionOperator, _expressions);
    }
  }

  private void pushdownFunctions(IndexSegment indexSegment, ExpressionContext expression,
      Set<String> projectionColumns) {
    if (expression.getType() != ExpressionContext.Type.FUNCTION) {
      return;
    }
    FunctionContext function = expression.getFunction();
    switch (function.getFunctionName().toUpperCase()) {
      case "DATETRUNC":
        if (function.getArguments().size() != 2) {
          break;
        }
        String columnWithGranularity =
            TimestampIndexGranularity.getColumnNameWithGranularity(function.getArguments().get(1).getIdentifier(),
                TimestampIndexGranularity.valueOf(function.getArguments().get(0).getLiteral().toUpperCase()));
        if (indexSegment.getDataSource(columnWithGranularity) != null) {
          projectionColumns.add(columnWithGranularity);
        }
        break;
      default:
        break;
    }
    for (ExpressionContext argument : function.getArguments()) {
      pushdownFunctions(indexSegment, argument, projectionColumns);
    }
  }
}
