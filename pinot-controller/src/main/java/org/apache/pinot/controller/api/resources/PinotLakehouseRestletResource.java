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
package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metadata.lakehouse.TabletMetadataEnvelope;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.lakehouse.LakehouseTableManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * REST endpoints for lakehouse-native table administration.
 *
 * <p>Provides endpoints to inspect tablets, refresh snapshot metadata, and validate
 * lakehouse configuration for Iceberg-native tables.</p>
 */
@Api(tags = Constants.LAKEHOUSE_TAG, authorizations = {
    @Authorization(value = SWAGGER_AUTHORIZATION_KEY)
})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is ``auto <token>``")
}))
@Path("/lakehouse")
public class PinotLakehouseRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotLakehouseRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  LakehouseTableManager _lakehouseTableManager;

  @GET
  @Path("/tables/{tableName}/tablets")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List tablets for a lakehouse-native table",
      notes = "Returns the current tablet metadata envelopes for the specified lakehouse table.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found or not lakehouse-enabled")
  })
  public List<TabletMetadataEnvelope> getTableTablets(
      @ApiParam(value = "Table name with type suffix", required = true)
      @PathParam("tableName") String tableName) {
    String tableNameWithType = resolveTableName(tableName);
    validateLakehouseTable(tableNameWithType);

    List<TabletMetadataEnvelope> tablets = _lakehouseTableManager.getTableTablets(tableNameWithType);
    return tablets != null ? tablets : Collections.emptyList();
  }

  @POST
  @Path("/tables/{tableName}/refresh")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Refresh lakehouse table metadata",
      notes = "Forces a refresh of Iceberg snapshot and tablet metadata for the specified table.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Refresh completed"),
      @ApiResponse(code = 404, message = "Table not found or not lakehouse-enabled"),
      @ApiResponse(code = 500, message = "Refresh failed")
  })
  public Response refreshTableMetadata(
      @ApiParam(value = "Table name with type suffix", required = true)
      @PathParam("tableName") String tableName) {
    String tableNameWithType = resolveTableName(tableName);
    TableConfig tableConfig = validateLakehouseTable(tableNameWithType);

    try {
      List<TabletMetadataEnvelope> tablets = _lakehouseTableManager.refreshTableTablets(tableNameWithType, tableConfig);
      Map<String, Object> result = new HashMap<>();
      result.put("tableName", tableNameWithType);
      result.put("tabletCount", tablets.size());
      result.put("status", "SUCCESS");
      if (!tablets.isEmpty()) {
        result.put("snapshotId", tablets.get(0).getSnapshotId());
      }
      return Response.ok(result).build();
    } catch (Exception e) {
      LOGGER.error("Failed to refresh lakehouse metadata for table: {}", tableNameWithType, e);
      Map<String, Object> error = new HashMap<>();
      error.put("tableName", tableNameWithType);
      error.put("status", "FAILURE");
      error.put("error", e.getMessage());
      return Response.serverError().entity(error).build();
    }
  }

  @GET
  @Path("/tables/{tableName}/tablets/{tabletId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get a specific tablet envelope",
      notes = "Returns the metadata envelope for a specific tablet.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table or tablet not found")
  })
  public TabletMetadataEnvelope getTablet(
      @ApiParam(value = "Table name with type suffix", required = true)
      @PathParam("tableName") String tableName,
      @ApiParam(value = "Tablet identifier", required = true)
      @PathParam("tabletId") String tabletId) {
    String tableNameWithType = resolveTableName(tableName);
    validateLakehouseTable(tableNameWithType);

    List<TabletMetadataEnvelope> tablets = _lakehouseTableManager.getTableTablets(tableNameWithType);
    if (tablets != null) {
      for (TabletMetadataEnvelope envelope : tablets) {
        if (envelope.getTabletId().equals(tabletId)) {
          return envelope;
        }
      }
    }
    throw new ControllerApplicationException(LOGGER, "Tablet not found: " + tabletId, Response.Status.NOT_FOUND);
  }

  @GET
  @Path("/tables/{tableName}/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get lakehouse table summary",
      notes = "Returns a summary of the lakehouse table including snapshot info and tablet statistics.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found or not lakehouse-enabled")
  })
  public Map<String, Object> getTableSummary(
      @ApiParam(value = "Table name with type suffix", required = true)
      @PathParam("tableName") String tableName) {
    String tableNameWithType = resolveTableName(tableName);
    TableConfig tableConfig = validateLakehouseTable(tableNameWithType);

    Map<String, Object> summary = new HashMap<>();
    summary.put("tableName", tableNameWithType);
    summary.put("lakehouseMode", tableConfig.getLakehouseConfig().getMode().name());
    summary.put("catalogType", tableConfig.getLakehouseConfig().getCatalog().getType().name());
    summary.put("tableIdentifier", tableConfig.getLakehouseConfig().getCatalog().getTableIdentifier());

    List<TabletMetadataEnvelope> tablets = _lakehouseTableManager.getTableTablets(tableNameWithType);
    if (tablets != null && !tablets.isEmpty()) {
      summary.put("tabletCount", tablets.size());
      summary.put("snapshotId", tablets.get(0).getSnapshotId());

      long totalRows = 0;
      long totalBytes = 0;
      int totalMicrosegments = 0;
      for (TabletMetadataEnvelope t : tablets) {
        totalRows += t.getRowCount();
        totalBytes += t.getSizeBytes();
        totalMicrosegments += t.getMicrosegmentCount();
      }
      summary.put("totalRows", totalRows);
      summary.put("totalBytes", totalBytes);
      summary.put("totalMicrosegments", totalMicrosegments);
    } else {
      summary.put("tabletCount", 0);
      summary.put("snapshotId", "N/A");
    }

    return summary;
  }

  private String resolveTableName(String tableName) {
    // If no type suffix, try OFFLINE first
    if (!tableName.endsWith("_OFFLINE") && !tableName.endsWith("_REALTIME")) {
      String offlineName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      if (_pinotHelixResourceManager.hasTable(offlineName)) {
        return offlineName;
      }
      String realtimeName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      if (_pinotHelixResourceManager.hasTable(realtimeName)) {
        return realtimeName;
      }
      throw new ControllerApplicationException(LOGGER, "Table not found: " + tableName, Response.Status.NOT_FOUND);
    }
    return tableName;
  }

  private TableConfig validateLakehouseTable(String tableNameWithType) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      throw new ControllerApplicationException(LOGGER, "Table not found: " + tableNameWithType,
          Response.Status.NOT_FOUND);
    }
    if (!tableConfig.isLakehouseEnabled()) {
      throw new ControllerApplicationException(LOGGER,
          "Table is not lakehouse-enabled: " + tableNameWithType, Response.Status.BAD_REQUEST);
    }
    return tableConfig;
  }
}
