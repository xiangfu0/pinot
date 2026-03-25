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
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.Comparator;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseConfig;
import org.apache.pinot.spi.config.table.lakehouse.LakehouseMode;
import org.apache.pinot.spi.lakehouse.TabletMetadataEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Read-only controller API for inspecting lakehouse tablet metadata envelopes stored in ZooKeeper.
 *
 * <p>The resource only exposes Phase 1 inspect operations: list all tablet envelopes for a table and fetch one
 * envelope by tablet id. It validates that the target table is an enabled {@code ICEBERG_NATIVE} lakehouse table
 * before reading the ZK-backed metadata.</p>
 */
@Api(tags = Constants.TABLE_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY),
    @Authorization(value = DATABASE)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = {
    @ApiKeyAuthDefinition(name = HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER,
        key = SWAGGER_AUTHORIZATION_KEY,
        description = "The format of the key is  ```\"Basic <token>\" or \"Bearer <token>\"```"),
    @ApiKeyAuthDefinition(name = DATABASE, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = DATABASE,
        description = "Database context passed through http header. If no context is provided 'default' database "
            + "context will be considered.")}))
@Path("/")
public class PinotLakehouseTabletMetadataRestletResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotLakehouseTabletMetadataRestletResource.class);

  @Inject
  private PinotHelixResourceManager _pinotHelixResourceManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableNameWithType}/lakehouse/tablets")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableNameWithType", action = Actions.Table.GET_TABLE_CONFIGS)
  @ApiOperation(value = "Lists lakehouse tablet metadata envelopes for a table",
      notes = "Lists ZK-backed tablet envelopes for an enabled lakehouse-native table")
  public List<TabletMetadataEnvelope> listTabletMetadataEnvelopes(
      @PathParam("tableNameWithType") String tableNameWithType, @Context HttpHeaders headers) {
    String resolvedTableNameWithType = translateTableName(tableNameWithType, headers);
    validateLakehouseTable(resolvedTableNameWithType);

    List<TabletMetadataEnvelope> tabletMetadataEnvelopes =
        ZKMetadataProvider.getTabletMetadataEnvelopes(_pinotHelixResourceManager.getPropertyStore(),
            resolvedTableNameWithType);
    tabletMetadataEnvelopes.sort(Comparator.comparing(TabletMetadataEnvelope::getTabletId,
        Comparator.nullsLast(String::compareTo)));
    return tabletMetadataEnvelopes;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableNameWithType}/lakehouse/tablets/{tabletId}")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableNameWithType", action = Actions.Table.GET_TABLE_CONFIGS)
  @ApiOperation(value = "Gets a lakehouse tablet metadata envelope",
      notes = "Gets one ZK-backed tablet envelope by tablet id for an enabled lakehouse-native table")
  public TabletMetadataEnvelope getTabletMetadataEnvelope(
      @PathParam("tableNameWithType") String tableNameWithType, @PathParam("tabletId") String tabletId,
      @Context HttpHeaders headers) {
    if (StringUtils.isBlank(tabletId)) {
      throw new ControllerApplicationException(LOGGER, "tabletId must not be blank", Response.Status.BAD_REQUEST);
    }

    String resolvedTableNameWithType = translateTableName(tableNameWithType, headers);
    validateLakehouseTable(resolvedTableNameWithType);

    TabletMetadataEnvelope tabletMetadataEnvelope =
        ZKMetadataProvider.getTabletMetadataEnvelope(_pinotHelixResourceManager.getPropertyStore(),
            resolvedTableNameWithType, tabletId);
    if (tabletMetadataEnvelope == null) {
      throw new ControllerApplicationException(LOGGER,
          "Lakehouse tablet metadata envelope '" + tabletId + "' not found for table: " + resolvedTableNameWithType,
          Response.Status.NOT_FOUND);
    }
    return tabletMetadataEnvelope;
  }

  private String translateTableName(String tableNameWithType, HttpHeaders headers) {
    return DatabaseUtils.translateTableName(tableNameWithType, headers);
  }

  private void validateLakehouseTable(String tableNameWithType) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      throw new ControllerApplicationException(LOGGER,
          "Table '" + tableNameWithType + "' not found", Response.Status.NOT_FOUND);
    }

    LakehouseConfig lakehouseConfig = tableConfig.getLakehouseConfig();
    if (lakehouseConfig == null || !lakehouseConfig.isEnabled()
        || lakehouseConfig.getMode() != LakehouseMode.ICEBERG_NATIVE) {
      throw new ControllerApplicationException(LOGGER,
          "Lakehouse tablet metadata inspection is only available for enabled ICEBERG_NATIVE tables in Phase 1: "
              + tableNameWithType, Response.Status.BAD_REQUEST);
    }
  }
}
