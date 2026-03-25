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
import java.io.IOException;
import java.nio.file.Paths;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.lakehouse.LakehouseCatalogAdapterProvider;
import org.apache.pinot.controller.lakehouse.LakehouseTableRefreshResponse;
import org.apache.pinot.controller.lakehouse.LakehouseTabletPlanner;
import org.apache.pinot.controller.lakehouse.LocalFileSystemLakehouseManifestStore;
import org.apache.pinot.controller.lakehouse.PinotLakehouseTableService;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotDescriptor;
import org.apache.pinot.spi.lakehouse.LakehouseSnapshotRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.DATABASE;
import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * Controller API for resolving lakehouse snapshot references and refreshing tablet metadata.
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
public class PinotLakehouseRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotLakehouseRestletResource.class);

  @Inject
  private ControllerConf _controllerConf;
  @Inject
  private PinotHelixResourceManager _pinotHelixResourceManager;
  @Inject
  private ControllerMetrics _controllerMetrics;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableNameWithType}/lakehouse/refs")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableNameWithType", action = Actions.Table.GET_TABLE_CONFIGS)
  @ApiOperation(value = "Resolves a lakehouse snapshot reference",
      notes = "Resolves the snapshot or ref for an enabled lakehouse-native table using the configured adapter")
  public LakehouseSnapshotDescriptor getSnapshotReference(@PathParam("tableNameWithType") String tableNameWithType,
      @QueryParam("snapshotId") Long snapshotId, @QueryParam("branch") String branch, @QueryParam("tag") String tag,
      @QueryParam("asOfTimestampMs") Long asOfTimestampMs, @Context HttpHeaders headers) {
    try {
      validateSnapshotSelectorCount(snapshotId, branch, tag, asOfTimestampMs);
      return newLakehouseTableService().resolveSnapshot(translateTableName(tableNameWithType, headers),
          buildSnapshotRequest(snapshotId, branch, tag, asOfTimestampMs));
    } catch (IllegalStateException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/tables/{tableNameWithType}/lakehouse/refresh")
  @Authorize(targetType = TargetType.TABLE, paramName = "tableNameWithType", action = Actions.Table.UPDATE_TABLE_CONFIG)
  @ApiOperation(value = "Refreshes lakehouse tablets for a table",
      notes = "Resolves the configured snapshot, groups files into tablets, persists manifests, and publishes tablet "
          + "metadata envelopes to ZooKeeper")
  public LakehouseTableRefreshResponse refreshLakehouseTable(
      @PathParam("tableNameWithType") String tableNameWithType, @QueryParam("snapshotId") Long snapshotId,
      @QueryParam("branch") String branch, @QueryParam("tag") String tag,
      @QueryParam("asOfTimestampMs") Long asOfTimestampMs, @Context HttpHeaders headers) {
    try {
      validateSnapshotSelectorCount(snapshotId, branch, tag, asOfTimestampMs);
      return newLakehouseTableService().refreshTable(translateTableName(tableNameWithType, headers),
          buildSnapshotRequest(snapshotId, branch, tag, asOfTimestampMs));
    } catch (IllegalStateException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.BAD_REQUEST, e);
    } catch (IOException e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  private PinotLakehouseTableService newLakehouseTableService() {
    return new PinotLakehouseTableService(_pinotHelixResourceManager, _controllerMetrics,
        new LakehouseCatalogAdapterProvider(), new LakehouseTabletPlanner(),
        new LocalFileSystemLakehouseManifestStore(Paths.get(_controllerConf.getDataDir(), "lakehouse-manifests")));
  }

  private static LakehouseSnapshotRequest buildSnapshotRequest(Long snapshotId, String branch, String tag,
      Long asOfTimestampMs) {
    LakehouseSnapshotRequest snapshotRequest = new LakehouseSnapshotRequest();
    snapshotRequest.setSnapshotId(snapshotId);
    snapshotRequest.setBranch(branch);
    snapshotRequest.setTag(tag);
    snapshotRequest.setAsOfTimeMillis(asOfTimestampMs);
    return snapshotRequest;
  }

  private static String translateTableName(String tableNameWithType, HttpHeaders headers) {
    return DatabaseUtils.translateTableName(tableNameWithType, headers);
  }

  private static void validateSnapshotSelectorCount(Long snapshotId, String branch, String tag, Long asOfTimestampMs) {
    int selectorCount = snapshotId != null ? 1 : 0;
    selectorCount += StringUtils.isNotBlank(branch) ? 1 : 0;
    selectorCount += StringUtils.isNotBlank(tag) ? 1 : 0;
    selectorCount += asOfTimestampMs != null ? 1 : 0;
    if (selectorCount > 1) {
      throw new IllegalStateException(
          "Only one lakehouse snapshot selector can be set on refresh/refs APIs at a time");
    }
  }
}
