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
package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.ServiceStatus.Status;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.server.api.AdminApiApplication;


/**
 * REST API to do health check through ServiceStatus.
 */
@Api(tags = "Health")
@Path("/")
public class HealthCheckResource {

  @Inject
  private AtomicBoolean _shutDownInProgress;

  @Inject
  @Named(AdminApiApplication.SERVER_INSTANCE_ID)
  private String _instanceId;

  @Inject
  private ServerMetrics _serverMetrics;

  @Inject
  @Named(AdminApiApplication.START_TIME)
  private Instant _startTime;

  @GET
  @Path("/health")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Checking server health")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Server is healthy"),
      @ApiResponse(code = 503, message = "Server is not healthy")
  })
  public String checkHealth(
      @ApiParam(value = "health check type: liveness or readiness") @QueryParam("checkType") @Nullable
      String checkType) {
    if ("liveness".equalsIgnoreCase(checkType)) {
      return checkLiveness();
    } else {
      return checkReadiness();
    }
  }

  @GET
  @Path("/health/liveness")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Checking server liveness status.")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Server is live"),
      @ApiResponse(code = 503, message = "Server is not live")
  })
  public String checkLiveness() {
    // Returns OK since if we reached here, the admin application is running.
    return "OK";
  }

  @GET
  @Path("/health/readiness")
  @Produces(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Checking server readiness status")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Server is ready to serve queries"),
      @ApiResponse(code = 503, message = "Server is not ready to serve queries")
  })
  public String checkReadiness() {
    if (_shutDownInProgress.get()) {
      String errMessage = "Server is shutting down";
      throw new WebApplicationException(errMessage,
          Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(errMessage).build());
    }
    Status status = ServiceStatus.getServiceStatus(_instanceId);
    if (status == Status.GOOD) {
      _serverMetrics.addMeteredGlobalValue(ServerMeter.READINESS_CHECK_OK_CALLS, 1);
      return "OK";
    }
    _serverMetrics.addMeteredGlobalValue(ServerMeter.READINESS_CHECK_BAD_CALLS, 1);
    String errMessage = String.format("Pinot server status is %s", status);
    Response response =
        Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(errMessage).build();
    throw new WebApplicationException(errMessage, response);
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  @Path("uptime")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_HEALTH)
  @ApiOperation(value = "Get server uptime")
  public long getUptime() {
    if (_startTime == null) {
      return 0;
    }
    Instant now = Instant.now();
    Duration uptime = Duration.between(_startTime, now);
    return uptime.getSeconds();
  }

  @GET
  @Path("start-time")
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_HEALTH)
  @ApiOperation(value = "Get server start time")
  @Produces(MediaType.TEXT_PLAIN)
  public String getStartTime() {
    DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    return _startTime != null ? formatter.format(_startTime) : "";
  }
}
