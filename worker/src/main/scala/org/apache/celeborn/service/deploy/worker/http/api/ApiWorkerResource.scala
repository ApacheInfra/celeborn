/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.http.api

import javax.ws.rs.{GET, Path, POST, QueryParam}
import javax.ws.rs.core.MediaType

import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.responses.ApiResponse

import org.apache.celeborn.server.common.http.api.ApiRequestContext

@Path("/")
class ApiWorkerResource extends ApiRequestContext {
  @Path("/listPartitionLocationInfo")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "List all the living PartitionLocation information in that worker.")
  @GET
  def listPartitionLocationInfo: String = rs.listPartitionLocationInfo

  @Path("/unavailablePeers")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      "List the unavailable peers of the worker, this always means the worker connect to the peer failed.")
  @GET
  def unavailablePeers: String = rs.getUnavailablePeers

  @Path("/isShutdown")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "Show if the worker is during the process of shutdown.")
  @GET
  def isShutdown: String = rs.isShutdown

  @Path("/isRegistered")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description = "Show if the worker is registered to the master success.")
  @GET
  def isRegistered: String = rs.isShutdown

  @Path("/exit")
  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.TEXT_PLAIN)),
    description =
      "Trigger this worker to exit. Legal types are 'DECOMMISSION', 'GRACEFUL' and 'IMMEDIATELY'.")
  @POST
  def exit(@QueryParam("TYPE") exitType: String): String = {
    rs.exit(trim(exitType))
  }
}
