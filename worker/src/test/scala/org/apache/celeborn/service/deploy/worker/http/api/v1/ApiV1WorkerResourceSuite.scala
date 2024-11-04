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

package org.apache.celeborn.service.deploy.worker.http.api.v1

import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.MediaType

import org.apache.celeborn.rest.v1.model.{AppDiskUsagesResponse, ApplicationsResponse, ShufflePartitionsResponse, ShufflesResponse, UnAvailablePeersResponse, WorkerInfoResponse}
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.api.v1.ApiV1BaseResourceSuite
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker

class ApiV1WorkerResourceSuite extends ApiV1BaseResourceSuite with MiniClusterFeature {
  private var worker: Worker = _
  override protected def httpService: HttpService = worker

  override def beforeAll(): Unit = {
    logInfo("test initialized, setup celeborn mini cluster")
    val (m, w) =
      setupMiniClusterWithRandomPorts(workerConf = celebornConf.getAll.toMap, workerNum = 1)
    worker = w.head
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    logInfo("all test complete, stop celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("shuffle resource") {
    var response = webTarget.path("shuffles").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[ShufflesResponse]).getShuffleIds.isEmpty)

    response = webTarget.path("shuffles/partitions").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    val partitions = response.readEntity(classOf[ShufflePartitionsResponse])
    assert(partitions.getPrimaryPartitions.isEmpty)
    assert(partitions.getReplicaPartitions.isEmpty)
  }

  test("application resource") {
    var response = webTarget.path("applications").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[ApplicationsResponse]).getApplications.isEmpty)

    response =
      webTarget.path("applications/top_disk_usages").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[AppDiskUsagesResponse]).getAppDiskUsages.isEmpty)
  }

  test("worker resource") {
    var response = webTarget.path("workers").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    val workerData = response.readEntity(classOf[WorkerInfoResponse])
    assert(workerData.getIsRegistered)
    assert(!workerData.getIsShutdown)
    assert(!workerData.getIsDecommissioning)
    assert(workerData.getVersion === org.apache.celeborn.common.CELEBORN_VERSION)

    response = webTarget.path("workers/unavailable_peers").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[UnAvailablePeersResponse]).getPeers.isEmpty)
  }
}
