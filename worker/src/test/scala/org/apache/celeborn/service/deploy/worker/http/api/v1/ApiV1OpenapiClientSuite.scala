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

import java.util.Collections
import javax.servlet.http.HttpServletResponse

import scala.collection.JavaConverters._

import org.apache.celeborn.rest.v1.master._
import org.apache.celeborn.rest.v1.master.invoker._
import org.apache.celeborn.rest.v1.model.{ExcludeWorkerRequest, RemoveWorkersUnavailableInfoRequest, SendWorkerEventRequest, WorkerId}
import org.apache.celeborn.rest.v1.model.SendWorkerEventRequest.EventTypeEnum

class ApiV1OpenapiClientSuite extends ApiV1WorkerOpenapiClientSuite {
  private var masterApiClient: ApiClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    masterApiClient = new ApiClient().setBasePath(s"http://${master.connectionUrl}")
  }

  test("master: default api") {
    val api = new DefaultApi(masterApiClient)
    assert(!api.getThreadDump.getThreadStacks.isEmpty)
    assert(
      api.getContainerInfo.getContainerAddress.nonEmpty && api.getContainerInfo.getContainerAddress.nonEmpty)
  }

  test("master: conf api") {
    val api = new ConfApi(masterApiClient)
    assert(!api.getConf.getConfigs.isEmpty)
    val e = intercept[ApiException](api.getDynamicConf("", "", ""))
    assert(e.getCode == HttpServletResponse.SC_SERVICE_UNAVAILABLE)
    assert(e.getMessage.contains("Dynamic configuration is disabled"))
  }

  test("master: application api") {
    val api = new ApplicationApi(masterApiClient)
    assert(api.getApplications.getApplications.isEmpty)
    assert(api.getApplicationHostNames.getHostnames.isEmpty)
    assert(api.getApplicationsDiskUsageSnapshots.getSnapshots.isEmpty)
  }

  test("master: master api") {
    val api = new MasterApi(masterApiClient)
    val e = intercept[ApiException](api.getMasterGroupInfo)
    assert(e.getCode == HttpServletResponse.SC_BAD_REQUEST)
    assert(e.getMessage.contains("HA is not enabled"))
  }

  test("master: shuffle api") {
    val api = new ShuffleApi(masterApiClient)
    assert(api.getShuffles.getShuffleIds.isEmpty)
  }

  test("master: worker api") {
    val api = new WorkerApi(masterApiClient)
    var workersResponse = api.getWorkers
    assert(!workersResponse.getWorkers.isEmpty)
    assert(workersResponse.getWorkers.asScala.forall(
      _.getVersion === org.apache.celeborn.common.CELEBORN_VERSION))
    assert(workersResponse.getLostWorkers.isEmpty)
    assert(workersResponse.getExcludedWorkers.isEmpty)
    assert(workersResponse.getManualExcludedWorkers.isEmpty)
    assert(workersResponse.getShutdownWorkers.isEmpty)
    assert(workersResponse.getDecommissioningWorkers.isEmpty)

    val workerData = workersResponse.getWorkers.get(0)
    val workerId = new WorkerId()
      .host(workerData.getHost)
      .rpcPort(workerData.getRpcPort)
      .pushPort(workerData.getPushPort)
      .fetchPort(workerData.getFetchPort)
      .replicatePort(workerData.getReplicatePort)
    var handleResponse = api.excludeWorker(
      new ExcludeWorkerRequest().addAddItem(workerId).remove(Collections.emptyList()))
    assert(handleResponse.getSuccess)

    workersResponse = api.getWorkers
    assert(!workersResponse.getWorkers.isEmpty)
    assert(!workersResponse.getExcludedWorkers.isEmpty)
    assert(!workersResponse.getManualExcludedWorkers.isEmpty)

    handleResponse = api.excludeWorker(
      new ExcludeWorkerRequest().addRemoveItem(workerId).add(Collections.emptyList()))
    assert(handleResponse.getSuccess)

    handleResponse = api.removeWorkersUnavailableInfo(
      new RemoveWorkersUnavailableInfoRequest().addWorkersItem(workerId));
    assert(handleResponse.getSuccess)

    workersResponse = api.getWorkers
    assert(!workersResponse.getWorkers.isEmpty)
    assert(workersResponse.getExcludedWorkers.isEmpty)
    assert(workersResponse.getManualExcludedWorkers.isEmpty)

    assert(api.getWorkerEvents.getWorkerEvents.isEmpty)

    handleResponse = api.sendWorkerEvent(
      new SendWorkerEventRequest().addWorkersItem(workerId).eventType(
        EventTypeEnum.DECOMMISSIONTHENIDLE))
    assert(handleResponse.getSuccess)

    assert(!api.getWorkerEvents.getWorkerEvents.isEmpty)
  }
}
