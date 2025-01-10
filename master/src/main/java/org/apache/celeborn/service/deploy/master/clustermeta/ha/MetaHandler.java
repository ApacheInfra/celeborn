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

package org.apache.celeborn.service.deploy.master.clustermeta.ha;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.ApplicationMeta;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.meta.WorkerStatus;
import org.apache.celeborn.common.quota.ResourceConsumption;
import org.apache.celeborn.common.util.CollectionUtils;
import org.apache.celeborn.service.deploy.master.clustermeta.MetaUtil;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.ResourceResponse;

public class MetaHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetaHandler.class);

  private final HAMasterMetaManager metaSystem;

  public MetaHandler(HAMasterMetaManager metaSystem) {
    this.metaSystem = metaSystem;
  }

  public void setUpMasterRatisServer(CelebornConf conf, MasterClusterInfo masterClusterInfo)
      throws IOException {
    metaSystem.setRatisServer(
        HARaftServer.newMasterRatisServer(
            this, conf, masterClusterInfo.localNode(), masterClusterInfo.peerNodes()));
    metaSystem.getRatisServer().start();
  }

  /**
   * Get an initial MasterMetaResponse.Builder with proper request cmdType.
   *
   * @param request MasterMetaRequest.
   * @return MasterMetaResponse builder.
   */
  public static ResourceResponse.Builder getMasterMetaResponseBuilder(
      ResourceProtos.ResourceRequest request) {
    return ResourceResponse.newBuilder()
        .setCmdType(request.getCmdType())
        .setStatus(ResourceProtos.Status.OK)
        .setSuccess(true);
  }

  public ResourceResponse handleReadRequest(ResourceProtos.ResourceRequest request) {
    ResourceProtos.Type cmdType = request.getCmdType();
    ResourceResponse.Builder responseBuilder = getMasterMetaResponseBuilder(request);
    try {
      switch (cmdType) {
        default:
          throw new IOException("Can not parse this command!" + request);
      }
    } catch (IOException e) {
      LOG.warn("Handle meta read request {} failed!", cmdType, e);
      responseBuilder.setSuccess(false);
      responseBuilder.setStatus(ResourceProtos.Status.INTERNAL_ERROR);
      if (e.getMessage() != null) {
        responseBuilder.setMessage(e.getMessage());
      }
    }
    return responseBuilder.build();
  }

  public ResourceResponse handleWriteRequest(ResourceProtos.ResourceRequest request) {
    ResourceProtos.Type cmdType = request.getCmdType();
    ResourceResponse.Builder responseBuilder = getMasterMetaResponseBuilder(request);
    try {
      String shuffleKey;
      String appId;
      String host;
      int rpcPort;
      int pushPort;
      int fetchPort;
      int replicatePort;
      Map<String, DiskInfo> diskInfos;
      Map<UserIdentifier, ResourceConsumption> userResourceConsumption;
      WorkerStatus workerStatus;
      List<Integer> lostShuffles;
      switch (cmdType) {
        case ReviseLostShuffles:
          appId = request.getReviseLostShufflesRequest().getAppId();
          lostShuffles = request.getReviseLostShufflesRequest().getLostShufflesList();
          LOG.info(
              "Handle revise lost shuffles for {} {}", appId, StringUtils.join(lostShuffles, ","));
          metaSystem.reviseLostShuffles(appId, lostShuffles);
          break;

        case RequestSlots:
          shuffleKey = request.getRequestSlotsRequest().getShuffleKey();
          LOG.debug("Handle request slots for {}", shuffleKey);
          metaSystem.updateRequestSlotsMeta(
              shuffleKey, request.getRequestSlotsRequest().getHostName(), new HashMap<>());
          break;

        case ReleaseSlots:
          break;

        case UnRegisterShuffle:
          shuffleKey = request.getUnregisterShuffleRequest().getShuffleKey();
          LOG.debug("Handle unregister shuffle for {}", shuffleKey);
          metaSystem.updateUnregisterShuffleMeta(shuffleKey);
          break;

        case BatchUnRegisterShuffle:
          List<String> shuffleKeys =
              request.getBatchUnregisterShuffleRequest().getShuffleKeysList();
          metaSystem.updateBatchUnregisterShuffleMeta(shuffleKeys);
          LOG.debug("Handle batch unregister shuffle for {}", shuffleKeys);
          break;

        case AppHeartbeat:
          appId = request.getAppHeartbeatRequest().getAppId();
          long time = request.getAppHeartbeatRequest().getTime();
          long totalWritten = request.getAppHeartbeatRequest().getTotalWritten();
          long fileCount = request.getAppHeartbeatRequest().getFileCount();
          long shuffleCount = request.getAppHeartbeatRequest().getShuffleCount();
          long applicationCount = request.getAppHeartbeatRequest().getApplicationCount();
          LOG.debug(
              "Handle app heartbeat for {} with shuffle count {} and application count {}",
              appId,
              shuffleCount,
              applicationCount);
          Map<String, Long> shuffleFallbackCounts =
              request.getAppHeartbeatRequest().getShuffleFallbackCountsMap();
          if (CollectionUtils.isNotEmpty(shuffleFallbackCounts)) {
            LOG.warn(
                "{} shuffle fallbacks in app {}",
                shuffleFallbackCounts.values().stream().mapToLong(v -> v).sum(),
                appId);
          }
          Map<String, Long> applicationFallbackCounts =
              request.getAppHeartbeatRequest().getApplicationFallbackCountsMap();
          metaSystem.updateAppHeartbeatMeta(
              appId,
              time,
              totalWritten,
              fileCount,
              shuffleCount,
              applicationCount,
              shuffleFallbackCounts,
              applicationFallbackCounts);
          break;

        case AppLost:
          appId = request.getAppLostRequest().getAppId();
          LOG.debug("Handle app lost for {}", appId);
          metaSystem.updateAppLostMeta(appId);
          break;

        case WorkerExclude:
          List<ResourceProtos.WorkerAddress> addAddresses =
              request.getWorkerExcludeRequest().getWorkersToAddList();
          List<ResourceProtos.WorkerAddress> removeAddresses =
              request.getWorkerExcludeRequest().getWorkersToRemoveList();
          List<WorkerInfo> workersToAdd =
              addAddresses.stream().map(MetaUtil::addrToInfo).collect(Collectors.toList());
          List<WorkerInfo> workersToRemove =
              removeAddresses.stream().map(MetaUtil::addrToInfo).collect(Collectors.toList());
          metaSystem.updateManuallyExcludedWorkersMeta(workersToAdd, workersToRemove);
          break;

        case WorkerLost:
          host = request.getWorkerLostRequest().getHost();
          rpcPort = request.getWorkerLostRequest().getRpcPort();
          pushPort = request.getWorkerLostRequest().getPushPort();
          fetchPort = request.getWorkerLostRequest().getFetchPort();
          replicatePort = request.getWorkerLostRequest().getReplicatePort();
          LOG.debug("Handle worker lost for {} {}", host, pushPort);
          metaSystem.updateWorkerLostMeta(host, rpcPort, pushPort, fetchPort, replicatePort);
          break;

        case WorkerRemove:
          // TODO: Remove `WorkerRemove` in 0.7.x version to guarantee upgrade compatibility.
          host = request.getWorkerRemoveRequest().getHost();
          rpcPort = request.getWorkerRemoveRequest().getRpcPort();
          pushPort = request.getWorkerRemoveRequest().getPushPort();
          fetchPort = request.getWorkerRemoveRequest().getFetchPort();
          replicatePort = request.getWorkerRemoveRequest().getReplicatePort();
          LOG.debug("Handle worker remove for {} {}", host, pushPort);
          metaSystem.updateWorkerRemoveMeta(host, rpcPort, pushPort, fetchPort, replicatePort);
          break;

        case WorkerHeartbeat:
          host = request.getWorkerHeartbeatRequest().getHost();
          rpcPort = request.getWorkerHeartbeatRequest().getRpcPort();
          pushPort = request.getWorkerHeartbeatRequest().getPushPort();
          fetchPort = request.getWorkerHeartbeatRequest().getFetchPort();
          diskInfos = MetaUtil.fromPbDiskInfos(request.getWorkerHeartbeatRequest().getDisksMap());
          userResourceConsumption =
              MetaUtil.fromPbUserResourceConsumption(
                  request.getWorkerHeartbeatRequest().getUserResourceConsumptionMap());
          replicatePort = request.getWorkerHeartbeatRequest().getReplicatePort();
          boolean highWorkload = request.getWorkerHeartbeatRequest().getHighWorkload();
          if (request.getWorkerHeartbeatRequest().hasWorkerStatus()) {
            workerStatus =
                MetaUtil.fromPbWorkerStatus(request.getWorkerHeartbeatRequest().getWorkerStatus());
          } else {
            workerStatus = WorkerStatus.normalWorkerStatus();
          }

          LOG.debug(
              "Handle worker heartbeat for {} {} {} {} {} {} {}",
              host,
              rpcPort,
              pushPort,
              fetchPort,
              replicatePort,
              diskInfos,
              userResourceConsumption);
          metaSystem.updateWorkerHeartbeatMeta(
              host,
              rpcPort,
              pushPort,
              fetchPort,
              replicatePort,
              diskInfos,
              userResourceConsumption,
              request.getWorkerHeartbeatRequest().getTime(),
              workerStatus,
              highWorkload);
          break;

        case RegisterWorker:
          host = request.getRegisterWorkerRequest().getHost();
          rpcPort = request.getRegisterWorkerRequest().getRpcPort();
          pushPort = request.getRegisterWorkerRequest().getPushPort();
          fetchPort = request.getRegisterWorkerRequest().getFetchPort();
          replicatePort = request.getRegisterWorkerRequest().getReplicatePort();
          String networkLocation = request.getRegisterWorkerRequest().getNetworkLocation();
          int internalPort = request.getRegisterWorkerRequest().getInternalPort();
          diskInfos = MetaUtil.fromPbDiskInfos(request.getRegisterWorkerRequest().getDisksMap());
          userResourceConsumption =
              MetaUtil.fromPbUserResourceConsumption(
                  request.getRegisterWorkerRequest().getUserResourceConsumptionMap());
          LOG.debug(
              "Handle worker register for {} {} {} {} {} {} {} {}",
              host,
              rpcPort,
              pushPort,
              fetchPort,
              replicatePort,
              internalPort,
              diskInfos,
              userResourceConsumption);
          metaSystem.updateRegisterWorkerMeta(
              host,
              rpcPort,
              pushPort,
              fetchPort,
              replicatePort,
              internalPort,
              networkLocation,
              diskInfos,
              userResourceConsumption);
          break;

        case ReportWorkerUnavailable:
          List<ResourceProtos.WorkerAddress> failedAddress =
              request.getReportWorkerUnavailableRequest().getUnavailableList();
          List<WorkerInfo> failedWorkers =
              failedAddress.stream().map(MetaUtil::addrToInfo).collect(Collectors.toList());
          metaSystem.updateMetaByReportWorkerUnavailable(failedWorkers);
          break;

        case UpdatePartitionSize:
          metaSystem.updatePartitionSize();
          break;

        case RemoveWorkersUnavailableInfo:
          List<ResourceProtos.WorkerAddress> unavailableList =
              request.getRemoveWorkersUnavailableInfoRequest().getUnavailableList();
          List<WorkerInfo> unavailableWorkers =
              unavailableList.stream().map(MetaUtil::addrToInfo).collect(Collectors.toList());
          metaSystem.removeWorkersUnavailableInfoMeta(unavailableWorkers);
          break;

        case WorkerEvent:
          List<ResourceProtos.WorkerAddress> workerAddresses =
              request.getWorkerEventRequest().getWorkerAddressList();
          List<WorkerInfo> workerInfoList =
              workerAddresses.stream().map(MetaUtil::addrToInfo).collect(Collectors.toList());
          metaSystem.updateWorkerEventMeta(
              request.getWorkerEventRequest().getWorkerEventType().getNumber(), workerInfoList);
          break;

        case ApplicationMeta:
          appId = request.getApplicationMetaRequest().getAppId();
          String secret = request.getApplicationMetaRequest().getSecret();
          metaSystem.updateApplicationMeta(new ApplicationMeta(appId, secret));
          break;

        case ReportWorkerDecommission:
          List<ResourceProtos.WorkerAddress> decommissionList =
              request.getReportWorkerDecommissionRequest().getWorkersList();
          List<WorkerInfo> decommissionWorkers =
              decommissionList.stream().map(MetaUtil::addrToInfo).collect(Collectors.toList());
          metaSystem.updateMetaByReportWorkerDecommission(decommissionWorkers);
          break;

        default:
          throw new IOException("Can not parse this command!" + request);
      }
      responseBuilder.setStatus(ResourceProtos.Status.OK);
    } catch (IOException e) {
      LOG.warn("Handle meta write request {} failed!", cmdType, e);
      responseBuilder.setSuccess(false);
      responseBuilder.setStatus(ResourceProtos.Status.INTERNAL_ERROR);
      if (e.getMessage() != null) {
        responseBuilder.setMessage(e.getMessage());
      }
    }
    return responseBuilder.build();
  }

  public void writeToSnapShot(File file) throws IOException {
    try {
      metaSystem.writeMetaInfoToFile(file);
    } catch (RuntimeException e) {
      throw new IOException(e.getCause());
    }
  }

  public void loadSnapShot(File file) throws IOException {
    try {
      metaSystem.restoreMetaFromFile(file);
    } catch (RuntimeException e) {
      throw new IOException(e.getCause());
    }
  }
}
