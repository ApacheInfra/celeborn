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

package org.apache.celeborn.service.deploy.master.clustermeta;

import java.util.List;
import java.util.Map;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.ApplicationMeta;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.meta.WorkerStatus;
import org.apache.celeborn.common.quota.ResourceConsumption;

public interface IMetadataHandler {
  void handleRequestSlots(
      String shuffleKey,
      String hostName,
      Map<String, Map<String, Integer>> workerToAllocatedSlots,
      String requestId);

  void handleUnRegisterShuffle(String shuffleKey, String requestId);

  void handleBatchUnRegisterShuffles(List<String> shuffleKeys, String requestId);

  void handleAppHeartbeat(
      String appId,
      long totalWritten,
      long fileCount,
      long shuffleCount,
      long applicationCount,
      Map<String, Long> shuffleFallbackCounts,
      Map<String, Long> applicationFallbackCounts,
      long time,
      String requestId);

  void handleAppLost(String appId, String requestId);

  void handleWorkerExclude(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove, String requestId);

  void handleReviseLostShuffles(String appId, List<Integer> shuffles, String requestId);

  void handleWorkerLost(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort, String requestId);

  void handleRemoveWorkersUnavailableInfo(List<WorkerInfo> unavailableWorkers, String requestId);

  void handleWorkerHeartbeat(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Map<String, DiskInfo> disks,
      Map<UserIdentifier, ResourceConsumption> userResourceConsumption,
      long time,
      boolean highWorkload,
      WorkerStatus workerStatus,
      String requestId);

  void handleRegisterWorker(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      int internalPort,
      String networkLocation,
      Map<String, DiskInfo> disks,
      Map<UserIdentifier, ResourceConsumption> userResourceConsumption,
      String requestId);

  void handleReportWorkerUnavailable(List<WorkerInfo> failedNodes, String requestId);

  void handleWorkerEvent(
      int workerEventTypeValue, List<WorkerInfo> workerInfoList, String requestId);

  void handleUpdatePartitionSize();

  void handleApplicationMeta(ApplicationMeta applicationMeta);

  void handleReportWorkerDecommission(List<WorkerInfo> workers, String requestId);
}
