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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import scala.Option;
import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.ApplicationMeta;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.WorkerEventInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.meta.WorkerStatus;
import org.apache.celeborn.common.network.CelebornRackResolver;
import org.apache.celeborn.common.protocol.PbSnapshotMetaInfo;
import org.apache.celeborn.common.protocol.PbWorkerStatus;
import org.apache.celeborn.common.quota.ResourceConsumption;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.PbSerDeUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.util.WorkerStatusUtils;

/**
 * Note: Do not update the worker collections directly from outside the metadata manager, especially
 * {@link #workersMap}, {@link #workerEventInfos}, {@link #shutdownWorkers}, {@link
 * #excludedWorkers}, {@link #manuallyExcludedWorkers}, {@link #availableWorkers}.
 *
 * <p>All updates should be done through the provided methods to ensure consistency.
 */
public abstract class AbstractMetaManager implements IMetadataHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetaManager.class);

  // Metadata for master service
  public final Map<String, Set<Integer>> registeredAppAndShuffles =
      JavaUtils.newConcurrentHashMap();
  public final Set<String> hostnameSet = ConcurrentHashMap.newKeySet();
  public final Map<String, WorkerInfo> workersMap = JavaUtils.newConcurrentHashMap();
  public final Set<WorkerInfo> availableWorkers = ConcurrentHashMap.newKeySet();

  public final ConcurrentHashMap<WorkerInfo, Long> lostWorkers = JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<WorkerInfo, WorkerEventInfo> workerEventInfos =
      JavaUtils.newConcurrentHashMap();
  public final ConcurrentHashMap<String, Long> appHeartbeatTime = JavaUtils.newConcurrentHashMap();
  public final Set<WorkerInfo> excludedWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> manuallyExcludedWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> shutdownWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> decommissionWorkers = ConcurrentHashMap.newKeySet();
  public final Set<WorkerInfo> workerLostEvents = ConcurrentHashMap.newKeySet();

  protected RpcEnv rpcEnv;
  protected CelebornConf conf;
  protected CelebornRackResolver rackResolver;

  public long initialEstimatedPartitionSize;
  public long estimatedPartitionSize;
  public double unhealthyDiskRatioThreshold;
  public final LongAdder partitionTotalWritten = new LongAdder();
  public final LongAdder partitionTotalFileCount = new LongAdder();
  public final LongAdder shuffleTotalCount = new LongAdder();
  public final Map<String, Long> shuffleFallbackCounts = JavaUtils.newConcurrentHashMap();

  public final ConcurrentHashMap<String, ApplicationMeta> applicationMetas =
      JavaUtils.newConcurrentHashMap();

  public void updateRequestSlotsMeta(
      String shuffleKey, String hostName, Map<String, Map<String, Integer>> workerWithAllocations) {
    Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
    registeredAppAndShuffles
        .computeIfAbsent(appIdShuffleId._1(), v -> new HashSet<>())
        .add((Integer) appIdShuffleId._2);

    String appId = appIdShuffleId._1;
    appHeartbeatTime.compute(
        appId,
        (applicationId, oldTimestamp) -> {
          long oldTime = System.currentTimeMillis();
          if (oldTimestamp != null) {
            oldTime = oldTimestamp;
          }
          return Math.max(System.currentTimeMillis(), oldTime);
        });

    if (hostName != null) {
      hostnameSet.add(hostName);
    }
  }

  public void updateUnregisterShuffleMeta(String shuffleKey) {
    Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
    Set<Integer> shuffleIds = registeredAppAndShuffles.get(appIdShuffleId._1());
    if (shuffleIds != null) {
      shuffleIds.remove(appIdShuffleId._2);
      registeredAppAndShuffles.compute(
          appIdShuffleId._1(),
          (s, shuffles) -> {
            if (shuffles.size() == 0) {
              return null;
            }
            return shuffles;
          });
    }
  }

  public void updateBatchUnregisterShuffleMeta(List<String> shuffleKeys) {
    for (String shuffleKey : shuffleKeys) {
      Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
      String appId = appIdShuffleId._1;
      if (registeredAppAndShuffles.containsKey(appId)) {
        registeredAppAndShuffles.get(appId).remove(appIdShuffleId._2);
      }
    }
  }

  public void updateAppHeartbeatMeta(
      String appId,
      long time,
      long totalWritten,
      long fileCount,
      long shuffleCount,
      Map<String, Long> shuffleFallbackCounts) {
    appHeartbeatTime.put(appId, time);
    partitionTotalWritten.add(totalWritten);
    partitionTotalFileCount.add(fileCount);
    shuffleTotalCount.add(shuffleCount);
    addShuffleFallbackCounts(shuffleFallbackCounts);
  }

  public void updateAppLostMeta(String appId) {
    registeredAppAndShuffles.remove(appId);
    appHeartbeatTime.remove(appId);
    applicationMetas.remove(appId);
  }

  @VisibleForTesting
  public void updateExcludedWorkersMeta(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove) {
    workersToAdd.forEach(
        worker -> {
          excludedWorkers.add(worker);
          availableWorkers.remove(worker);
        });
    workersToRemove.forEach(
        worker -> {
          excludedWorkers.remove(worker);
          updateAvailableWorkers(worker);
        });
  }

  public void updateManuallyExcludedWorkersMeta(
      List<WorkerInfo> workersToAdd, List<WorkerInfo> workersToRemove) {
    workersToAdd.forEach(
        worker -> {
          manuallyExcludedWorkers.add(worker);
          availableWorkers.remove(worker);
        });
    workersToRemove.forEach(
        worker -> {
          manuallyExcludedWorkers.remove(worker);
          updateAvailableWorkers(worker);
        });
  }

  public void reviseLostShuffles(String appId, List<Integer> lostShuffles) {
    registeredAppAndShuffles.computeIfAbsent(appId, v -> new HashSet<>()).addAll(lostShuffles);
  }

  public void deleteApp(String appId) {
    registeredAppAndShuffles.remove(appId);
  }

  public void updateWorkerLostMeta(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    WorkerInfo worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
    workerLostEvents.add(worker);
    // remove worker from workers
    synchronized (workersMap) {
      workersMap.remove(worker.toUniqueId());
      lostWorkers.put(worker, System.currentTimeMillis());
      availableWorkers.remove(worker);
    }
    excludedWorkers.remove(worker);
    workerLostEvents.remove(worker);
  }

  public void updateWorkerRemoveMeta(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    WorkerInfo worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
    // remove worker from workers
    synchronized (workersMap) {
      workersMap.remove(worker.toUniqueId());
      lostWorkers.put(worker, System.currentTimeMillis());
      availableWorkers.remove(worker);
    }
    excludedWorkers.remove(worker);
  }

  public void removeWorkersUnavailableInfoMeta(List<WorkerInfo> unavailableWorkers) {
    synchronized (workersMap) {
      for (WorkerInfo workerInfo : unavailableWorkers) {
        if (lostWorkers.containsKey(workerInfo)) {
          lostWorkers.remove(workerInfo);
          shutdownWorkers.remove(workerInfo);
          workerEventInfos.remove(workerInfo);
          decommissionWorkers.remove(workerInfo);
          updateAvailableWorkers(workerInfo);
        }
      }
    }
  }

  public void updateWorkerHeartbeatMeta(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Map<String, DiskInfo> disks,
      long time,
      WorkerStatus workerStatus,
      boolean highWorkload) {
    WorkerInfo worker =
        new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort, -1, disks, null);
    AtomicLong availableSlots = new AtomicLong();
    LOG.debug("update worker {}:{} heartbeat {}", host, rpcPort, disks);
    synchronized (workersMap) {
      Optional<WorkerInfo> workerInfo = Optional.ofNullable(workersMap.get(worker.toUniqueId()));
      workerInfo.ifPresent(
          info -> {
            info.updateThenGetDiskInfos(disks, Option.apply(estimatedPartitionSize));
            availableSlots.set(info.totalAvailableSlots());
            info.lastHeartbeat_$eq(time);
            info.setWorkerStatus(workerStatus);
          });
    }

    WorkerEventInfo workerEventInfo = workerEventInfos.get(worker);
    if (workerEventInfo != null
        && WorkerStatusUtils.meetFinalState(workerEventInfo, workerStatus)) {
      workerEventInfos.remove(worker);
      if (workerStatus.getState() == PbWorkerStatus.State.Normal) {
        shutdownWorkers.remove(worker);
      }
    }

    // If using HDFSONLY mode, workers with empty disks should not be put into excluded worker list.
    long unhealthyDiskNum =
        disks.values().stream().filter(s -> !s.status().equals(DiskStatus.HEALTHY)).count();
    boolean exceed = unhealthyDiskNum * 1.0 / disks.size() >= unhealthyDiskRatioThreshold;
    if (!excludedWorkers.contains(worker)
        && (((disks.isEmpty() || exceed) && !conf.hasHDFSStorage() && !conf.hasS3Storage())
            || highWorkload)) {
      LOG.warn(
          "Worker {} (unhealthy disks num: {}) adds to excluded workers", worker, unhealthyDiskNum);
      excludedWorkers.add(worker);
    } else if ((availableSlots.get() > 0 || conf.hasHDFSStorage() || conf.hasS3Storage())
        && !highWorkload) {
      // only unblack if numSlots larger than 0
      excludedWorkers.remove(worker);
    }

    // try to update the available workers if the worker status is Normal
    if (workerStatus.getState() == PbWorkerStatus.State.Normal) {
      updateAvailableWorkers(worker);
    }
  }

  public void updateRegisterWorkerMeta(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      int internalPort,
      String networkLocation,
      Map<String, DiskInfo> disks,
      Map<UserIdentifier, ResourceConsumption> userResourceConsumption) {
    WorkerInfo workerInfo =
        new WorkerInfo(
            host,
            rpcPort,
            pushPort,
            fetchPort,
            replicatePort,
            internalPort,
            disks,
            userResourceConsumption);
    workerInfo.lastHeartbeat_$eq(System.currentTimeMillis());
    if (networkLocation != null
        && !networkLocation.isEmpty()
        && !NetworkTopology.DEFAULT_RACK.equals(networkLocation)) {
      workerInfo.networkLocation_$eq(networkLocation);
    } else {
      workerInfo.networkLocation_$eq(rackResolver.resolve(host).getNetworkLocation());
    }
    workerInfo.updateDiskMaxSlots(estimatedPartitionSize);
    synchronized (workersMap) {
      workersMap.putIfAbsent(workerInfo.toUniqueId(), workerInfo);
      shutdownWorkers.remove(workerInfo);
      lostWorkers.remove(workerInfo);
      excludedWorkers.remove(workerInfo);
      workerEventInfos.remove(workerInfo);
      decommissionWorkers.remove(workerInfo);
      updateAvailableWorkers(workerInfo);
    }
  }

  /**
   * Used for ratis state machine to take snapshot
   *
   * @param file
   * @throws IOException
   */
  public void writeMetaInfoToFile(File file) throws IOException, RuntimeException {
    byte[] snapshotBytes =
        PbSerDeUtils.toPbSnapshotMetaInfo(
                estimatedPartitionSize,
                registeredAppAndShuffles,
                hostnameSet,
                excludedWorkers,
                manuallyExcludedWorkers,
                workerLostEvents,
                appHeartbeatTime,
                new HashSet(workersMap.values()),
                partitionTotalWritten.sum(),
                partitionTotalFileCount.sum(),
                shuffleTotalCount.sum(),
                shuffleFallbackCounts,
                lostWorkers,
                shutdownWorkers,
                workerEventInfos,
                applicationMetas,
                decommissionWorkers)
            .toByteArray();
    Files.write(file.toPath(), snapshotBytes);
  }

  /**
   * Used for ratis state machine to load snapshot
   *
   * @param file
   * @throws IOException
   */
  public void restoreMetaFromFile(File file) throws IOException {
    try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
      PbSnapshotMetaInfo snapshotMetaInfo = PbSnapshotMetaInfo.parseFrom(in);
      cleanUpState();

      estimatedPartitionSize = snapshotMetaInfo.getEstimatedPartitionSize();

      for (String shuffleKey : snapshotMetaInfo.getRegisteredShuffleList()) {
        Tuple2<String, Object> appIdShuffleId = Utils.splitShuffleKey(shuffleKey);
        registeredAppAndShuffles
            .computeIfAbsent(appIdShuffleId._1, v -> new HashSet<>())
            .add((Integer) appIdShuffleId._2);
      }
      hostnameSet.addAll(snapshotMetaInfo.getHostnameSetList());
      excludedWorkers.addAll(
          snapshotMetaInfo.getExcludedWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));
      manuallyExcludedWorkers.addAll(
          snapshotMetaInfo.getManuallyExcludedWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));
      workerLostEvents.addAll(
          snapshotMetaInfo.getWorkerLostEventsList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));
      appHeartbeatTime.putAll(snapshotMetaInfo.getAppHeartbeatTimeMap());

      registeredAppAndShuffles.forEach(
          (appId, shuffleId) -> {
            if (!appHeartbeatTime.containsKey(appId)) {
              appHeartbeatTime.put(appId, System.currentTimeMillis());
            }
          });

      Set<WorkerInfo> workerInfoSet =
          snapshotMetaInfo.getWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet());
      List<String> workerHostList =
          workerInfoSet.stream()
              .filter(w -> NetworkTopology.DEFAULT_RACK.equals(w.networkLocation()))
              .map(WorkerInfo::host)
              .collect(Collectors.toList());
      scala.collection.immutable.Map<String, Node> resolveMap =
          rackResolver.resolveToMap(workerHostList);
      workersMap.putAll(
          workerInfoSet.stream()
              .peek(
                  workerInfo -> {
                    // Reset worker's network location with current master's configuration.
                    if (NetworkTopology.DEFAULT_RACK.equals(workerInfo.networkLocation())) {
                      workerInfo.networkLocation_$eq(
                          resolveMap.get(workerInfo.host()).get().getNetworkLocation());
                    }
                  })
              .collect(Collectors.toMap(WorkerInfo::toUniqueId, w -> w)));

      snapshotMetaInfo
          .getLostWorkersMap()
          .forEach((key, value) -> lostWorkers.put(WorkerInfo.fromUniqueId(key), value));

      snapshotMetaInfo
          .getWorkerEventInfosMap()
          .entrySet()
          .forEach(
              entry ->
                  workerEventInfos.put(
                      WorkerInfo.fromUniqueId(entry.getKey()),
                      PbSerDeUtils.fromPbWorkerEventInfo(entry.getValue())));

      shutdownWorkers.addAll(
          snapshotMetaInfo.getShutdownWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));

      decommissionWorkers.addAll(
          snapshotMetaInfo.getDecommissionWorkersList().stream()
              .map(PbSerDeUtils::fromPbWorkerInfo)
              .collect(Collectors.toSet()));

      partitionTotalWritten.add(snapshotMetaInfo.getPartitionTotalWritten());
      partitionTotalFileCount.add(snapshotMetaInfo.getPartitionTotalFileCount());
      shuffleTotalCount.add(snapshotMetaInfo.getShuffleTotalCount());
      addShuffleFallbackCounts(snapshotMetaInfo.getShuffleFallbackCountsMap());

      snapshotMetaInfo
          .getApplicationMetasMap()
          .forEach(
              (key, value) -> applicationMetas.put(key, PbSerDeUtils.fromPbApplicationMeta(value)));

      availableWorkers.addAll(
          workersMap.values().stream()
              .filter(worker -> isWorkerAvailable(worker))
              .collect(Collectors.toSet()));
    } catch (Exception e) {
      throw new IOException(e);
    }
    LOG.info("Successfully restore meta info from snapshot {}", file.getAbsolutePath());
    LOG.info(
        "Worker size: {}, Registered shuffle size: {}. Worker excluded list size: {}. Manually Excluded list size: {}",
        workersMap.size(),
        registeredAppAndShuffles.size(),
        excludedWorkers.size(),
        manuallyExcludedWorkers.size());
    workersMap.values().forEach(workerInfo -> LOG.info(workerInfo.toString()));
    registeredAppAndShuffles.forEach(
        (appId, shuffleId) -> LOG.info("RegisteredShuffle {}-{}", appId, shuffleId));
  }

  private void cleanUpState() {
    registeredAppAndShuffles.clear();
    hostnameSet.clear();
    workersMap.clear();
    availableWorkers.clear();
    lostWorkers.clear();
    appHeartbeatTime.clear();
    excludedWorkers.clear();
    shutdownWorkers.clear();
    decommissionWorkers.clear();
    manuallyExcludedWorkers.clear();
    workerLostEvents.clear();
    partitionTotalWritten.reset();
    partitionTotalFileCount.reset();
    shuffleTotalCount.reset();
    shuffleFallbackCounts.clear();
    workerEventInfos.clear();
    applicationMetas.clear();
  }

  public void updateMetaByReportWorkerUnavailable(List<WorkerInfo> failedWorkers) {
    synchronized (this.workersMap) {
      shutdownWorkers.addAll(failedWorkers);
      availableWorkers.removeAll(failedWorkers);
    }
  }

  public void updateWorkerEventMeta(int workerEventTypeValue, List<WorkerInfo> workerInfoList) {
    long eventTime = System.currentTimeMillis();
    ResourceProtos.WorkerEventType eventType =
        ResourceProtos.WorkerEventType.forNumber(workerEventTypeValue);
    synchronized (this.workersMap) {
      for (WorkerInfo workerInfo : workerInfoList) {
        WorkerEventInfo workerEventInfo = workerEventInfos.get(workerInfo);
        LOG.info("Received worker event: {} for worker: {}", eventType, workerInfo.toUniqueId());
        if (workerEventInfo == null || !workerEventInfo.isSameEvent(eventType.getNumber())) {
          if (eventType == ResourceProtos.WorkerEventType.None) {
            workerEventInfos.remove(workerInfo);
            updateAvailableWorkers(workerInfo);
          } else {
            workerEventInfos.put(workerInfo, new WorkerEventInfo(eventType.getNumber(), eventTime));
            availableWorkers.remove(workerInfo);
          }
        }
      }
    }
  }

  public void updateMetaByReportWorkerDecommission(List<WorkerInfo> workers) {
    synchronized (this.workersMap) {
      decommissionWorkers.addAll(workers);
      availableWorkers.removeAll(workers);
    }
  }

  public void updatePartitionSize() {
    long oldEstimatedPartitionSize = estimatedPartitionSize;
    long tmpTotalWritten = partitionTotalWritten.sumThenReset();
    long tmpFileCount = partitionTotalFileCount.sumThenReset();
    LOG.debug(
        "update partition size total written {}, file count {}",
        Utils.bytesToString(tmpTotalWritten),
        tmpFileCount);
    if (tmpFileCount != 0) {
      estimatedPartitionSize =
          Math.max(
              conf.minPartitionSizeToEstimate(),
              Math.min(tmpTotalWritten / tmpFileCount, conf.maxPartitionSizeToEstimate()));
    } else {
      estimatedPartitionSize = initialEstimatedPartitionSize;
    }

    // Do not trigger update is estimated partition size value is unchanged
    if (estimatedPartitionSize == oldEstimatedPartitionSize) {
      return;
    }

    LOG.warn(
        "Celeborn cluster estimated partition size changed from {} to {}",
        Utils.bytesToString(oldEstimatedPartitionSize),
        Utils.bytesToString(estimatedPartitionSize));

    HashSet<WorkerInfo> workers = new HashSet(workersMap.values());
    excludedWorkers.forEach(workers::remove);
    manuallyExcludedWorkers.forEach(workers::remove);
    workers.forEach(workerInfo -> workerInfo.updateDiskMaxSlots(estimatedPartitionSize));
  }

  private boolean isWorkerAvailable(WorkerInfo workerInfo) {
    return (workerInfo.getWorkerStatus().getState() == PbWorkerStatus.State.Normal
            && !workerEventInfos.containsKey(workerInfo))
        && !excludedWorkers.contains(workerInfo)
        && !shutdownWorkers.contains(workerInfo)
        && !manuallyExcludedWorkers.contains(workerInfo);
  }

  private void updateAvailableWorkers(WorkerInfo worker) {
    synchronized (workersMap) {
      Optional<WorkerInfo> workerInfo = Optional.ofNullable(workersMap.get(worker.toUniqueId()));
      if (workerInfo.map(this::isWorkerAvailable).orElse(false)) {
        availableWorkers.add(workerInfo.get());
      } else {
        availableWorkers.remove(worker);
      }
    }
  }

  public void updateApplicationMeta(ApplicationMeta applicationMeta) {
    applicationMetas.putIfAbsent(applicationMeta.appId(), applicationMeta);
  }

  public void removeApplicationMeta(String appId) {
    applicationMetas.remove(appId);
  }

  public int registeredShuffleCount() {
    return registeredAppAndShuffles.values().stream().mapToInt(Set::size).sum();
  }

  private void addShuffleFallbackCounts(Map<String, Long> fallbackCounts) {
    for (String fallbackPolicy : fallbackCounts.keySet()) {
      shuffleFallbackCounts.compute(
          fallbackPolicy, (k, v) -> v == null ? fallbackCounts.get(k) : v + fallbackCounts.get(k));
    }
  }

  public void updateWorkerResourceConsumptions(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Map<UserIdentifier, ResourceConsumption> resourceConsumptions) {
    WorkerInfo worker =
        new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort, -1, null, null);
    synchronized (workersMap) {
      Optional<WorkerInfo> workerInfo = Optional.ofNullable(workersMap.get(worker.toUniqueId()));
      workerInfo.ifPresent(info -> info.updateThenGetUserResourceConsumption(resourceConsumptions));
    }
  }
}
