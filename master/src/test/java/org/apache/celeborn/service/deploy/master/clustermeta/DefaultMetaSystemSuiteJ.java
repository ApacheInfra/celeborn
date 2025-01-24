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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.client.MasterClient;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.ApplicationMeta;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.meta.WorkerStatus;
import org.apache.celeborn.common.quota.ResourceConsumption;
import org.apache.celeborn.common.rpc.RpcEndpointAddress;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.rpc.netty.NettyRpcEndpointRef;

public class DefaultMetaSystemSuiteJ {

  private final RpcEnv mockRpcEnv = mock(RpcEnv.class);
  private final CelebornConf conf = new CelebornConf();
  private AbstractMetaManager statusSystem;
  private final RpcEndpointRef dummyRef =
      new NettyRpcEndpointRef(
          new CelebornConf(), RpcEndpointAddress.apply("localhost", 111, "dummy"), null);
  private final AtomicLong callerId = new AtomicLong();

  private static final String HOSTNAME1 = "host1";
  private static final int RPCPORT1 = 1111;
  private static final int PUSHPORT1 = 1112;
  private static final int FETCHPORT1 = 1113;
  private static final int REPLICATEPORT1 = 1114;
  private static final int INTERNALPORT1 = 1115;
  private static final String NETWORK_LOCATION1 = "networkLocation1";
  private static final Map<String, DiskInfo> disks1 = new HashMap<>();
  private static final Map<UserIdentifier, ResourceConsumption> userResourceConsumption1 =
      new HashMap<>();

  private static final String HOSTNAME2 = "host2";
  private static final int RPCPORT2 = 2111;
  private static final int PUSHPORT2 = 2112;
  private static final int FETCHPORT2 = 2113;
  private static final int REPLICATEPORT2 = 2114;
  private static final int INTERNALPORT2 = 2115;
  private static final String NETWORK_LOCATION2 = "networkLocation2";
  private static final Map<String, DiskInfo> disks2 = new HashMap<>();
  private static final Map<UserIdentifier, ResourceConsumption> userResourceConsumption2 =
      new HashMap<>();

  private static final String HOSTNAME3 = "host3";
  private static final int RPCPORT3 = 3111;
  private static final int PUSHPORT3 = 3112;
  private static final int FETCHPORT3 = 3113;
  private static final int REPLICATEPORT3 = 3114;
  private static final int INTERNALPORT3 = 3115;
  private static final String NETWORK_LOCATION3 = "networkLocation3";
  private static final String POLICY1 = "ShufflePartitionsFallbackPolicy";
  private static final String POLICY2 = "QuotaFallbackPolicy";
  private static final Map<String, DiskInfo> disks3 = new HashMap<>();
  private static final Map<UserIdentifier, ResourceConsumption> userResourceConsumption3 =
      new HashMap<>();

  private static final WorkerStatus workerStatus = WorkerStatus.normalWorkerStatus();

  @Before
  public void setUp() {
    when(mockRpcEnv.setupEndpointRef(any(), any())).thenReturn(dummyRef);
    statusSystem = new SingleMasterMetaManager(mockRpcEnv, conf);

    disks1.clear();
    disks1.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks1.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks1.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks1.put("disk4", new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 100, 0));

    disks2.clear();
    disks2.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks2.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks2.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks2.put("disk4", new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 100, 0));

    disks3.clear();
    disks3.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks3.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks3.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks3.put("disk4", new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
  }

  @After
  public void tearDown() {}

  private String getNewReqeustId() {
    return MasterClient.encodeRequestId(UUID.randomUUID().toString(), callerId.incrementAndGet());
  }

  @Test
  public void testHandleRegisterWorker() {

    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    assertEquals(3, statusSystem.workersMap.size());
    assertEquals(3, statusSystem.availableWorkers.size());
  }

  @Test
  public void testHandleWorkerExclude() {
    WorkerInfo workerInfo1 =
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            INTERNALPORT1,
            disks1,
            userResourceConsumption1);
    WorkerInfo workerInfo2 =
        new WorkerInfo(
            HOSTNAME2,
            RPCPORT2,
            PUSHPORT2,
            FETCHPORT2,
            REPLICATEPORT2,
            INTERNALPORT2,
            disks2,
            userResourceConsumption2);

    statusSystem.handleRegisterWorker(
        workerInfo1.host(),
        workerInfo1.rpcPort(),
        workerInfo1.pushPort(),
        workerInfo1.fetchPort(),
        workerInfo1.replicatePort(),
        workerInfo1.internalPort(),
        workerInfo1.networkLocation(),
        workerInfo1.diskInfos(),
        workerInfo1.userResourceConsumption(),
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        workerInfo2.host(),
        workerInfo2.rpcPort(),
        workerInfo2.pushPort(),
        workerInfo2.fetchPort(),
        workerInfo2.replicatePort(),
        workerInfo2.internalPort(),
        workerInfo2.networkLocation(),
        workerInfo2.diskInfos(),
        workerInfo2.userResourceConsumption(),
        getNewReqeustId());

    statusSystem.handleWorkerExclude(
        Arrays.asList(workerInfo1, workerInfo2), Collections.emptyList(), getNewReqeustId());
    assertEquals(2, statusSystem.manuallyExcludedWorkers.size());
    assertEquals(0, statusSystem.availableWorkers.size());

    statusSystem.handleWorkerExclude(
        Collections.emptyList(), Collections.singletonList(workerInfo1), getNewReqeustId());
    assertEquals(1, statusSystem.manuallyExcludedWorkers.size());
    assertEquals(1, statusSystem.availableWorkers.size());
  }

  @Test
  public void testHandleWorkerLost() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    statusSystem.handleWorkerLost(
        HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1, getNewReqeustId());
    assertEquals(2, statusSystem.workersMap.size());
    assertEquals(2, statusSystem.availableWorkers.size());
  }

  private static final String APPID1 = "appId1";
  private static final int SHUFFLEID1 = 1;
  private static final String SHUFFLEKEY1 = APPID1 + "-" + SHUFFLEID1;

  @Test
  public void testHandleRequestSlots() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    WorkerInfo workerInfo1 =
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            INTERNALPORT1,
            disks1,
            userResourceConsumption1);
    WorkerInfo workerInfo2 =
        new WorkerInfo(
            HOSTNAME2,
            RPCPORT2,
            PUSHPORT2,
            FETCHPORT2,
            REPLICATEPORT2,
            INTERNALPORT2,
            disks2,
            userResourceConsumption2);
    WorkerInfo workerInfo3 =
        new WorkerInfo(
            HOSTNAME3,
            RPCPORT3,
            PUSHPORT3,
            FETCHPORT3,
            REPLICATEPORT3,
            INTERNALPORT3,
            disks3,
            userResourceConsumption3);

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation = new HashMap<>();
    allocation.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocation);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocation);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());

    assertEquals(0, workerInfo1.usedSlots());
    assertEquals(0, workerInfo2.usedSlots());
    assertEquals(0, workerInfo3.usedSlots());
  }

  @Test
  public void testHandleReleaseSlots() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    assertEquals(3, statusSystem.workersMap.size());

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation = new HashMap<>();
    allocation.put("disk1", 5);
    workersToAllocate.put(
        statusSystem.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME1))
            .findFirst()
            .get()
            .toUniqueId(),
        allocation);
    workersToAllocate.put(
        statusSystem.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME2))
            .findFirst()
            .get()
            .toUniqueId(),
        allocation);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());
    assertEquals(
        0,
        statusSystem.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME1))
            .findFirst()
            .get()
            .usedSlots());
  }

  @Test
  public void testHandleAppLost() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    WorkerInfo workerInfo1 =
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            INTERNALPORT1,
            disks1,
            userResourceConsumption1);
    WorkerInfo workerInfo2 =
        new WorkerInfo(
            HOSTNAME2,
            RPCPORT2,
            PUSHPORT2,
            FETCHPORT2,
            REPLICATEPORT2,
            INTERNALPORT2,
            disks2,
            userResourceConsumption2);

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation = new HashMap<>();
    allocation.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocation);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocation);

    statusSystem.handleApplicationMeta(new ApplicationMeta(APPID1, "testSecret"));
    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());

    assertEquals(1, statusSystem.registeredAppAndShuffles.size());
    assertEquals(1, statusSystem.applicationMetas.size());
    statusSystem.handleAppLost(APPID1, getNewReqeustId());

    assertTrue(statusSystem.registeredAppAndShuffles.isEmpty());
    assertTrue(statusSystem.applicationMetas.isEmpty());
  }

  @Test
  public void testHandleUnRegisterShuffle() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    WorkerInfo workerInfo1 =
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            INTERNALPORT1,
            disks1,
            userResourceConsumption1);
    WorkerInfo workerInfo2 =
        new WorkerInfo(
            HOSTNAME2,
            RPCPORT2,
            PUSHPORT2,
            FETCHPORT2,
            REPLICATEPORT2,
            INTERNALPORT2,
            disks2,
            userResourceConsumption2);

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation = new HashMap<>();
    allocation.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocation);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocation);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());

    assertEquals(1, statusSystem.registeredAppAndShuffles.size());

    statusSystem.handleUnRegisterShuffle(SHUFFLEKEY1, getNewReqeustId());

    assertTrue(statusSystem.registeredAppAndShuffles.isEmpty());
  }

  @Test
  public void testHandleBatchUnRegisterShuffle() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    WorkerInfo workerInfo1 =
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            INTERNALPORT1,
            disks1,
            userResourceConsumption1);
    WorkerInfo workerInfo2 =
        new WorkerInfo(
            HOSTNAME2,
            RPCPORT2,
            PUSHPORT2,
            FETCHPORT2,
            REPLICATEPORT2,
            INTERNALPORT2,
            disks2,
            userResourceConsumption2);

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation = new HashMap<>();
    allocation.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocation);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocation);

    List<String> shuffleKeysAll = new ArrayList<>();
    for (int i = 1; i <= 4; i++) {
      String shuffleKey = APPID1 + "-" + i;
      shuffleKeysAll.add(shuffleKey);
      statusSystem.handleRequestSlots(shuffleKey, HOSTNAME1, workersToAllocate, getNewReqeustId());
    }
    Assert.assertEquals(4, statusSystem.registeredShuffleCount());

    List<String> shuffleKeys1 = new ArrayList<>();
    shuffleKeys1.add(shuffleKeysAll.get(0));

    statusSystem.handleBatchUnRegisterShuffles(shuffleKeys1, getNewReqeustId());
    Assert.assertEquals(3, statusSystem.registeredShuffleCount());

    statusSystem.handleBatchUnRegisterShuffles(shuffleKeysAll, getNewReqeustId());

    Assert.assertTrue(statusSystem.registeredShuffleCount() == 0);
  }

  @Test
  public void testHandleAppHeartbeat() {
    Long dummy = 1235L;
    statusSystem.handleAppHeartbeat(
        APPID1, 1, 1, 1, 1, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());
    assertEquals(dummy, statusSystem.appHeartbeatTime.get(APPID1));

    String appId2 = "app02";
    statusSystem.handleAppHeartbeat(
        appId2, 1, 1, 2, 2, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());
    assertEquals(dummy, statusSystem.appHeartbeatTime.get(appId2));

    assertEquals(2, statusSystem.appHeartbeatTime.size());
  }

  @Test
  public void testHandleWorkerHeartbeat() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        new HashMap<>(),
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        new HashMap<>(),
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        new HashMap<>(),
        userResourceConsumption3,
        getNewReqeustId());

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        new HashMap<>(),
        userResourceConsumption1,
        1,
        false,
        workerStatus,
        getNewReqeustId());

    assertEquals(statusSystem.excludedWorkers.size(), 1);
    assertEquals(statusSystem.availableWorkers.size(), 2);

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        new HashMap<>(),
        userResourceConsumption2,
        1,
        false,
        workerStatus,
        getNewReqeustId());

    assertEquals(statusSystem.excludedWorkers.size(), 2);
    assertEquals(statusSystem.availableWorkers.size(), 1);

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        1,
        false,
        workerStatus,
        getNewReqeustId());

    assertEquals(statusSystem.excludedWorkers.size(), 2);
    assertEquals(statusSystem.availableWorkers.size(), 1);

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        disks3,
        userResourceConsumption3,
        1,
        true,
        workerStatus,
        getNewReqeustId());

    assertEquals(statusSystem.excludedWorkers.size(), 3);
    assertEquals(statusSystem.availableWorkers.size(), 0);
  }

  @Test
  public void testHandleReportWorkerFailure() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());

    List<WorkerInfo> failedWorkers = new ArrayList<>();
    failedWorkers.add(
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            INTERNALPORT1,
            disks1,
            userResourceConsumption1));

    statusSystem.handleReportWorkerUnavailable(failedWorkers, getNewReqeustId());
    assertEquals(1, statusSystem.shutdownWorkers.size());
    assertTrue(statusSystem.excludedWorkers.isEmpty());
    assertEquals(2, statusSystem.availableWorkers.size());
  }

  @Test
  public void testHandleUpdatePartitionSize() {
    statusSystem.partitionTotalWritten.reset();
    statusSystem.partitionTotalFileCount.reset();

    // Default size
    statusSystem.handleUpdatePartitionSize();
    Assert.assertEquals(statusSystem.estimatedPartitionSize, conf.initialEstimatedPartitionSize());

    Long dummy = 1235L;
    statusSystem.handleAppHeartbeat(
        APPID1, 10000000000l, 1, 1, 1, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());
    String appId2 = "app02";
    statusSystem.handleAppHeartbeat(
        appId2, 1, 1, 2, 2, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());

    // Max size
    statusSystem.handleUpdatePartitionSize();
    Assert.assertEquals(statusSystem.estimatedPartitionSize, conf.maxPartitionSizeToEstimate());

    statusSystem.handleAppHeartbeat(
        APPID1, 1000000000l, 1, 1, 1, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());
    statusSystem.handleAppHeartbeat(
        appId2, 1, 1, 2, 2, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());

    // Size between minEstimateSize -> maxEstimateSize
    statusSystem.handleUpdatePartitionSize();
    Assert.assertEquals(statusSystem.estimatedPartitionSize, 500000000);

    statusSystem.handleAppHeartbeat(
        APPID1, 1000l, 1, 1, 1, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());
    statusSystem.handleAppHeartbeat(
        appId2, 1000l, 1, 2, 2, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());

    // Min size
    statusSystem.handleUpdatePartitionSize();
    Assert.assertEquals(statusSystem.estimatedPartitionSize, conf.minPartitionSizeToEstimate());
  }

  @Test
  public void testHandleApplicationMeta() {
    String appSecret = "testSecret";
    statusSystem.handleApplicationMeta(new ApplicationMeta(APPID1, appSecret));
    assertEquals(appSecret, statusSystem.applicationMetas.get(APPID1).secret());

    String appId2 = "app02";
    String appSecret2 = "testSecret2";
    statusSystem.handleApplicationMeta(new ApplicationMeta(appId2, appSecret2));
    assertEquals(appSecret2, statusSystem.applicationMetas.get(appId2).secret());
    assertEquals(2, statusSystem.applicationMetas.size());
  }

  @Test
  public void testHandleReportWorkerDecommission() {
    statusSystem.handleRegisterWorker(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        INTERNALPORT1,
        NETWORK_LOCATION1,
        disks1,
        userResourceConsumption1,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME2,
        RPCPORT2,
        PUSHPORT2,
        FETCHPORT2,
        REPLICATEPORT2,
        INTERNALPORT2,
        NETWORK_LOCATION2,
        disks2,
        userResourceConsumption2,
        getNewReqeustId());
    statusSystem.handleRegisterWorker(
        HOSTNAME3,
        RPCPORT3,
        PUSHPORT3,
        FETCHPORT3,
        REPLICATEPORT3,
        INTERNALPORT3,
        NETWORK_LOCATION3,
        disks3,
        userResourceConsumption3,
        getNewReqeustId());
    List<WorkerInfo> workers = new ArrayList<>();
    workers.add(
        new WorkerInfo(
            HOSTNAME1,
            RPCPORT1,
            PUSHPORT1,
            FETCHPORT1,
            REPLICATEPORT1,
            INTERNALPORT1,
            disks1,
            userResourceConsumption1));
    statusSystem.handleReportWorkerDecommission(workers, getNewReqeustId());
    assertEquals(1, statusSystem.decommissionWorkers.size());
    assertTrue(statusSystem.excludedWorkers.isEmpty());
    assertEquals(2, statusSystem.availableWorkers.size());
  }

  @Test
  public void testShuffleAndApplicationCountWithFallback() {
    statusSystem.shuffleTotalCount.reset();
    statusSystem.applicationTotalCount.reset();
    statusSystem.shuffleFallbackCounts.clear();
    statusSystem.applicationFallbackCounts.clear();

    Long dummy = 1235L;
    Map<String, Long> shuffleFallbackCounts = new HashMap<>();
    Map<String, Long> applicationFallbackCounts = new HashMap<>();
    shuffleFallbackCounts.put(POLICY1, 2L);
    applicationFallbackCounts.put(POLICY1, 1L);
    statusSystem.handleAppHeartbeat(
        APPID1,
        1000l,
        1,
        2,
        1,
        shuffleFallbackCounts,
        applicationFallbackCounts,
        dummy,
        getNewReqeustId());
    shuffleFallbackCounts.put(POLICY1, 1L);
    shuffleFallbackCounts.put(POLICY2, 2L);
    applicationFallbackCounts.put(POLICY1, 1L);
    applicationFallbackCounts.put(POLICY2, 1L);
    statusSystem.handleAppHeartbeat(
        APPID1,
        1000l,
        1,
        3,
        2,
        shuffleFallbackCounts,
        applicationFallbackCounts,
        dummy,
        getNewReqeustId());

    assertEquals(statusSystem.shuffleTotalCount.longValue(), 5);
    assertEquals(statusSystem.applicationTotalCount.longValue(), 3);
    assertEquals(statusSystem.shuffleFallbackCounts.get(POLICY1).longValue(), 3);
    assertEquals(statusSystem.shuffleFallbackCounts.get(POLICY2).longValue(), 2);
    assertEquals(statusSystem.applicationFallbackCounts.get(POLICY1).longValue(), 2);
    assertEquals(statusSystem.applicationFallbackCounts.get(POLICY2).longValue(), 1);
  }
}
