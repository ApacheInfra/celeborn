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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import scala.Tuple2;

import com.google.common.collect.Lists;
import org.junit.*;
import org.mockito.Mockito;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.client.MasterClient;
import org.apache.celeborn.common.exception.CelebornRuntimeException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.meta.WorkerStatus;
import org.apache.celeborn.common.protocol.WorkerEventType;
import org.apache.celeborn.common.quota.ResourceConsumption;
import org.apache.celeborn.common.rpc.RpcEndpointAddress;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.rpc.netty.NettyRpcEndpointRef;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.util.Utils$;
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos;

public class RatisMasterStatusSystemSuiteJ {
  protected static HARaftServer RATISSERVER1 = null;
  protected static HARaftServer RATISSERVER2 = null;
  protected static HARaftServer RATISSERVER3 = null;
  protected static HAMasterMetaManager STATUSSYSTEM1 = null;
  protected static HAMasterMetaManager STATUSSYSTEM2 = null;
  protected static HAMasterMetaManager STATUSSYSTEM3 = null;

  private static final RpcEndpointRef dummyRef =
      new NettyRpcEndpointRef(
          new CelebornConf(), RpcEndpointAddress.apply("localhost", 111, "dummy"), null);

  protected static RpcEnv mockRpcEnv = Mockito.mock(RpcEnv.class);
  protected static RpcEndpointRef mockRpcEndpoint = Mockito.mock(RpcEndpointRef.class);

  @BeforeClass
  public static void init() throws Exception {
    resetRaftServer(
        configureServerConf(new CelebornConf(), 1),
        configureServerConf(new CelebornConf(), 2),
        configureServerConf(new CelebornConf(), 3),
        false);
  }

  private static void stopAllRaftServers() {
    if (RATISSERVER1 != null) {
      RATISSERVER1.stop();
    }
    if (RATISSERVER2 != null) {
      RATISSERVER2.stop();
    }
    if (RATISSERVER3 != null) {
      RATISSERVER3.stop();
    }
  }

  static CelebornConf configureServerConf(CelebornConf conf, int id) throws IOException {
    File tmpDir = File.createTempFile("celeborn-ratis" + id, "for-test-only");
    tmpDir.delete();
    tmpDir.mkdirs();
    conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR().key(), tmpDir.getAbsolutePath());
    return conf;
  }

  public static void resetRaftServer(
      CelebornConf conf1, CelebornConf conf2, CelebornConf conf3, boolean sslEnabled)
      throws IOException, InterruptedException {
    Mockito.when(mockRpcEnv.setupEndpointRef(Mockito.any(), Mockito.any()))
        .thenReturn(mockRpcEndpoint);
    when(mockRpcEnv.setupEndpointRef(any(), any())).thenReturn(dummyRef);

    stopAllRaftServers();

    int retryCount = 0;
    boolean serversStarted = false;

    while (!serversStarted) {
      try {
        STATUSSYSTEM1 = new HAMasterMetaManager(mockRpcEnv, new CelebornConf());
        STATUSSYSTEM2 = new HAMasterMetaManager(mockRpcEnv, new CelebornConf());
        STATUSSYSTEM3 = new HAMasterMetaManager(mockRpcEnv, new CelebornConf());

        MetaHandler handler1 = new MetaHandler(STATUSSYSTEM1);
        MetaHandler handler2 = new MetaHandler(STATUSSYSTEM2);
        MetaHandler handler3 = new MetaHandler(STATUSSYSTEM3);

        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        String id3 = UUID.randomUUID().toString();

        int ratisPort1 = Utils$.MODULE$.selectRandomInt(1024, 65535);
        int ratisPort2 = ratisPort1 + 1;
        int ratisPort3 = ratisPort2 + 1;

        MasterNode masterNode1 =
            new MasterNode.Builder()
                .setHost(Utils.localHostName(conf1))
                .setRatisPort(ratisPort1)
                .setRpcPort(ratisPort1)
                .setInternalRpcPort(ratisPort1)
                .setSslEnabled(sslEnabled)
                .setNodeId(id1)
                .build();
        MasterNode masterNode2 =
            new MasterNode.Builder()
                .setHost(Utils.localHostName(conf2))
                .setRatisPort(ratisPort2)
                .setRpcPort(ratisPort2)
                .setInternalRpcPort(ratisPort2)
                .setSslEnabled(sslEnabled)
                .setNodeId(id2)
                .build();
        MasterNode masterNode3 =
            new MasterNode.Builder()
                .setHost(Utils.localHostName(conf3))
                .setRatisPort(ratisPort3)
                .setRpcPort(ratisPort3)
                .setInternalRpcPort(ratisPort3)
                .setSslEnabled(sslEnabled)
                .setNodeId(id3)
                .build();

        List<MasterNode> peersForNode1 = Arrays.asList(masterNode2, masterNode3);
        List<MasterNode> peersForNode2 = Arrays.asList(masterNode1, masterNode3);
        List<MasterNode> peersForNode3 = Arrays.asList(masterNode1, masterNode2);

        RATISSERVER1 =
            HARaftServer.newMasterRatisServer(handler1, conf1, masterNode1, peersForNode1);
        RATISSERVER2 =
            HARaftServer.newMasterRatisServer(handler2, conf2, masterNode2, peersForNode2);
        RATISSERVER3 =
            HARaftServer.newMasterRatisServer(handler3, conf3, masterNode3, peersForNode3);

        STATUSSYSTEM1.setRatisServer(RATISSERVER1);
        STATUSSYSTEM2.setRatisServer(RATISSERVER2);
        STATUSSYSTEM3.setRatisServer(RATISSERVER3);

        RATISSERVER1.start();
        RATISSERVER2.start();
        RATISSERVER3.start();
        Thread.sleep(15 * 1000);
        serversStarted = true;
      } catch (Exception e) {
        stopAllRaftServers();
        retryCount += 1;
        if (retryCount == 3) {
          throw e;
        }
      }
    }
  }

  @Test
  public void testLeaderAvailable() {
    boolean hasLeader =
        RATISSERVER1.isLeader() || RATISSERVER2.isLeader() || RATISSERVER3.isLeader();
    Assert.assertTrue(hasLeader);

    // Check if the rpc endpoint and internal rpc endpoint of the leader is as expected.

    HARaftServer leader =
        RATISSERVER1.isLeader()
            ? RATISSERVER1
            : (RATISSERVER2.isLeader() ? RATISSERVER2 : RATISSERVER3);
    // one of them must be the follower given the three servers we have
    HARaftServer follower = RATISSERVER1.isLeader() ? RATISSERVER2 : RATISSERVER1;

    // This is expected to be false, but as a side effect, updates getCachedLeaderPeerRpcEndpoint
    boolean isFollowerCurrentLeader = follower.isLeader();
    Assert.assertFalse(isFollowerCurrentLeader);

    Optional<HARaftServer.LeaderPeerEndpoints> cachedLeaderPeerRpcEndpoint =
        follower.getCachedLeaderPeerRpcEndpoint();

    Assert.assertTrue(cachedLeaderPeerRpcEndpoint.isPresent());

    Tuple2<String, String> rpcEndpointsPair = cachedLeaderPeerRpcEndpoint.get().rpcEndpoints;
    Tuple2<String, String> rpcInternalEndpointsPair =
        cachedLeaderPeerRpcEndpoint.get().rpcInternalEndpoints;

    // rpc endpoint may use custom host name then this ut need check ever ip/host
    Assert.assertTrue(
        leader.getRpcEndpoint().equals(rpcEndpointsPair._1)
            || leader.getRpcEndpoint().equals(rpcEndpointsPair._2));
    Assert.assertTrue(
        leader.getInternalRpcEndpoint().equals(rpcInternalEndpointsPair._1)
            || leader.getRpcEndpoint().equals(rpcInternalEndpointsPair._2));
  }

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
  private static final Map<String, DiskInfo> disks3 = new HashMap<>();
  private static final Map<UserIdentifier, ResourceConsumption> userResourceConsumption3 =
      new HashMap<>();

  private final AtomicLong callerId = new AtomicLong();
  private static final String APPID1 = "appId1";
  private static final int SHUFFLEID1 = 1;
  private static final String SHUFFLEKEY1 = APPID1 + "-" + SHUFFLEID1;
  private static final String POLICY1 = "ShufflePartitionsFallbackPolicy";
  private static final String POLICY2 = "QuotaFallbackPolicy";
  private static final WorkerStatus workerStatus = WorkerStatus.normalWorkerStatus();

  private String getNewReqeustId() {
    return MasterClient.encodeRequestId(UUID.randomUUID().toString(), callerId.incrementAndGet());
  }

  public HAMasterMetaManager pickLeaderStatusSystem() {
    if (RATISSERVER1.isLeader()) {
      return STATUSSYSTEM1;
    }
    if (RATISSERVER2.isLeader()) {
      return STATUSSYSTEM2;
    }
    if (RATISSERVER3.isLeader()) {
      return STATUSSYSTEM3;
    }
    return null;
  }

  private void stopNoneLeaderRaftServer(HARaftServer... raftServers) {
    for (HARaftServer raftServer : raftServers) {
      if (!raftServer.isLeader()) {
        raftServer.stop();
      }
    }
  }

  @Test
  public void testRaftSystemException() throws Exception {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);
    try {
      stopNoneLeaderRaftServer(RATISSERVER1, RATISSERVER2, RATISSERVER3);
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
      Assert.fail();
    } catch (CelebornRuntimeException e) {
      Assert.assertTrue(true);
    } finally {
      init();
    }
  }

  @Test
  public void testHandleRegisterWorker() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
    Thread.sleep(3000L);

    Assert.assertEquals(3, STATUSSYSTEM1.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM2.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM3.workersMap.size());

    Assert.assertEquals(3, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(3, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(3, STATUSSYSTEM3.availableWorkers.size());

    assertWorkers(STATUSSYSTEM1.workersMap.values());
    assertWorkers(STATUSSYSTEM2.workersMap.values());
    assertWorkers(STATUSSYSTEM3.workersMap.values());
  }

  private void assertWorkers(Collection<WorkerInfo> workerInfos) {
    for (WorkerInfo workerInfo : workerInfos) {
      assertWorker(workerInfo);
    }
  }

  private void assertWorker(WorkerInfo workerInfo) {
    Map<String, DiskInfo> diskInfos = workerInfo.diskInfos();
    Assert.assertEquals(96 * 1024 * 1024 * 1024L, diskInfos.get("disk1").totalSpace());
    Assert.assertEquals(96 * 1024 * 1024 * 1024L, diskInfos.get("disk2").totalSpace());
    Assert.assertEquals(96 * 1024 * 1024 * 1024L, diskInfos.get("disk3").totalSpace());
    Assert.assertEquals(96 * 1024 * 1024 * 1024L, diskInfos.get("disk4").totalSpace());
  }

  @Test
  public void testHandleWorkerExclude() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
    Thread.sleep(3000L);

    Assert.assertEquals(2, STATUSSYSTEM1.manuallyExcludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.manuallyExcludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.manuallyExcludedWorkers.size());

    Assert.assertEquals(0, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM3.availableWorkers.size());

    statusSystem.handleWorkerExclude(
        Collections.emptyList(), Collections.singletonList(workerInfo1), getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(1, STATUSSYSTEM1.manuallyExcludedWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.manuallyExcludedWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.manuallyExcludedWorkers.size());

    Assert.assertEquals(1, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.availableWorkers.size());
  }

  @Test
  public void testHandleWorkerLost() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
    Thread.sleep(3000L);

    Assert.assertEquals(2, STATUSSYSTEM1.workersMap.size());
    Assert.assertEquals(2, STATUSSYSTEM2.workersMap.size());
    Assert.assertEquals(2, STATUSSYSTEM3.workersMap.size());

    Assert.assertEquals(2, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.availableWorkers.size());
  }

  @Test
  public void testHandleRequestSlots() {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
    Map<String, Integer> allocation1 = new HashMap<>();
    allocation1.put("disk1", 15);
    Map<String, Integer> allocation2 = new HashMap<>();
    allocation2.put("disk2", 25);
    Map<String, Integer> allocation3 = new HashMap<>();
    allocation3.put("disk3", 35);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocation1);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocation2);
    workersToAllocate.put(workerInfo3.toUniqueId(), allocation3);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());

    // Do not update diskinfo's activeslots

    Assert.assertEquals(
        0,
        statusSystem.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME1))
            .findFirst()
            .get()
            .usedSlots());
    Assert.assertEquals(
        0,
        statusSystem.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME2))
            .findFirst()
            .get()
            .usedSlots());
    Assert.assertEquals(
        0,
        statusSystem.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME3))
            .findFirst()
            .get()
            .usedSlots());
  }

  @Test
  public void testHandleReleaseSlots() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
        NETWORK_LOCATION3,
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
    Thread.sleep(3000L);

    Assert.assertEquals(3, STATUSSYSTEM1.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM2.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM3.workersMap.size());

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocations = new HashMap<>();
    allocations.put("disk1", 5);
    workersToAllocate.put(
        statusSystem.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME1))
            .findFirst()
            .get()
            .toUniqueId(),
        allocations);
    workersToAllocate.put(
        statusSystem.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME2))
            .findFirst()
            .get()
            .toUniqueId(),
        allocations);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());
    Thread.sleep(3000L);

    // Do not update diskinfo's activeslots

    Assert.assertEquals(
        0,
        STATUSSYSTEM1.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME1))
            .findFirst()
            .get()
            .usedSlots());
    Assert.assertEquals(
        0,
        STATUSSYSTEM2.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME1))
            .findFirst()
            .get()
            .usedSlots());
    Assert.assertEquals(
        0,
        STATUSSYSTEM3.workersMap.values().stream()
            .filter(w -> w.host().equals(HOSTNAME1))
            .findFirst()
            .get()
            .usedSlots());
  }

  @Test
  public void testHandleAppLost() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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

    Thread.sleep(3000L);
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
    Map<String, Integer> allocations = new HashMap<>();
    allocations.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocations);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocations);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(1, STATUSSYSTEM1.registeredAppAndShuffles.size());
    Assert.assertEquals(1, STATUSSYSTEM2.registeredAppAndShuffles.size());
    Assert.assertEquals(1, STATUSSYSTEM3.registeredAppAndShuffles.size());

    statusSystem.handleAppLost(APPID1, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertTrue(STATUSSYSTEM1.registeredAppAndShuffles.isEmpty());
    Assert.assertTrue(STATUSSYSTEM2.registeredAppAndShuffles.isEmpty());
    Assert.assertTrue(STATUSSYSTEM3.registeredAppAndShuffles.isEmpty());
  }

  @Test
  public void testHandleUnRegisterShuffle() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
    Map<String, Integer> allocations = new HashMap<>();
    allocations.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocations);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocations);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(1, STATUSSYSTEM1.registeredAppAndShuffles.size());
    Assert.assertEquals(1, STATUSSYSTEM2.registeredAppAndShuffles.size());
    Assert.assertEquals(1, STATUSSYSTEM3.registeredAppAndShuffles.size());

    statusSystem.handleUnRegisterShuffle(SHUFFLEKEY1, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertTrue(STATUSSYSTEM1.registeredAppAndShuffles.isEmpty());
    Assert.assertTrue(STATUSSYSTEM2.registeredAppAndShuffles.isEmpty());
    Assert.assertTrue(STATUSSYSTEM3.registeredAppAndShuffles.isEmpty());
  }

  @Test
  public void testHandleBatchUnRegisterShuffle() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
    Map<String, Integer> allocations = new HashMap<>();
    allocations.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocations);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocations);

    List<String> shuffleKeysAll = new ArrayList<>();
    for (int i = 1; i <= 4; i++) {
      String shuffleKey = APPID1 + "-" + i;
      shuffleKeysAll.add(shuffleKey);
      statusSystem.handleRequestSlots(shuffleKey, HOSTNAME1, workersToAllocate, getNewReqeustId());
    }

    Thread.sleep(3000L);

    Assert.assertEquals(4, STATUSSYSTEM1.registeredShuffleCount());
    Assert.assertEquals(4, STATUSSYSTEM2.registeredShuffleCount());
    Assert.assertEquals(4, STATUSSYSTEM3.registeredShuffleCount());

    List<String> shuffleKeys1 = new ArrayList<>();
    shuffleKeys1.add(shuffleKeysAll.get(0));

    statusSystem.handleBatchUnRegisterShuffles(shuffleKeys1, getNewReqeustId());
    Thread.sleep(3000L);
    Assert.assertEquals(3, STATUSSYSTEM1.registeredShuffleCount());
    Assert.assertEquals(3, STATUSSYSTEM2.registeredShuffleCount());
    Assert.assertEquals(3, STATUSSYSTEM3.registeredShuffleCount());

    statusSystem.handleBatchUnRegisterShuffles(shuffleKeysAll, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertTrue(STATUSSYSTEM1.registeredShuffleCount() == 0);
    Assert.assertTrue(STATUSSYSTEM2.registeredShuffleCount() == 0);
    Assert.assertTrue(STATUSSYSTEM3.registeredShuffleCount() == 0);
  }

  @Test
  public void testHandleAppHeartbeat() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    long dummy = 1235L;
    statusSystem.handleAppHeartbeat(
        APPID1, 1, 1, 1, 1, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());
    Thread.sleep(3000L);
    Assert.assertEquals(Long.valueOf(dummy), STATUSSYSTEM1.appHeartbeatTime.get(APPID1));
    Assert.assertEquals(Long.valueOf(dummy), STATUSSYSTEM2.appHeartbeatTime.get(APPID1));
    Assert.assertEquals(Long.valueOf(dummy), STATUSSYSTEM3.appHeartbeatTime.get(APPID1));

    String appId2 = "app02";
    statusSystem.handleAppHeartbeat(
        appId2, 1, 1, 2, 2, new HashMap<>(), new HashMap<>(), dummy, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(Long.valueOf(dummy), STATUSSYSTEM1.appHeartbeatTime.get(appId2));
    Assert.assertEquals(Long.valueOf(dummy), STATUSSYSTEM2.appHeartbeatTime.get(appId2));
    Assert.assertEquals(Long.valueOf(dummy), STATUSSYSTEM3.appHeartbeatTime.get(appId2));

    Assert.assertEquals(2, STATUSSYSTEM1.appHeartbeatTime.size());
    Assert.assertEquals(2, STATUSSYSTEM2.appHeartbeatTime.size());
    Assert.assertEquals(2, STATUSSYSTEM3.appHeartbeatTime.size());
  }

  @Test
  public void testHandleWorkerHeartbeat() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
    Thread.sleep(3000L);

    Assert.assertEquals(1, STATUSSYSTEM1.excludedWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.excludedWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.excludedWorkers.size());

    Assert.assertEquals(3, STATUSSYSTEM1.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM2.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM3.workersMap.size());

    Assert.assertEquals(2, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.availableWorkers.size());

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
    Thread.sleep(3000L);

    Assert.assertEquals(2, statusSystem.excludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM1.excludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.excludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.excludedWorkers.size());

    Assert.assertEquals(3, statusSystem.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM1.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM2.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM3.workersMap.size());

    Assert.assertEquals(1, statusSystem.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.availableWorkers.size());

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        disks1,
        userResourceConsumption1,
        1,
        false,
        workerStatus,
        getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(1, statusSystem.excludedWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM1.excludedWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.excludedWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.excludedWorkers.size());

    Assert.assertEquals(3, statusSystem.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM1.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM2.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM3.workersMap.size());

    Assert.assertEquals(2, statusSystem.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.availableWorkers.size());

    statusSystem.handleWorkerHeartbeat(
        HOSTNAME1,
        RPCPORT1,
        PUSHPORT1,
        FETCHPORT1,
        REPLICATEPORT1,
        disks1,
        userResourceConsumption1,
        1,
        true,
        workerStatus,
        getNewReqeustId());
    Thread.sleep(3000L);
    Assert.assertEquals(2, statusSystem.excludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM1.excludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.excludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.excludedWorkers.size());

    Assert.assertEquals(3, statusSystem.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM1.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM2.workersMap.size());
    Assert.assertEquals(3, STATUSSYSTEM3.workersMap.size());

    Assert.assertEquals(1, statusSystem.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.availableWorkers.size());
  }

  @Before
  public void resetStatus() {
    STATUSSYSTEM1.registeredAppAndShuffles.clear();
    STATUSSYSTEM1.hostnameSet.clear();
    STATUSSYSTEM1.workersMap.clear();
    STATUSSYSTEM1.availableWorkers.clear();
    STATUSSYSTEM1.appHeartbeatTime.clear();
    STATUSSYSTEM1.excludedWorkers.clear();
    STATUSSYSTEM1.workerLostEvents.clear();

    STATUSSYSTEM2.registeredAppAndShuffles.clear();
    STATUSSYSTEM2.hostnameSet.clear();
    STATUSSYSTEM2.workersMap.clear();
    STATUSSYSTEM2.availableWorkers.clear();
    STATUSSYSTEM2.appHeartbeatTime.clear();
    STATUSSYSTEM2.excludedWorkers.clear();
    STATUSSYSTEM2.workerLostEvents.clear();

    STATUSSYSTEM3.registeredAppAndShuffles.clear();
    STATUSSYSTEM3.hostnameSet.clear();
    STATUSSYSTEM3.workersMap.clear();
    STATUSSYSTEM3.availableWorkers.clear();
    STATUSSYSTEM3.appHeartbeatTime.clear();
    STATUSSYSTEM3.excludedWorkers.clear();
    STATUSSYSTEM3.workerLostEvents.clear();

    disks1.clear();
    disks1.put(
        "disk1",
        new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
    disks1.put(
        "disk2",
        new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
    disks1.put(
        "disk3",
        new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
    disks1.put(
        "disk4",
        new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));

    disks2.clear();
    disks2.put(
        "disk1",
        new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
    disks2.put(
        "disk2",
        new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
    disks2.put(
        "disk3",
        new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
    disks2.put(
        "disk4",
        new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));

    disks3.clear();
    disks3.put(
        "disk1",
        new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
    disks3.put(
        "disk2",
        new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
    disks3.put(
        "disk3",
        new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
    disks3.put(
        "disk4",
        new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 100, 0)
            .setTotalSpace(96 * 1024 * 1024 * 1024L));
  }

  @Test
  public void testHandleReportWorkerFailure() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
    Thread.sleep(3000L);
    Assert.assertEquals(1, STATUSSYSTEM1.shutdownWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.shutdownWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.shutdownWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM1.excludedWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM2.excludedWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM3.excludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.availableWorkers.size());
  }

  @Test
  public void testHandleRemoveWorkersUnavailableInfo() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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

    List<WorkerInfo> unavailableWorkers = new ArrayList<>();
    unavailableWorkers.add(workerInfo1);

    statusSystem.handleWorkerLost(
        HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1, getNewReqeustId());
    statusSystem.handleReportWorkerUnavailable(unavailableWorkers, getNewReqeustId());

    Thread.sleep(3000L);
    Assert.assertEquals(2, STATUSSYSTEM1.workersMap.size());

    Assert.assertEquals(1, STATUSSYSTEM1.shutdownWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.shutdownWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.shutdownWorkers.size());

    Assert.assertEquals(1, STATUSSYSTEM1.lostWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.lostWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.lostWorkers.size());

    Assert.assertEquals(2, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.availableWorkers.size());

    statusSystem.handleRemoveWorkersUnavailableInfo(unavailableWorkers, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(0, STATUSSYSTEM1.shutdownWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM2.shutdownWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM3.shutdownWorkers.size());

    Assert.assertEquals(0, STATUSSYSTEM1.lostWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM2.lostWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM3.lostWorkers.size());

    Assert.assertEquals(2, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.availableWorkers.size());
  }

  @Test
  public void testHandleUpdatePartitionSize() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    CelebornConf conf = new CelebornConf();
    statusSystem.partitionTotalWritten.reset();
    statusSystem.partitionTotalFileCount.reset();

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
  public void testHandleWorkerEvent() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
        WorkerInfo.fromUniqueId(
            HOSTNAME1 + ":" + RPCPORT1 + ":" + PUSHPORT1 + ":" + FETCHPORT1 + ":" + REPLICATEPORT1);
    WorkerInfo workerInfo2 =
        WorkerInfo.fromUniqueId(
            HOSTNAME2 + ":" + RPCPORT2 + ":" + PUSHPORT2 + ":" + FETCHPORT2 + ":" + REPLICATEPORT2);
    statusSystem.handleWorkerEvent(
        ResourceProtos.WorkerEventType.Decommission_VALUE,
        Lists.newArrayList(workerInfo1, workerInfo2),
        getNewReqeustId());

    Thread.sleep(3000L);
    Assert.assertEquals(2, STATUSSYSTEM1.workerEventInfos.size());
    Assert.assertEquals(2, STATUSSYSTEM2.workerEventInfos.size());
    Assert.assertEquals(2, STATUSSYSTEM3.workerEventInfos.size());

    Assert.assertEquals(1, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.availableWorkers.size());

    Assert.assertTrue(STATUSSYSTEM1.workerEventInfos.containsKey(workerInfo1));
    Assert.assertTrue(STATUSSYSTEM1.workerEventInfos.containsKey(workerInfo2));

    Assert.assertEquals(
        WorkerEventType.Decommission,
        STATUSSYSTEM1.workerEventInfos.get(workerInfo1).getEventType());

    statusSystem.handleWorkerEvent(
        ResourceProtos.WorkerEventType.None_VALUE,
        Lists.newArrayList(workerInfo1),
        getNewReqeustId());
    Thread.sleep(3000L);
    Assert.assertEquals(1, STATUSSYSTEM1.workerEventInfos.size());
    Assert.assertEquals(1, STATUSSYSTEM2.workerEventInfos.size());
    Assert.assertEquals(1, STATUSSYSTEM3.workerEventInfos.size());
    Assert.assertTrue(STATUSSYSTEM1.workerEventInfos.containsKey(workerInfo2));
    Assert.assertEquals(
        WorkerEventType.Decommission,
        STATUSSYSTEM1.workerEventInfos.get(workerInfo2).getEventType());

    Assert.assertEquals(2, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.availableWorkers.size());
  }

  @Test
  public void testReviseShuffles() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    statusSystem.handleReviseLostShuffles(
        "app-1", Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), getNewReqeustId());
    Thread.sleep(1000l);
    Assert.assertEquals(STATUSSYSTEM1.registeredShuffleCount(), 8);
    Assert.assertEquals(STATUSSYSTEM2.registeredShuffleCount(), 8);
    Assert.assertEquals(STATUSSYSTEM3.registeredShuffleCount(), 8);
  }

  @AfterClass
  public static void testNotifyLogFailed() {
    List<HARaftServer> list = Arrays.asList(RATISSERVER1, RATISSERVER2, RATISSERVER3);
    for (HARaftServer haRaftServer : list) {
      if (haRaftServer.isLeader()) {
        haRaftServer
            .getMasterStateMachine()
            .notifyLogFailed(new Exception("test leader step down"), null);
        Assert.assertFalse(haRaftServer.isLeader());
        break;
      }
    }
  }

  @Test
  public void testHandleReportWorkerDecommission() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

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
    Thread.sleep(3000L);
    Assert.assertEquals(1, STATUSSYSTEM1.decommissionWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM2.decommissionWorkers.size());
    Assert.assertEquals(1, STATUSSYSTEM3.decommissionWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM1.excludedWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM2.excludedWorkers.size());
    Assert.assertEquals(0, STATUSSYSTEM3.excludedWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM1.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.availableWorkers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.availableWorkers.size());
  }

  @Test
  public void testShuffleAndApplicationCountWithFallback() {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);
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
