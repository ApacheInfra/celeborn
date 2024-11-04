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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.client.MasterClient;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.AppDiskUsageSnapShot;
import org.apache.celeborn.common.meta.DiskInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.quota.ResourceConsumption;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.RequestSlotsRequest;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.ResourceRequest;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.ResourceResponse;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.Type;

public class MasterStateMachineSuiteJ extends RatisBaseSuiteJ {

  private final AtomicLong callerId = new AtomicLong();

  @Test
  public void testRunCommand() {
    StateMachine stateMachine = ratisServer.getMasterStateMachine();

    Map<String, Integer> allocations = new HashMap<>();
    allocations.put("disk1", 15);
    allocations.put("disk2", 20);

    Map<String, ResourceProtos.SlotInfo> workerAllocations = new HashMap<>();
    workerAllocations.put(
        new WorkerInfo("host1", 1, 2, 3, 10).toUniqueId(),
        ResourceProtos.SlotInfo.newBuilder().putAllSlot(allocations).build());
    workerAllocations.put(
        new WorkerInfo("host2", 2, 3, 4, 11).toUniqueId(),
        ResourceProtos.SlotInfo.newBuilder().putAllSlot(allocations).build());
    workerAllocations.put(
        new WorkerInfo("host3", 3, 4, 5, 12).toUniqueId(),
        ResourceProtos.SlotInfo.newBuilder().putAllSlot(allocations).build());

    RequestSlotsRequest requestSlots =
        RequestSlotsRequest.newBuilder()
            .setShuffleKey("appId-1-1")
            .setHostName("hostname")
            .putAllWorkerAllocations(workerAllocations)
            .build();

    ResourceRequest request =
        ResourceRequest.newBuilder()
            .setRequestSlotsRequest(requestSlots)
            .setCmdType(Type.RequestSlots)
            .setRequestId(UUID.randomUUID().toString())
            .build();

    ResourceResponse response = stateMachine.runCommand(request, -1);
    Assert.assertTrue(response.getSuccess());
  }

  @Test
  public void testTakeSnapshot() {
    final StateMachine stateMachine = ratisServer.getMasterStateMachine();

    stateMachine.notifyTermIndexUpdated(2020, 725);

    final long snapshot1Index = stateMachine.takeSnapshot();
    Assert.assertEquals(725, snapshot1Index);

    SnapshotInfo snapshot1 = stateMachine.getLatestSnapshot();
    Assert.assertEquals(2020, snapshot1.getTerm());
    Assert.assertEquals(725, snapshot1.getIndex());
    Assert.assertEquals(1, snapshot1.getFiles().size());

    stateMachine.notifyTermIndexUpdated(2020, 1005);
    final long snapshot2Index = stateMachine.takeSnapshot();
    Assert.assertEquals(1005, snapshot2Index);

    SnapshotInfo latest = stateMachine.getLatestSnapshot();
    Assert.assertEquals(2020, latest.getTerm());
    Assert.assertEquals(1005, latest.getIndex());
    Assert.assertEquals(1, latest.getFiles().size());
  }

  @Test
  public void testObjSerde() throws IOException, InterruptedException {
    CelebornConf conf = new CelebornConf();
    HAMasterMetaManager masterStatusSystem = new HAMasterMetaManager(null, conf);
    File tmpFile = File.createTempFile("tef", "test" + System.currentTimeMillis());

    Map<String, DiskInfo> disks1 = new HashMap<>();
    disks1.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks1.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks1.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    Map<UserIdentifier, ResourceConsumption> userResourceConsumption1 =
        JavaUtils.newConcurrentHashMap();
    userResourceConsumption1.put(
        new UserIdentifier("tenant1", "name1"), new ResourceConsumption(1000, 1, 1000, 1, null));
    userResourceConsumption1.put(
        new UserIdentifier("tenant1", "name2"),
        new ResourceConsumption(
            2000,
            2,
            2000,
            2,
            Collections.singletonMap("appId2", new ResourceConsumption(2000, 2, 2000, 2, null))));
    userResourceConsumption1.put(
        new UserIdentifier("tenant1", "name3"),
        new ResourceConsumption(
            3000,
            3,
            3000,
            3,
            Collections.singletonMap("appId3", new ResourceConsumption(2000, 2, 2000, 2, null))));

    Map<String, DiskInfo> disks2 = new HashMap<>();
    disks2.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks2.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks2.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    Map<UserIdentifier, ResourceConsumption> userResourceConsumption2 =
        JavaUtils.newConcurrentHashMap();
    userResourceConsumption2.put(
        new UserIdentifier("tenant2", "name1"), new ResourceConsumption(1000, 1, 1000, 1, null));
    userResourceConsumption2.put(
        new UserIdentifier("tenant2", "name2"),
        new ResourceConsumption(
            2000,
            2,
            2000,
            2,
            Collections.singletonMap("appId2", new ResourceConsumption(2000, 2, 2000, 2, null))));
    userResourceConsumption2.put(
        new UserIdentifier("tenant2", "name3"),
        new ResourceConsumption(
            3000,
            3,
            3000,
            3,
            Collections.singletonMap("appId3", new ResourceConsumption(2000, 2, 2000, 2, null))));

    Map<String, DiskInfo> disks3 = new HashMap<>();
    disks3.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks3.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    disks3.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 100, 0));
    Map<UserIdentifier, ResourceConsumption> userResourceConsumption3 =
        JavaUtils.newConcurrentHashMap();
    userResourceConsumption3.put(
        new UserIdentifier("tenant3", "name1"), new ResourceConsumption(1000, 1, 1000, 1, null));
    userResourceConsumption3.put(
        new UserIdentifier("tenant3", "name2"),
        new ResourceConsumption(
            2000,
            2,
            2000,
            2,
            Collections.singletonMap("appId2", new ResourceConsumption(2000, 2, 2000, 2, null))));
    userResourceConsumption3.put(
        new UserIdentifier("tenant3", "name3"),
        new ResourceConsumption(
            3000,
            3,
            3000,
            3,
            Collections.singletonMap("appId3", new ResourceConsumption(2000, 2, 2000, 2, null))));

    WorkerInfo info1 = new WorkerInfo("host1", 1, 2, 3, 10, 13, disks1, userResourceConsumption1);
    WorkerInfo info2 = new WorkerInfo("host2", 4, 5, 6, 11, 15, disks2, userResourceConsumption2);
    WorkerInfo info3 = new WorkerInfo("host3", 7, 8, 9, 12, 17, disks3, userResourceConsumption3);

    String host1 = "host1";
    String host2 = "host2";
    String host3 = "host3";

    masterStatusSystem.excludedWorkers.add(info1);
    masterStatusSystem.excludedWorkers.add(info2);
    masterStatusSystem.excludedWorkers.add(info3);

    masterStatusSystem.manuallyExcludedWorkers.add(info1);
    masterStatusSystem.manuallyExcludedWorkers.add(info2);

    masterStatusSystem.hostnameSet.add(host1);
    masterStatusSystem.hostnameSet.add(host2);
    masterStatusSystem.hostnameSet.add(host3);

    // Wait for update snapshot
    Thread.sleep(60000);
    Map<String, Long> appDiskUsage = JavaUtils.newConcurrentHashMap();
    appDiskUsage.put("app-1", 100L);
    appDiskUsage.put("app-2", 200L);
    masterStatusSystem.appDiskUsageMetric.update(appDiskUsage);
    appDiskUsage.put("app-3", 300L);
    appDiskUsage.put("app-1", 200L);
    masterStatusSystem.appDiskUsageMetric.update(appDiskUsage);
    // wait for snapshot updated
    Thread.sleep(3000);

    AppDiskUsageSnapShot[] originSnapshots = masterStatusSystem.appDiskUsageMetric.snapShots();
    AppDiskUsageSnapShot originCurrentSnapshot =
        masterStatusSystem.appDiskUsageMetric.currentSnapShot().get();

    WorkerInfo workerInfo1 = new WorkerInfo(host1, 9095, 9094, 9093, 9092, 9091);
    WorkerInfo workerInfo2 = new WorkerInfo(host2, 9095, 9094, 9093, 9092, 9091);
    WorkerInfo workerInfo3 = new WorkerInfo(host3, 9095, 9094, 9093, 9092, 9091);
    masterStatusSystem.workersMap.put(workerInfo1.toUniqueId(), workerInfo1);
    masterStatusSystem.workersMap.put(workerInfo2.toUniqueId(), workerInfo2);
    masterStatusSystem.workersMap.put(workerInfo3.toUniqueId(), workerInfo3);

    masterStatusSystem.writeMetaInfoToFile(tmpFile);

    masterStatusSystem.hostnameSet.clear();
    masterStatusSystem.excludedWorkers.clear();
    masterStatusSystem.manuallyExcludedWorkers.clear();
    masterStatusSystem.workersMap.clear();

    masterStatusSystem.restoreMetaFromFile(tmpFile);

    Assert.assertEquals(3, masterStatusSystem.workersMap.size());
    Assert.assertEquals(3, masterStatusSystem.excludedWorkers.size());
    Assert.assertEquals(2, masterStatusSystem.manuallyExcludedWorkers.size());
    Assert.assertEquals(3, masterStatusSystem.hostnameSet.size());
    Assert.assertEquals(
        conf.metricsAppTopDiskUsageWindowSize(),
        masterStatusSystem.appDiskUsageMetric.snapShots().length);
    Assert.assertEquals(
        conf.metricsAppTopDiskUsageCount(),
        masterStatusSystem.appDiskUsageMetric.currentSnapShot().get().topNItems().length);
    Assert.assertEquals(
        originCurrentSnapshot, masterStatusSystem.appDiskUsageMetric.currentSnapShot().get());
    Assert.assertArrayEquals(originSnapshots, masterStatusSystem.appDiskUsageMetric.snapShots());

    masterStatusSystem.restoreMetaFromFile(tmpFile);
    Assert.assertEquals(3, masterStatusSystem.workersMap.size());
  }

  private String getNewReqeustId() {
    return MasterClient.encodeRequestId(UUID.randomUUID().toString(), callerId.incrementAndGet());
  }

  private void pauseRaftServer(RaftServer server)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
          ClassNotFoundException {
    Method method = server.getClass().getDeclaredMethod("getImpls");
    method.setAccessible(true);
    Object serverImpl = ((List<Object>) method.invoke(server)).get(0);

    Class<?> privateClass = Class.forName("org.apache.ratis.server.impl.RaftServerImpl");
    Method pauseMethod = privateClass.getDeclaredMethod("pause");
    pauseMethod.setAccessible(true);
    pauseMethod.invoke(serverImpl);
  }

  private void resumeRaftServer(RaftServer server)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
          ClassNotFoundException {
    Method method = server.getClass().getDeclaredMethod("getImpls");
    method.setAccessible(true);
    Object serverImpl = ((List<Object>) method.invoke(server)).get(0);

    Class<?> privateClass = Class.forName("org.apache.ratis.server.impl.RaftServerImpl");
    Method pauseMethod = privateClass.getDeclaredMethod("resume");
    pauseMethod.setAccessible(true);
    pauseMethod.invoke(serverImpl);
  }

  public List<HAMasterMetaManager> startRaftServers() throws IOException, InterruptedException {
    CelebornConf conf1 = new CelebornConf();
    CelebornConf conf2 = new CelebornConf();
    CelebornConf conf3 = new CelebornConf();
    File tmpDir1 = File.createTempFile("celeborn-ratis" + 1, "for-test-only");
    tmpDir1.delete();
    tmpDir1.mkdirs();
    conf1.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR().key(), tmpDir1.getAbsolutePath());
    conf1.set(CelebornConf.HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD().key(), "100");
    conf1.set(CelebornConf.HA_MASTER_RATIS_LOG_PURGE_GAP().key(), "200");
    conf1.set(CelebornConf.HA_MASTER_RATIS_LOG_SEGMENT_SIZE_MAX().key(), "13490");

    File tmpDir2 = File.createTempFile("celeborn-ratis" + 2, "for-test-only");
    tmpDir2.delete();
    tmpDir2.mkdirs();
    conf2.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR().key(), tmpDir2.getAbsolutePath());
    conf2.set(CelebornConf.HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD().key(), "100");
    conf2.set(CelebornConf.HA_MASTER_RATIS_LOG_PURGE_GAP().key(), "200");
    conf2.set(CelebornConf.HA_MASTER_RATIS_LOG_SEGMENT_SIZE_MAX().key(), "13490");

    File tmpDir3 = File.createTempFile("celeborn-ratis" + 3, "for-test-only");
    tmpDir3.delete();
    tmpDir3.mkdirs();
    conf3.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR().key(), tmpDir3.getAbsolutePath());
    conf3.set(CelebornConf.HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD().key(), "100");
    conf3.set(CelebornConf.HA_MASTER_RATIS_LOG_PURGE_GAP().key(), "200");
    conf3.set(CelebornConf.HA_MASTER_RATIS_LOG_SEGMENT_SIZE_MAX().key(), "13490");

    RpcEnv mockRpcEnv = Mockito.mock(RpcEnv.class);
    HAMasterMetaManager masterStatusSystem1 = new HAMasterMetaManager(mockRpcEnv, conf1);
    HAMasterMetaManager masterStatusSystem2 = new HAMasterMetaManager(mockRpcEnv, conf2);
    HAMasterMetaManager masterStatusSystem3 = new HAMasterMetaManager(mockRpcEnv, conf3);
    MetaHandler handler1 = new MetaHandler(masterStatusSystem1);
    MetaHandler handler2 = new MetaHandler(masterStatusSystem2);
    MetaHandler handler3 = new MetaHandler(masterStatusSystem3);
    MasterNode masterNode1 =
        new MasterNode.Builder()
            .setHost(Utils.localHostName(conf1))
            .setRatisPort(9200)
            .setRpcPort(9201)
            .setInternalRpcPort(9203)
            .setSslEnabled(false)
            .setNodeId(UUID.randomUUID().toString())
            .build();
    MasterNode masterNode2 =
        new MasterNode.Builder()
            .setHost(Utils.localHostName(conf2))
            .setRatisPort(9204)
            .setRpcPort(9205)
            .setInternalRpcPort(9206)
            .setSslEnabled(false)
            .setNodeId(UUID.randomUUID().toString())
            .build();
    MasterNode masterNode3 =
        new MasterNode.Builder()
            .setHost(Utils.localHostName(conf3))
            .setRatisPort(9207)
            .setRpcPort(9208)
            .setInternalRpcPort(9209)
            .setSslEnabled(false)
            .setNodeId(UUID.randomUUID().toString())
            .build();
    HARaftServer raftServer1 =
        HARaftServer.newMasterRatisServer(
            handler1, conf1, masterNode1, Arrays.asList(masterNode2, masterNode3));
    HARaftServer raftServer2 =
        HARaftServer.newMasterRatisServer(
            handler2, conf2, masterNode2, Arrays.asList(masterNode1, masterNode3));
    HARaftServer raftServer3 =
        HARaftServer.newMasterRatisServer(
            handler3, conf3, masterNode3, Arrays.asList(masterNode1, masterNode2));
    masterStatusSystem1.setRatisServer(raftServer1);
    masterStatusSystem2.setRatisServer(raftServer2);
    masterStatusSystem3.setRatisServer(raftServer3);
    raftServer1.start();
    raftServer2.start();
    raftServer3.start();
    Thread.sleep(15 * 1000);

    HAMasterMetaManager leaderStatusSystem;
    HAMasterMetaManager followerStatusSystem1;
    HAMasterMetaManager followerStatusSystem2;
    if (raftServer1.isLeader()) {
      leaderStatusSystem = masterStatusSystem1;
      followerStatusSystem1 = masterStatusSystem2;
      followerStatusSystem2 = masterStatusSystem3;
    } else if (raftServer2.isLeader()) {
      leaderStatusSystem = masterStatusSystem2;
      followerStatusSystem1 = masterStatusSystem1;
      followerStatusSystem2 = masterStatusSystem3;
    } else {
      leaderStatusSystem = masterStatusSystem3;
      followerStatusSystem1 = masterStatusSystem1;
      followerStatusSystem2 = masterStatusSystem2;
    }
    return Arrays.asList(leaderStatusSystem, followerStatusSystem1, followerStatusSystem2);
  }

  public void stopRaftServers(List<HAMasterMetaManager> raftServers) {
    for (HAMasterMetaManager metaManager : raftServers) {
      metaManager.getRatisServer().stop();
    }
  }

  @Test
  public void testInstallSnapshot()
      throws IOException, InterruptedException, InvocationTargetException, NoSuchMethodException,
          IllegalAccessException, ClassNotFoundException {
    /**
     * We will first write 180 logs, then pause follower2, and then write 20 logs. At this time, the
     * leader will trigger a snapshot, at this time purge condition is reached, purge closed
     * segments. When follower2 resumes, install snapshot from leader will be triggered.
     */
    List<HAMasterMetaManager> raftServers = startRaftServers();
    HAMasterMetaManager leaderStatusSystem = raftServers.get(0);
    HAMasterMetaManager followerStatusSystem1 = raftServers.get(1);
    HAMasterMetaManager followerStatusSystem2 = raftServers.get(2);
    Map<String, DiskInfo> disks1 = new HashMap<>();
    Map<UserIdentifier, ResourceConsumption> userResourceConsumption1 = new HashMap<>();

    // per register produces 2 logs
    for (int i = 0; i < 50; i++) {
      leaderStatusSystem.handleRegisterWorker(
          "host1",
          1000 + i,
          2000 + i,
          3000 + i,
          4000 + i,
          5000 + i,
          "networkLocation1",
          null,
          disks1,
          userResourceConsumption1,
          getNewReqeustId());
    }
    // wait for taking snapshot
    Thread.sleep(2000);
    Assert.assertEquals(
        100,
        leaderStatusSystem.getRatisServer().getMasterStateMachine().getLatestSnapshot().getIndex());

    Assert.assertEquals(
        100,
        followerStatusSystem1
            .getRatisServer()
            .getMasterStateMachine()
            .getLatestSnapshot()
            .getIndex());

    Assert.assertEquals(
        100,
        followerStatusSystem2
            .getRatisServer()
            .getMasterStateMachine()
            .getLatestSnapshot()
            .getIndex());

    for (int i = 0; i < 40; i++) {
      leaderStatusSystem.handleRegisterWorker(
          "host1",
          1100 + i,
          2100 + i,
          3100 + i,
          4100 + i,
          5100 + i,
          "networkLocation1",
          null,
          disks1,
          userResourceConsumption1,
          getNewReqeustId());
    }
    // wait for sync
    Thread.sleep(2000);

    Assert.assertEquals(
        180,
        leaderStatusSystem
            .getRatisServer()
            .getMasterStateMachine()
            .getLastAppliedTermIndex()
            .getIndex());

    Assert.assertEquals(
        180,
        followerStatusSystem1
            .getRatisServer()
            .getMasterStateMachine()
            .getLastAppliedTermIndex()
            .getIndex());

    Assert.assertEquals(
        180,
        followerStatusSystem2
            .getRatisServer()
            .getMasterStateMachine()
            .getLastAppliedTermIndex()
            .getIndex());

    pauseRaftServer(followerStatusSystem2.getRatisServer().getServer());
    Thread.sleep(200);
    for (int i = 0; i < 10; i++) {
      leaderStatusSystem.handleRegisterWorker(
          "host1",
          1180 + i,
          2180 + i,
          3180 + i,
          4180 + i,
          5180 + i,
          "networkLocation1",
          null,
          disks1,
          userResourceConsumption1,
          getNewReqeustId());
    }
    Thread.sleep(2000);
    Assert.assertEquals(
        200,
        leaderStatusSystem.getRatisServer().getMasterStateMachine().getLatestSnapshot().getIndex());
    Assert.assertEquals(
        200,
        leaderStatusSystem
            .getRatisServer()
            .getMasterStateMachine()
            .getLastAppliedTermIndex()
            .getIndex());

    Assert.assertEquals(
        200,
        followerStatusSystem1
            .getRatisServer()
            .getMasterStateMachine()
            .getLatestSnapshot()
            .getIndex());
    Assert.assertEquals(
        200,
        followerStatusSystem1
            .getRatisServer()
            .getMasterStateMachine()
            .getLastAppliedTermIndex()
            .getIndex());

    Assert.assertEquals(
        100,
        followerStatusSystem2
            .getRatisServer()
            .getMasterStateMachine()
            .getLatestSnapshot()
            .getIndex());
    Assert.assertEquals(
        180,
        followerStatusSystem2
            .getRatisServer()
            .getMasterStateMachine()
            .getLastAppliedTermIndex()
            .getIndex());

    resumeRaftServer(followerStatusSystem2.getRatisServer().getServer());
    // install snapshot from leader
    Thread.sleep(5000);
    Assert.assertEquals(
        200,
        followerStatusSystem2
            .getRatisServer()
            .getMasterStateMachine()
            .getLatestSnapshot()
            .getIndex());
    Assert.assertEquals(
        200,
        followerStatusSystem2
            .getRatisServer()
            .getMasterStateMachine()
            .getLastAppliedTermIndex()
            .getIndex());
    stopRaftServers(raftServers);
  }
}
