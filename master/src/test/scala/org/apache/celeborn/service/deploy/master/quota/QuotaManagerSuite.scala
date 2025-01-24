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

package org.apache.celeborn.service.deploy.master.quota

import java.util

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.util.Random

import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.metrics.source.ResourceConsumptionSource
import org.apache.celeborn.common.protocol.message.ControlMessages.CheckQuotaResponse
import org.apache.celeborn.common.quota.{ResourceConsumption, StorageQuota}
import org.apache.celeborn.common.rpc.RpcEnv
import org.apache.celeborn.common.util.{JavaUtils, Utils}
import org.apache.celeborn.server.common.service.config.{ConfigService, DynamicConfigServiceFactory, FsConfigServiceImpl}
import org.apache.celeborn.service.deploy.master.MasterSource
import org.apache.celeborn.service.deploy.master.clustermeta.{AbstractMetaManager, SingleMasterMetaManager}

class QuotaManagerSuite extends CelebornFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Logging {
  protected var quotaManager: QuotaManager = _

  private var resourceConsumptionSource: ResourceConsumptionSource = _

  val worker = new WorkerInfo(
    "localhost",
    10001,
    10002,
    10003,
    10004)

  var statusSystem: AbstractMetaManager = _

  var rpcEnv: RpcEnv = _

  val workerToResourceConsumptions =
    JavaUtils.newConcurrentHashMap[String, util.Map[UserIdentifier, ResourceConsumption]]()

  val conf = new CelebornConf()

  var configService: ConfigService = _

  override def beforeAll(): Unit = {
    conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND, "FS")
    conf.set(
      CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH.key,
      getTestResourceFile("dynamicConfig-quota.yaml").getPath)
    resourceConsumptionSource = new ResourceConsumptionSource(conf, "Master")
    DynamicConfigServiceFactory.reset()
    configService = DynamicConfigServiceFactory.getConfigService(conf)

    rpcEnv = RpcEnv.create(
      "test-rpc",
      "rpc",
      "localhost",
      9001,
      conf,
      "master",
      None)
    statusSystem = new SingleMasterMetaManager(rpcEnv, conf)
    statusSystem.availableWorkers.add(worker)
    quotaManager = new QuotaManager(
      statusSystem,
      new MasterSource(conf),
      resourceConsumptionSource,
      conf,
      configService)
  }

  override def afterAll(): Unit = {
    rpcEnv.shutdown()
  }

  test("test celeborn quota conf") {
    configService.refreshCache()
    assertEquals(
      quotaManager.getUserStorageQuota(UserIdentifier("tenant_01", "Jerry")),
      StorageQuota(
        Utils.byteStringAsBytes("100G"),
        10000,
        Utils.byteStringAsBytes("10G"),
        Long.MaxValue))
    // Fallback to tenant level
    assertEquals(
      quotaManager.getUserStorageQuota(UserIdentifier("tenant_01", "name_not_exist")),
      StorageQuota(
        Utils.byteStringAsBytes("10G"),
        1000,
        Utils.byteStringAsBytes("10G"),
        Long.MaxValue))
    // Fallback to system level
    assertEquals(
      quotaManager.getUserStorageQuota(UserIdentifier("tenant_not_exist", "Tom")),
      StorageQuota(
        Utils.byteStringAsBytes("1G"),
        100,
        Utils.byteStringAsBytes("1G"),
        Long.MaxValue))
  }

  test("test check user quota return result") {
    val user = UserIdentifier("tenant_01", "Jerry")
    val rc1 =
      ResourceConsumption(Utils.byteStringAsBytes("10G"), 20, Utils.byteStringAsBytes("1G"), 40)
    val rc2 =
      ResourceConsumption(Utils.byteStringAsBytes("10G"), 20, Utils.byteStringAsBytes("30G"), 40)
    val rc3 =
      ResourceConsumption(
        Utils.byteStringAsBytes("200G"),
        20000,
        Utils.byteStringAsBytes("30G"),
        40)

    addUserConsumption(user, rc1)
    quotaManager.updateResourceConsumption()
    val res1 = checkUserQuota(user)

    addUserConsumption(user, rc2)
    quotaManager.updateResourceConsumption()
    val res2 = checkUserQuota(user)

    addUserConsumption(user, rc3)
    quotaManager.updateResourceConsumption()
    val res3 = checkUserQuota(user)

    val exp1 = CheckQuotaResponse(true, "")
    val exp2 = CheckQuotaResponse(
      false,
      s"Interrupt or reject application caused by the user storage usage reach threshold. " +
        s"user: `tenant_01`.`Jerry`.  " +
        s"HDFS_BYTES_WRITTEN(30.0 GiB) exceeds quota(10.0 GiB). ")
    val exp3 = CheckQuotaResponse(
      false,
      s"Interrupt or reject application caused by the user storage usage reach threshold. " +
        s"user: `tenant_01`.`Jerry`.  " +
        s"DISK_BYTES_WRITTEN(200.0 GiB) exceeds quota(100.0 GiB). " +
        s"DISK_FILE_COUNT(20000) exceeds quota(10000). " +
        s"HDFS_BYTES_WRITTEN(30.0 GiB) exceeds quota(10.0 GiB). ")

    assert(res1 == exp1)
    assert(res2 == exp2)
    assert(res3 == exp3)
    clearUserConsumption()
  }

  test("test check application quota return result") {
    val user = UserIdentifier("tenant_01", "Jerry")
    var rc =
      ResourceConsumption(
        Utils.byteStringAsBytes("200G"),
        20000,
        Utils.byteStringAsBytes("30G"),
        40)
    rc.withSubResourceConsumptions(
      Map(
        "app1" -> ResourceConsumption(
          Utils.byteStringAsBytes("150G"),
          15000,
          Utils.byteStringAsBytes("25G"),
          20),
        "app2" -> ResourceConsumption(
          Utils.byteStringAsBytes("50G"),
          5000,
          Utils.byteStringAsBytes("5G"),
          20)).asJava)

    addUserConsumption(user, rc)
    conf.set("celeborn.quota.cluster.diskBytesWritten", "60gb")
    configService.refreshCache()
    quotaManager.updateResourceConsumption()
    var res1 = checkUserQuota(user)
    var res2 = checkApplicationQuota(user, "app1")
    var res3 = checkApplicationQuota(user, "app2")

    val succeed = CheckQuotaResponse(true, "")
    val failed = CheckQuotaResponse(
      false,
      s"Interrupt or reject application caused by the user storage usage reach threshold. " +
        s"user: `tenant_01`.`Jerry`.  " +
        s"DISK_BYTES_WRITTEN(200.0 GiB) exceeds quota(100.0 GiB). " +
        s"DISK_FILE_COUNT(20000) exceeds quota(10000). " +
        s"HDFS_BYTES_WRITTEN(30.0 GiB) exceeds quota(10.0 GiB). ")
    assert(res1 == failed)
    assert(res2 == CheckQuotaResponse(
      false,
      "Interrupt or reject application caused by the user storage usage reach threshold. " +
        "Used: " +
        "ResourceConsumption(" +
        "diskBytesWritten: 150.0 GiB, " +
        "diskFileCount: 15000, " +
        "hdfsBytesWritten: 25.0 GiB, " +
        "hdfsFileCount: 20), " +
        "Threshold: " +
        "Quota[" +
        "diskBytesWritten=100.0 GiB, " +
        "diskFileCount=10000, " +
        "hdfsBytesWritten=10.0 GiB, " +
        "hdfsFileCount=9223372036854775807]"))
    assert(res3 == succeed)

    conf.set("celeborn.quota.cluster.diskBytesWritten", "50gb")
    configService.refreshCache()
    quotaManager.updateResourceConsumption()
    res1 = checkUserQuota(user)
    res2 = checkApplicationQuota(user, "app1")
    res3 = checkApplicationQuota(user, "app2")

    assert(res1 == failed)
    assert(res2 == CheckQuotaResponse(
      false,
      "Interrupt or reject application caused by the user storage usage reach threshold. " +
        "Used: ResourceConsumption(" +
        "diskBytesWritten: 150.0 GiB, " +
        "diskFileCount: 15000, " +
        "hdfsBytesWritten: 25.0 GiB, " +
        "hdfsFileCount: 20), " +
        "Threshold: Quota[" +
        "diskBytesWritten=100.0 GiB, " +
        "diskFileCount=10000, " +
        "hdfsBytesWritten=10.0 GiB, " +
        "hdfsFileCount=9223372036854775807]"))
    assert(res3 == CheckQuotaResponse(
      false,
      "Interrupt application caused by the cluster storage usage reach threshold. " +
        "Used: ResourceConsumption(" +
        "diskBytesWritten: 50.0 GiB, " +
        "diskFileCount: 5000, " +
        "hdfsBytesWritten: 5.0 GiB, " +
        "hdfsFileCount: 20), " +
        "Threshold: " +
        "Quota[" +
        "diskBytesWritten=50.0 GiB, " +
        "diskFileCount=9223372036854775807, " +
        "hdfsBytesWritten=8.0 EiB, " +
        "hdfsFileCount=9223372036854775807]"))
    clearUserConsumption()

    rc =
      ResourceConsumption(
        Utils.byteStringAsBytes("50G"),
        1000,
        Utils.byteStringAsBytes("5G"),
        40)
    rc.withSubResourceConsumptions(
      Map(
        "app1" -> ResourceConsumption(
          Utils.byteStringAsBytes("40G"),
          500,
          Utils.byteStringAsBytes("3G"),
          20),
        "app2" -> ResourceConsumption(
          Utils.byteStringAsBytes("10G"),
          500,
          Utils.byteStringAsBytes("2G"),
          20)).asJava)

    addUserConsumption(user, rc)
    conf.set("celeborn.quota.cluster.diskBytesWritten", "20gb")
    configService.refreshCache()
    quotaManager.updateResourceConsumption()

    res1 = checkUserQuota(user)
    res2 = checkApplicationQuota(user, "app1")
    res3 = checkApplicationQuota(user, "app2")

    assert(res1 == CheckQuotaResponse(
      false,
      "Interrupt application caused by the cluster storage usage reach threshold. " +
        "DISK_BYTES_WRITTEN(50.0 GiB) exceeds quota(20.0 GiB). "))
    assert(res2 == CheckQuotaResponse(
      false,
      "Interrupt application caused by the cluster storage usage reach threshold. " +
        "Used: " +
        "ResourceConsumption(" +
        "diskBytesWritten: 40.0 GiB, " +
        "diskFileCount: 500, " +
        "hdfsBytesWritten: 3.0 GiB, " +
        "hdfsFileCount: 20), " +
        "Threshold: " +
        "Quota[diskBytesWritten=20.0 GiB, " +
        "diskFileCount=9223372036854775807, " +
        "hdfsBytesWritten=8.0 EiB, " +
        "hdfsFileCount=9223372036854775807]"))
    assert(res3 == CheckQuotaResponse(true, ""))

    clearUserConsumption()
  }

  test("test handleResourceConsumption time - case1") {
    // 1000 users 100wapplications, all exceeded
    conf.set("celeborn.quota.tenant.diskBytesWritten", "1mb")
    conf.set("celeborn.quota.cluster.diskBytesWritten", "1mb")
    configService.refreshCache()
    val MAX = 2L * 1024 * 1024 * 1024
    val MIN = 1L * 1024 * 1024 * 1024
    val random = new Random()
    for (i <- 0 until 1000) {
      val user = UserIdentifier("default", s"user$i")
      val subResourceConsumption = (0 until 1000).map {
        index =>
          val appId = s"$user$i app$index"
          val consumption = ResourceConsumption(
            MIN + Math.abs(random.nextLong()) % (MAX - MIN),
            MIN + Math.abs(random.nextLong()) % (MAX - MIN),
            MIN + Math.abs(random.nextLong()) % (MAX - MIN),
            MIN + Math.abs(random.nextLong()) % (MAX - MIN))
          (appId, consumption)
      }.toMap
      val userConsumption = subResourceConsumption.values.foldRight(
        ResourceConsumption(0, 0, 0, 0))(_ add _)
      userConsumption.subResourceConsumptions = subResourceConsumption.asJava
      addUserConsumption(user, userConsumption)
    }

    val start = System.currentTimeMillis()
    quotaManager.updateResourceConsumption()
    val duration = System.currentTimeMillis() - start
    print(s"duration=$duration")

    val res = resourceConsumptionSource.getMetrics
    for (i <- 0 until 1000) {
      val user = UserIdentifier("default", s"user$i")
      assert(res.contains(
        s"""metrics_diskFileCount_Value{name="user$i",role="Master",tenantId="default"}"""))
      assert(res.contains(
        s"""metrics_diskFileCount_Value{name="user$i",role="Master",tenantId="default"}"""))
      assert(res.contains(
        s"""metrics_hdfsFileCount_Value{name="user$i",role="Master",tenantId="default"}"""))
      assert(res.contains(
        s"""metrics_hdfsBytesWritten_Value{name="user$i",role="Master",tenantId="default"}"""))
      assertFalse(quotaManager.checkUserQuotaStatus(user).isAvailable)
      (0 until 1000).foreach {
        index =>
          val appId = s"$user$i app$index"
          assertFalse(quotaManager.checkApplicationQuotaStatus(appId).isAvailable)
      }
    }
    clearUserConsumption()
  }

  test("test handleResourceConsumption time - case2") {
    // 1000 users 2000000 applications, all exceeded
    conf.set("celeborn.quota.tenant.diskBytesWritten", "1mb")
    conf.set("celeborn.quota.cluster.diskBytesWritten", "1mb")
    configService.refreshCache()
    val MAX = 2L * 1024 * 1024 * 1024
    val MIN = 1L * 1024 * 1024 * 1024
    val random = new Random()
    for (i <- 0 until 1000) {
      val user = UserIdentifier("default", s"user$i")
      val subResourceConsumption =
        if (i < 100) {
          (0 until 1000).map {
            index =>
              val appId = s"$user$i app$index"
              val consumption = ResourceConsumption(
                MIN + Math.abs(random.nextLong()) % (MAX - MIN),
                MIN + Math.abs(random.nextLong()) % (MAX - MIN),
                MIN + Math.abs(random.nextLong()) % (MAX - MIN),
                MIN + Math.abs(random.nextLong()) % (MAX - MIN))
              (appId, consumption)
          }.toMap
        } else {
          (0 until 1000).map {
            index =>
              val appId = s"$user$i app$index"
              val consumption = ResourceConsumption(0, 0, 0, 0)
              (appId, consumption)
          }.toMap
        }
      val userConsumption = subResourceConsumption.values.foldRight(
        ResourceConsumption(0, 0, 0, 0))(_ add _)
      userConsumption.subResourceConsumptions = subResourceConsumption.asJava
      addUserConsumption(user, userConsumption)
    }

    val start = System.currentTimeMillis()
    quotaManager.updateResourceConsumption()
    val duration = System.currentTimeMillis() - start
    print(s"duration=$duration")

    val res = resourceConsumptionSource.getMetrics
    for (i <- 0 until 1000) {
      val user = UserIdentifier("default", s"user$i")
      assert(res.contains(
        s"""metrics_diskFileCount_Value{name="user$i",role="Master",tenantId="default"}"""))
      assert(res.contains(
        s"""metrics_diskFileCount_Value{name="user$i",role="Master",tenantId="default"}"""))
      assert(res.contains(
        s"""metrics_hdfsFileCount_Value{name="user$i",role="Master",tenantId="default"}"""))
      assert(res.contains(
        s"""metrics_hdfsBytesWritten_Value{name="user$i",role="Master",tenantId="default"}"""))
      assertFalse(quotaManager.checkUserQuotaStatus(user).isAvailable)
      (0 until 1000).foreach {
        index =>
          val appId = s"$user$i app$index"
          if (i < 100) {
            assertFalse(quotaManager.checkApplicationQuotaStatus(appId).isAvailable)
          } else {
            assertTrue(quotaManager.checkApplicationQuotaStatus(appId).isAvailable)
          }
      }
    }
    clearUserConsumption()
  }

  test("test user level conf") {
    val conf1 = new CelebornConf()
    conf1.set(CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND, "FS")
    conf1.set(
      CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH.key,
      getTestResourceFile("dynamicConfig-quota-2.yaml").getPath)
    val rpcEnv = RpcEnv.create(
      "test-rpc",
      "rpc",
      "localhost",
      9002,
      conf,
      "master",
      None)
    val statusSystem1 = new SingleMasterMetaManager(rpcEnv, conf)
    statusSystem1.availableWorkers.add(worker)
    val quotaManager1 = new QuotaManager(
      statusSystem1,
      new MasterSource(conf1),
      resourceConsumptionSource,
      conf1,
      new FsConfigServiceImpl(conf1))

    val user = UserIdentifier("tenant_01", "Jerry")
    val user1 = UserIdentifier("tenant_01", "John")

    val rc =
      ResourceConsumption(
        Utils.byteStringAsBytes("200G"),
        20000,
        Utils.byteStringAsBytes("30G"),
        40)
    rc.withSubResourceConsumptions(
      Map(
        "app1" -> ResourceConsumption(
          Utils.byteStringAsBytes("150G"),
          15000,
          Utils.byteStringAsBytes("25G"),
          20),
        "app2" -> ResourceConsumption(
          Utils.byteStringAsBytes("50G"),
          5000,
          Utils.byteStringAsBytes("5G"),
          20)).asJava)

    val rc1 =
      ResourceConsumption(
        Utils.byteStringAsBytes("80G"),
        0,
        0,
        0)

    rc1.withSubResourceConsumptions(
      Map(
        "app3" -> ResourceConsumption(
          Utils.byteStringAsBytes("80G"),
          0,
          0,
          0)).asJava)

    addUserConsumption(user, rc)
    addUserConsumption(user1, rc1)

    quotaManager1.updateResourceConsumption()
    val res1 = quotaManager1.checkUserQuotaStatus(user)
    val res2 = quotaManager1.checkApplicationQuotaStatus("app1")
    val res3 = quotaManager1.checkApplicationQuotaStatus("app2")
    val res4 = quotaManager1.checkApplicationQuotaStatus("app3")
    assert(res1 == CheckQuotaResponse(
      false,
      s"Interrupt or reject application caused by the user storage usage reach threshold. " +
        s"user: `tenant_01`.`Jerry`.  " +
        s"DISK_BYTES_WRITTEN(200.0 GiB) exceeds quota(100.0 GiB). " +
        s"DISK_FILE_COUNT(20000) exceeds quota(10000). " +
        s"HDFS_BYTES_WRITTEN(30.0 GiB) exceeds quota(10.0 GiB). "))
    assert(res2 == CheckQuotaResponse(
      false,
      "Interrupt or reject application caused by the user storage usage reach threshold. " +
        "Used: ResourceConsumption(" +
        "diskBytesWritten: 150.0 GiB, " +
        "diskFileCount: 15000, " +
        "hdfsBytesWritten: 25.0 GiB, " +
        "hdfsFileCount: 20), " +
        "Threshold: " +
        "Quota[" +
        "diskBytesWritten=100.0 GiB, " +
        "diskFileCount=10000, " +
        "hdfsBytesWritten=10.0 GiB, " +
        "hdfsFileCount=9223372036854775807]"))
    assert(res3 == CheckQuotaResponse(true, ""))
    assert(res4 == CheckQuotaResponse(
      false,
      "Interrupt or reject application caused by the user storage usage reach threshold. " +
        "Used: " +
        "ResourceConsumption(" +
        "diskBytesWritten: 80.0 GiB, " +
        "diskFileCount: 0, " +
        "hdfsBytesWritten: 0.0 B, " +
        "hdfsFileCount: 0), " +
        "Threshold: " +
        "Quota[" +
        "diskBytesWritten=10.0 GiB, " +
        "diskFileCount=1000, " +
        "hdfsBytesWritten=10.0 GiB, " +
        "hdfsFileCount=9223372036854775807]"))

    clearUserConsumption()
  }

  test("test tenant level conf") {
    val conf1 = new CelebornConf()
    conf1.set(CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND, "FS")
    conf1.set(
      CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH.key,
      getTestResourceFile("dynamicConfig-quota-3.yaml").getPath)
    val rpcEnv = RpcEnv.create(
      "test-rpc",
      "rpc",
      "localhost",
      9003,
      conf,
      "master",
      None)
    val statusSystem1 = new SingleMasterMetaManager(rpcEnv, conf)
    statusSystem1.availableWorkers.add(worker)
    val quotaManager1 = new QuotaManager(
      statusSystem1,
      new MasterSource(conf1),
      resourceConsumptionSource,
      conf1,
      new FsConfigServiceImpl(conf1))

    val user1 = UserIdentifier("tenant_01", "Jerry")
    val user2 = UserIdentifier("tenant_01", "John")

    val rc1 =
      ResourceConsumption(
        Utils.byteStringAsBytes("230G"),
        0,
        0,
        0)
    rc1.withSubResourceConsumptions(
      Map(
        "app1" -> ResourceConsumption(
          Utils.byteStringAsBytes("150G"),
          0,
          0,
          0),
        "app2" -> ResourceConsumption(
          Utils.byteStringAsBytes("80G"),
          0,
          0,
          0)).asJava)

    val rc2 =
      ResourceConsumption(
        Utils.byteStringAsBytes("220G"),
        0,
        0,
        0)

    rc2.withSubResourceConsumptions(
      Map(
        "app3" -> ResourceConsumption(
          Utils.byteStringAsBytes("150G"),
          0,
          0,
          0),
        "app4" -> ResourceConsumption(
          Utils.byteStringAsBytes("70G"),
          0,
          0,
          0)).asJava)

    addUserConsumption(user1, rc1)
    addUserConsumption(user2, rc2)

    quotaManager1.updateResourceConsumption()
    val res1 = quotaManager1.checkUserQuotaStatus(user1)
    val res2 = quotaManager1.checkUserQuotaStatus(user2)
    val res3 = quotaManager1.checkApplicationQuotaStatus("app1")
    val res4 = quotaManager1.checkApplicationQuotaStatus("app2")
    val res5 = quotaManager1.checkApplicationQuotaStatus("app3")
    val res6 = quotaManager1.checkApplicationQuotaStatus("app4")
    assert(res1 == CheckQuotaResponse(
      false,
      "" +
        "Interrupt or reject application caused by the user storage usage reach threshold. " +
        "user: `tenant_01`.`Jerry`.  DISK_BYTES_WRITTEN(230.0 GiB) exceeds quota(100.0 GiB). "))
    assert(res2 == CheckQuotaResponse(
      false,
      "Interrupt or reject application caused by the user storage usage reach threshold. " +
        "user: `tenant_01`.`John`.  DISK_BYTES_WRITTEN(220.0 GiB) exceeds quota(100.0 GiB). "))
    assert(res3 == CheckQuotaResponse(
      false,
      "Interrupt or reject application caused by the user storage usage reach threshold. " +
        "Used: ResourceConsumption(" +
        "diskBytesWritten: 150.0 GiB, " +
        "diskFileCount: 0, " +
        "hdfsBytesWritten: 0.0 B, " +
        "hdfsFileCount: 0), " +
        "Threshold: Quota[" +
        "diskBytesWritten=100.0 GiB, " +
        "diskFileCount=10000, " +
        "hdfsBytesWritten=8.0 EiB, " +
        "hdfsFileCount=9223372036854775807]"))
    assert(res4 == CheckQuotaResponse(
      false,
      "Interrupt application caused by the tenant storage usage reach threshold. " +
        "Used: ResourceConsumption(" +
        "diskBytesWritten: 80.0 GiB, " +
        "diskFileCount: 0, " +
        "hdfsBytesWritten: 0.0 B, " +
        "hdfsFileCount: 0), " +
        "Threshold: Quota[" +
        "diskBytesWritten=150.0 GiB, " +
        "diskFileCount=1500, " +
        "hdfsBytesWritten=8.0 EiB, " +
        "hdfsFileCount=9223372036854775807]"))
    assert(res5 == CheckQuotaResponse(
      false,
      "Interrupt or reject application caused by the user storage usage reach threshold. " +
        "Used: ResourceConsumption(" +
        "diskBytesWritten: 150.0 GiB, " +
        "diskFileCount: 0, " +
        "hdfsBytesWritten: 0.0 B, " +
        "hdfsFileCount: 0), " +
        "Threshold: Quota[" +
        "diskBytesWritten=100.0 GiB, " +
        "diskFileCount=10000, " +
        "hdfsBytesWritten=8.0 EiB, " +
        "hdfsFileCount=9223372036854775807]"))
    assert(res6 == CheckQuotaResponse(true, ""))
    clearUserConsumption()
  }

  def checkUserQuota(userIdentifier: UserIdentifier): CheckQuotaResponse = {
    quotaManager.checkUserQuotaStatus(userIdentifier)
  }

  def checkApplicationQuota(
      userIdentifier: UserIdentifier,
      applicationId: String): CheckQuotaResponse = {
    quotaManager.checkApplicationQuotaStatus(applicationId)
  }

  def addUserConsumption(
      userIdentifier: UserIdentifier,
      resourceConsumption: ResourceConsumption): Unit = {
    worker.userResourceConsumption.put(userIdentifier, resourceConsumption)
    workerToResourceConsumptions.put(worker.toUniqueId, worker.userResourceConsumption)
  }

  def clearUserConsumption(): Unit = {
    val applicationSet = worker.userResourceConsumption.asScala.values.flatMap { consumption =>
      Option(consumption.subResourceConsumptions).map(_.asScala.keySet)
    }.flatten.toSet

    applicationSet.foreach(quotaManager.handleAppLost)
    worker.userResourceConsumption.clear()
  }
}
