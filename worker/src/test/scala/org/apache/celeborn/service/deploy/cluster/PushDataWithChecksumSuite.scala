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

package org.apache.celeborn.service.deploy.cluster

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import io.netty.buffer.Unpooled
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer
import org.apache.celeborn.common.network.client.RpcResponseCallback
import org.apache.celeborn.common.network.protocol.PushData
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.common.util.{PushDataHeaderUtils, Utils}
import org.apache.celeborn.service.deploy.MiniClusterFeature

class PushDataWithChecksumSuite extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {
  var masterPort = 19097

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    val workerConf = Map(
      CelebornConf.WORKER_CHECKSUM_VERIFY_ENABLED.key -> "true")
    val (master, _) = setupMiniClusterWithRandomPorts(masterConf = Map(), workerConf)
    masterPort = master.conf.masterPort
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop Celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("push data and checksum error") {
    val APP = s"app-${System.currentTimeMillis()}"
    val SHUFFLE_ID = 0
    val MAP_ID = 0
    val ATTEMPT_ID = 0
    val MAP_NUM = 1
    val PARTITION_NUM = 3

    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.CLIENT_SHUFFLE_CHECKSUM_ENABLED.key, "true")
    val lifecycleManager = new LifecycleManager(APP, clientConf)
    val shuffleClient = new ShuffleClientImpl(APP, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    // ping and reserveSlots
    val DATA0 = RandomStringUtils.secure().next(10).getBytes(StandardCharsets.UTF_8)
    shuffleClient.pushData(
      SHUFFLE_ID,
      MAP_ID,
      ATTEMPT_ID,
      0,
      DATA0,
      0,
      DATA0.length,
      MAP_NUM,
      PARTITION_NUM)

    val partitionLocationMap =
      shuffleClient.getPartitionLocation(SHUFFLE_ID, MAP_NUM, PARTITION_NUM)
    val location = partitionLocationMap.get(SHUFFLE_ID)
    val shuffleKey = Utils.makeShuffleKey(APP, SHUFFLE_ID)

    val batchId = 123 // mock batch id
    val bodyLength = 100
    val buffer: Array[Byte] = Array.fill(PushDataHeaderUtils.BATCH_HEADER_SIZE)(
      0.toByte) ++ RandomStringUtils.secure().next(bodyLength).getBytes(StandardCharsets.UTF_8)
    PushDataHeaderUtils.buildDataHeader(buffer, MAP_ID, ATTEMPT_ID, batchId, bodyLength, true)
    val checksum = PushDataHeaderUtils.computeHeaderChecksum32(buffer)
    Platform.putInt(buffer, PushDataHeaderUtils.CHECKSUM_OFFSET, checksum + 1) // error checksum

    val nettyBuffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(buffer));
    val client =
      shuffleClient.getDataClientFactory.createClient(location.getHost, location.getPushPort)
    val pushData = new PushData(
      PartitionLocation.Mode.PRIMARY.mode,
      shuffleKey,
      location.getUniqueId,
      nettyBuffer)
    val callback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        fail("push data should fail")
      }

      override def onFailure(e: Throwable): Unit = {
        assert(
          e.getMessage == StatusCode.PUSH_DATA_CHECKSUM_FAIL.toString,
          "push data should fail for checksum error, but get " + e.getMessage)
      }
    }
    val pushDataTimeout = 120 * 1000
    client.pushData(pushData, pushDataTimeout, callback)
  }
}
