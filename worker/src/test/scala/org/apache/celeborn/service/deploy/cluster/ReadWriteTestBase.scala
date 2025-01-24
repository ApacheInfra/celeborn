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

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.control.Breaks

import org.apache.commons.lang3.RandomStringUtils
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.CompressionCodec
import org.apache.celeborn.service.deploy.MiniClusterFeature

trait ReadWriteTestBase extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {

  var masterPort = 0

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    val (m, _) = setupMiniClusterWithRandomPorts()
    masterPort = m.conf.masterPort
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop Celeborn mini cluster")
    shutdownMiniCluster()
  }

  def testReadWriteByCode(codec: CompressionCodec, readLocalShuffle: Boolean = false): Unit = {
    val APP = "app-1"

    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, codec.name)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
      .set(CelebornConf.READ_LOCAL_SHUFFLE_FILE, readLocalShuffle)
      .set("celeborn.data.io.numConnectionsPerPeer", "1")
    val lifecycleManager = new LifecycleManager(APP, clientConf)
    val shuffleClient = new ShuffleClientImpl(APP, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)
    val dataPrefix = Array("000000", "111111", "222222", "333333")
    val dataPrefixMap = new mutable.HashMap[String, String]
    val STR1 = dataPrefix(0) + RandomStringUtils.random(1024)
    dataPrefixMap.put(dataPrefix(0), STR1)
    val DATA1 = STR1.getBytes(StandardCharsets.UTF_8)
    val OFFSET1 = 0
    val LENGTH1 = DATA1.length

    val dataSize1 = shuffleClient.pushData(1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 1, 1)
    logInfo(s"push data data size $dataSize1")

    val STR2 = dataPrefix(1) + RandomStringUtils.random(32 * 1024)
    dataPrefixMap.put(dataPrefix(1), STR2)
    val DATA2 = STR2.getBytes(StandardCharsets.UTF_8)
    val OFFSET2 = 0
    val LENGTH2 = DATA2.length
    val dataSize2 = shuffleClient.pushData(1, 0, 0, 0, DATA2, OFFSET2, LENGTH2, 1, 1)
    logInfo("push data data size " + dataSize2)

    val STR3 = dataPrefix(2) + RandomStringUtils.random(32 * 1024)
    dataPrefixMap.put(dataPrefix(2), STR3)
    val DATA3 = STR3.getBytes(StandardCharsets.UTF_8)
    val LENGTH3 = DATA3.length
    shuffleClient.mergeData(1, 0, 0, 0, DATA3, 0, LENGTH3, 1, 1)

    val STR4 = dataPrefix(3) + RandomStringUtils.random(16 * 1024)
    dataPrefixMap.put(dataPrefix(3), STR4)
    val DATA4 = STR4.getBytes(StandardCharsets.UTF_8)
    val LENGTH4 = DATA4.length
    shuffleClient.mergeData(1, 0, 0, 0, DATA4, 0, LENGTH4, 1, 1)
    shuffleClient.pushMergedData(1, 0, 0)
    Thread.sleep(1000)

    shuffleClient.mapperEnd(1, 0, 0, 1)

    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesWritten: Long): Unit = {}
      override def incReadTime(time: Long): Unit = {}
    }
    val inputStream = shuffleClient.readPartition(
      1,
      1,
      0,
      0,
      0,
      0,
      Integer.MAX_VALUE,
      null,
      null,
      null,
      null,
      null,
      null,
      metricsCallback)
    val outputStream = new ByteArrayOutputStream()

    var b = inputStream.read()
    while (b != -1) {
      outputStream.write(b)
      b = inputStream.read()
    }

    val readBytes = outputStream.toByteArray
    val readStringMap = getReadStringMap(readBytes, dataPrefix, dataPrefixMap)

    Assert.assertEquals(LENGTH1 + LENGTH2 + LENGTH3 + LENGTH4, readBytes.length)
    for ((prefix, data) <- readStringMap) {
      Assert.assertEquals(dataPrefixMap.get(prefix).get, data)
    }

    Thread.sleep(5000L)
    shuffleClient.shutdown()
    lifecycleManager.rpcEnv.shutdown()

  }

  def getReadStringMap(
      readBytes: Array[Byte],
      dataPrefix: Array[String],
      dataPrefixMap: mutable.HashMap[String, String]): mutable.HashMap[String, String] = {
    var readString = new String(readBytes, StandardCharsets.UTF_8)
    val prefixStringMap = new mutable.HashMap[String, String]
    val loop = new Breaks;
    for (i <- 0 to 4) {
      loop.breakable {
        for (prefix <- dataPrefix) {
          if (readString.startsWith(prefix)) {
            val subString = readString.substring(0, dataPrefixMap.get(prefix).get.length)
            prefixStringMap.put(prefix, subString)
            println(
              s"readString before: ${readString.length}, ${dataPrefixMap.get(prefix).get.length}")
            readString = readString.substring(dataPrefixMap.get(prefix).get.length)
            println(s"readString after: ${readString.length}")
            loop.break()
          }
        }
      }
    }
    prefixStringMap
  }
}
