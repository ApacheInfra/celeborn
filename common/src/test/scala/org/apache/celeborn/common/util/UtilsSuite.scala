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

package org.apache.celeborn.common.util

import java.util
import java.util.Collections

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.client.{MasterEndpointResolver, StaticMasterEndpointResolver}
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.identity.DefaultIdentityProvider
import org.apache.celeborn.common.protocol.{PartitionLocation, TransportModuleConstants}
import org.apache.celeborn.common.protocol.message.ControlMessages.{GetReducerFileGroupResponse, MapperEnd}
import org.apache.celeborn.common.protocol.message.StatusCode

class UtilsSuite extends CelebornFunSuite {

  test("stringToSeq") {
    val seq1 = Seq("asd", "bcd", "def")
    assert(seq1 == Utils.stringToSeq("asd,bcd,def"))

    val seq2 = Seq("a", "b", "d")
    assert(seq2 == Utils.stringToSeq("a,,b,d,"))
  }

  test("byteStringAsKB") {
    assert(1 == Utils.byteStringAsKb("1KB"))
    assert(1024 == Utils.byteStringAsKb("1MB"))
  }

  test("byteStringAsMb") {
    assert(16 == Utils.byteStringAsMb("16384KB"))
    assert(1 == Utils.byteStringAsMb("1MB"))
  }

  test("byteStringAsGb") {
    assert(16 == Utils.byteStringAsGb("16384MB"))
    assert(1 == Utils.byteStringAsGb("1GB"))
  }

  test("memoryStringToMb") {
    assert(16 == Utils.memoryStringToMb("16MB"))
    assert(16384 == Utils.memoryStringToMb("16GB"))
  }

  test("bytesToString") {
    assert("16.0 KiB" == Utils.bytesToString(16384))
    assert("16.0 MiB" == Utils.bytesToString(16777216))
    assert("16.0 GiB" == Utils.bytesToString(17179869184L))
  }

  test("msDurationToString") {
    assert(Utils.msDurationToString(1) === "1 ms")
    assert(Utils.msDurationToString(1234) === "1.2 s")
    assert(Utils.msDurationToString(67890) === "1.1 m")
    assert(Utils.msDurationToString(3678000) === "1.02 h")
  }

  test("nanoDurationToString") {
    assert(Utils.nanoDurationToString(1) === "1 ns")
    assert(Utils.nanoDurationToString(123456) === "123456 ns")
    assert(Utils.nanoDurationToString(1234567) === "1.2 ms")
    assert(Utils.nanoDurationToString(123456789L) === "123.5 ms")
    assert(Utils.nanoDurationToString(1234567890L) === "1.2 s")
    assert(Utils.nanoDurationToString(1234567890123L) === "20.6 m")
    assert(Utils.nanoDurationToString(12345678901234L) === "3.43 h")
  }

  test("extractHostPortFromCelebornUrl") {
    val target = ("abc", 123)
    val result = Utils.extractHostPortFromCelebornUrl("celeborn://abc:123")
    assert(target.equals(result))
  }

  test("tryOrExit") {
    Utils.tryOrExit({
      val a = 1
      val b = 3
      a + b
    })
  }

  test("encodeFileNameToURIRawPath") {
    assert("abc%3F" == Utils.encodeFileNameToURIRawPath("abc?"))
    assert("abc%3E" == Utils.encodeFileNameToURIRawPath("abc>"))
  }

  test("classIsLoadable") {
    assert(Utils.classIsLoadable("java.lang.String"))
    assert(false == Utils.classIsLoadable("a.b.c.d.e.f"))
  }

  test("instantiateMasterEndpointResolver") {
    val celebornConf = new CelebornConf()
    val masterEndpointResolver = Utils.instantiateMasterEndpointResolver[MasterEndpointResolver](
      celebornConf.masterEndpointResolver,
      celebornConf,
      isWorker = true)

    assert(masterEndpointResolver.isInstanceOf[MasterEndpointResolver])
    assert(masterEndpointResolver.isInstanceOf[StaticMasterEndpointResolver])
  }

  test("instantiateMasterEndpointResolver invalid resolver classname") {
    val celebornConf = new CelebornConf()
    val invalidClassName = "invalidClassName"

    val e = intercept[CelebornException] {
      Utils.instantiateMasterEndpointResolver[MasterEndpointResolver](
        invalidClassName,
        celebornConf,
        isWorker = true)
    }
    assert(s"Failed to instantiate masterEndpointResolver $invalidClassName." === e.getMessage)
  }

  test("splitPartitionLocationUniqueId") {
    assert((1, 1).equals(Utils.splitPartitionLocationUniqueId("1-1")))
  }

  test("bytesToInt") {
    assert(1229202015 == Utils.bytesToInt(Array(73.toByte, 68.toByte, 34.toByte, 95.toByte)))

    assert(1596081225 == Utils.bytesToInt(Array(73.toByte, 68.toByte, 34.toByte, 95.toByte), false))
  }

  test("getThreadDump") {
    assert(Utils.getThreadDump().nonEmpty)
  }

  test("MapperEnd class convert with pb") {
    val mapperEnd = MapperEnd(1, 1, 1, 2, 1, Collections.emptyMap())
    val mapperEndTrans =
      Utils.fromTransportMessage(Utils.toTransportMessage(mapperEnd)).asInstanceOf[MapperEnd]
    assert(mapperEnd == mapperEndTrans)
  }

  test("validate HDFS compatible fs path") {
    val hdfsPath = "hdfs://xxx:9000/xxxx/xx-xx/x-x-x"
    val simpleHdfsPath = "hdfs:///xxxx/xx-xx/x-x-x"
    val sortedHdfsPath = "hdfs://xxx:9000/xxxx/xx-xx/x-x-x.sorted"
    val indexHdfsPath = "hdfs://xxx:9000/xxxx/xx-xx/x-x-x.index"
    assert(true == Utils.isHdfsPath(hdfsPath))
    assert(true == Utils.isHdfsPath(sortedHdfsPath))
    assert(true == Utils.isHdfsPath(indexHdfsPath))
    assert(true == Utils.isHdfsPath(simpleHdfsPath))

    val juicePath = "jfs://xxxx/xx-xx/x-x-x"
    val sortedJuicePath = "jfs://xxxx/xx-xx/x-x-x.sorted"
    val indexJuicePath = "jfs://xxxx/xx-xx/x-x-x.index"
    assert(true == Utils.isHdfsPath(juicePath))
    assert(true == Utils.isHdfsPath(sortedJuicePath))
    assert(true == Utils.isHdfsPath(indexJuicePath))

    val ossPath = "oss://xxxx/xx-xx/x-x-x"
    val sortedOssPath = "oss://xxxx/xx-xx/x-x-x.sorted"
    val indexOssPath = "oss://xxxx/xx-xx/x-x-x.index"
    assert(true == Utils.isHdfsPath(ossPath))
    assert(true == Utils.isHdfsPath(sortedOssPath))
    assert(true == Utils.isHdfsPath(indexOssPath))

    val localPath = "/xxx/xxx/xx-xx/x-x-x"
    assert(false == Utils.isHdfsPath(localPath))
  }

  test("validate s3 compatible fs path") {
    val hdfsPath = "hdfs://xxx:9000/xxxx/xx-xx/x-x-x"
    val simpleS3Path = "s3a://xxxx/xx-xx/x-x-x"
    val sortedS3Path = "s3a://xxx/xxxx/xx-xx/x-x-x.sorted"
    val indexS3Path = "s3a://xxx/xxxx/xx-xx/x-x-x.index"
    assert(false == Utils.isS3Path(hdfsPath))
    assert(false == Utils.isHdfsPath(simpleS3Path))
    assert(true == Utils.isS3Path(simpleS3Path))
    assert(true == Utils.isS3Path(sortedS3Path))
    assert(true == Utils.isS3Path(indexS3Path))
  }

  test("GetReducerFileGroupResponse class convert with pb") {
    val fileGroup = new util.HashMap[Integer, util.Set[PartitionLocation]]
    fileGroup.put(0, partitionLocation(0))
    fileGroup.put(1, partitionLocation(1))
    fileGroup.put(2, partitionLocation(2))

    val attempts = Array(0, 0, 1)
    val response = GetReducerFileGroupResponse(StatusCode.STAGE_ENDED, fileGroup, attempts)
    val responseTrans = Utils.fromTransportMessage(Utils.toTransportMessage(response)).asInstanceOf[
      GetReducerFileGroupResponse]

    assert(response.status == responseTrans.status)
    assert(util.Arrays.equals(response.attempts, responseTrans.attempts))
    val set =
      (response.fileGroup.values().toArray diff responseTrans.fileGroup.values().toArray).toSet
    assert(set.size == 0)
  }

  test("validate number of client/server netty threads") {
    val celebornConf = new CelebornConf()
    celebornConf.set("celeborn.io.maxDefaultNettyThreads", "100")
    val transportConf =
      Utils.fromCelebornConf(celebornConf, TransportModuleConstants.PUSH_MODULE, 120)
    assert(transportConf.serverThreads() == 100)
    assert(transportConf.clientThreads() == 100)
  }

  test("parse colon separated host ports") {
    val ipV4Host = "192.168.0.1"
    val ipV6Host = "2600:1f13:9f:2d00:4a70:cc69:737d:7cb0"
    assert(Utils.parseColonSeparatedHostPorts(s"$ipV4Host:1000", 1).sameElements(
      Array[Object](ipV4Host, "1000")))
    assert(Utils.parseColonSeparatedHostPorts(s"$ipV4Host:1:2:3:4", 4).sameElements(
      Array[Object](ipV4Host, "1", "2", "3", "4")))
    assert(Utils.parseColonSeparatedHostPorts(s"$ipV6Host:1000", 1).sameElements(
      Array[Object](ipV6Host, "1000")))
    assert(Utils.parseColonSeparatedHostPorts(s"$ipV6Host:1:2:3:4", 4).sameElements(
      Array[Object](ipV6Host, "1", "2", "3", "4")))
  }

  def partitionLocation(partitionId: Int): util.HashSet[PartitionLocation] = {
    val partitionSet = new util.HashSet[PartitionLocation]
    for (i <- 0 until 3) {
      partitionSet.add(new PartitionLocation(
        partitionId,
        i,
        "host",
        100,
        1000,
        1001,
        100,
        PartitionLocation.Mode.PRIMARY))
    }
    partitionSet
  }

  test("test instantiate") {
    val celebornConf = new CelebornConf()
    assert(Utils.instantiate[DefaultIdentityProvider](celebornConf.identityProviderClass)
      .isInstanceOf[DefaultIdentityProvider])
  }
}
