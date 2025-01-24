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

import java.io.File
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import com.google.common.collect.{Lists, Sets}
import org.apache.hadoop.shaded.org.apache.commons.lang3.RandomStringUtils

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta._
import org.apache.celeborn.common.protocol.{PartitionLocation, PbPackedWorkerResource, PbWorkerResource, StorageInfo}
import org.apache.celeborn.common.protocol.PartitionLocation.Mode
import org.apache.celeborn.common.protocol.message.{ControlMessages, StatusCode}
import org.apache.celeborn.common.protocol.message.ControlMessages.{GetReducerFileGroupResponse, WorkerResource}
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.util.PbSerDeUtils.{fromPbPackedPartitionLocationsPair, toPbPackedPartitionLocationsPair}
import org.apache.celeborn.common.write.PushFailedBatch

class PbSerDeUtilsTest extends CelebornFunSuite {

  val fileSet = new util.HashSet[String]()
  fileSet.add("a")
  fileSet.add("b")
  fileSet.add("c")

  val major = 1
  val minor = 0

  val file1 = new File("/mnt/disk/1")
  val file2 = new File("/mnt/disk/2")
  val files = List(file1, file2)

  val device = new DeviceInfo("device-a")
  val diskInfo1 = new DiskInfo("/mnt/disk/0", 1000, 1000, 1000, 1000, files, device)
  val diskInfo2 = new DiskInfo("/mnt/disk/1", 2000, 2000, 2000, 2000, files, device)
  diskInfo1.setTotalSpace(100000000)
  diskInfo1.setTotalSpace(200000000)

  val diskInfos = new util.HashMap[String, DiskInfo]()
  diskInfos.put("disk1", diskInfo1)
  diskInfos.put("disk2", diskInfo2)

  val userIdentifier1 = UserIdentifier("tenant-a", "user-a")
  val userIdentifier2 = UserIdentifier("tenant-b", "user-b")

  val chunkOffsets1 = util.Arrays.asList[java.lang.Long](1000L, 2000L, 3000L)
  val chunkOffsets2 = util.Arrays.asList[java.lang.Long](2000L, 4000L, 6000L)

  val fileInfo1 = new DiskFileInfo(
    userIdentifier1,
    true,
    new ReduceFileMeta(chunkOffsets1, 123),
    file1.getAbsolutePath,
    3000L)
  val fileInfo2 = new DiskFileInfo(
    userIdentifier2,
    true,
    new ReduceFileMeta(chunkOffsets2, 123),
    file2.getAbsolutePath,
    6000L)
  val mapFileInfo1 = new DiskFileInfo(
    userIdentifier1,
    true,
    new MapFileMeta(1024, 10),
    file1.getAbsolutePath,
    6000L)
  val mapFileInfo2 = new DiskFileInfo(
    userIdentifier2,
    true,
    new MapFileMeta(1024, 10),
    file2.getAbsolutePath,
    6000L)
  val fileInfoMap = JavaUtils.newConcurrentHashMap[String, DiskFileInfo]()
  mapFileInfo1.setMountPoint("/mnt")
  mapFileInfo2.setMountPoint("/mnt")

  fileInfoMap.put("file1", fileInfo1)
  fileInfoMap.put("file2", fileInfo2)
  fileInfoMap.put("mapFile1", mapFileInfo1)
  fileInfoMap.put("mapFile2", mapFileInfo2)
  val mountPoints = new util.HashSet[String]
  mountPoints.add("/mnt")
  val cache = JavaUtils.newConcurrentHashMap[String, UserIdentifier]()

  val resourceConsumption1 = ResourceConsumption(1000, 2000, 3000, 4000)
  val resourceConsumption2 = ResourceConsumption(
    2000,
    4000,
    6000,
    8000,
    Map("appld2" -> ResourceConsumption(2000, 4000, 6000, 8000)).asJava)
  val userResourceConsumption = new util.HashMap[UserIdentifier, ResourceConsumption]()
  userResourceConsumption.put(userIdentifier1, resourceConsumption1)
  userResourceConsumption.put(userIdentifier2, resourceConsumption2)

  val workerInfo1 =
    new WorkerInfo(
      "localhost",
      1001,
      1002,
      1003,
      1004,
      1005,
      diskInfos,
      userResourceConsumption)
  workerInfo1.networkLocation_$eq("/1")
  val workerInfo2 =
    new WorkerInfo(
      "localhost",
      2001,
      2002,
      2003,
      2004,
      2005,
      diskInfos,
      userResourceConsumption)

  val partitionLocation1 =
    new PartitionLocation(0, 0, "host1", 10, 9, 8, 14, PartitionLocation.Mode.REPLICA)
  val partitionLocation2 =
    new PartitionLocation(1, 1, "host2", 20, 19, 18, 24, PartitionLocation.Mode.REPLICA)

  val partitionLocation3 =
    new PartitionLocation(2, 2, "host3", 30, 29, 28, 27, PartitionLocation.Mode.PRIMARY)
  val partitionLocation4 =
    new PartitionLocation(
      3,
      3,
      "host4",
      40,
      39,
      38,
      37,
      PartitionLocation.Mode.REPLICA,
      partitionLocation3,
      new StorageInfo(
        StorageInfo.Type.HDD,
        "mountPoint",
        false,
        "filePath",
        StorageInfo.LOCAL_DISK_MASK),
      null)
  val partitionLocationIPv6 =
    // some random ipv6 address
    new PartitionLocation(
      2,
      2,
      "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]",
      30,
      29,
      28,
      27,
      PartitionLocation.Mode.PRIMARY)

  val partitionLocation5 =
    new PartitionLocation(
      4,
      4,
      "host5",
      50,
      49,
      48,
      47,
      PartitionLocation.Mode.PRIMARY)
  val partitionLocation6 =
    new PartitionLocation(
      5,
      5,
      "host6",
      60,
      59,
      58,
      57,
      PartitionLocation.Mode.REPLICA,
      null,
      new StorageInfo(
        StorageInfo.Type.HDD,
        "",
        false,
        null,
        StorageInfo.LOCAL_DISK_MASK,
        5,
        null),
      null)

  val workerResource = new WorkerResource()
  workerResource.put(
    workerInfo1,
    (util.Arrays.asList(partitionLocation1), util.Arrays.asList(partitionLocation2)))

  val workerEventInfo = new WorkerEventInfo(1, System.currentTimeMillis());
  val workerStatus = new WorkerStatus(1, System.currentTimeMillis());

  test("fromAndToPbSortedShuffleFileSet") {
    val pbFileSet = PbSerDeUtils.toPbSortedShuffleFileSet(fileSet)
    val restoredFileSet = PbSerDeUtils.fromPbSortedShuffleFileSet(pbFileSet)

    assert(restoredFileSet.equals(fileSet))
    // test if the restored is mutable
    restoredFileSet.add("d")
    assert(restoredFileSet.size() == 4)
  }

  test("fromAndToPbStoreVersion") {
    val pbVersion = PbSerDeUtils.toPbStoreVersion(major, minor)
    val restoredVersion = PbSerDeUtils.fromPbStoreVersion(pbVersion)

    assert(restoredVersion.get(0) == major)
    assert(restoredVersion.get(1) == minor)
  }

  test("fromAndToPbDiskInfo") {
    val pbDiskInfo = PbSerDeUtils.toPbDiskInfo(diskInfo1)
    val restoredDiskInfo = PbSerDeUtils.fromPbDiskInfo(pbDiskInfo)

    assert(restoredDiskInfo.mountPoint.equals(diskInfo1.mountPoint))
    assert(restoredDiskInfo.actualUsableSpace.equals(diskInfo1.actualUsableSpace))
    assert(restoredDiskInfo.avgFlushTime.equals(diskInfo1.avgFlushTime))
    assert(restoredDiskInfo.avgFetchTime.equals(diskInfo1.avgFetchTime))
    assert(restoredDiskInfo.activeSlots.equals(diskInfo1.activeSlots))
    assert(restoredDiskInfo.totalSpace.equals(diskInfo1.totalSpace))

    assert(restoredDiskInfo.dirs.equals(List.empty))
    assert(restoredDiskInfo.deviceInfo == null)
  }

  test("fromAndToPbFileInfo") {
    val pbFileInfo = PbSerDeUtils.toPbFileInfo(fileInfo1)
    val restoredFileInfo = PbSerDeUtils.fromPbFileInfo(pbFileInfo)

    assert(
      restoredFileInfo.getFilePath.equals(fileInfo1.getFilePath))
    assert(restoredFileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets.equals(
      fileInfo1.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets))
    assert(restoredFileInfo.getUserIdentifier.equals(fileInfo1.getUserIdentifier))
    assert(restoredFileInfo.getFileMeta.isInstanceOf[ReduceFileMeta])
  }

  test("fromAndToPbFileInfoMap") {
    val pbFileInfoMap = PbSerDeUtils.toPbFileInfoMap(fileInfoMap)
    val restoredFileInfoMap = PbSerDeUtils.fromPbFileInfoMap(pbFileInfoMap, cache, mountPoints)
    val restoredFileInfo1 = restoredFileInfoMap.get("file1")
    val restoredFileInfo2 = restoredFileInfoMap.get("file2")

    assert(restoredFileInfoMap.size().equals(fileInfoMap.size()))
    assert(
      restoredFileInfo1.getFilePath.equals(fileInfo1.getFilePath))
    assert(restoredFileInfo1.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets.equals(
      fileInfo1.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets))
    assert(restoredFileInfo1.getUserIdentifier.equals(fileInfo1.getUserIdentifier))
    assert(
      restoredFileInfo2.getFilePath.equals(fileInfo2.getFilePath))
    assert(restoredFileInfo2.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets.equals(
      fileInfo2.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets))
    assert(restoredFileInfo2.getUserIdentifier.equals(fileInfo2.getUserIdentifier))
  }

  test("fromAndToPBFileInfoMapMountPoint") {
    val pbFileInfoMap = PbSerDeUtils.toPbFileInfoMap(fileInfoMap)
    val restoredFileInfoMap = PbSerDeUtils.fromPbFileInfoMap(pbFileInfoMap, cache, mountPoints)
    val restoredFileInfo1 = restoredFileInfoMap.get("mapFile1")
    val restoredFileInfo2 = restoredFileInfoMap.get("mapFile2")

    assert(restoredFileInfo1.getMountPoint.equals(mapFileInfo1.getMountPoint))
    assert(restoredFileInfo2.getMountPoint.equals(mapFileInfo2.getMountPoint))
  }

  test("fromAndToPbUserIdentifier") {
    val pbUserIdentifier = PbSerDeUtils.toPbUserIdentifier(userIdentifier1)
    val restoredUserIdentifier = PbSerDeUtils.fromPbUserIdentifier(pbUserIdentifier)

    assert(restoredUserIdentifier.equals(userIdentifier1))
  }

  test("fromAndToPbResourceConsumption") {
    testFromAndToPbResourceConsumption(resourceConsumption1)
    testFromAndToPbResourceConsumption(resourceConsumption2)
  }

  def testFromAndToPbResourceConsumption(resourceConsumption: ResourceConsumption): Unit = {
    val pbResourceConsumption = PbSerDeUtils.toPbResourceConsumption(resourceConsumption)
    val restoredResourceConsumption = PbSerDeUtils.fromPbResourceConsumption(pbResourceConsumption)

    assert(restoredResourceConsumption.equals(resourceConsumption))
  }

  test("fromAndToPbUserResourceConsumption") {
    val pbUserResourceConsumption =
      PbSerDeUtils.toPbUserResourceConsumption(userResourceConsumption)
    val restoredUserResourceConsumption =
      PbSerDeUtils.fromPbUserResourceConsumption(pbUserResourceConsumption)

    assert(restoredUserResourceConsumption.equals(userResourceConsumption))
  }

  test("fromAndToPbWorkerInfo") {
    Seq(false, true).foreach { b =>
      PbSerDeUtils.setMasterPersistWorkerNetworkLocation(b)
      val pbWorkerInfo = PbSerDeUtils.toPbWorkerInfo(workerInfo1, false, false)
      val pbWorkerInfoWithEmptyResource = PbSerDeUtils.toPbWorkerInfo(workerInfo1, true, false)
      val restoredWorkerInfo = PbSerDeUtils.fromPbWorkerInfo(pbWorkerInfo)
      val restoredWorkerInfoWithEmptyResource =
        PbSerDeUtils.fromPbWorkerInfo(pbWorkerInfoWithEmptyResource)

      assert(restoredWorkerInfo.equals(workerInfo1))
      assert(restoredWorkerInfoWithEmptyResource.userResourceConsumption.equals(new util.HashMap[
        UserIdentifier,
        ResourceConsumption]()))
    }
  }

  test("fromAndToPbPartitionLocation") {
    val pbPartitionLocation = PbSerDeUtils.toPbPartitionLocation(partitionLocation1)
    val restoredPartitionLocation = PbSerDeUtils.fromPbPartitionLocation(pbPartitionLocation)

    assert(restoredPartitionLocation.equals(partitionLocation1))
  }

  test("fromAndToPbWorkerResource") {
    val pbWorkerResource = PbSerDeUtils.toPbWorkerResource(workerResource)
    val restoredWorkerResource = PbSerDeUtils.fromPbWorkerResource(pbWorkerResource)

    assert(restoredWorkerResource.equals(workerResource))
  }

  test("testPbStorageInfo") {
    val pbPartitionLocation3 = PbSerDeUtils.toPbPartitionLocation(partitionLocation3)
    val pbPartitionLocation4 = PbSerDeUtils.toPbPartitionLocation(partitionLocation4)

    val restoredPartitionLocation3 = PbSerDeUtils.fromPbPartitionLocation(pbPartitionLocation3)
    val restoredPartitionLocation4 = PbSerDeUtils.fromPbPartitionLocation(pbPartitionLocation4)

    assert(restoredPartitionLocation3.equals(partitionLocation3))
    assert(restoredPartitionLocation4.equals(partitionLocation4))
    assert(restoredPartitionLocation4.getStorageInfo.equals(partitionLocation4.getStorageInfo))
  }

  test("fromAndToPbWorkerEventInfo") {
    val pbWorkerEventInfo = PbSerDeUtils.toPbWorkerEventInfo(workerEventInfo)
    val restoredWorkerEventInfo = PbSerDeUtils.fromPbWorkerEventInfo(pbWorkerEventInfo)

    assert(restoredWorkerEventInfo.equals(workerEventInfo))
  }

  test("fromAndToPbWorkerStatus") {
    val pbWorkerStatus = PbSerDeUtils.toPbWorkerStatus(workerStatus)
    val restoredWorkerStatus = PbSerDeUtils.fromPbWorkerStatus(pbWorkerStatus)

    assert(restoredWorkerStatus.equals(workerStatus))
  }

  test("fromAndToPbApplicationMeta") {
    val applicationMeta = new ApplicationMeta("app1", "secret1")
    val pbApplicationMeta = PbSerDeUtils.toPbApplicationMeta(applicationMeta)
    val restoredApplicationMeta = PbSerDeUtils.fromPbApplicationMeta(pbApplicationMeta)

    assert(restoredApplicationMeta.equals(applicationMeta))
  }

  test("testPackedPartitionLocationPairCase1") {
    partitionLocation3.setPeer(partitionLocation2)
    val pairPb = PbSerDeUtils.toPbPackedPartitionLocationsPair(
      List(partitionLocation3, partitionLocation2))
    val rePb = PbSerDeUtils.fromPbPackedPartitionLocationsPair(pairPb)

    val loc1 = rePb._1.get(0)
    val loc2 = rePb._2.get(0)

    assert(partitionLocation3 == loc1)
    assert(partitionLocation2 == loc2)
  }

  test("testPackedPartitionLocationPairCase2") {
    val pairPb = PbSerDeUtils.toPbPackedPartitionLocationsPair(
      List(partitionLocation3))
    val rePb = PbSerDeUtils.fromPbPackedPartitionLocationsPair(pairPb)

    val loc1 = rePb._1.get(0)

    assert(partitionLocation3 == loc1)
  }

  test("testPackedPartitionLocationPairCase3") {
    partitionLocation5.setStorageInfo(new StorageInfo(
      StorageInfo.Type.HDD,
      "",
      false,
      null,
      StorageInfo.LOCAL_DISK_MASK,
      5,
      Lists.newArrayList(0, 5, 10)))
    partitionLocation5.setPeer(partitionLocation6)
    val pairPb = PbSerDeUtils.toPbPackedPartitionLocationsPair(
      List(partitionLocation5, partitionLocation6))
    val rePb = PbSerDeUtils.fromPbPackedPartitionLocationsPair(pairPb)

    val loc1 = rePb._1.get(0)
    val loc2 = rePb._2.get(0)

    assert(partitionLocation5 == loc1)
    assert(partitionLocation6 == loc2)
    assert(loc1.getStorageInfo.getFileSize == partitionLocation5.getStorageInfo.getFileSize)
    assert(loc1.getStorageInfo.getChunkOffsets == partitionLocation5.getStorageInfo.getChunkOffsets)

    assert(loc2.getStorageInfo.getFileSize == partitionLocation6.getStorageInfo.getFileSize)
    assert(loc2.getStorageInfo.getChunkOffsets.isEmpty)
  }

  test("testPackedPartitionLocationPairCase4") {
    partitionLocation5.setStorageInfo(new StorageInfo(
      StorageInfo.Type.HDD,
      "",
      false,
      null,
      StorageInfo.LOCAL_DISK_MASK,
      5,
      null))
    val pairPb = PbSerDeUtils.toPbPackedPartitionLocationsPair(
      List(partitionLocation5))
    val rePb = PbSerDeUtils.fromPbPackedPartitionLocationsPair(pairPb)

    val loc1 = rePb._1.get(0)

    assert(partitionLocation5 == loc1)
    assert(loc1.getStorageInfo.getFileSize == partitionLocation5.getStorageInfo.getFileSize)
    assert(loc1.getStorageInfo.getChunkOffsets.isEmpty)
  }

  test("testPackedPartitionLocationPairCase5") {
    partitionLocation5.setStorageInfo(new StorageInfo(
      StorageInfo.Type.HDD,
      "",
      false,
      null,
      StorageInfo.LOCAL_DISK_MASK))
    val pairPb = PbSerDeUtils.toPbPackedPartitionLocationsPair(
      List(partitionLocation5))
    val rePb = PbSerDeUtils.fromPbPackedPartitionLocationsPair(pairPb)

    val loc1 = rePb._1.get(0)

    assert(partitionLocation5 == loc1)
    assert(loc1.getStorageInfo.getFileSize == partitionLocation5.getStorageInfo.getFileSize)
    assert(loc1.getStorageInfo.getChunkOffsets.isEmpty)
  }

  test("testPackedPartitionLocationPairIPv6") {
    val pairPb = PbSerDeUtils.toPbPackedPartitionLocationsPair(
      List(partitionLocationIPv6))
    val rePb = PbSerDeUtils.fromPbPackedPartitionLocationsPair(pairPb)

    val loc1 = rePb._1.get(0)

    assert(partitionLocationIPv6 == loc1)
  }

  private def testSerializationPerformance(scale: Int): Unit = {
    val mountPoints = List(
      "/mnt/disk1/celeborn/",
      "/mnt/disk2/celeborn/",
      "/mnt/disk3/celeborn/",
      "/mnt/disk4/celeborn/",
      "/mnt/disk5/celeborn/",
      "/mnt/disk6/celeborn/",
      "/mnt/disk7/celeborn/",
      "/mnt/disk8/celeborn/")
    val hosts = (0 to 50).map(f =>
      (
        s"host${f}",
        Random.nextInt(65535),
        Random.nextInt(65535),
        Random.nextInt(65535),
        Random.nextInt(65535))).toList
    val (primaryLocations, replicaLocations) = (0 to scale).map(i => {
      val host = hosts(Random.nextInt(50))
      val mountPoint = mountPoints(Random.nextInt(8))
      val primary = new PartitionLocation(
        i,
        0,
        host._1,
        host._2,
        host._3,
        host._4,
        host._5,
        PartitionLocation.Mode.PRIMARY,
        null,
        new StorageInfo(
          StorageInfo.Type.HDD,
          mountPoint,
          false,
          mountPoint + "/application/0/" + RandomStringUtils.randomNumeric(6),
          StorageInfo.LOCAL_DISK_MASK),
        null)

      val rHost = hosts(Random.nextInt(50))
      val rMountPoint = mountPoints(Random.nextInt(8))

      val replicate = new PartitionLocation(
        i,
        0,
        rHost._1,
        rHost._2,
        rHost._3,
        rHost._4,
        rHost._5,
        PartitionLocation.Mode.REPLICA,
        null,
        new StorageInfo(
          StorageInfo.Type.HDD,
          rMountPoint,
          false,
          rMountPoint + "/application-xxxsdsada-1/0/" + RandomStringUtils.randomNumeric(6),
          StorageInfo.LOCAL_DISK_MASK),
        null)
      primary.setPeer(replicate)
      replicate.setPeer(primary)
      (primary, replicate)
    }).toList.unzip

    val workerResourceSize = PbWorkerResource.newBuilder()
      .addAllPrimaryPartitions(primaryLocations.map(PbSerDeUtils.toPbPartitionLocation).asJava)
      .addAllReplicaPartitions(replicaLocations.map(PbSerDeUtils.toPbPartitionLocation).asJava)
      .setNetworkLocation("location1")
      .build().toByteArray.length

    val pbPackedWorkerResource = PbPackedWorkerResource.newBuilder()
      .setLocationPairs(toPbPackedPartitionLocationsPair(
        primaryLocations ++ replicaLocations))
      .setNetworkLocation("location1")
      .build()
    val packedWorkerResourceSize = pbPackedWorkerResource.toByteArray.length

    val (locs1, locs2) = fromPbPackedPartitionLocationsPair(pbPackedWorkerResource.getLocationPairs)

    assert(primaryLocations.size === locs1.size())
    assert(replicaLocations.size === locs2.size())

    assert(primaryLocations.zip(locs1.asScala).count(x => x._1 != x._2) == 0)
    assert(replicaLocations.zip(locs2.asScala).count(x => x._1 != x._2) == 0)

    assert(packedWorkerResourceSize < workerResourceSize)
    log.info(s"Packed size : ${packedWorkerResourceSize} unpacked size :${workerResourceSize}")
    log.info(
      s"Reduced size : ${(workerResourceSize - packedWorkerResourceSize) / (workerResourceSize * 1.0f) * 100} %")
  }

  test("serializationComparasion") {
    testSerializationPerformance(100)
  }

  test("GetReduceFileGroup with primary and replica locations") {
    val shuffleMap: util.Map[Integer, util.Set[PartitionLocation]] =
      JavaUtils.newConcurrentHashMap()
    val locationSet = new util.LinkedHashSet[PartitionLocation]()
    val uniqueIds = mutable.Set("0-0", "1-0", "2-0", "3-0", "4-0")
    locationSet.add(new PartitionLocation(0, 0, "h", 1, 1, 1, 1, Mode.REPLICA))
    locationSet.add(new PartitionLocation(1, 0, "h", 1, 1, 1, 1, Mode.PRIMARY))
    locationSet.add(new PartitionLocation(2, 0, "h", 1, 1, 1, 1, Mode.PRIMARY))
    locationSet.add(new PartitionLocation(3, 0, "h", 1, 1, 1, 1, Mode.REPLICA))
    locationSet.add(new PartitionLocation(4, 0, "h", 1, 1, 1, 1, Mode.PRIMARY))
    shuffleMap.put(1, locationSet)
    val attempts = Array.fill(10)(10)
    val succeedPartitions = Array.fill(10)(10).map(java.lang.Integer.valueOf).toSet.asJava

    val GetReducerFileGroupResponseMsg = GetReducerFileGroupResponse(
      StatusCode.SUCCESS,
      shuffleMap,
      attempts,
      succeedPartitions)

    val transportGetReducerFileGroup =
      ControlMessages.toTransportMessage(GetReducerFileGroupResponseMsg)
    val fromTransportGetReducerFileGroup: GetReducerFileGroupResponse =
      ControlMessages.fromTransportMessage(
        transportGetReducerFileGroup).asInstanceOf[GetReducerFileGroupResponse]

    val locations = fromTransportGetReducerFileGroup.fileGroup.get(1)
    locations.asScala.foreach(p => uniqueIds.remove(p.getUniqueId))
    assert(uniqueIds.isEmpty)
  }

  test("GetReduceFileGroup with primary location only") {
    val shuffleMap: util.Map[Integer, util.Set[PartitionLocation]] =
      JavaUtils.newConcurrentHashMap()
    val locationSet = new util.LinkedHashSet[PartitionLocation]()
    val uniqueIds = mutable.Set("0-0", "1-0", "2-0", "3-0", "4-0")
    locationSet.add(new PartitionLocation(0, 0, "h", 1, 1, 1, 1, Mode.PRIMARY))
    locationSet.add(new PartitionLocation(1, 0, "h", 1, 1, 1, 1, Mode.PRIMARY))
    locationSet.add(new PartitionLocation(2, 0, "h", 1, 1, 1, 1, Mode.PRIMARY))
    locationSet.add(new PartitionLocation(3, 0, "h", 1, 1, 1, 1, Mode.PRIMARY))
    locationSet.add(new PartitionLocation(4, 0, "h", 1, 1, 1, 1, Mode.PRIMARY))
    shuffleMap.put(1, locationSet)
    val attempts = Array.fill(10)(10)
    val succeedPartitions = Array.fill(10)(10).map(java.lang.Integer.valueOf).toSet.asJava

    val GetReducerFileGroupResponseMsg = GetReducerFileGroupResponse(
      StatusCode.SUCCESS,
      shuffleMap,
      attempts,
      succeedPartitions)

    val transportGetReducerFileGroup =
      ControlMessages.toTransportMessage(GetReducerFileGroupResponseMsg)
    val fromTransportGetReducerFileGroup: GetReducerFileGroupResponse =
      ControlMessages.fromTransportMessage(
        transportGetReducerFileGroup).asInstanceOf[GetReducerFileGroupResponse]

    val locations = fromTransportGetReducerFileGroup.fileGroup.get(1)
    locations.asScala.foreach(p => uniqueIds.remove(p.getUniqueId))
    assert(uniqueIds.isEmpty)
  }

  test("GetReduceFileGroup with replica location only") {
    val shuffleMap: util.Map[Integer, util.Set[PartitionLocation]] =
      JavaUtils.newConcurrentHashMap()
    val locationSet = new util.LinkedHashSet[PartitionLocation]()
    val uniqueIds = mutable.Set("0-0", "1-0", "2-0", "3-0", "4-0")
    locationSet.add(new PartitionLocation(0, 0, "h", 1, 1, 1, 1, Mode.REPLICA))
    locationSet.add(new PartitionLocation(1, 0, "h", 1, 1, 1, 1, Mode.REPLICA))
    locationSet.add(new PartitionLocation(2, 0, "h", 1, 1, 1, 1, Mode.REPLICA))
    locationSet.add(new PartitionLocation(3, 0, "h", 1, 1, 1, 1, Mode.REPLICA))
    locationSet.add(new PartitionLocation(4, 0, "h", 1, 1, 1, 1, Mode.REPLICA))
    shuffleMap.put(1, locationSet)
    val attempts = Array.fill(10)(10)
    val succeedPartitions = Array.fill(10)(10).map(java.lang.Integer.valueOf).toSet.asJava

    val GetReducerFileGroupResponseMsg = GetReducerFileGroupResponse(
      StatusCode.SUCCESS,
      shuffleMap,
      attempts,
      succeedPartitions)

    val transportGetReducerFileGroup =
      ControlMessages.toTransportMessage(GetReducerFileGroupResponseMsg)
    val fromTransportGetReducerFileGroup: GetReducerFileGroupResponse =
      ControlMessages.fromTransportMessage(
        transportGetReducerFileGroup).asInstanceOf[GetReducerFileGroupResponse]

    val locations = fromTransportGetReducerFileGroup.fileGroup.get(1)
    locations.asScala.foreach(p => uniqueIds.remove(p.getUniqueId))
    assert(uniqueIds.isEmpty)
  }

  test("fromAndToPushFailedBatch") {
    val failedBatch = new PushFailedBatch(1, 1, 2)
    val pbPushFailedBatch = PbSerDeUtils.toPbPushFailedBatch(failedBatch)
    val restoredFailedBatch = PbSerDeUtils.fromPbPushFailedBatch(pbPushFailedBatch)

    assert(restoredFailedBatch.equals(failedBatch))
  }

  test("fromAndToPushFailedBatchSet") {
    val failedBatchSet = Sets.newHashSet(new PushFailedBatch(1, 1, 2), new PushFailedBatch(2, 2, 3))
    val pbPushFailedBatchSet = PbSerDeUtils.toPbPushFailedBatchSet(failedBatchSet)
    val restoredFailedBatchSet = PbSerDeUtils.fromPbPushFailedBatchSet(pbPushFailedBatchSet)

    assert(restoredFailedBatchSet.equals(failedBatchSet))
  }

}
