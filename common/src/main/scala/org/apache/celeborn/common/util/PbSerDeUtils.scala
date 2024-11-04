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
import java.util.concurrent.ConcurrentHashMap
import java.util.function.IntFunction

import scala.collection.JavaConverters._

import com.google.protobuf.InvalidProtocolBufferException

import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{AppDiskUsage, AppDiskUsageSnapShot, ApplicationMeta, DiskFileInfo, DiskInfo, MapFileMeta, ReduceFileMeta, WorkerEventInfo, WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.meta.MapFileMeta.SegmentIndex
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.PartitionLocation.Mode
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.util.{CollectionUtils => localCollectionUtils}

object PbSerDeUtils {

  private var masterPersistWorkerNetworkLocation: Boolean = false

  def setMasterPersistWorkerNetworkLocation(masterPersistWorkerNetworkLocation: Boolean) = {
    this.masterPersistWorkerNetworkLocation = masterPersistWorkerNetworkLocation
  }

  @throws[InvalidProtocolBufferException]
  def fromPbSortedShuffleFileSet(data: Array[Byte]): util.Set[String] = {
    val pbSortedShuffleFileSet = PbSortedShuffleFileSet.parseFrom(data)
    val files = ConcurrentHashMap.newKeySet[String]()
    files.addAll(pbSortedShuffleFileSet.getFilesList)
    files
  }

  def toPbSortedShuffleFileSet(files: util.Set[String]): Array[Byte] =
    PbSortedShuffleFileSet.newBuilder
      .addAllFiles(files)
      .build
      .toByteArray

  @throws[InvalidProtocolBufferException]
  def fromPbStoreVersion(data: Array[Byte]): util.ArrayList[Integer] = {
    val pbStoreVersion = PbStoreVersion.parseFrom(data)
    val versions = new util.ArrayList[Integer]()
    versions.add(pbStoreVersion.getMajor)
    versions.add(pbStoreVersion.getMinor)
    versions
  }

  def toPbStoreVersion(major: Int, minor: Int): Array[Byte] =
    PbStoreVersion.newBuilder
      .setMajor(major)
      .setMinor(minor)
      .build.toByteArray

  def fromPbDiskInfo(pbDiskInfo: PbDiskInfo): DiskInfo = {
    val diskInfo = new DiskInfo(
      pbDiskInfo.getMountPoint,
      pbDiskInfo.getUsableSpace,
      pbDiskInfo.getAvgFlushTime,
      pbDiskInfo.getAvgFetchTime,
      pbDiskInfo.getUsedSlots)
      .setStatus(Utils.toDiskStatus(pbDiskInfo.getStatus))
      .setTotalSpace(pbDiskInfo.getTotalSpace)
    diskInfo.setStorageType(StorageInfo.typesMap.get(pbDiskInfo.getStorageType))
    diskInfo
  }

  def toPbDiskInfo(diskInfo: DiskInfo): PbDiskInfo =
    PbDiskInfo.newBuilder
      .setMountPoint(diskInfo.mountPoint)
      .setUsableSpace(diskInfo.actualUsableSpace)
      .setAvgFlushTime(diskInfo.avgFlushTime)
      .setAvgFetchTime(diskInfo.avgFetchTime)
      .setUsedSlots(diskInfo.activeSlots)
      .setStatus(diskInfo.status.getValue)
      .setStorageType(diskInfo.storageType.getValue)
      .setTotalSpace(diskInfo.totalSpace)
      .build

  def fromPbFileInfo(pbFileInfo: PbFileInfo): DiskFileInfo =
    fromPbFileInfo(pbFileInfo, fromPbUserIdentifier(pbFileInfo.getUserIdentifier))

  def fromPbFileInfo(pbFileInfo: PbFileInfo, userIdentifier: UserIdentifier) = {
    val meta = Utils.toPartitionType(pbFileInfo.getPartitionType) match {
      case PartitionType.REDUCE =>
        new ReduceFileMeta(pbFileInfo.getChunkOffsetsList)
      case PartitionType.MAP =>
        val fileMeta = new MapFileMeta(
          pbFileInfo.getBufferSize,
          pbFileInfo.getNumSubpartitions,
          pbFileInfo.getIsSegmentGranularityVisible,
          pbFileInfo.getPartitionWritingSegmentMap,
          fromPbSegmentIndexList(pbFileInfo.getSegmentIndexList))
        // writer always closed as this is committed file info.
        fileMeta.setIsWriterClosed(true)
        fileMeta
      case PartitionType.MAPGROUP =>
        throw new NotImplementedError("Map group is not implemented")
    }
    new DiskFileInfo(
      userIdentifier,
      pbFileInfo.getPartitionSplitEnabled,
      meta,
      pbFileInfo.getFilePath,
      pbFileInfo.getBytesFlushed)
  }

  private def fromPbSegmentIndexList(
      segmentIndexList: util.List[PbSegmentIndex]): util.List[SegmentIndex] = {
    segmentIndexList.asScala.map(firstBufferIndexToSegment =>
      new SegmentIndex(
        firstBufferIndexToSegment.getFirstBufferIndexToSegmentMap)).asJava
  }

  private def toPbSegmentIndex(
      firstBufferIndexToSegment: SegmentIndex): PbSegmentIndex = {
    PbSegmentIndex.newBuilder().putAllFirstBufferIndexToSegment(
      firstBufferIndexToSegment.getFirstBufferIndexToSegment).build()
  }

  def toPbFileInfo(fileInfo: DiskFileInfo): PbFileInfo = {
    val builder = PbFileInfo.newBuilder
      .setFilePath(fileInfo.getFilePath)
      .setUserIdentifier(toPbUserIdentifier(fileInfo.getUserIdentifier))
      .setBytesFlushed(fileInfo.getFileLength)
      .setPartitionSplitEnabled(fileInfo.isPartitionSplitEnabled)
    if (fileInfo.getFileMeta.isInstanceOf[MapFileMeta]) {
      val mapFileMeta = fileInfo.getFileMeta.asInstanceOf[MapFileMeta]
      builder.setPartitionType(PartitionType.MAP.getValue)
      builder.setBufferSize(mapFileMeta.getBufferSize)
      builder.setNumSubpartitions(mapFileMeta.getNumSubpartitions)
      builder.setIsSegmentGranularityVisible(mapFileMeta.isSegmentGranularityVisible)
      builder.putAllPartitionWritingSegment(mapFileMeta.getSubPartitionWritingSegmentId)
      builder.addAllSegmentIndex(
        mapFileMeta.getSubPartitionSegmentIndexes.asScala.map(toPbSegmentIndex).asJava)
    } else {
      val reduceFileMeta = fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta]
      builder.setPartitionType(PartitionType.REDUCE.getValue)
      builder.addAllChunkOffsets(reduceFileMeta.getChunkOffsets)
    }
    builder.build
  }

  @throws[InvalidProtocolBufferException]
  def fromPbFileInfoMap(
      data: Array[Byte],
      cache: ConcurrentHashMap[String, UserIdentifier]): ConcurrentHashMap[String, DiskFileInfo] = {
    val pbFileInfoMap = PbFileInfoMap.parseFrom(data)
    val fileInfoMap = JavaUtils.newConcurrentHashMap[String, DiskFileInfo]
    pbFileInfoMap.getValuesMap.entrySet().asScala.foreach { entry =>
      val fileName = entry.getKey
      val pbFileInfo = entry.getValue
      val pbUserIdentifier = pbFileInfo.getUserIdentifier
      val userIdentifierKey = pbUserIdentifier.getTenantId + "-" + pbUserIdentifier.getName
      if (!cache.containsKey(userIdentifierKey)) {
        val fileInfo = fromPbFileInfo(pbFileInfo)
        cache.put(userIdentifierKey, fileInfo.getUserIdentifier)
        fileInfoMap.put(fileName, fileInfo)
      } else {
        val fileInfo = fromPbFileInfo(pbFileInfo, cache.get(userIdentifierKey))
        fileInfoMap.put(fileName, fileInfo)
      }
    }
    fileInfoMap
  }

  def toPbFileInfoMap(fileInfoMap: ConcurrentHashMap[String, DiskFileInfo]): Array[Byte] = {
    val pbFileInfoMap = JavaUtils.newConcurrentHashMap[String, PbFileInfo]()
    fileInfoMap.entrySet().asScala.foreach { entry =>
      pbFileInfoMap.put(entry.getKey, toPbFileInfo(entry.getValue))
    }
    PbFileInfoMap.newBuilder.putAllValues(pbFileInfoMap).build.toByteArray
  }

  def fromPbUserIdentifier(pbUserIdentifier: PbUserIdentifier): UserIdentifier =
    UserIdentifier(pbUserIdentifier.getTenantId, pbUserIdentifier.getName)

  def toPbUserIdentifier(userIdentifier: UserIdentifier): PbUserIdentifier =
    PbUserIdentifier.newBuilder
      .setTenantId(userIdentifier.tenantId)
      .setName(userIdentifier.name)
      .build

  def fromPbResourceConsumption(pbResourceConsumption: PbResourceConsumption) = ResourceConsumption(
    pbResourceConsumption.getDiskBytesWritten,
    pbResourceConsumption.getDiskFileCount,
    pbResourceConsumption.getHdfsBytesWritten,
    pbResourceConsumption.getHdfsFileCount,
    fromPbSubResourceConsumptions(pbResourceConsumption.getSubResourceConsumptionsMap))

  def toPbResourceConsumption(resourceConsumption: ResourceConsumption): PbResourceConsumption = {
    PbResourceConsumption.newBuilder
      .setDiskBytesWritten(resourceConsumption.diskBytesWritten)
      .setDiskFileCount(resourceConsumption.diskFileCount)
      .setHdfsBytesWritten(resourceConsumption.hdfsBytesWritten)
      .setHdfsFileCount(resourceConsumption.hdfsFileCount)
      .putAllSubResourceConsumptions(toPbSubResourceConsumptions(
        resourceConsumption.subResourceConsumptions))
      .build
  }

  def fromPbSubResourceConsumptions(pbSubResourceConsumptions: util.Map[
    String,
    PbResourceConsumption]): util.Map[String, ResourceConsumption] =
    if (CollectionUtils.isEmpty(pbSubResourceConsumptions))
      null
    else pbSubResourceConsumptions.asScala.map { case (key, pbResourceConsumption) =>
      (key, fromPbResourceConsumption(pbResourceConsumption))
    }.asJava

  def toPbSubResourceConsumptions(subResourceConsumptions: util.Map[String, ResourceConsumption])
      : util.Map[String, PbResourceConsumption] =
    if (CollectionUtils.isEmpty(subResourceConsumptions))
      new util.HashMap[String, PbResourceConsumption]
    else subResourceConsumptions.asScala.map { case (key, resourceConsumption) =>
      (key, toPbResourceConsumption(resourceConsumption))
    }.asJava

  def fromPbUserResourceConsumption(pbUserResourceConsumption: util.Map[
    String,
    PbResourceConsumption]): util.Map[UserIdentifier, ResourceConsumption] = {
    pbUserResourceConsumption.asScala.map { case (userIdentifierString, pbResourceConsumption) =>
      (UserIdentifier(userIdentifierString), fromPbResourceConsumption(pbResourceConsumption))
    }.asJava
  }

  def toPbUserResourceConsumption(userResourceConsumption: util.Map[
    UserIdentifier,
    ResourceConsumption]): util.Map[String, PbResourceConsumption] = {
    userResourceConsumption.asScala.map { case (userIdentifier, resourceConsumption) =>
      (userIdentifier.toString, toPbResourceConsumption(resourceConsumption))
    }.asJava
  }

  def fromPbWorkerInfo(pbWorkerInfo: PbWorkerInfo): WorkerInfo = {
    val disks = JavaUtils.newConcurrentHashMap[String, DiskInfo]
    if (pbWorkerInfo.getDisksCount > 0) {
      pbWorkerInfo.getDisksList.asScala.foreach(pbDiskInfo =>
        disks.put(pbDiskInfo.getMountPoint, fromPbDiskInfo(pbDiskInfo)))
    }
    val userResourceConsumption =
      PbSerDeUtils.fromPbUserResourceConsumption(pbWorkerInfo.getUserResourceConsumptionMap)
    val workerInfo = new WorkerInfo(
      pbWorkerInfo.getHost,
      pbWorkerInfo.getRpcPort,
      pbWorkerInfo.getPushPort,
      pbWorkerInfo.getFetchPort,
      pbWorkerInfo.getReplicatePort,
      pbWorkerInfo.getInternalPort,
      pbWorkerInfo.getVersion,
      disks,
      userResourceConsumption)
    if (masterPersistWorkerNetworkLocation) {
      workerInfo.networkLocation_$eq(pbWorkerInfo.getNetworkLocation)
    }
    workerInfo
  }

  def toPbWorkerInfo(
      workerInfo: WorkerInfo,
      eliminateUserResourceConsumption: Boolean,
      eliminateDiskInfo: Boolean): PbWorkerInfo = {
    val builder = PbWorkerInfo.newBuilder
      .setHost(workerInfo.host)
      .setRpcPort(workerInfo.rpcPort)
      .setFetchPort(workerInfo.fetchPort)
      .setPushPort(workerInfo.pushPort)
      .setReplicatePort(workerInfo.replicatePort)
      .setInternalPort(workerInfo.internalPort)
      .setVersion(workerInfo.version)
    if (masterPersistWorkerNetworkLocation) {
      builder.setNetworkLocation(workerInfo.networkLocation)
    }

    if (!eliminateUserResourceConsumption) {
      builder.putAllUserResourceConsumption(
        PbSerDeUtils.toPbUserResourceConsumption(workerInfo.userResourceConsumption))
    }
    if (!eliminateDiskInfo) {
      val diskInfos = workerInfo.diskInfos.values
      val pbDiskInfos = new util.ArrayList[PbDiskInfo]()
      diskInfos.asScala.foreach(diskInfo => pbDiskInfos.add(PbSerDeUtils.toPbDiskInfo(diskInfo)))
      builder.addAllDisks(pbDiskInfos)
    }
    builder.build
  }

  def fromPbPartitionLocation(pbLoc: PbPartitionLocation): PartitionLocation = {
    var mode = Mode.PRIMARY
    if (pbLoc.getMode.equals(PbPartitionLocation.Mode.Replica)) {
      mode = Mode.REPLICA
    }
    val partitionLocation = new PartitionLocation(
      pbLoc.getId,
      pbLoc.getEpoch,
      pbLoc.getHost,
      pbLoc.getRpcPort,
      pbLoc.getPushPort,
      pbLoc.getFetchPort,
      pbLoc.getReplicatePort,
      mode,
      null,
      StorageInfo.fromPb(pbLoc.getStorageInfo),
      Utils.byteStringToRoaringBitmap(pbLoc.getMapIdBitmap))
    if (pbLoc.hasPeer) {
      val peerPb = pbLoc.getPeer
      var peerMode = Mode.PRIMARY
      if (peerPb.getMode eq PbPartitionLocation.Mode.Replica) peerMode = Mode.REPLICA
      val peerLocation = new PartitionLocation(
        peerPb.getId,
        peerPb.getEpoch,
        peerPb.getHost,
        peerPb.getRpcPort,
        peerPb.getPushPort,
        peerPb.getFetchPort,
        peerPb.getReplicatePort,
        peerMode,
        partitionLocation,
        StorageInfo.fromPb(peerPb.getStorageInfo),
        Utils.byteStringToRoaringBitmap(peerPb.getMapIdBitmap))
      partitionLocation.setPeer(peerLocation)
    }
    partitionLocation
  }

  def toPbPartitionLocation(location: PartitionLocation): PbPartitionLocation = {
    val builder = PbPartitionLocation.newBuilder
    if (location.getMode eq Mode.PRIMARY) {
      builder.setMode(PbPartitionLocation.Mode.Primary)
    } else {
      builder.setMode(PbPartitionLocation.Mode.Replica)
    }
    builder
      .setHost(location.getHost)
      .setEpoch(location.getEpoch)
      .setId(location.getId)
      .setRpcPort(location.getRpcPort)
      .setPushPort(location.getPushPort)
      .setFetchPort(location.getFetchPort)
      .setReplicatePort(location.getReplicatePort)
      .setStorageInfo(StorageInfo.toPb(location.getStorageInfo))
      .setMapIdBitmap(Utils.roaringBitmapToByteString(location.getMapIdBitMap))
    if (location.hasPeer) {
      val peerBuilder = PbPartitionLocation.newBuilder
      if (location.getPeer.getMode eq Mode.PRIMARY) {
        peerBuilder.setMode(PbPartitionLocation.Mode.Primary)
      } else {
        peerBuilder.setMode(PbPartitionLocation.Mode.Replica)
      }
      peerBuilder
        .setHost(location.getPeer.getHost)
        .setEpoch(location.getPeer.getEpoch)
        .setId(location.getPeer.getId)
        .setRpcPort(location.getPeer.getRpcPort)
        .setPushPort(location.getPeer.getPushPort)
        .setFetchPort(location.getPeer.getFetchPort)
        .setReplicatePort(location.getPeer.getReplicatePort)
        .setStorageInfo(StorageInfo.toPb(location.getPeer.getStorageInfo))
        .setMapIdBitmap(Utils.roaringBitmapToByteString(location.getMapIdBitMap))
      builder.setPeer(peerBuilder.build)
    }
    builder.build
  }

  def fromPbWorkerResource(pbWorkerResource: util.Map[String, PbWorkerResource]): WorkerResource = {
    val slots = new WorkerResource()
    pbWorkerResource.asScala.foreach { case (uniqueId, pbWorkerResource) =>
      val networkLocation = pbWorkerResource.getNetworkLocation
      val workerInfo = WorkerInfo.fromUniqueId(uniqueId)
      workerInfo.networkLocation = networkLocation
      val primaryPartitionLocation = new util.ArrayList[PartitionLocation](pbWorkerResource
        .getPrimaryPartitionsList.asScala.map(PbSerDeUtils.fromPbPartitionLocation).asJava)
      val replicaPartitionLocation = new util.ArrayList[PartitionLocation](pbWorkerResource
        .getReplicaPartitionsList.asScala.map(PbSerDeUtils.fromPbPartitionLocation).asJava)
      slots.put(workerInfo, (primaryPartitionLocation, replicaPartitionLocation))
    }
    slots
  }

  def toPbWorkerResource(workerResource: WorkerResource): util.Map[String, PbWorkerResource] = {
    workerResource.asScala.map { case (workerInfo, (primaryLocations, replicaLocations)) =>
      val primaryPartitions =
        primaryLocations.asScala.map(PbSerDeUtils.toPbPartitionLocation).asJava
      val replicaPartitions =
        replicaLocations.asScala.map(PbSerDeUtils.toPbPartitionLocation).asJava
      val pbWorkerResource = PbWorkerResource.newBuilder()
        .addAllPrimaryPartitions(primaryPartitions)
        .addAllReplicaPartitions(replicaPartitions)
        .setNetworkLocation(workerInfo.networkLocation)
        .build()
      workerInfo.toUniqueId() -> pbWorkerResource
    }.asJava
  }

  def fromPbAppDiskUsage(pbAppDiskUsage: PbAppDiskUsage): AppDiskUsage = {
    AppDiskUsage(pbAppDiskUsage.getAppId, pbAppDiskUsage.getEstimatedUsage)
  }

  def toPbAppDiskUsage(appDiskUsage: AppDiskUsage): PbAppDiskUsage = {
    PbAppDiskUsage.newBuilder()
      .setAppId(appDiskUsage.appId)
      .setEstimatedUsage(appDiskUsage.estimatedUsage)
      .build()
  }

  def fromPbAppDiskUsageSnapshot(
      pbAppDiskUsageSnapShot: PbAppDiskUsageSnapshot): AppDiskUsageSnapShot = {
    val snapShot = new AppDiskUsageSnapShot(pbAppDiskUsageSnapShot.getTopItemCount)
    snapShot.startSnapShotTime = pbAppDiskUsageSnapShot.getStartSnapShotTime
    snapShot.endSnapShotTime = pbAppDiskUsageSnapShot.getEndSnapshotTime
    snapShot.restoreFromSnapshot(
      pbAppDiskUsageSnapShot
        .getTopNItemsList
        .asScala
        .map(fromPbAppDiskUsage)
        .asJava
        .stream()
        .toArray(new IntFunction[Array[AppDiskUsage]]() {
          override def apply(value: Int): Array[AppDiskUsage] = new Array[AppDiskUsage](value)
        }))
    snapShot
  }

  def toPbAppDiskUsageSnapshot(snapshots: AppDiskUsageSnapShot): PbAppDiskUsageSnapshot = {
    PbAppDiskUsageSnapshot.newBuilder()
      .setTopItemCount(snapshots.topItemCount)
      .setStartSnapShotTime(snapshots.startSnapShotTime)
      .setEndSnapshotTime(snapshots.endSnapShotTime)
      // topNItems some value could be null
      .addAllTopNItems(snapshots.topNItems.filter(_ != null).map(toPbAppDiskUsage).toList.asJava)
      .build()
  }

  def toPbSnapshotMetaInfo(
      estimatedPartitionSize: java.lang.Long,
      registeredShuffle: java.util.Map[String, java.util.Set[Integer]],
      hostnameSet: java.util.Set[String],
      excludedWorkers: java.util.Set[WorkerInfo],
      manuallyExcludedWorkers: java.util.Set[WorkerInfo],
      workerLostEvent: java.util.Set[WorkerInfo],
      appHeartbeatTime: java.util.Map[String, java.lang.Long],
      workers: java.util.Set[WorkerInfo],
      partitionTotalWritten: java.lang.Long,
      partitionTotalFileCount: java.lang.Long,
      appDiskUsageMetricSnapshots: Array[AppDiskUsageSnapShot],
      currentAppDiskUsageMetricsSnapshot: AppDiskUsageSnapShot,
      lostWorkers: ConcurrentHashMap[WorkerInfo, java.lang.Long],
      shutdownWorkers: java.util.Set[WorkerInfo],
      workerEventInfos: ConcurrentHashMap[WorkerInfo, WorkerEventInfo],
      applicationMetas: ConcurrentHashMap[String, ApplicationMeta],
      decommissionWorkers: java.util.Set[WorkerInfo]): PbSnapshotMetaInfo = {
    val builder = PbSnapshotMetaInfo.newBuilder()
      .setEstimatedPartitionSize(estimatedPartitionSize)
      .addAllRegisteredShuffle(registeredShuffle.asScala.flatMap { appIdAndShuffleId =>
        appIdAndShuffleId._2.asScala.map(i => Utils.makeShuffleKey(appIdAndShuffleId._1, i))
      }.asJava)
      .addAllHostnameSet(hostnameSet)
      .addAllExcludedWorkers(excludedWorkers.asScala.map(toPbWorkerInfo(_, true, false)).asJava)
      .addAllManuallyExcludedWorkers(manuallyExcludedWorkers.asScala
        .map(toPbWorkerInfo(_, true, false)).asJava)
      .addAllWorkerLostEvents(workerLostEvent.asScala.map(toPbWorkerInfo(_, true, false)).asJava)
      .putAllAppHeartbeatTime(appHeartbeatTime)
      .addAllWorkers(workers.asScala.map(toPbWorkerInfo(_, true, false)).asJava)
      .setPartitionTotalWritten(partitionTotalWritten)
      .setPartitionTotalFileCount(partitionTotalFileCount)
      // appDiskUsageMetricSnapshots can have null values,
      // protobuf repeated value can't support null value in list.
      .addAllAppDiskUsageMetricSnapshots(appDiskUsageMetricSnapshots.filter(_ != null)
        .map(toPbAppDiskUsageSnapshot).toList.asJava)
      .putAllLostWorkers(lostWorkers.asScala.map {
        case (worker: WorkerInfo, time: java.lang.Long) => (worker.toUniqueId(), time)
      }.asJava)
      .addAllShutdownWorkers(shutdownWorkers.asScala.map(toPbWorkerInfo(_, true, false)).asJava)
      .putAllWorkerEventInfos(workerEventInfos.asScala.map {
        case (worker, workerEventInfo) =>
          (worker.toUniqueId(), PbSerDeUtils.toPbWorkerEventInfo(workerEventInfo))
      }.asJava)
      .addAllDecommissionWorkers(decommissionWorkers.asScala.map(
        toPbWorkerInfo(_, true, false)).asJava)

    if (currentAppDiskUsageMetricsSnapshot != null) {
      builder.setCurrentAppDiskUsageMetricsSnapshot(
        toPbAppDiskUsageSnapshot(currentAppDiskUsageMetricsSnapshot))
    }
    val pbApplicationMetas = applicationMetas.asScala.map {
      case (appId, applicationMeta) => (appId, toPbApplicationMeta(applicationMeta))
    }.asJava
    if (localCollectionUtils.isNotEmpty(pbApplicationMetas)) {
      builder.putAllApplicationMetas(pbApplicationMetas)
    }
    builder.build()
  }

  def toPbApplicationMeta(meta: ApplicationMeta): PbApplicationMeta = {
    PbApplicationMeta.newBuilder()
      .setAppId(meta.appId)
      .setSecret(meta.secret).build()
  }

  def fromPbApplicationMeta(pbApplicationMeta: PbApplicationMeta): ApplicationMeta = {
    new ApplicationMeta(pbApplicationMeta.getAppId, pbApplicationMeta.getSecret)
  }

  def toPbWorkerStatus(workerStatus: WorkerStatus): PbWorkerStatus = {
    PbWorkerStatus.newBuilder()
      .setState(workerStatus.getState)
      .setStateStartTime(workerStatus.getStateStartTime)
      .build()
  }

  def fromPbWorkerStatus(pbWorkerStatus: PbWorkerStatus): WorkerStatus = {
    new WorkerStatus(pbWorkerStatus.getState.getNumber, pbWorkerStatus.getStateStartTime)
  }

  def toPbWorkerEventInfo(workerEventInfo: WorkerEventInfo): PbWorkerEventInfo = {
    PbWorkerEventInfo.newBuilder()
      .setEventStartTime(workerEventInfo.getEventStartTime)
      .setWorkerEventType(workerEventInfo.getEventType)
      .build()
  }

  def fromPbWorkerEventInfo(pbWorkerEventInfo: PbWorkerEventInfo): WorkerEventInfo = {
    new WorkerEventInfo(
      pbWorkerEventInfo.getWorkerEventType.getNumber,
      pbWorkerEventInfo.getEventStartTime())
  }

  private def toPackedPartitionLocation(
      pbPackedLocationsBuilder: PbPackedPartitionLocations.Builder,
      workerIdIndex: Map[String, Int],
      mountPointsIndex: Map[String, Int],
      location: PartitionLocation): PbPackedPartitionLocations.Builder = {
    pbPackedLocationsBuilder.addIds(location.getId)
    pbPackedLocationsBuilder.addEpoches(location.getEpoch)
    pbPackedLocationsBuilder.addWorkerIds(workerIdIndex(location.getWorker.toUniqueId()))
    pbPackedLocationsBuilder.addMapIdBitMap(
      Utils.roaringBitmapToByteString(location.getMapIdBitMap))
    pbPackedLocationsBuilder.addTypes(location.getStorageInfo.getType.getValue)
    pbPackedLocationsBuilder.addMountPoints(
      mountPointsIndex(location.getStorageInfo.getMountPoint))
    pbPackedLocationsBuilder.addFinalResult(location.getStorageInfo.isFinalResult)
    if (location.getStorageInfo.getFilePath != null && location.getStorageInfo.getFilePath.nonEmpty) {
      pbPackedLocationsBuilder.addFilePaths(location.getStorageInfo.getFilePath
        .substring(location.getStorageInfo.getMountPoint.length))
    } else {
      pbPackedLocationsBuilder.addFilePaths("")
    }
    pbPackedLocationsBuilder.addAvailableStorageTypes(location.getStorageInfo.availableStorageTypes)
    pbPackedLocationsBuilder.addModes(location.getMode.mode())
  }

  def toPbPackedPartitionLocationsPair(inputLocations: List[PartitionLocation])
      : PbPackedPartitionLocationsPair = {
    val packedLocationPairsBuilder = PbPackedPartitionLocationsPair.newBuilder()
    val packedLocationsBuilder = PbPackedPartitionLocations.newBuilder()

    val implicateLocations = inputLocations.map(_.getPeer).filterNot(_ == null)

    val allLocations = (inputLocations ++ implicateLocations)
    val workerIdList = new util.ArrayList[String](
      allLocations.map(_.getWorker.toUniqueId()).toSet.asJava)
    val workerIdIndex = workerIdList.asScala.zipWithIndex.toMap
    val mountPointsList = new util.ArrayList[String](
      allLocations.map(
        _.getStorageInfo.getMountPoint).toSet.asJava)
    val mountPointsIndex = mountPointsList.asScala.zipWithIndex.toMap

    packedLocationsBuilder.addAllWorkerIdsSet(workerIdList)
    packedLocationsBuilder.addAllMountPointsSet(mountPointsList)

    val locationIndexes = allLocations.zipWithIndex.toMap

    for (location <- allLocations) {
      toPackedPartitionLocation(
        packedLocationsBuilder,
        workerIdIndex,
        mountPointsIndex,
        location)
      if (location.getPeer != null) {
        packedLocationPairsBuilder.addPeerIndexes(
          locationIndexes(location.getPeer))
      } else {
        packedLocationPairsBuilder.addPeerIndexes(Integer.MAX_VALUE)
      }
    }

    packedLocationPairsBuilder.setInputLocationSize(inputLocations.size)
    packedLocationPairsBuilder.setLocations(packedLocationsBuilder.build()).build()
  }

  def fromPbPackedPartitionLocationsPair(pbPartitionLocationsPair: PbPackedPartitionLocationsPair)
      : (util.List[PartitionLocation], util.List[PartitionLocation]) = {
    val primaryLocations = new util.ArrayList[PartitionLocation]()
    val replicateLocations = new util.ArrayList[PartitionLocation]()
    val pbPackedPartitionLocations = pbPartitionLocationsPair.getLocations
    val inputLocationSize = pbPartitionLocationsPair.getInputLocationSize
    val idList = pbPackedPartitionLocations.getIdsList
    val locationCount = idList.size()
    var index = 0

    val locations = new util.ArrayList[PartitionLocation]()
    while (index < locationCount) {
      val loc =
        fromPackedPartitionLocations(pbPackedPartitionLocations, index)
      if (index < inputLocationSize) {
        if (loc.getMode == Mode.PRIMARY) {
          primaryLocations.add(loc)
        } else {
          replicateLocations.add(loc)
        }
      }
      locations.add(loc)
      index = index + 1
    }

    index = 0
    while (index < locationCount) {
      val replicateIndex = pbPartitionLocationsPair.getPeerIndexes(index)
      if (replicateIndex != Integer.MAX_VALUE) {
        locations.get(index).setPeer(locations.get(replicateIndex))
      }
      index = index + 1
    }

    (primaryLocations, replicateLocations)
  }

  private def fromPackedPartitionLocations(
      pbPackedPartitionLocations: PbPackedPartitionLocations,
      index: Int): PartitionLocation = {
    val workerIdParts = Utils.parseColonSeparatedHostPorts(
      pbPackedPartitionLocations.getWorkerIdsSet(
        pbPackedPartitionLocations.getWorkerIds(index)),
      4).map(_.trim)
    var filePath = pbPackedPartitionLocations.getFilePaths(index)
    if (filePath != "") {
      filePath = pbPackedPartitionLocations.getMountPointsSet(
        pbPackedPartitionLocations.getMountPoints(index)) +
        pbPackedPartitionLocations.getFilePaths(index)
    }

    val mode =
      if (pbPackedPartitionLocations.getModes(index) == Mode.PRIMARY.mode()) {
        Mode.PRIMARY
      } else {
        Mode.REPLICA
      }

    new PartitionLocation(
      pbPackedPartitionLocations.getIds(index),
      pbPackedPartitionLocations.getEpoches(index),
      workerIdParts(0),
      workerIdParts(1).toInt,
      workerIdParts(2).toInt,
      workerIdParts(3).toInt,
      workerIdParts(4).toInt,
      mode,
      null,
      new StorageInfo(
        StorageInfo.typesMap.get(pbPackedPartitionLocations.getTypes(index)),
        pbPackedPartitionLocations.getMountPointsSet(
          pbPackedPartitionLocations.getMountPoints(index)),
        pbPackedPartitionLocations.getFinalResult(index),
        filePath,
        pbPackedPartitionLocations.getAvailableStorageTypes(index)),
      Utils.byteStringToRoaringBitmap(pbPackedPartitionLocations.getMapIdBitMap(index)))
  }

  def fromPbPackedWorkerResource(pbWorkerResource: util.Map[String, PbPackedWorkerResource])
      : WorkerResource = {
    val slots = new WorkerResource()
    pbWorkerResource.asScala.foreach { case (uniqueId, pbPackedWorkerResource) =>
      val networkLocation = pbPackedWorkerResource.getNetworkLocation
      val workerInfo = WorkerInfo.fromUniqueId(uniqueId)
      workerInfo.networkLocation = networkLocation
      val (primaryLocations, replicateLocations) =
        fromPbPackedPartitionLocationsPair(pbPackedWorkerResource.getLocationPairs)
      slots.put(workerInfo, (primaryLocations, replicateLocations))
    }
    slots
  }

  def toPbPackedWorkerResource(workerResource: WorkerResource)
      : util.Map[String, PbPackedWorkerResource] = {
    workerResource.asScala.map { case (workerInfo, (primaryLocations, replicaLocations)) =>
      val pbWorkerResource = PbPackedWorkerResource.newBuilder()
        .setLocationPairs(toPbPackedPartitionLocationsPair(
          primaryLocations.asScala.toList ++ replicaLocations.asScala.toList))
        .setNetworkLocation(workerInfo.networkLocation)
        .build()
      workerInfo.toUniqueId() -> pbWorkerResource
    }.asJava
  }

}
