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

package org.apache.celeborn.common.meta

import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.hadoop.net.NetworkTopology

import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.rpc.RpcEndpointRef
import org.apache.celeborn.common.rpc.netty.NettyRpcEndpointRef
import org.apache.celeborn.common.util.{JavaUtils, Utils}

class WorkerInfo(
    val host: String,
    val rpcPort: Int,
    val pushPort: Int,
    val fetchPort: Int,
    val replicatePort: Int,
    val internalPort: Int,
    _version: String,
    _diskInfos: util.Map[String, DiskInfo],
    _userResourceConsumption: util.Map[UserIdentifier, ResourceConsumption]) extends Serializable
  with Logging {
  var networkLocation = NetworkTopology.DEFAULT_RACK
  val version = Option(_version).getOrElse(WorkerInfo.UNKNOWN_VERSION)
  var lastHeartbeat: Long = 0
  var workerStatus = WorkerStatus.normalWorkerStatus()
  val diskInfos =
    if (_diskInfos != null) JavaUtils.newConcurrentHashMap[String, DiskInfo](_diskInfos)
    else null
  val userResourceConsumption =
    if (_userResourceConsumption != null)
      JavaUtils.newConcurrentHashMap[UserIdentifier, ResourceConsumption](_userResourceConsumption)
    else null
  var endpoint: RpcEndpointRef = null

  def this(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int) = {
    this(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      -1,
      WorkerInfo.UNKNOWN_VERSION,
      new util.HashMap[String, DiskInfo](),
      new util.HashMap[UserIdentifier, ResourceConsumption]())
  }

  def this(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      internalPort: Int) = {
    this(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      internalPort,
      WorkerInfo.UNKNOWN_VERSION,
      new util.HashMap[String, DiskInfo](),
      new util.HashMap[UserIdentifier, ResourceConsumption]())
  }

  def this(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      internalPort: Int,
      diskInfos: util.Map[String, DiskInfo],
      userResourceConsumption: util.Map[UserIdentifier, ResourceConsumption]) = {
    this(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      internalPort,
      WorkerInfo.UNKNOWN_VERSION,
      diskInfos,
      userResourceConsumption)
  }

  def isActive: Boolean = {
    endpoint.asInstanceOf[NettyRpcEndpointRef].client.isActive
  }

  def usedSlots(): Long = this.synchronized {
    // HDFS or OSS do not treat as a used slot
    diskInfos.asScala.map(_._2.activeSlots).sum
  }

  def allocateSlots(shuffleKey: String, slotsPerDisk: util.Map[String, Integer]): Unit = {
    logDebug(s"Shuffle $shuffleKey, allocations $slotsPerDisk")
    this.synchronized {
      slotsPerDisk.asScala.foreach { case (disk, slots) =>
        val diskInfo = diskInfos.get(disk)
        if (diskInfo == null) {
          logDebug(s"Unknown disk $disk when allocateSlots")
        } else {
          diskInfo.allocateSlots(shuffleKey, slots)
        }
      }
    }
  }

  def releaseSlots(shuffleKey: String, slots: util.Map[String, Integer]): Unit = this.synchronized {
    slots.asScala.foreach { case (disk, slot) =>
      if (diskInfos.containsKey(disk)) {
        diskInfos.get(disk).releaseSlots(shuffleKey, slot)
      }
    }
  }

  def releaseSlots(shuffleKey: String): Unit = this.synchronized {
    diskInfos.asScala.foreach(_._2.releaseSlots(shuffleKey))
  }

  def getShuffleKeySet: util.HashSet[String] = this.synchronized {
    val shuffleKeySet = new util.HashSet[String]()
    diskInfos.values().asScala.foreach { diskInfo =>
      shuffleKeySet.addAll(diskInfo.getShuffleKeySet())
    }
    shuffleKeySet
  }

  def getApplicationIdSet: util.HashSet[String] = this.synchronized {
    val applicationIdSet = new util.HashSet[String]()
    diskInfos.values().asScala.foreach { diskInfo =>
      applicationIdSet.addAll(diskInfo.getApplicationIdSet())
    }
    applicationIdSet
  }

  def hasSameInfoWith(other: WorkerInfo): Boolean = {
    rpcPort == other.rpcPort &&
    pushPort == other.pushPort &&
    host == other.host &&
    fetchPort == other.fetchPort &&
    replicatePort == other.replicatePort
  }

  def setupEndpoint(endpointRef: RpcEndpointRef): Unit = {
    if (this.endpoint == null) {
      this.endpoint = endpointRef
    }
  }

  def readableAddress(): String = {
    s"Host:$host:RpcPort:$rpcPort:PushPort:$pushPort:" +
      s"FetchPort:$fetchPort:ReplicatePort:$replicatePort" +
      (if (internalPort > 0) s":InternalPort:$internalPort" else "")
  }

  def toUniqueId(): String = {
    s"$host:$rpcPort:$pushPort:$fetchPort:$replicatePort"
  }

  def slotAvailable(): Boolean = this.synchronized {
    diskInfos.asScala.exists { case (_, disk) => (disk.maxSlots - disk.activeSlots) > 0 }
  }

  def getTotalSlots(): Long = this.synchronized {
    diskInfos.asScala.map(_._2.maxSlots).sum
  }

  def getWorkerStatus(): WorkerStatus = {
    workerStatus
  }

  def setWorkerStatus(workerStatus: WorkerStatus): Unit = {
    this.workerStatus = workerStatus;
  }

  def updateDiskMaxSlots(estimatedPartitionSize: Long): Unit = this.synchronized {
    diskInfos.asScala.foreach { case (_, disk) =>
      disk.maxSlots_$eq(disk.actualUsableSpace / estimatedPartitionSize)
    }
  }

  def totalAvailableSlots(): Long = this.synchronized {
    diskInfos.asScala.map(_._2.availableSlots()).sum
  }

  def totalSpace(): Long = this.synchronized {
    diskInfos.asScala.map(_._2.totalSpace).sum
  }

  def totalActualUsableSpace(): Long = this.synchronized {
    diskInfos.asScala.map(_._2.actualUsableSpace).sum
  }

  def updateThenGetDiskInfos(
      newDiskInfos: java.util.Map[String, DiskInfo],
      estimatedPartitionSize: Option[Long] = None): util.Map[String, DiskInfo] =
    this.synchronized {
      import scala.collection.JavaConverters._
      for (newDisk <- newDiskInfos.values().asScala) {
        val mountPoint: String = newDisk.mountPoint
        val curDisk = diskInfos.get(mountPoint)
        if (curDisk != null) {
          curDisk.actualUsableSpace = newDisk.actualUsableSpace
          curDisk.totalSpace = newDisk.totalSpace
          // Update master's diskinfo activeslots to worker's value
          curDisk.activeSlots = newDisk.activeSlots
          curDisk.avgFlushTime = newDisk.avgFlushTime
          curDisk.avgFetchTime = newDisk.avgFetchTime
          if (estimatedPartitionSize.nonEmpty && curDisk.storageType != StorageInfo.Type.HDFS && curDisk.storageType != StorageInfo.Type.S3) {
            curDisk.maxSlots = curDisk.actualUsableSpace / estimatedPartitionSize.get
          }
          curDisk.setStatus(newDisk.status)
        } else {
          if (estimatedPartitionSize.nonEmpty && newDisk.storageType != StorageInfo.Type.HDFS && newDisk.storageType != StorageInfo.Type.S3) {
            newDisk.maxSlots = newDisk.actualUsableSpace / estimatedPartitionSize.get
          }
          diskInfos.put(mountPoint, newDisk)
        }
      }

      val nonExistsMountPoints: java.util.Set[String] = new util.HashSet[String]
      nonExistsMountPoints.addAll(diskInfos.keySet)
      nonExistsMountPoints.removeAll(newDiskInfos.keySet)
      if (!nonExistsMountPoints.isEmpty) {
        for (nonExistsMountPoint <- nonExistsMountPoints.asScala) {
          diskInfos.remove(nonExistsMountPoint)
        }
      }
      JavaUtils.newConcurrentHashMap[String, DiskInfo](diskInfos)
    }

  def updateThenGetUserResourceConsumption(resourceConsumptions: util.Map[
    UserIdentifier,
    ResourceConsumption]): util.Map[UserIdentifier, ResourceConsumption] = {
    userResourceConsumption.keys().asScala.filterNot(resourceConsumptions.containsKey).foreach {
      identifier =>
        userResourceConsumption.put(identifier, ResourceConsumption(0, 0, 0, 0))
    }
    userResourceConsumption.putAll(resourceConsumptions)
    userResourceConsumption
  }

  override def toString: String = {
    val (diskInfosString, slots) =
      if (diskInfos == null || diskInfos.isEmpty) {
        ("empty", 0)
      } else if (diskInfos != null) {
        val str = diskInfos.values().asScala.zipWithIndex.map { case (diskInfo, index) =>
          s"\n  DiskInfo${index}: ${diskInfo}"
        }.mkString("")
        (str, usedSlots)
      }
    val userResourceConsumptionString =
      if (userResourceConsumption == null || userResourceConsumption.isEmpty) {
        "empty"
      } else if (userResourceConsumption != null) {
        userResourceConsumption.asScala.map { case (userIdentifier, resourceConsumption) =>
          s"\n  UserIdentifier: ${userIdentifier}, ResourceConsumption: ${resourceConsumption}"
        }.mkString("")
      }
    s"""
       |Host: $host
       |RpcPort: $rpcPort
       |PushPort: $pushPort
       |FetchPort: $fetchPort
       |ReplicatePort: $replicatePort
       |InternalPort: $internalPort
       |SlotsUsed: $slots
       |LastHeartbeat: $lastHeartbeat
       |HeartbeatElapsedSeconds: ${TimeUnit.MILLISECONDS.toSeconds(
      System.currentTimeMillis() - lastHeartbeat)}
       |Disks: $diskInfosString
       |UserResourceConsumption: $userResourceConsumptionString
       |WorkerRef: $endpoint
       |WorkerStatus: $workerStatus
       |NetworkLocation: $networkLocation
       |""".stripMargin
  }

  override def equals(other: Any): Boolean = other match {
    case that: WorkerInfo =>
      host == that.host &&
        rpcPort == that.rpcPort &&
        pushPort == that.pushPort &&
        fetchPort == that.fetchPort &&
        replicatePort == that.replicatePort
    case _ => false
  }

  override def hashCode(): Int = {
    var result = host.hashCode()
    result = 31 * result + rpcPort.hashCode()
    result = 31 * result + pushPort.hashCode()
    result = 31 * result + fetchPort.hashCode()
    result = 31 * result + replicatePort.hashCode()
    result
  }

  def haveDisk(): Boolean = {
    diskInfos.values().asScala.exists(p =>
      p.storageType == StorageInfo.Type.SSD || p.storageType == StorageInfo.Type.HDD)
  }
}

object WorkerInfo {

  def fromUniqueId(id: String): WorkerInfo = {
    val Array(host, rpcPort, pushPort, fetchPort, replicatePort) =
      Utils.parseColonSeparatedHostPorts(id, portsNum = 4)
    new WorkerInfo(host, rpcPort.toInt, pushPort.toInt, fetchPort.toInt, replicatePort.toInt)
  }

  val UNKNOWN_VERSION = "<UNKNOWN>"
}
