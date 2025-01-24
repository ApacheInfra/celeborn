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

package org.apache.celeborn.client.commit

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{Callable, ConcurrentHashMap, ThreadPoolExecutor, TimeUnit}
import java.util.function

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.collect.Sets

import org.apache.celeborn.client.{ClientUtils, ShuffleCommittedInfo, WorkerStatusTracker}
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.{ShuffleAllocatedWorkers, ShuffleFailedWorkers}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.ShufflePartitionLocationInfo
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType}
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext
import org.apache.celeborn.common.rpc.netty.{LocalNettyRpcCallContext, RemoteNettyRpcCallContext}
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.common.write.PushFailedBatch

/**
 * This commit handler is for ReducePartition ShuffleType, which means that a Reduce Partition contains all data
 * produced by all upstream MapTasks, and data in a Reduce Partition would only be consumed by one ReduceTask. If the
 * ReduceTask has multiple inputs, each will be a ReducePartition
 *
 * @see [[org.apache.celeborn.common.protocol.PartitionType.REDUCE]]
 */
class ReducePartitionCommitHandler(
    appUniqueId: String,
    conf: CelebornConf,
    shuffleAllocatedWorkers: ShuffleAllocatedWorkers,
    committedPartitionInfo: CommittedPartitionInfo,
    workerStatusTracker: WorkerStatusTracker,
    sharedRpcPool: ThreadPoolExecutor)
  extends CommitHandler(
    appUniqueId,
    conf,
    committedPartitionInfo,
    workerStatusTracker,
    sharedRpcPool)
  with Logging {

  private val getReducerFileGroupRequest =
    JavaUtils.newConcurrentHashMap[Int, util.Set[RpcCallContext]]()
  private val dataLostShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  private val stageEndShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  private val inProcessStageEndShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  private val shuffleMapperAttempts = JavaUtils.newConcurrentHashMap[Int, Array[Int]]()
  private val stageEndTimeout = conf.clientPushStageEndTimeout
  private val mockShuffleLost = conf.testMockShuffleLost
  private val mockShuffleLostShuffle = conf.testMockShuffleLostShuffle

  private val rpcCacheSize = conf.clientRpcCacheSize
  private val rpcCacheConcurrencyLevel = conf.clientRpcCacheConcurrencyLevel
  private val rpcCacheExpireTime = conf.clientRpcCacheExpireTime

  // noinspection UnstableApiUsage
  private val getReducerFileGroupRpcCache: Cache[Int, ByteBuffer] = CacheBuilder.newBuilder()
    .concurrencyLevel(rpcCacheConcurrencyLevel)
    .expireAfterWrite(rpcCacheExpireTime, TimeUnit.MILLISECONDS)
    .maximumSize(rpcCacheSize)
    .build().asInstanceOf[Cache[Int, ByteBuffer]]

  private val newShuffleId2PushFailedBatchMapFunc
      : function.Function[Int, util.HashMap[String, util.Set[PushFailedBatch]]] =
    new util.function.Function[Int, util.HashMap[String, util.Set[PushFailedBatch]]]() {
      override def apply(s: Int): util.HashMap[String, util.Set[PushFailedBatch]] = {
        new util.HashMap[String, util.Set[PushFailedBatch]]()
      }
    }

  private val uniqueId2PushFailedBatchMapFunc
      : function.Function[String, util.Set[PushFailedBatch]] =
    new util.function.Function[String, util.Set[PushFailedBatch]]() {
      override def apply(s: String): util.Set[PushFailedBatch] = {
        Sets.newHashSet[PushFailedBatch]()
      }
    }

  override def getPartitionType(): PartitionType = {
    PartitionType.REDUCE
  }

  override def isStageEnd(shuffleId: Int): Boolean = {
    stageEndShuffleSet.contains(shuffleId)
  }

  override def isStageEndOrInProcess(shuffleId: Int): Boolean = {
    inProcessStageEndShuffleSet.contains(shuffleId) ||
    stageEndShuffleSet.contains(shuffleId)
  }

  override def isStageDataLost(shuffleId: Int): Boolean = {
    if (mockShuffleLost) {
      mockShuffleLostShuffle == shuffleId
    } else {
      dataLostShuffleSet.contains(shuffleId)
    }
  }

  override def isPartitionInProcess(shuffleId: Int, partitionId: Int): Boolean = {
    isStageEndOrInProcess(shuffleId)
  }

  override def setStageEnd(shuffleId: Int): Unit = {
    getReducerFileGroupRequest synchronized {
      stageEndShuffleSet.add(shuffleId)
    }

    val requests = getReducerFileGroupRequest.remove(shuffleId)
    // Set empty HashSet during register shuffle.
    // In case of stage with no shuffle data, register shuffle will not be called,
    // so here we still need to check null.
    if (requests != null && !requests.isEmpty) {
      requests.asScala.foreach(replyGetReducerFileGroup(_, shuffleId))
    }
  }

  override def removeExpiredShuffle(shuffleId: Int): Unit = {
    dataLostShuffleSet.remove(shuffleId)
    stageEndShuffleSet.remove(shuffleId)
    inProcessStageEndShuffleSet.remove(shuffleId)
    shuffleMapperAttempts.remove(shuffleId)
    super.removeExpiredShuffle(shuffleId)
  }

  override def tryFinalCommit(
      shuffleId: Int,
      recordWorkerFailure: ShuffleFailedWorkers => Unit): Boolean = {
    if (isStageEnd(shuffleId)) {
      logInfo(s"[handleStageEnd] Shuffle $shuffleId already ended!")
      return false
    } else {
      inProcessStageEndShuffleSet.synchronized {
        if (inProcessStageEndShuffleSet.contains(shuffleId)) {
          logWarning(s"[handleStageEnd] Shuffle $shuffleId is in process!")
          return false
        } else {
          inProcessStageEndShuffleSet.add(shuffleId)
        }
      }
    }

    // ask allLocations workers holding partitions to commit files
    val allocatedWorkers = shuffleAllocatedWorkers.get(shuffleId)
    val (dataLost, commitFailedWorkers) = handleFinalCommitFiles(shuffleId, allocatedWorkers)
    recordWorkerFailure(commitFailedWorkers)
    // reply
    if (!dataLost) {
      logInfo(s"Succeed to handle stageEnd for $shuffleId.")
      // record in stageEndShuffleSet
      setStageEnd(shuffleId)
    } else {
      logError(s"Failed to handle stageEnd for $shuffleId, lost file!")
      dataLostShuffleSet.add(shuffleId)
      // record in stageEndShuffleSet
      setStageEnd(shuffleId)
    }
    inProcessStageEndShuffleSet.remove(shuffleId)
    true
  }

  private def handleFinalCommitFiles(
      shuffleId: Int,
      allocatedWorkers: util.Map[String, ShufflePartitionLocationInfo])
      : (Boolean, ShuffleFailedWorkers) = {
    val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)

    // commit files
    val parallelCommitResult = parallelCommitFiles(shuffleId, allocatedWorkers, None)

    // check all inflight request complete
    waitInflightRequestComplete(shuffleCommittedInfo)

    // check data lost
    val dataLost = checkDataLost(
      shuffleId,
      shuffleCommittedInfo.failedPrimaryPartitionIds,
      shuffleCommittedInfo.failedReplicaPartitionIds)

    // collect result
    if (!dataLost) {
      collectResult(
        shuffleId,
        shuffleCommittedInfo,
        getPartitionUniqueIds(shuffleCommittedInfo.committedPrimaryIds),
        getPartitionUniqueIds(shuffleCommittedInfo.committedReplicaIds),
        parallelCommitResult.primaryPartitionLocationMap,
        parallelCommitResult.replicaPartitionLocationMap)
    }

    (dataLost, parallelCommitResult.commitFilesFailedWorkers)
  }

  override def getUnhandledPartitionLocations(
      shuffleId: Int,
      shuffleCommittedInfo: ShuffleCommittedInfo): mutable.Set[PartitionLocation] = {
    shuffleCommittedInfo.unhandledPartitionLocations.asScala.filterNot { partitionLocation =>
      shuffleCommittedInfo.handledPartitionLocations.contains(partitionLocation)
    }
  }

  private def waitInflightRequestComplete(shuffleCommittedInfo: ShuffleCommittedInfo): Unit = {
    while (shuffleCommittedInfo.allInFlightCommitRequestNum.get() > 0) {
      Thread.sleep(1000)
    }
  }

  private def getPartitionUniqueIds(ids: ConcurrentHashMap[Int, util.List[String]])
      : util.Iterator[String] = {
    ids.asScala.flatMap(_._2.asScala).toIterator.asJava
  }

  /**
   * For reduce partition shuffle type If shuffle registered and corresponding map finished, reply true.
   * For map partition shuffle type always return false
   * reduce partition type
   *
   * @param shuffleId
   * @param mapId
   * @return
   */
  override def isMapperEnded(shuffleId: Int, mapId: Int): Boolean = {
    shuffleMapperAttempts.containsKey(shuffleId) && shuffleMapperAttempts.get(shuffleId)(
      mapId) != -1
  }

  override def getMapperAttempts(shuffleId: Int): Array[Int] = {
    shuffleMapperAttempts.get(shuffleId)
  }

  override def finishMapperAttempt(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      partitionId: Int,
      pushFailedBatches: util.Map[String, util.Set[PushFailedBatch]],
      recordWorkerFailure: ShuffleFailedWorkers => Unit): (Boolean, Boolean) = {
    shuffleMapperAttempts.synchronized {
      if (getMapperAttempts(shuffleId) == null) {
        logDebug(s"[handleMapperEnd] $shuffleId not registered, create one.")
        initMapperAttempts(shuffleId, numMappers)
      }

      val attempts = shuffleMapperAttempts.get(shuffleId)
      if (attempts(mapId) < 0) {
        attempts(mapId) = attemptId
        if (null != pushFailedBatches && !pushFailedBatches.isEmpty) {
          val pushFailedBatchesMap = shufflePushFailedBatches.computeIfAbsent(
            shuffleId,
            newShuffleId2PushFailedBatchMapFunc)
          for ((partitionUniqId, pushFailedBatchSet) <- pushFailedBatches.asScala) {
            val partitionPushFailedBatches = pushFailedBatchesMap.computeIfAbsent(
              partitionUniqId,
              uniqueId2PushFailedBatchMapFunc)
            partitionPushFailedBatches.addAll(pushFailedBatchSet)
          }
        }
        // Mapper with this attemptId finished, also check all other mapper finished or not.
        (true, ClientUtils.areAllMapperAttemptsFinished(attempts))
      } else {
        // Mapper with another attemptId finished, skip this request
        (false, false)
      }
    }
  }

  override def registerShuffle(
      shuffleId: Int,
      numMappers: Int,
      isSegmentGranularityVisible: Boolean): Unit = {
    super.registerShuffle(shuffleId, numMappers, isSegmentGranularityVisible)
    getReducerFileGroupRequest.put(shuffleId, new util.HashSet[RpcCallContext]())
    initMapperAttempts(shuffleId, numMappers)
  }

  private def initMapperAttempts(shuffleId: Int, numMappers: Int): Unit = {
    shuffleMapperAttempts.synchronized {
      if (!shuffleMapperAttempts.containsKey(shuffleId)) {
        val attempts = new Array[Int](numMappers)
        0 until numMappers foreach (idx => attempts(idx) = -1)
        shuffleMapperAttempts.put(shuffleId, attempts)
      }
    }
  }

  private def replyGetReducerFileGroup(context: RpcCallContext, shuffleId: Int): Unit = {
    if (isStageDataLost(shuffleId)) {
      context.reply(
        GetReducerFileGroupResponse(
          StatusCode.SHUFFLE_DATA_LOST,
          JavaUtils.newConcurrentHashMap(),
          Array.empty,
          new util.HashSet[Integer]()))
    } else {
      // LocalNettyRpcCallContext is for the UTs
      if (context.isInstanceOf[LocalNettyRpcCallContext]) {
        context.reply(GetReducerFileGroupResponse(
          StatusCode.SUCCESS,
          reducerFileGroupsMap.getOrDefault(shuffleId, JavaUtils.newConcurrentHashMap()),
          getMapperAttempts(shuffleId)))
      } else {
        val cachedMsg = getReducerFileGroupRpcCache.get(
          shuffleId,
          new Callable[ByteBuffer]() {
            override def call(): ByteBuffer = {
              val returnedMsg = GetReducerFileGroupResponse(
                StatusCode.SUCCESS,
                reducerFileGroupsMap.getOrDefault(shuffleId, JavaUtils.newConcurrentHashMap()),
                getMapperAttempts(shuffleId),
                pushFailedBatches =
                  shufflePushFailedBatches.getOrDefault(
                    shuffleId,
                    new util.HashMap[String, util.Set[PushFailedBatch]]()))
              context.asInstanceOf[RemoteNettyRpcCallContext].nettyEnv.serialize(returnedMsg)
            }
          })
        context.asInstanceOf[RemoteNettyRpcCallContext].callback.onSuccess(cachedMsg)
      }
    }
  }

  override def handleGetReducerFileGroup(context: RpcCallContext, shuffleId: Int): Unit = {
    // Quick return for ended stage, avoid occupy sync lock.
    if (isStageEnd(shuffleId)) {
      replyGetReducerFileGroup(context, shuffleId)
    } else {
      getReducerFileGroupRequest.synchronized {
        // If setStageEnd() called after isStageEnd and before got lock, should reply here.
        if (isStageEnd(shuffleId)) {
          replyGetReducerFileGroup(context, shuffleId)
        } else {
          getReducerFileGroupRequest.get(shuffleId).add(context)
        }
      }
    }
  }

  override def waitStageEnd(shuffleId: Int): (Boolean, Long) = {
    var timeout = stageEndTimeout
    val delta = 100
    while (!isStageEnd(shuffleId) && timeout > 0) {
      Thread.sleep(delta)
      timeout = timeout - delta
    }

    (timeout <= 0, stageEndTimeout - timeout)
  }
}
