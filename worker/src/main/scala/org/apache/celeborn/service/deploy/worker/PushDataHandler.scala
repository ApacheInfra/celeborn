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

package org.apache.celeborn.service.deploy.worker

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicIntegerArray}

import scala.collection.mutable
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import com.google.protobuf.GeneratedMessageV3
import io.netty.buffer.ByteBuf

import org.apache.celeborn.common.exception.{AlreadyClosedException, CelebornChecksumException, CelebornIOException}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskStatus, WorkerInfo, WorkerPartitionLocationInfo}
import org.apache.celeborn.common.metrics.source.Source
import org.apache.celeborn.common.network.buffer.{NettyManagedBuffer, NioManagedBuffer}
import org.apache.celeborn.common.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.celeborn.common.network.protocol.{Message, PushData, PushDataHandShake, PushMergedData, RegionFinish, RegionStart, RequestMessage, RpcFailure, RpcRequest, RpcResponse, TransportMessage}
import org.apache.celeborn.common.network.protocol.Message.Type
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, PbPushDataHandShake, PbPushMergedDataSplitPartitionInfo, PbRegionFinish, PbRegionStart, PbSegmentStart}
import org.apache.celeborn.common.protocol.PbPartitionLocation.Mode
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.common.util.{ExceptionUtils, PushDataHeaderUtils, Utils}
import org.apache.celeborn.service.deploy.worker.congestcontrol.CongestionController
import org.apache.celeborn.service.deploy.worker.storage.{HdfsFlusher, LocalFlusher, MapPartitionDataWriter, PartitionDataWriter, S3Flusher, StorageManager}
import org.apache.celeborn.service.deploy.worker.storage.segment.SegmentMapPartitionFileWriter

class PushDataHandler(val workerSource: WorkerSource) extends BaseMessageHandler with Logging {

  private var partitionLocationInfo: WorkerPartitionLocationInfo = _
  private var shuffleMapperAttempts: ConcurrentHashMap[String, AtomicIntegerArray] = _
  private var shufflePartitionType: ConcurrentHashMap[String, PartitionType] = _
  private var shufflePushDataTimeout: ConcurrentHashMap[String, Long] = _
  private var replicateThreadPool: ThreadPoolExecutor = _
  private var unavailablePeers: ConcurrentHashMap[WorkerInfo, Long] = _
  private var replicateClientFactory: TransportClientFactory = _
  private var registered: Option[AtomicBoolean] = None
  private var workerInfo: WorkerInfo = _
  private var partitionSplitMinimumSize: Long = _
  private var partitionSplitMaximumSize: Long = _
  private var shutdown: AtomicBoolean = _
  private var storageManager: StorageManager = _
  private var workerPartitionSplitEnabled: Boolean = _
  private var workerReplicateRandomConnectionEnabled: Boolean = _
  private var workerChecksumVerifyEnabled: Boolean = _

  private var testPushPrimaryDataTimeout: Boolean = _
  private var testPushReplicaDataTimeout: Boolean = _

  def init(worker: Worker): Unit = {
    partitionLocationInfo = worker.partitionLocationInfo
    shufflePartitionType = worker.shufflePartitionType
    shufflePushDataTimeout = worker.shufflePushDataTimeout
    shuffleMapperAttempts = worker.shuffleMapperAttempts
    replicateThreadPool = worker.replicateThreadPool
    unavailablePeers = worker.unavailablePeers
    replicateClientFactory = worker.replicateClientFactory
    workerInfo = worker.workerInfo
    partitionSplitMinimumSize = worker.conf.partitionSplitMinimumSize
    partitionSplitMaximumSize = worker.conf.partitionSplitMaximumSize
    storageManager = worker.storageManager
    shutdown = worker.shutdown
    workerPartitionSplitEnabled = worker.conf.workerPartitionSplitEnabled
    workerReplicateRandomConnectionEnabled = worker.conf.workerReplicateRandomConnectionEnabled
    workerChecksumVerifyEnabled = worker.conf.workerChecksumVerifyEnabled

    testPushPrimaryDataTimeout = worker.conf.testPushPrimaryDataTimeout
    testPushReplicaDataTimeout = worker.conf.testPushReplicaDataTimeout
    registered = Some(worker.registered)
  }

  override def receive(
      client: TransportClient,
      msg: RequestMessage,
      callback: RpcResponseCallback): Unit = {
    handleRpcRequest(client, msg.asInstanceOf[RpcRequest], callback)
  }

  override def receive(client: TransportClient, msg: RequestMessage): Unit =
    msg match {
      case pushData: PushData =>
        workerSource.recordAppActiveConnection(client, pushData.shuffleKey)
        val callback = new SimpleRpcResponseCallback(
          client,
          pushData.requestId,
          pushData.shuffleKey)
        handleCore(
          client,
          pushData,
          pushData.requestId,
          pushData.shuffleKey,
          () => {
            val partitionType =
              shufflePartitionType.getOrDefault(pushData.shuffleKey, PartitionType.REDUCE)
            partitionType match {
              case PartitionType.REDUCE => handlePushData(
                  pushData,
                  callback)
              case PartitionType.MAP => handleMapPartitionPushData(
                  pushData,
                  callback)
              case _ => throw new UnsupportedOperationException(s"Not support $partitionType yet")
            }
          },
          callback)
      case pushMergedData: PushMergedData =>
        workerSource.recordAppActiveConnection(client, pushMergedData.shuffleKey)
        val callback = new SimpleRpcResponseCallback(
          client,
          pushMergedData.requestId,
          pushMergedData.shuffleKey)
        handleCore(
          client,
          pushMergedData,
          pushMergedData.requestId,
          pushMergedData.shuffleKey,
          () =>
            handlePushMergedData(
              pushMergedData,
              callback),
          callback)
    }

  def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isPrimary = mode == PartitionLocation.Mode.PRIMARY

    // For test
    if (isPrimary && testPushPrimaryDataTimeout &&
      !PushDataHandler.pushPrimaryDataTimeoutTested.getAndSet(true)) {
      return
    }

    if (!isPrimary && testPushReplicaDataTimeout &&
      !PushDataHandler.pushReplicaDataTimeoutTested.getAndSet(true)) {
      return
    }

    val key = s"${pushData.requestId}"
    val callbackWithTimer =
      if (isPrimary) {
        new RpcResponseCallbackWithTimer(
          workerSource,
          WorkerSource.PRIMARY_PUSH_DATA_TIME,
          key,
          callback)
      } else {
        new RpcResponseCallbackWithTimer(
          workerSource,
          WorkerSource.REPLICA_PUSH_DATA_TIME,
          key,
          callback)
      }

    // find FileWriter responsible for the data
    val location =
      if (isPrimary) {
        partitionLocationInfo.getPrimaryLocation(shuffleKey, pushData.partitionUniqueId)
      } else {
        partitionLocationInfo.getReplicaLocation(shuffleKey, pushData.partitionUniqueId)
      }

    // Fetch real batchId from body will add more cost and no meaning for replicate.
    val doReplicate = location != null && location.hasPeer && isPrimary
    var softSplit = false

    if (location == null) {
      val (mapId, attemptId) = getMapAttempt(body)
      // MapperAttempts for a shuffle exists after any CommitFiles request succeeds.
      // A shuffle can trigger multiple CommitFiles requests, for reasons like: HARD_SPLIT happens, StageEnd.
      // If MapperAttempts but the value is -1 for the mapId(-1 means the map has not yet finished),
      // it's probably because commitFiles for HARD_SPLIT happens.
      if (shuffleMapperAttempts.containsKey(shuffleKey)) {
        if (-1 != shuffleMapperAttempts.get(shuffleKey).get(mapId)) {
          // partition data has already been committed
          logDebug(
            s"[Case1] Receive push data from speculative task(shuffle $shuffleKey, map $mapId, " +
              s" attempt $attemptId), but this mapper has already been ended.")
          callbackWithTimer.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.MAP_ENDED.getValue)))
        } else {
          logDebug(
            s"Receive push data for committed hard split partition of (shuffle $shuffleKey, " +
              s"map $mapId attempt $attemptId)")
          workerSource.incCounter(WorkerSource.WRITE_DATA_HARD_SPLIT_COUNT)
          callbackWithTimer.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
        }
      } else {
        if (storageManager.shuffleKeySet().contains(shuffleKey)) {
          // If there is no shuffle key in shuffleMapperAttempts but there is shuffle key
          // in StorageManager. This partition should be HARD_SPLIT partition and
          // after worker restart, some tasks still push data to this HARD_SPLIT partition.
          logDebug(s"[Case2] Receive push data for committed hard split partition of " +
            s"(shuffle $shuffleKey, map $mapId attempt $attemptId)")
          callbackWithTimer.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
        } else {
          logWarning(s"While handle PushData, Partition location wasn't found for " +
            s"task(shuffle $shuffleKey, map $mapId, attempt $attemptId, uniqueId ${pushData.partitionUniqueId}).")
          callbackWithTimer.onFailure(
            new CelebornIOException(StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND))
        }
      }
      return
    }

    // During worker shutdown, worker will return HARD_SPLIT for all existed partition.
    // This should before return exception to make current push data can revive and retry.
    if (shutdown.get()) {
      logInfo(s"Push data return HARD_SPLIT for shuffle $shuffleKey since worker shutdown.")
      callbackWithTimer.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
      return
    }

    val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
    val exception = fileWriter.getException
    if (exception != null) {
      val cause =
        if (isPrimary) {
          StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY
        } else {
          StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA
        }
      logError(
        s"While handling PushData, throw $cause, fileWriter $fileWriter has exception.",
        exception)
      workerSource.incCounter(WorkerSource.WRITE_DATA_FAIL_COUNT)
      callbackWithTimer.onFailure(new CelebornIOException(cause))
      return
    }

    val splitStatus = checkDiskFullAndSplit(fileWriter, isPrimary)
    if (splitStatus == StatusCode.HARD_SPLIT) {
      workerSource.incCounter(WorkerSource.WRITE_DATA_HARD_SPLIT_COUNT)
      callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
      return
    } else if (splitStatus == StatusCode.SOFT_SPLIT) {
      softSplit = true
    }

    fileWriter.incrementPendingWrites()

    if (fileWriter.isClosed) {
      val fileInfo = fileWriter.getCurrentFileInfo
      logWarning(
        s"[handlePushData] FileWriter is already closed! File path ${fileInfo.getFilePath} " +
          s"length ${fileInfo.getFileLength}")
      callbackWithTimer.onFailure(new CelebornIOException("File already closed!"))
      fileWriter.decrementPendingWrites()
      return
    }
    val writePromise = Promise[Array[StatusCode]]()
    // for primary, send data to replica
    if (doReplicate) {
      val peer = location.getPeer
      val peerWorker = new WorkerInfo(
        peer.getHost,
        peer.getRpcPort,
        peer.getPushPort,
        peer.getFetchPort,
        peer.getReplicatePort)
      if (unavailablePeers.containsKey(peerWorker)) {
        fileWriter.decrementPendingWrites()
        handlePushDataConnectionFail(callbackWithTimer, location)
        return
      }

      pushData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          if (unavailablePeers.containsKey(peerWorker)) {
            pushData.body().release()
            handlePushDataConnectionFail(callbackWithTimer, location)
            return
          }
          // Handle the response from replica
          val wrappedCallback = new RpcResponseCallback() {
            override def onSuccess(response: ByteBuffer): Unit = {
              Try(Await.result(writePromise.future, Duration.Inf)) match {
                case Success(result) =>
                  if (result(0) != StatusCode.SUCCESS) {
                    callback.onSuccess(ByteBuffer.wrap(Array[Byte](result(0).getValue)))
                  } else {
                    if (response.remaining() > 0) {
                      val resp = ByteBuffer.allocate(response.remaining())
                      resp.put(response)
                      resp.flip()
                      callbackWithTimer.onSuccess(resp)
                    } else if (softSplit) {
                      // TODO Currently if the worker is in soft split status, given the guess that the client
                      // will fast stop pushing data to the worker, we won't return congest status. But
                      // in the long term, especially if this issue could frequently happen, we may need to return
                      // congest&softSplit status together
                      callbackWithTimer.onSuccess(
                        ByteBuffer.wrap(Array[Byte](StatusCode.SOFT_SPLIT.getValue)))
                    } else {
                      Option(CongestionController.instance()) match {
                        case Some(congestionController) =>
                          if (congestionController.isUserCongested(
                              fileWriter.getUserCongestionControlContext)) {
                            // Check whether primary congest the data though the replicas doesn't congest
                            // it(the response is empty)
                            callbackWithTimer.onSuccess(
                              ByteBuffer.wrap(
                                Array[Byte](
                                  StatusCode.PUSH_DATA_SUCCESS_PRIMARY_CONGESTED.getValue)))
                          } else {
                            callbackWithTimer.onSuccess(ByteBuffer.wrap(Array[Byte]()))
                          }
                        case None =>
                          callbackWithTimer.onSuccess(ByteBuffer.wrap(Array[Byte]()))
                      }
                    }
                  }
                case Failure(e) => callbackWithTimer.onFailure(e)
              }
            }

            override def onFailure(e: Throwable): Unit = {
              logError(s"PushData replication failed for partitionLocation: $location", e)
              // 1. Throw PUSH_DATA_WRITE_FAIL_REPLICA by replica peer worker
              // 2. Throw PUSH_DATA_TIMEOUT_REPLICA by TransportResponseHandler
              // 3. Throw IOException by channel, convert to PUSH_DATA_CONNECTION_EXCEPTION_REPLICA
              if (e.getMessage.startsWith(StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA.name())) {
                workerSource.incCounter(WorkerSource.REPLICATE_DATA_WRITE_FAIL_COUNT)
                callbackWithTimer.onFailure(e)
              } else if (e.getMessage.startsWith(StatusCode.PUSH_DATA_TIMEOUT_REPLICA.name())) {
                workerSource.incCounter(WorkerSource.REPLICATE_DATA_TIMEOUT_COUNT)
                callbackWithTimer.onFailure(e)
              } else if (ExceptionUtils.connectFail(e.getMessage)) {
                workerSource.incCounter(WorkerSource.REPLICATE_DATA_CONNECTION_EXCEPTION_COUNT)
                callbackWithTimer.onFailure(
                  new CelebornIOException(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_REPLICA))
              } else {
                workerSource.incCounter(WorkerSource.REPLICATE_DATA_FAIL_NON_CRITICAL_CAUSE_COUNT)
                callbackWithTimer.onFailure(
                  new CelebornIOException(StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_REPLICA))
              }
            }
          }
          try {
            val client = getReplicateClient(peer.getHost, peer.getReplicatePort, location.getId)
            val newPushData = new PushData(
              PartitionLocation.Mode.REPLICA.mode(),
              shuffleKey,
              pushData.partitionUniqueId,
              pushData.body)
            client.pushData(newPushData, shufflePushDataTimeout.get(shuffleKey), wrappedCallback)
          } catch {
            case e: Exception =>
              pushData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              workerSource.incCounter(WorkerSource.REPLICATE_DATA_CREATE_CONNECTION_FAIL_COUNT)
              logError(
                s"PushData replication failed during connecting peer for partitionLocation: $location",
                e)
              callbackWithTimer.onFailure(
                new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA))
          }
        }
      })
      writeLocalData(Seq(fileWriter), body, shuffleKey, isPrimary, None, writePromise)
    } else {
      // The codes here could be executed if
      // 1. the client doesn't enable push data to the replica, the primary worker could hit here
      // 2. the client enables push data to the replica, and the replica worker could hit here
      // TODO Currently if the worker is in soft split status, given the guess that the client
      // will fast stop pushing data to the worker, we won't return congest status. But
      // in the long term, especially if this issue could frequently happen, we may need to return
      // congest&softSplit status together
      writeLocalData(Seq(fileWriter), body, shuffleKey, isPrimary, None, writePromise)
      Try(Await.result(writePromise.future, Duration.Inf)) match {
        case Success(result) =>
          if (result(0) != StatusCode.SUCCESS) {
            callback.onSuccess(ByteBuffer.wrap(Array[Byte](result(0).getValue)))
          } else {
            if (softSplit) {
              callbackWithTimer.onSuccess(
                ByteBuffer.wrap(Array[Byte](StatusCode.SOFT_SPLIT.getValue)))
            } else {
              Option(CongestionController.instance()) match {
                case Some(congestionController) =>
                  if (congestionController.isUserCongested(
                      fileWriter.getUserCongestionControlContext)) {
                    if (isPrimary) {
                      callbackWithTimer.onSuccess(
                        ByteBuffer.wrap(
                          Array[Byte](StatusCode.PUSH_DATA_SUCCESS_PRIMARY_CONGESTED.getValue)))
                    } else {
                      callbackWithTimer.onSuccess(
                        ByteBuffer.wrap(
                          Array[Byte](StatusCode.PUSH_DATA_SUCCESS_REPLICA_CONGESTED.getValue)))
                    }
                  } else {
                    callbackWithTimer.onSuccess(ByteBuffer.wrap(Array[Byte]()))
                  }
                case None =>
                  callbackWithTimer.onSuccess(ByteBuffer.wrap(Array[Byte]()))
              }
            }
          }
        case Failure(e) => callbackWithTimer.onFailure(e)
      }
    }
  }

  def handlePushDataConnectionFail(
      callbackWithTimer: RpcResponseCallback,
      location: PartitionLocation): Unit = {
    workerSource.incCounter(WorkerSource.REPLICATE_DATA_CREATE_CONNECTION_FAIL_COUNT)
    logError(
      s"PushData replication failed caused by unavailable peer for partitionLocation: $location")
    callbackWithTimer.onFailure(
      new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA))
  }

  def handlePushMergedDataConnectionFail(
      pushMergedDataCallback: PushMergedDataCallback,
      location: PartitionLocation): Unit = {
    workerSource.incCounter(WorkerSource.REPLICATE_DATA_CREATE_CONNECTION_FAIL_COUNT)
    logError(
      s"PushMergedData replication failed caused by unavailable peer for partitionLocation: $location")
    pushMergedDataCallback.onFailure(
      new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA))
  }

  def handlePushMergedData(
      pushMergedData: PushMergedData,
      callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushMergedData.shuffleKey
    val mode = PartitionLocation.getMode(pushMergedData.mode)
    val batchOffsets = pushMergedData.batchOffsets
    val body = pushMergedData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isPrimary = mode == PartitionLocation.Mode.PRIMARY
    val (mapId, attemptId) = getMapAttempt(body)

    val key = s"${pushMergedData.requestId}"
    val callbackWithTimer =
      if (isPrimary) {
        new RpcResponseCallbackWithTimer(
          workerSource,
          WorkerSource.PRIMARY_PUSH_DATA_TIME,
          key,
          callback)
      } else {
        new RpcResponseCallbackWithTimer(
          workerSource,
          WorkerSource.REPLICA_PUSH_DATA_TIME,
          key,
          callback)
      }
    val pushMergedDataCallback = new PushMergedDataCallback(callbackWithTimer)

    // For test
    if (isPrimary && testPushPrimaryDataTimeout &&
      !PushDataHandler.pushPrimaryMergeDataTimeoutTested.getAndSet(true)) {
      return
    }

    if (!isPrimary && testPushReplicaDataTimeout &&
      !PushDataHandler.pushReplicaMergeDataTimeoutTested.getAndSet(true)) {
      return
    }

    val partitionIdToLocations =
      if (isPrimary) {
        partitionLocationInfo.getPrimaryLocations(shuffleKey, pushMergedData.partitionUniqueIds)
      } else {
        partitionLocationInfo.getReplicaLocations(shuffleKey, pushMergedData.partitionUniqueIds)
      }

    // Fetch real batchId from body will add more cost and no meaning for replicate.
    val doReplicate =
      partitionIdToLocations.head._2 != null && partitionIdToLocations.head._2.hasPeer && isPrimary

    // find FileWriters responsible for the data
    var index = 0
    while (index < partitionIdToLocations.length) {
      val (id, loc) = partitionIdToLocations(index)
      if (loc == null) {
        // MapperAttempts for a shuffle exists after any CommitFiles request succeeds.
        // A shuffle can trigger multiple CommitFiles requests, for reasons like: HARD_SPLIT happens, StageEnd.
        // If MapperAttempts but the value is -1 for the mapId(-1 means the map has not yet finished),
        // it's probably because commitFiles for HARD_SPLIT happens.
        if (shuffleMapperAttempts.containsKey(shuffleKey)) {
          if (-1 != shuffleMapperAttempts.get(shuffleKey).get(mapId)) {
            logDebug(s"Receive push merged data from speculative " +
              s"task(shuffle $shuffleKey, map $mapId, attempt $attemptId), " +
              s"but this mapper has already been ended.")
            pushMergedDataCallback.onSuccess(StatusCode.MAP_ENDED)
            return
          } else {
            logDebug(s"[Case1] Receive push merged data for committed hard split partition of " +
              s"(shuffle $shuffleKey, map $mapId attempt $attemptId)")
            workerSource.incCounter(WorkerSource.WRITE_DATA_HARD_SPLIT_COUNT)
            pushMergedDataCallback.addSplitPartition(index, StatusCode.HARD_SPLIT)
          }
        } else {
          if (storageManager.shuffleKeySet().contains(shuffleKey)) {
            // If there is no shuffle key in shuffleMapperAttempts but there is shuffle key
            // in StorageManager. This partition should be HARD_SPLIT partition and
            // after worker restart, some tasks still push data to this HARD_SPLIT partition.
            logDebug(s"[Case2] Receive push merged data for committed hard split partition of " +
              s"(shuffle $shuffleKey, map $mapId attempt $attemptId)")
            workerSource.incCounter(WorkerSource.WRITE_DATA_HARD_SPLIT_COUNT)
            pushMergedDataCallback.addSplitPartition(index, StatusCode.HARD_SPLIT)
          } else {
            logWarning(s"While handling PushMergedData, Partition location wasn't found for " +
              s"task(shuffle $shuffleKey, map $mapId, attempt $attemptId, uniqueId $id).")
            pushMergedDataCallback.onFailure(
              new CelebornIOException(StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND))
            return
          }
        }
      }
      index += 1
    }

    // During worker shutdown, worker will return HARD_SPLIT for all existed partition.
    // This should before return exception to make current push data can revive and retry.
    if (shutdown.get()) {
      partitionIdToLocations.indices.foreach(index =>
        pushMergedDataCallback.addSplitPartition(index, StatusCode.HARD_SPLIT))
      pushMergedDataCallback.onSuccess(StatusCode.HARD_SPLIT)
      return
    }

    val (fileWriters, exceptionFileWriterIndexOpt) = getFileWriters(partitionIdToLocations)
    if (exceptionFileWriterIndexOpt.isDefined) {
      val fileWriterWithException = fileWriters(exceptionFileWriterIndexOpt.get)
      val cause =
        if (isPrimary) {
          StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY
        } else {
          StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA
        }
      logError(
        s"While handling PushMergedData, throw $cause, fileWriter $fileWriterWithException has exception.",
        fileWriterWithException.getException)
      workerSource.incCounter(WorkerSource.WRITE_DATA_FAIL_COUNT)
      pushMergedDataCallback.onFailure(new CelebornIOException(cause))
      return
    }

    var fileWriterIndex = 0
    val totalFileWriters = fileWriters.length
    while (fileWriterIndex < totalFileWriters) {
      val fileWriter = fileWriters(fileWriterIndex)
      if (fileWriter == null) {
        if (!pushMergedDataCallback.isHardSplitPartition(fileWriterIndex)) {
          pushMergedDataCallback.onFailure(
            new CelebornIOException(s"Partition $fileWriterIndex's fileWriter not found," +
              s" but it hasn't been identified in the previous validation step."))
          return
        }
      } else {
        if (fileWriter.isClosed) {
          val fileInfo = fileWriter.getCurrentFileInfo
          logWarning(
            s"[handlePushMergedData] FileWriter is already closed! File path ${fileInfo.getFilePath} " +
              s"length ${fileInfo.getFileLength}")
          pushMergedDataCallback.addSplitPartition(fileWriterIndex, StatusCode.HARD_SPLIT)
        } else {
          val splitStatus = checkDiskFullAndSplit(fileWriter, isPrimary)
          if (splitStatus == StatusCode.HARD_SPLIT) {
            logWarning(
              s"return hard split for disk full with shuffle $shuffleKey map $mapId attempt $attemptId")
            workerSource.incCounter(WorkerSource.WRITE_DATA_HARD_SPLIT_COUNT)
            pushMergedDataCallback.addSplitPartition(fileWriterIndex, StatusCode.HARD_SPLIT)
          } else if (splitStatus == StatusCode.SOFT_SPLIT) {
            pushMergedDataCallback.addSplitPartition(fileWriterIndex, StatusCode.SOFT_SPLIT)
          }
        }
        if (!pushMergedDataCallback.isHardSplitPartition(fileWriterIndex)) {
          fileWriter.incrementPendingWrites()
        }
      }
      fileWriterIndex += 1
    }

    val hardSplitIndexes = pushMergedDataCallback.getHardSplitIndexes
    val writePromise = Promise[Array[StatusCode]]()
    // for primary, send data to replica
    if (doReplicate) {
      val location = partitionIdToLocations.head._2
      val peer = location.getPeer
      val peerWorker = new WorkerInfo(
        peer.getHost,
        peer.getRpcPort,
        peer.getPushPort,
        peer.getFetchPort,
        peer.getReplicatePort)
      if (unavailablePeers.containsKey(peerWorker)) {
        for (fileWriterIndex <- 0 until totalFileWriters) {
          val fileWriter = fileWriters(fileWriterIndex)
          if (fileWriter != null && !pushMergedDataCallback.isHardSplitPartition(fileWriterIndex)) {
            fileWriter.decrementPendingWrites()
          }
        }
        handlePushMergedDataConnectionFail(pushMergedDataCallback, location)
        return
      }
      pushMergedData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          if (unavailablePeers.containsKey(peerWorker)) {
            pushMergedData.body().release()
            handlePushMergedDataConnectionFail(pushMergedDataCallback, location)
            return
          }
          // Handle the response from replica
          val wrappedCallback = new RpcResponseCallback() {
            override def onSuccess(response: ByteBuffer): Unit = {
              val replicaReason = response.get()
              if (replicaReason == StatusCode.HARD_SPLIT.getValue) {
                if (response.remaining() > 0) {
                  try {
                    val pushMergedDataResponse: PbPushMergedDataSplitPartitionInfo =
                      TransportMessage.fromByteBuffer(
                        response).getParsedPayload[PbPushMergedDataSplitPartitionInfo]()
                    pushMergedDataCallback.unionReplicaSplitPartitions(
                      pushMergedDataResponse.getSplitPartitionIndexesList,
                      pushMergedDataResponse.getStatusCodesList)
                  } catch {
                    case e: CelebornIOException =>
                      pushMergedDataCallback.onFailure(e)
                      return
                    case e: IllegalArgumentException =>
                      pushMergedDataCallback.onFailure(new CelebornIOException(e))
                      return
                  }
                } else {
                  // During the rolling upgrade of the worker cluster, it is possible for the primary worker
                  // to be upgraded to a new version that includes the changes from [CELEBORN-1721], while
                  // the replica worker is still running on an older version that does not have these changes.
                  // In this scenario, the replica may return a response with a status of HARD_SPLIT, but
                  // will not provide a PbPushMergedDataSplitPartitionInfo.
                  logWarning(
                    s"The response status from the replica (shuffle $shuffleKey map $mapId attempt $attemptId) is HARD_SPLIT, but no PbPushMergedDataSplitPartitionInfo is present.")
                  partitionIdToLocations.indices.foreach(index =>
                    pushMergedDataCallback.addSplitPartition(index, StatusCode.HARD_SPLIT))
                  pushMergedDataCallback.onSuccess(StatusCode.HARD_SPLIT)
                  return
                }
              }
              Try(Await.result(writePromise.future, Duration.Inf)) match {
                case Success(result) =>
                  var index = 0
                  while (index < result.length) {
                    if (result(index) == StatusCode.HARD_SPLIT) {
                      pushMergedDataCallback.addSplitPartition(index, result(index))
                    }
                    index += 1
                  }
                  // Only primary data enable replication will push data to replica
                  Option(CongestionController.instance()) match {
                    case Some(congestionController) if fileWriters.nonEmpty =>
                      if (congestionController.isUserCongested(
                          fileWriters.head.getUserCongestionControlContext)) {
                        // Check whether primary congest the data though the replicas doesn't congest
                        // it(the response is empty)
                        pushMergedDataCallback.onSuccess(
                          StatusCode.PUSH_DATA_SUCCESS_PRIMARY_CONGESTED)
                      } else {
                        if (replicaReason == StatusCode.PUSH_DATA_SUCCESS_REPLICA_CONGESTED.getValue) {
                          pushMergedDataCallback.onSuccess(
                            StatusCode.PUSH_DATA_SUCCESS_REPLICA_CONGESTED)
                        } else {
                          pushMergedDataCallback.onSuccess(StatusCode.SUCCESS)
                        }
                      }
                    case _ =>
                      if (replicaReason == StatusCode.PUSH_DATA_SUCCESS_REPLICA_CONGESTED.getValue) {
                        pushMergedDataCallback.onSuccess(
                          StatusCode.PUSH_DATA_SUCCESS_REPLICA_CONGESTED)
                      } else {
                        pushMergedDataCallback.onSuccess(StatusCode.SUCCESS)
                      }
                  }
                case Failure(e) => callbackWithTimer.onFailure(e)
              }
            }

            override def onFailure(e: Throwable): Unit = {
              logError(s"PushMergedData replicate failed for partitionLocation: $location", e)
              // 1. Throw PUSH_DATA_WRITE_FAIL_REPLICA by replica peer worker
              // 2. Throw PUSH_DATA_TIMEOUT_REPLICA by TransportResponseHandler
              // 3. Throw IOException by channel, convert to PUSH_DATA_CONNECTION_EXCEPTION_REPLICA
              if (e.getMessage.startsWith(StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA.name())) {
                workerSource.incCounter(WorkerSource.REPLICATE_DATA_WRITE_FAIL_COUNT)
                pushMergedDataCallback.onFailure(e)
              } else if (e.getMessage.startsWith(StatusCode.PUSH_DATA_TIMEOUT_REPLICA.name())) {
                workerSource.incCounter(WorkerSource.REPLICATE_DATA_TIMEOUT_COUNT)
                pushMergedDataCallback.onFailure(e)
              } else if (ExceptionUtils.connectFail(e.getMessage)) {
                workerSource.incCounter(WorkerSource.REPLICATE_DATA_CONNECTION_EXCEPTION_COUNT)
                pushMergedDataCallback.onFailure(
                  new CelebornIOException(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_REPLICA))
              } else {
                workerSource.incCounter(WorkerSource.REPLICATE_DATA_FAIL_NON_CRITICAL_CAUSE_COUNT)
                pushMergedDataCallback.onFailure(
                  new CelebornIOException(StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_REPLICA))
              }
            }
          }

          try {
            val client = getReplicateClient(peer.getHost, peer.getReplicatePort, location.getId)
            val newPushMergedData = new PushMergedData(
              PartitionLocation.Mode.REPLICA.mode(),
              shuffleKey,
              pushMergedData.partitionUniqueIds,
              batchOffsets,
              pushMergedData.body)
            client.pushMergedData(
              newPushMergedData,
              shufflePushDataTimeout.get(shuffleKey),
              wrappedCallback)
          } catch {
            case e: Exception =>
              pushMergedData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              workerSource.incCounter(WorkerSource.REPLICATE_DATA_CREATE_CONNECTION_FAIL_COUNT)
              logError(
                s"PushMergedData replication failed during connecting peer for partitionLocation: $location",
                e)
              pushMergedDataCallback.onFailure(
                new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA))
          }
        }
      })
      writeLocalData(
        fileWriters,
        body,
        shuffleKey,
        isPrimary,
        Some(batchOffsets),
        writePromise,
        hardSplitIndexes)
    } else {
      // The codes here could be executed if
      // 1. the client doesn't enable push data to the replica, the primary worker could hit here
      // 2. the client enables push data to the replica, and the replica worker could hit here
      writeLocalData(
        fileWriters,
        body,
        shuffleKey,
        isPrimary,
        Some(batchOffsets),
        writePromise,
        hardSplitIndexes)
      Try(Await.result(writePromise.future, Duration.Inf)) match {
        case Success(result) =>
          var index = 0
          while (index < result.length) {
            if (result(index) == StatusCode.HARD_SPLIT) {
              pushMergedDataCallback.addSplitPartition(index, result(index))
            }
            index += 1
          }
          Option(CongestionController.instance()) match {
            case Some(congestionController) if fileWriters.nonEmpty =>
              if (congestionController.isUserCongested(
                  fileWriters.head.getUserCongestionControlContext)) {
                if (isPrimary) {
                  pushMergedDataCallback.onSuccess(StatusCode.PUSH_DATA_SUCCESS_PRIMARY_CONGESTED)
                } else {
                  pushMergedDataCallback.onSuccess(StatusCode.PUSH_DATA_SUCCESS_REPLICA_CONGESTED)
                }
              } else {
                pushMergedDataCallback.onSuccess(StatusCode.SUCCESS)
              }
            case _ =>
              pushMergedDataCallback.onSuccess(StatusCode.SUCCESS)
          }
        case Failure(e) => pushMergedDataCallback.onFailure(e)
      }
    }
  }

  /**
   * returns an array of FileWriters from partition locations along with an optional index for any FileWriter that
   * encountered an exception.
   */
  private def getFileWriters(
      partitionIdToLocations: Array[(String, PartitionLocation)])
      : (Array[PartitionDataWriter], Option[Int]) = {
    val fileWriters = new Array[PartitionDataWriter](partitionIdToLocations.length)
    var i = 0
    var exceptionFileWriterIndex: Option[Int] = None
    while (i < partitionIdToLocations.length) {
      val (_, workingPartition) = partitionIdToLocations(i)
      if (workingPartition != null) {
        val fileWriter = workingPartition.asInstanceOf[WorkingPartition].getFileWriter
        if (fileWriter.getException != null) {
          exceptionFileWriterIndex = Some(i)
        }
        fileWriters(i) = fileWriter
      } else {
        fileWriters(i) = null
      }
      i += 1
    }
    (fileWriters, exceptionFileWriterIndex)
  }

  private def getMapAttempt(body: ByteBuf): (Int, Int) = {
    // header: mapId attemptId batchId compressedTotalSize
    val header = new Array[Byte](8)
    body.getBytes(body.readerIndex(), header)
    val mapId = PushDataHeaderUtils.getMapId(header)
    val attemptId = PushDataHeaderUtils.getAttemptId(header)
    (mapId, attemptId)
  }

  override def checkRegistered(): Boolean = registered.exists(_.get)

  class RpcResponseCallbackWithTimer(
      source: Source,
      metricName: String,
      key: String,
      callback: RpcResponseCallback)
    extends RpcResponseCallback {
    source.startTimer(metricName, key)

    override def onSuccess(response: ByteBuffer): Unit = {
      callback.onSuccess(response)
      source.stopTimer(metricName, key)
    }

    override def onFailure(e: Throwable): Unit = {
      callback.onFailure(e)
      source.stopTimer(metricName, key)
    }
  }

  class SimpleRpcResponseCallback(
      client: TransportClient,
      requestId: Long,
      shuffleKey: String)
    extends RpcResponseCallback {
    override def onSuccess(response: ByteBuffer): Unit = {
      client.getChannel.writeAndFlush(new RpcResponse(
        requestId,
        new NioManagedBuffer(response)))
    }

    override def onFailure(e: Throwable): Unit = {
      client.getChannel.writeAndFlush(new RpcFailure(requestId, e.getMessage))
    }
  }

  class PushMergedDataCallback(callback: RpcResponseCallback) {
    private val splitPartitionStatuses = new mutable.HashMap[Int, Byte]()

    def addSplitPartition(index: Int, statusCode: StatusCode): Unit = {
      splitPartitionStatuses.put(index, statusCode.getValue)
    }

    def isHardSplitPartition(index: Int): Boolean = {
      splitPartitionStatuses.getOrElse(index, -1) == StatusCode.HARD_SPLIT.getValue
    }

    def unionReplicaSplitPartitions(
        replicaPartitionIndexes: util.List[Integer],
        replicaStatusCodes: util.List[Integer]): Unit = {
      if (replicaPartitionIndexes.size() != replicaStatusCodes.size()) {
        throw new IllegalArgumentException(
          "replicaPartitionIndexes and replicaStatusCodes must have the same size")
      }
      for (i <- 0 until replicaPartitionIndexes.size()) {
        val index = replicaPartitionIndexes.get(i)
        // The priority of HARD_SPLIT is higher than that of SOFT_SPLIT.
        if (!isHardSplitPartition(index)) {
          splitPartitionStatuses.put(index, replicaStatusCodes.get(i).byteValue())
        }
      }
    }

    /**
     * Returns the ordered indexes of partitions that are not writable.
     * A partition is considered not writable if it is marked as HARD_SPLIT or failed.
     */
    def getHardSplitIndexes: Array[Int] = {
      splitPartitionStatuses.collect {
        case (partitionIndex, statusCode) if statusCode == StatusCode.HARD_SPLIT.getValue =>
          partitionIndex
      }.toSeq.sorted.toArray
    }

    def onSuccess(status: StatusCode): Unit = {
      val splitPartitionIndexes = new util.ArrayList[Integer]()
      val statusCodes = new util.ArrayList[Integer]()
      splitPartitionStatuses.foreach {
        case (partitionIndex, statusCode) =>
          splitPartitionIndexes.add(partitionIndex)
          statusCodes.add(statusCode)
      }
      if (splitPartitionStatuses.isEmpty || status == StatusCode.MAP_ENDED) {
        callback.onSuccess(
          ByteBuffer.wrap(Array[Byte](status.getValue)))
      } else {
        val pushMergedDataInfo = PbPushMergedDataSplitPartitionInfo.newBuilder()
          .addAllSplitPartitionIndexes(splitPartitionIndexes)
          .addAllStatusCodes(statusCodes)
          .build()
        val pushMergedDataInfoByteBuffer = Utils.toTransportMessage(pushMergedDataInfo)
          .asInstanceOf[TransportMessage]
          .toByteBuffer
        val response = ByteBuffer.allocate(1 + pushMergedDataInfoByteBuffer.remaining())
        response.put(StatusCode.HARD_SPLIT.getValue)
        response.put(pushMergedDataInfoByteBuffer)
        response.flip()
        callback.onSuccess(response)
      }
    }

    def onFailure(exception: Throwable): Unit = {
      callback.onFailure(exception)
    }
  }

  private def handleCore(
      client: TransportClient,
      message: RequestMessage,
      requestId: Long,
      shuffleKey: String,
      handler: () => Unit,
      callback: RpcResponseCallback): Unit = {
    checkAuth(client, Utils.splitShuffleKey(shuffleKey)._1)
    try {
      handler()
    } catch {
      case e: Exception =>
        logError(s"Error while handle${message.`type`()} $message", e)
        callback.onFailure(e)
    }
  }

  def handleMapPartitionPushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isPrimary = mode == PartitionLocation.Mode.PRIMARY

    val key = s"${pushData.requestId}"
    if (isPrimary) {
      workerSource.startTimer(WorkerSource.PRIMARY_PUSH_DATA_TIME, key)
    } else {
      workerSource.startTimer(WorkerSource.REPLICA_PUSH_DATA_TIME, key)
    }

    // find FileWriter responsible for the data
    val location =
      if (isPrimary) {
        partitionLocationInfo.getPrimaryLocation(shuffleKey, pushData.partitionUniqueId)
      } else {
        partitionLocationInfo.getReplicaLocation(shuffleKey, pushData.partitionUniqueId)
      }

    val wrappedCallback =
      new WrappedRpcResponseCallback(
        pushData.`type`(),
        isPrimary,
        pushData.requestId,
        null,
        location,
        if (isPrimary) WorkerSource.PRIMARY_PUSH_DATA_TIME else WorkerSource.REPLICA_PUSH_DATA_TIME,
        callback)

    if (locationIsNull(
        pushData.`type`(),
        shuffleKey,
        pushData.partitionUniqueId,
        location,
        callback)) return

    val fileWriter =
      getFileWriterAndCheck(pushData.`type`(), location, isPrimary, callback) match {
        case (true, _) => return
        case (false, f: PartitionDataWriter) => f
      }

    // for mappartition we will not check whether disk full or split partition

    fileWriter.incrementPendingWrites()

    if (fileWriter.isClosed) {
      val fileInfo = fileWriter.getCurrentFileInfo
      logWarning(
        s"[handleMapPartitionPushData] FileWriter is already closed! File path ${fileInfo.getFilePath} " +
          s"length ${fileInfo.getFileLength}")
      callback.onFailure(new CelebornIOException("File already closed!"))
      fileWriter.decrementPendingWrites()
      return
    }
    val writePromise = Promise[Array[StatusCode]]()
    writeLocalData(Seq(fileWriter), body, shuffleKey, isPrimary, None, writePromise)
    // for primary, send data to replica
    if (location.hasPeer && isPrimary) {
      // to do
      Try(Await.result(writePromise.future, Duration.Inf)) match {
        case Success(result) =>
          if (result(0) != StatusCode.SUCCESS) {
            wrappedCallback.onFailure(new CelebornIOException("Write data failed!"))
          } else {
            wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
          }
        case Failure(e) => wrappedCallback.onFailure(e)
      }
    } else {
      Try(Await.result(writePromise.future, Duration.Inf)) match {
        case Success(result) =>
          if (result(0) != StatusCode.SUCCESS) {
            wrappedCallback.onFailure(new CelebornIOException("Write data failed!"))
          } else {
            wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
          }
        case Failure(e) => wrappedCallback.onFailure(e)
      }
    }
  }

  private def handleRpcRequest(
      client: TransportClient,
      rpcRequest: RpcRequest,
      callback: RpcResponseCallback): Unit = {
    val requestId = rpcRequest.requestId
    val (pbMsg, msg, isLegacy, messageType, mode, shuffleKey, partitionUniqueId, checkSplit) =
      mapPartitionRpcRequest(rpcRequest)
    workerSource.recordAppActiveConnection(client, shuffleKey)
    handleCore(
      client,
      rpcRequest,
      requestId,
      shuffleKey,
      () =>
        handleMapPartitionRpcRequestCore(
          requestId,
          pbMsg,
          msg,
          isLegacy,
          messageType,
          mode,
          shuffleKey,
          partitionUniqueId,
          checkSplit,
          new SimpleRpcResponseCallback(
            client,
            requestId,
            shuffleKey)),
      callback)
  }

  private def mapPartitionRpcRequest(rpcRequest: RpcRequest)
      : (GeneratedMessageV3, Message, Boolean, Type, Mode, String, String, Boolean) = {
    try {
      val msg = TransportMessage.fromByteBuffer(
        rpcRequest.body().nioByteBuffer()).getParsedPayload.asInstanceOf[GeneratedMessageV3]
      msg match {
        case p: PbPushDataHandShake =>
          (
            msg,
            null,
            false,
            Type.PUSH_DATA_HAND_SHAKE,
            p.getMode,
            p.getShuffleKey,
            p.getPartitionUniqueId,
            true)
        case rs: PbRegionStart =>
          (
            msg,
            null,
            false,
            Type.REGION_START,
            rs.getMode,
            rs.getShuffleKey,
            rs.getPartitionUniqueId,
            true)
        case rf: PbRegionFinish =>
          (
            msg,
            null,
            false,
            Type.REGION_FINISH,
            rf.getMode,
            rf.getShuffleKey,
            rf.getPartitionUniqueId,
            false)
        case ss: PbSegmentStart =>
          (
            msg,
            null,
            false,
            Type.SEGMENT_START,
            ss.getMode,
            ss.getShuffleKey,
            ss.getPartitionUniqueId,
            false)
      }
    } catch {
      case _: Exception =>
        val msg = Message.decode(rpcRequest.body().nioByteBuffer())
        msg match {
          case p: PushDataHandShake =>
            (
              null,
              msg,
              true,
              Type.PUSH_DATA_HAND_SHAKE,
              Mode.forNumber(p.mode),
              p.shuffleKey,
              p.partitionUniqueId,
              true)
          case rs: RegionStart =>
            (
              null,
              msg,
              true,
              Type.REGION_START,
              Mode.forNumber(rs.mode),
              rs.shuffleKey,
              rs.partitionUniqueId,
              true)
          case rf: RegionFinish =>
            (
              null,
              msg,
              true,
              Type.REGION_FINISH,
              Mode.forNumber(rf.mode),
              rf.shuffleKey,
              rf.partitionUniqueId,
              false)
        }
    }
  }

  private def handleMapPartitionRpcRequestCore(
      requestId: Long,
      pbMsg: GeneratedMessageV3,
      msg: Message,
      isLegacy: Boolean,
      messageType: Message.Type,
      mode: Mode,
      shuffleKey: String,
      partitionUniqueId: String,
      checkSplit: Boolean,
      callback: RpcResponseCallback): Unit = {
    log.debug(
      s"requestId:$requestId, pushdata rpc:$messageType, mode:$mode, shuffleKey:$shuffleKey, " +
        s"partitionUniqueId:$partitionUniqueId")
    val isPrimary = mode == Mode.Primary
    val (workerSourcePrimary, workerSourceReplica) =
      messageType match {
        case Type.PUSH_DATA_HAND_SHAKE =>
          (
            WorkerSource.PRIMARY_PUSH_DATA_HANDSHAKE_TIME,
            WorkerSource.REPLICA_PUSH_DATA_HANDSHAKE_TIME)
        case Type.REGION_START =>
          (WorkerSource.PRIMARY_REGION_START_TIME, WorkerSource.REPLICA_REGION_START_TIME)
        case Type.REGION_FINISH =>
          (WorkerSource.PRIMARY_REGION_FINISH_TIME, WorkerSource.REPLICA_REGION_FINISH_TIME)
        case Type.SEGMENT_START =>
          (WorkerSource.PRIMARY_SEGMENT_START_TIME, WorkerSource.REPLICA_SEGMENT_START_TIME)
        case _ => throw new IllegalArgumentException(s"Not support $messageType yet")
      }

    val location =
      if (isPrimary) {
        partitionLocationInfo.getPrimaryLocation(shuffleKey, partitionUniqueId)
      } else {
        partitionLocationInfo.getReplicaLocation(shuffleKey, partitionUniqueId)
      }
    workerSource.startTimer(
      if (isPrimary) workerSourcePrimary else workerSourceReplica,
      s"$requestId")
    val wrappedCallback =
      new WrappedRpcResponseCallback(
        messageType,
        isPrimary,
        requestId,
        null,
        location,
        if (isPrimary) workerSourcePrimary else workerSourceReplica,
        callback)

    if (locationIsNull(
        messageType,
        shuffleKey,
        partitionUniqueId,
        location,
        callback)) return

    val fileWriter =
      getFileWriterAndCheck(messageType, location, isPrimary, callback) match {
        case (true, _) => return
        case (false, f: PartitionDataWriter) => f
      }

    // During worker shutdown, worker will return HARD_SPLIT for all existed partition.
    // This should before return exception to make current push request revive and retry.
    val isPartitionSplitEnabled = fileWriter.getCurrentFileInfo.isPartitionSplitEnabled

    if (shutdown.get() && (messageType == Type.REGION_START || messageType ==
        Type.PUSH_DATA_HAND_SHAKE) && isPartitionSplitEnabled) {
      logInfo(s"$messageType return HARD_SPLIT for shuffle $shuffleKey since worker shutdown.")
      callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
      return
    }

    if (checkSplit && (messageType == Type.REGION_START || messageType ==
        Type.PUSH_DATA_HAND_SHAKE) && isPartitionSplitEnabled && checkDiskFullAndSplit(
        fileWriter,
        isPrimary) == StatusCode.HARD_SPLIT) {
      workerSource.incCounter(WorkerSource.WRITE_DATA_HARD_SPLIT_COUNT)
      callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
      return
    }

    try {
      messageType match {
        case Type.PUSH_DATA_HAND_SHAKE =>
          val (numPartitions, bufferSize) =
            if (isLegacy)
              (
                msg.asInstanceOf[PushDataHandShake].numPartitions,
                msg.asInstanceOf[PushDataHandShake].bufferSize)
            else
              (
                pbMsg.asInstanceOf[PbPushDataHandShake].getNumPartitions,
                pbMsg.asInstanceOf[PbPushDataHandShake].getBufferSize)
          fileWriter.asInstanceOf[MapPartitionDataWriter].pushDataHandShake(
            numPartitions,
            bufferSize)
        case Type.REGION_START =>
          val (currentRegionIndex, isBroadcast: Boolean) =
            if (isLegacy)
              (
                msg.asInstanceOf[RegionStart].currentRegionIndex,
                msg.asInstanceOf[RegionStart].isBroadcast)
            else
              (
                pbMsg.asInstanceOf[PbRegionStart].getCurrentRegionIndex,
                pbMsg.asInstanceOf[PbRegionStart].getIsBroadcast)
          fileWriter.asInstanceOf[MapPartitionDataWriter].regionStart(
            currentRegionIndex,
            isBroadcast)
        case Type.REGION_FINISH =>
          fileWriter.asInstanceOf[MapPartitionDataWriter].regionFinish()
        case Type.SEGMENT_START =>
          val (subPartitionId, segmentId) =
            (
              pbMsg.asInstanceOf[PbSegmentStart].getSubPartitionId,
              pbMsg.asInstanceOf[PbSegmentStart].getSegmentId)
          fileWriter.asInstanceOf[SegmentMapPartitionFileWriter].segmentStart(
            subPartitionId,
            segmentId)
        case _ => throw new IllegalArgumentException(s"Not support $messageType yet")
      }
      // for primary , send data to replica
      if (location.hasPeer && isPrimary) {
        // TODO replica
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
      } else {
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
      }
    } catch {
      case t: Throwable =>
        callback.onFailure(new CelebornIOException(s"$messageType failed", t))
    }
  }

  class WrappedRpcResponseCallback(
      messageType: Message.Type,
      isPrimary: Boolean,
      requestId: Long,
      softSplit: AtomicBoolean,
      location: PartitionLocation,
      workerSourceTime: String,
      callback: RpcResponseCallback)
    extends RpcResponseCallback {
    override def onSuccess(response: ByteBuffer): Unit = {
      workerSource.stopTimer(workerSourceTime, s"$requestId")
      if (isPrimary) {
        if (response.remaining() > 0) {
          val resp = ByteBuffer.allocate(response.remaining())
          resp.put(response)
          resp.flip()
          callback.onSuccess(resp)
        } else if (softSplit != null && softSplit.get()) {
          callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.SOFT_SPLIT.getValue)))
        } else {
          callback.onSuccess(response)
        }
      } else {
        callback.onSuccess(response)
      }
    }

    override def onFailure(e: Throwable): Unit = {
      workerSource.stopTimer(workerSourceTime, s"$requestId")
      if (location != null) {
        logError(s"[handle$messageType.onFailure] partitionLocation: $location")
      }
      messageType match {
        case Type.PUSH_DATA_HAND_SHAKE =>
          workerSource.incCounter(WorkerSource.PUSH_DATA_HANDSHAKE_FAIL_COUNT)
          callback.onFailure(new CelebornIOException(
            StatusCode.PUSH_DATA_HANDSHAKE_FAIL_REPLICA,
            e))
        case Type.REGION_START =>
          workerSource.incCounter(WorkerSource.REGION_START_FAIL_COUNT)
          callback.onFailure(new CelebornIOException(StatusCode.REGION_START_FAIL_REPLICA, e))
        case Type.REGION_FINISH =>
          workerSource.incCounter(WorkerSource.REGION_FINISH_FAIL_COUNT)
          callback.onFailure(new CelebornIOException(StatusCode.REGION_FINISH_FAIL_REPLICA, e))
        case Type.SEGMENT_START =>
          workerSource.incCounter(WorkerSource.SEGMENT_START_FAIL_COUNT)
          callback.onFailure(new CelebornIOException(StatusCode.SEGMENT_START_FAIL_REPLICA, e))
        case _ =>
          workerSource.incCounter(WorkerSource.REPLICATE_DATA_FAIL_COUNT)
          if (e.isInstanceOf[CelebornIOException]) {
            callback.onFailure(e)
          } else {
            callback.onFailure(new CelebornIOException(StatusCode.REPLICATE_DATA_FAILED, e))
          }
      }
    }
  }

  private def locationIsNull(
      messageType: Message.Type,
      shuffleKey: String,
      partitionUniqueId: String,
      location: PartitionLocation,
      callback: RpcResponseCallback): Boolean = {
    if (location == null) {
      val msg =
        s"Partition location wasn't found for task(shuffle $shuffleKey, uniqueId $partitionUniqueId)."
      logWarning(s"[handle$messageType] $msg")
      messageType match {
        case Type.PUSH_MERGED_DATA => callback.onFailure(new CelebornIOException(msg))
        case _ => callback.onFailure(
            new CelebornIOException(StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND))
      }
      return true
    }
    false
  }

  private def checkFileWriterException(
      messageType: Message.Type,
      isPrimary: Boolean,
      fileWriter: PartitionDataWriter,
      callback: RpcResponseCallback): Unit = {
    logWarning(
      s"[handle$messageType] fileWriter $fileWriter has Exception ${fileWriter.getException}")

    val (messagePrimary, messageReplica) =
      messageType match {
        case Type.PUSH_DATA =>
          (
            StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY,
            StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA)
        case Type.PUSH_DATA_HAND_SHAKE => (
            StatusCode.PUSH_DATA_HANDSHAKE_FAIL_PRIMARY,
            StatusCode.PUSH_DATA_HANDSHAKE_FAIL_REPLICA)
        case Type.REGION_START => (
            StatusCode.REGION_START_FAIL_PRIMARY,
            StatusCode.REGION_START_FAIL_REPLICA)
        case Type.REGION_FINISH => (
            StatusCode.REGION_FINISH_FAIL_PRIMARY,
            StatusCode.REGION_FINISH_FAIL_REPLICA)
        case Type.SEGMENT_START => (
            StatusCode.SEGMENT_START_FAIL_PRIMARY,
            StatusCode.SEGMENT_START_FAIL_REPLICA)
        case _ => throw new IllegalArgumentException(s"Not support $messageType yet")
      }
    callback.onFailure(new CelebornIOException(
      if (isPrimary) messagePrimary else messageReplica,
      fileWriter.getException))
  }

  private def getFileWriterAndCheck(
      messageType: Message.Type,
      location: PartitionLocation,
      isPrimary: Boolean,
      callback: RpcResponseCallback): (Boolean, PartitionDataWriter) = {
    val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
    val exception = fileWriter.getException
    if (exception != null) {
      checkFileWriterException(messageType, isPrimary, fileWriter, callback)
      return (true, fileWriter)
    }
    (false, fileWriter)
  }

  private def checkDiskFull(fileWriter: PartitionDataWriter): Boolean = {
    if (fileWriter.flusher == null || fileWriter.flusher.isInstanceOf[
        HdfsFlusher] || fileWriter.flusher.isInstanceOf[S3Flusher]) {
      return false
    }
    val mountPoint = fileWriter.flusher.asInstanceOf[LocalFlusher].mountPoint
    val diskInfo = workerInfo.diskInfos.get(mountPoint)
    val diskFull =
      diskInfo.status.equals(DiskStatus.HIGH_DISK_USAGE) || diskInfo.actualUsableSpace <= 0
    diskFull
  }

  private def checkDiskFullAndSplit(
      fileWriter: PartitionDataWriter,
      isPrimary: Boolean): StatusCode = {
    if (fileWriter.needHardSplitForMemoryShuffleStorage()) {
      logInfo(
        s"Do hardSplit for memory shuffle file fileLength:${fileWriter.getMemoryFileInfo.getFileLength}")
      return StatusCode.HARD_SPLIT
    }
    val diskFull = checkDiskFull(fileWriter)
    logTrace(
      s"""
         |CheckDiskFullAndSplit in
         |diskFull:$diskFull,
         |partitionSplitMinimumSize:$partitionSplitMinimumSize,
         |splitThreshold:${fileWriter.getSplitThreshold},
         |fileLength:${fileWriter.getCurrentFileInfo.getFileLength}
         |fileName:${fileWriter.getCurrentFileInfo.getFilePath}
         |""".stripMargin)
    val diskFileInfo = fileWriter.getDiskFileInfo
    if (diskFileInfo != null) {
      if (workerPartitionSplitEnabled && ((diskFull && diskFileInfo.getFileLength > partitionSplitMinimumSize) ||
          (isPrimary && diskFileInfo.getFileLength > fileWriter.getSplitThreshold))) {
        if (fileWriter.getSplitMode == PartitionSplitMode.SOFT &&
          (fileWriter.getDiskFileInfo.getFileLength < partitionSplitMaximumSize)) {
          return StatusCode.SOFT_SPLIT
        } else {
          logInfo(
            s"""
               |CheckDiskFullAndSplit hardSplit
               |diskFull:$diskFull,
               |partitionSplitMinimumSize:$partitionSplitMinimumSize,
               |splitThreshold:${fileWriter.getSplitThreshold},
               |fileLength:${diskFileInfo.getFileLength},
               |fileName:${diskFileInfo.getFilePath}
               |""".stripMargin)
          return StatusCode.HARD_SPLIT
        }
      }
    }
    StatusCode.NO_SPLIT
  }

  private def getReplicateClient(host: String, port: Int, partitionId: Int): TransportClient = {
    if (workerReplicateRandomConnectionEnabled) {
      replicateClientFactory.createClient(host, port)
    } else {
      replicateClientFactory.createClient(host, port, partitionId)
    }
  }

  private def writeLocalData(
      fileWriters: Seq[PartitionDataWriter],
      body: ByteBuf,
      shuffleKey: String,
      isPrimary: Boolean,
      batchOffsets: Option[Array[Int]],
      writePromise: Promise[Array[StatusCode]],
      hardSplitIndexes: Array[Int] = Array.empty[Int]): Unit = {
    val length = fileWriters.length
    val result = new Array[StatusCode](length)

    def verifyDataChecksum(body: ByteBuf): Boolean = {
      val header = new Array[Byte](PushDataHeaderUtils.BATCH_HEADER_SIZE_WITHOUT_CHECKSUM)
      body.getBytes(body.readerIndex(), header)
      if (PushDataHeaderUtils.hasChecksumFlag(header)) {
        val checksumBytes = new Array[Byte](4)
        body.getBytes(
          body.readerIndex() + PushDataHeaderUtils.BATCH_HEADER_SIZE_WITHOUT_CHECKSUM,
          checksumBytes)
        val expectedChecksum = Platform.getInt(checksumBytes, Platform.BYTE_ARRAY_OFFSET)
        val currentChecksum = PushDataHeaderUtils.computeHeaderChecksum32(header)
        expectedChecksum == currentChecksum
      } else {
        logDebug(s"Checksum flag is not set, skip checksum verification for $shuffleKey")
        true
      }
    }

    def writeData(
        fileWriter: PartitionDataWriter,
        body: ByteBuf,
        shuffleKey: String,
        index: Int): Unit = {
      try {
        if (workerChecksumVerifyEnabled) {
          if (!verifyDataChecksum(body)) {
            throw new CelebornChecksumException(StatusCode.PUSH_DATA_CHECKSUM_FAIL)
          }
        }
        fileWriter.write(body)
        result(index) = StatusCode.SUCCESS
      } catch {
        case e: Throwable =>
          val (mapId, attemptId) = getMapAttempt(body)
          if (e.isInstanceOf[AlreadyClosedException]) {
            val endedAttempt =
              if (shuffleMapperAttempts.containsKey(shuffleKey)) {
                shuffleMapperAttempts.get(shuffleKey).get(mapId)
              } else -1
            // TODO just info log for ended attempt
            logWarning(s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
              s" $attemptId), caused by AlreadyClosedException, endedAttempt $endedAttempt, error message: ${e.getMessage}")
            workerSource.incCounter(WorkerSource.WRITE_DATA_HARD_SPLIT_COUNT)
            result(index) = StatusCode.HARD_SPLIT
          } else if (e.isInstanceOf[CelebornChecksumException]) {
            logError(
              s"Checksum verification failed for task(shuffle $shuffleKey, map $mapId, attempt $attemptId")
            workerSource.incCounter(WorkerSource.CHECKSUM_VERIFICATION_FAIL_COUNT)
            writePromise.failure(e)
          } else {
            logError("Exception encountered when write.", e)
            workerSource.incCounter(WorkerSource.WRITE_DATA_FAIL_COUNT)
            val cause =
              if (isPrimary) {
                StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY
              } else {
                StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA
              }
            writePromise.failure(new CelebornIOException(cause))
          }
          fileWriter.decrementPendingWrites()
      }
    }
    batchOffsets match {
      case Some(batchOffsets) =>
        var index = 0
        val hardSplitIterator = hardSplitIndexes.iterator
        var currentHardSplitIndex = nextValueOrElse(hardSplitIterator, -1)
        var fileWriter: PartitionDataWriter = null
        while (index < fileWriters.length) {
          if (index == currentHardSplitIndex) {
            currentHardSplitIndex = nextValueOrElse(hardSplitIterator, -1)
          } else {
            fileWriter = fileWriters(index)
            if (!writePromise.isCompleted) {
              val offset = body.readerIndex() + batchOffsets(index)
              val length =
                if (index == fileWriters.length - 1) {
                  body.readableBytes() - batchOffsets(index)
                } else {
                  batchOffsets(index + 1) - batchOffsets(index)
                }
              val batchBody = body.slice(offset, length)
              writeData(fileWriter, batchBody, shuffleKey, index)
            } else {
              fileWriter.decrementPendingWrites()
            }
          }
          index += 1
        }
      case _ =>
        writeData(fileWriters.head, body, shuffleKey, 0)
    }
    if (!writePromise.isCompleted) {
      workerSource.incCounter(WorkerSource.WRITE_DATA_SUCCESS_COUNT)
      writePromise.success(result)
    }
  }

  private def nextValueOrElse(iterator: Iterator[Int], defaultValue: Int): Int = {
    if (iterator.hasNext) {
      iterator.next()
    } else {
      defaultValue
    }
  }

  /**
   * Invoked when the channel associated with the given client is active.
   */
  override def channelActive(client: TransportClient): Unit = {
    workerSource.connectionActive(client)
    super.channelActive(client)
  }

  /**
   * Invoked when the channel associated with the given client is inactive.
   * No further requests will come from this client.
   */
  override def channelInactive(client: TransportClient): Unit = {
    workerSource.connectionInactive(client)
    super.channelInactive(client)
  }
}

object PushDataHandler {
  // for testing
  @volatile private[celeborn] var pushPrimaryDataTimeoutTested = new AtomicBoolean(false)
  @volatile private[celeborn] var pushReplicaDataTimeoutTested = new AtomicBoolean(false)
  @volatile private[celeborn] var pushPrimaryMergeDataTimeoutTested = new AtomicBoolean(false)
  @volatile private[celeborn] var pushReplicaMergeDataTimeoutTested = new AtomicBoolean(false)
}
