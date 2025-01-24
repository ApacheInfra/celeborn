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

import java.io.{FileNotFoundException, IOException}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter

import com.google.common.base.Throwables
import com.google.protobuf.GeneratedMessageV3
import io.netty.util.concurrent.{Future, GenericFutureListener}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.MAX_CHUNKS_BEING_TRANSFERRED
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskFileInfo, FileInfo, MapFileMeta, MemoryFileInfo, ReduceFileMeta}
import org.apache.celeborn.common.network.buffer.{FileChunkBuffers, MemoryChunkBuffers, NioManagedBuffer}
import org.apache.celeborn.common.network.client.{RpcResponseCallback, TransportClient}
import org.apache.celeborn.common.network.protocol._
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.network.util.{NettyUtils, TransportConf}
import org.apache.celeborn.common.protocol.{MessageType, PbBufferStreamEnd, PbChunkFetchRequest, PbNotifyRequiredSegment, PbOpenStream, PbOpenStreamList, PbOpenStreamListResponse, PbReadAddCredit, PbStreamHandler, PbStreamHandlerOpt, StreamType}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{ExceptionUtils, JavaUtils, Utils}
import org.apache.celeborn.service.deploy.worker.storage.{ChunkStreamManager, CreditStreamManager, PartitionFilesSorter, StorageManager}

class FetchHandler(
    val conf: CelebornConf,
    val transportConf: TransportConf,
    val workerSource: WorkerSource)
  extends BaseMessageHandler with Logging {

  val chunkStreamManager = new ChunkStreamManager()
  val maxChunkBeingTransferred: Option[Long] = conf.shuffleIoMaxChunksBeingTransferred

  val creditStreamManager = new CreditStreamManager(
    conf.partitionReadBuffersMin,
    conf.partitionReadBuffersMax,
    conf.creditStreamThreadsPerMountpoint,
    conf.readBuffersToTriggerReadMin)
  var storageManager: StorageManager = _
  var partitionsSorter: PartitionFilesSorter = _
  var sortFileChecker: ScheduledExecutorService = _
  var registered: Option[AtomicBoolean] = None
  // shuffleKey -> (fileRequest -> SortFileInfo)
  var sortFilesInfos: ConcurrentHashMap[String, ConcurrentHashMap[String, SortFileInfo]] = _

  // batchId: shuffleKey-requestId -> (Index ->  PbStreamHandlerOpt)
  var pbOpenStreamListResponseMap
      : ConcurrentHashMap[String, ConcurrentHashMap[Int, PbStreamHandlerOpt]] = _
  // batchId: shuffleKey-requestId -> batchSortingCnt
  var batchSortingCnt: ConcurrentHashMap[String, Int] = _

  val workerSortFilesCheckInterval = conf.workerSortFilesCheckInterval

  def init(worker: Worker): Unit = {
    workerSource.addGauge(WorkerSource.ACTIVE_CHUNK_STREAM_COUNT) { () =>
      chunkStreamManager.getStreamsCount
    }

    workerSource.addGauge(WorkerSource.ACTIVE_CREDIT_STREAM_COUNT) { () =>
      creditStreamManager.getStreamsCount
    }

    workerSource.addGauge(WorkerSource.ACTIVE_MAP_PARTITION_COUNT) { () =>
      creditStreamManager.getActiveMapPartitionCount
    }

    this.storageManager = worker.storageManager
    this.partitionsSorter = worker.partitionsSorter
    this.registered = Some(worker.registered)

    this.sortFilesInfos = worker.sortFilesInfo
    this.pbOpenStreamListResponseMap = worker.pbOpenStreamListResponseMap
    this.batchSortingCnt = worker.batchSortingCnt
    this.sortFileChecker = worker.sortFileChecker
    this.sortFileChecker.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          getAndReplySortedFiles()
        }
      },
      0,
      workerSortFilesCheckInterval,
      TimeUnit.MILLISECONDS)

  }

  def getRawFileInfo(
      shuffleKey: String,
      fileName: String): FileInfo = {
    // find FileWriter responsible for the data
    val fileInfo = storageManager.getFileInfo(shuffleKey, fileName)
    if (fileInfo == null) {
      val errMsg = s"Could not find file $fileName for $shuffleKey."
      logWarning(errMsg)
      throw new FileNotFoundException(errMsg)
    }
    fileInfo
  }

  override def receive(
      client: TransportClient,
      msg: RequestMessage,
      callback: RpcResponseCallback): Unit = {
    handleRpcRequest(client, msg.asInstanceOf[RpcRequest], callback)
  }

  override def receive(client: TransportClient, msg: RequestMessage): Unit = {
    msg match {
      case r: BufferStreamEnd =>
        handleEndStreamFromClient(client, r.getStreamId)
      case r: ReadAddCredit =>
        handleReadAddCredit(client, r.getCredit, r.getStreamId)
      case r: ChunkFetchRequest =>
        handleChunkFetchRequest(client, r.streamChunkSlice, r)
      case unknown: RequestMessage =>
        throw new IllegalArgumentException(s"Unknown message type id: ${unknown.`type`.id}")
    }
  }

  private def handleRpcRequest(
      client: TransportClient,
      rpcRequest: RpcRequest,
      callback: RpcResponseCallback): Unit = {
    var message: GeneratedMessageV3 = null
    try {
      message = TransportMessage.fromByteBuffer(rpcRequest.body().nioByteBuffer())
        .getParsedPayload[GeneratedMessageV3]
    } catch {
      case exception: CelebornIOException =>
        logWarning("Handle request with legacy RPCs", exception)
        return handleLegacyRpcMessage(client, rpcRequest, callback)
    }
    message match {
      case openStream: PbOpenStream =>
        handleOpenStreamInternal(
          client,
          openStream.getShuffleKey,
          openStream.getFileName,
          openStream.getStartIndex,
          openStream.getEndIndex,
          openStream.getInitialCredit,
          rpcRequest.requestId,
          isLegacy = false,
          openStream.getReadLocalShuffle,
          callback)
      case openStreamList: PbOpenStreamList =>
        val shuffleKey = openStreamList.getShuffleKey()
        val files = openStreamList.getFileNameList
        val startIndices = openStreamList.getStartIndexList
        val endIndices = openStreamList.getEndIndexList
        val readLocalFlags = openStreamList.getReadLocalShuffleList
        checkAuth(client, Utils.splitShuffleKey(shuffleKey)._1)

        val batchId = shuffleKey + "-" + client.getChannel.id() + "-" + rpcRequest.requestId
        pbOpenStreamListResponseMap.putIfAbsent(
          batchId,
          JavaUtils.newConcurrentHashMap[Int, PbStreamHandlerOpt]())
        batchSortingCnt.putIfAbsent(batchId, files.size)

        0 until files.size() foreach { idx =>
          handleReduceOpenStreamInternal(
            client,
            shuffleKey,
            files.get(idx),
            startIndices.get(idx),
            endIndices.get(idx),
            rpcRequest.requestId,
            isLegacy = false,
            null,
            idx,
            readLocalFlags.get(idx))
        }

      case bufferStreamEnd: PbBufferStreamEnd =>
        handleEndStreamFromClient(
          client,
          bufferStreamEnd.getStreamId,
          bufferStreamEnd.getStreamType)
      case readAddCredit: PbReadAddCredit =>
        handleReadAddCredit(client, readAddCredit.getCredit, readAddCredit.getStreamId)
      case notifyRequiredSegment: PbNotifyRequiredSegment =>
        handleNotifyRequiredSegment(
          client,
          notifyRequiredSegment.getRequiredSegmentId,
          notifyRequiredSegment.getStreamId,
          notifyRequiredSegment.getSubPartitionId)
      case chunkFetchRequest: PbChunkFetchRequest =>
        handleChunkFetchRequest(
          client,
          StreamChunkSlice.fromProto(chunkFetchRequest.getStreamChunkSlice),
          rpcRequest)
      case message: GeneratedMessageV3 =>
        logError(s"Unknown message $message")
    }
  }

  private def handleLegacyRpcMessage(
      client: TransportClient,
      rpcRequest: RpcRequest,
      callback: RpcResponseCallback): Unit = {
    try {
      val message = Message.decode(rpcRequest.body().nioByteBuffer())
      message.`type`() match {
        case Message.Type.OPEN_STREAM =>
          val openStream = message.asInstanceOf[OpenStream]
          handleOpenStreamInternal(
            client,
            new String(openStream.shuffleKey, StandardCharsets.UTF_8),
            new String(openStream.fileName, StandardCharsets.UTF_8),
            openStream.startMapIndex,
            openStream.endMapIndex,
            initialCredit = 0,
            rpcRequestId = rpcRequest.requestId,
            isLegacy = true,
            // legacy [[OpenStream]] doesn't support read local shuffle
            readLocalShuffle = false,
            callback)
        case Message.Type.OPEN_STREAM_WITH_CREDIT =>
          val openStreamWithCredit = message.asInstanceOf[OpenStreamWithCredit]
          handleOpenStreamInternal(
            client,
            new String(openStreamWithCredit.shuffleKey, StandardCharsets.UTF_8),
            new String(openStreamWithCredit.fileName, StandardCharsets.UTF_8),
            openStreamWithCredit.startIndex,
            openStreamWithCredit.endIndex,
            openStreamWithCredit.initialCredit,
            rpcRequestId = rpcRequest.requestId,
            isLegacy = true,
            readLocalShuffle = false,
            callback)
        case _ =>
          logError(s"Received an unknown message type id: ${message.`type`.id}")
      }
    } catch {
      case e: Exception =>
        logError("Catch an error when handle legacy rpc message.", e)
    }

  }

  private def handleOpenStreamInternal(
      client: TransportClient,
      shuffleKey: String,
      fileName: String,
      startIndex: Int,
      endIndex: Int,
      initialCredit: Int,
      rpcRequestId: Long,
      isLegacy: Boolean,
      readLocalShuffle: Boolean = false,
      callback: RpcResponseCallback): Unit = {
    checkAuth(client, Utils.splitShuffleKey(shuffleKey)._1)
    workerSource.recordAppActiveConnection(client, shuffleKey)
    // todo async timer might not be accurate
    workerSource.startTimer(WorkerSource.OPEN_STREAM_TIME, shuffleKey)
    val fileInfo = getRawFileInfo(shuffleKey, fileName)
    fileInfo.getFileMeta match {
      case _: ReduceFileMeta =>
        handleReduceOpenStreamInternal(
          client,
          shuffleKey,
          fileName,
          startIndex,
          endIndex,
          rpcRequestId,
          isLegacy,
          callback,
          -1,
          readLocalShuffle)
      case _: MapFileMeta =>
        val creditStreamHandler =
          new Consumer[java.lang.Long] {
            override def accept(streamId: java.lang.Long): Unit = {
              val pbStreamHandler = PbStreamHandler.newBuilder
                .setStreamId(streamId)
                .setNumChunks(0)
                .build()
              replyStreamHandler(client, rpcRequestId, pbStreamHandler, isLegacy)
            }
          }

        creditStreamManager.registerStream(
          creditStreamHandler,
          client.getChannel,
          shuffleKey,
          initialCredit,
          startIndex,
          endIndex,
          fileInfo.asInstanceOf[DiskFileInfo])
        workerSource.incCounter(WorkerSource.OPEN_STREAM_SUCCESS_COUNT)
        workerSource.stopTimer(WorkerSource.OPEN_STREAM_TIME, shuffleKey)
    }
  }

  private def handleReduceOpenStreamInternal(
      client: TransportClient,
      shuffleKey: String,
      fileName: String,
      startIndex: Int,
      endIndex: Int,
      rpcRequestId: Long,
      isLegacy: Boolean,
      callback: RpcResponseCallback,
      fileIndex: Int,
      readLocalShuffle: Boolean = false): Unit = {

    logDebug(s"Received open stream request shuffleKey-fileName-startIndex-endIndex:" +
      s" $shuffleKey-$fileName-$startIndex-$endIndex get file $fileName from client channel " +
      s"${NettyUtils.getRemoteAddress(client.getChannel)}")
    val fileInfo = getRawFileInfo(shuffleKey, fileName)
    val streamId = chunkStreamManager.nextStreamId()
    val fileId = shuffleKey + "-" + fileName
    // clientId + rpcRequestId is same in batch condition
    val fileRequest = client.getChannel.id() + "-" + rpcRequestId + "-" + fileId

    sortFilesInfos.putIfAbsent(shuffleKey, JavaUtils.newConcurrentHashMap[String, SortFileInfo]())
    val fileInfoMap = sortFilesInfos.get(shuffleKey)
    val startSortTIme = System.currentTimeMillis()

    fileInfo match {
      case memoryFileInfo: MemoryFileInfo =>
        fileInfoMap.putIfAbsent(
          fileRequest,
          new SortFileInfo(
            fileInfo,
            SortFileInfo.SORT_NOTSTARTED,
            client,
            fileName,
            startIndex,
            endIndex,
            readLocalShuffle,
            startSortTIme,
            rpcRequestId,
            isLegacy,
            callback,
            memoryFileInfo.getUserIdentifier,
            null,
            null,
            null,
            fileIndex))
      case diskFileInfo: DiskFileInfo =>
        val sortedFilePath = Utils.getSortedFilePath(diskFileInfo.getFilePath)
        val indexFilePath = Utils.getIndexFilePath(diskFileInfo.getFilePath)
        fileInfoMap.putIfAbsent(
          fileRequest,
          new SortFileInfo(
            fileInfo,
            SortFileInfo.SORT_NOTSTARTED,
            client,
            fileName,
            startIndex,
            endIndex,
            readLocalShuffle,
            startSortTIme,
            rpcRequestId,
            isLegacy,
            callback,
            diskFileInfo.getUserIdentifier,
            sortedFilePath,
            indexFilePath,
            null,
            fileIndex))
    }

    // we must get sorted fileInfo for the following cases.
    // 1. when the current request is a non-range openStream, but the original unsorted file
    //    has been deleted by another range's openStream request.
    // 2. when the current request is a range openStream request.
    if ((endIndex != Int.MaxValue) || (endIndex == Int.MaxValue
        && !fileInfo.addStream(streamId))) {
      if (fileInfo.isInstanceOf[MemoryFileInfo]) {
        val newFileInfo: FileInfo =
          partitionsSorter.sortAndGetMemoryFileInfo(fileInfo, startIndex, endIndex)
        val sortFileInfo = fileInfoMap.get(fileRequest)
        sortFileInfo.synchronized {
          sortFileInfo.fileInfo = newFileInfo
          sortFileInfo.status = SortFileInfo.SORT_FINISHED
        }
      } else {
        try {
          partitionsSorter.submitDiskFileSortingTask(shuffleKey, fileName, fileInfo)
          val sortFileInfo = fileInfoMap.get(fileRequest)
          sortFileInfo.synchronized {
            sortFileInfo.status = SortFileInfo.SORTING
          }
        } catch {
          case e: IOException =>
            logError(s"submit diskFile sorting task failed, error: ${e.getMessage}")
            val sortFileInfo = fileInfoMap.get(fileRequest)
            sortFileInfo.synchronized {
              sortFileInfo.status = SortFileInfo.SORT_FAILED
              sortFileInfo.error = e.getMessage
            }
        }
      }
    } else {
      // mark status as finished if there is no need to sort
      val sortFileInfo = fileInfoMap.get(fileRequest)
      sortFileInfo.synchronized {
        sortFileInfo.status = SortFileInfo.SORT_FINISHED
      }
    }
  }

  private def getAndReplySortedFiles(): Unit = {
    val currentTime = System.currentTimeMillis()
    val sortFileInfoIterator = sortFilesInfos.entrySet().iterator()
    while (sortFileInfoIterator.hasNext) {
      val sortFileInfoEntry = sortFileInfoIterator.next()
      val shuffleKey = sortFileInfoEntry.getKey
      val fileInfoMap = sortFileInfoEntry.getValue
      val fileInfoIterator = fileInfoMap.entrySet().iterator()

      while (fileInfoIterator.hasNext && sortFilesInfos.containsKey(shuffleKey)) {
        val fileInfoEntry = fileInfoIterator.next()
        val fileRequest = fileInfoEntry.getKey
        val sortFileInfo = fileInfoEntry.getValue

        try {
          sortFileInfo.synchronized {

            if (sortFileInfo.status == SortFileInfo.SORTING) {
              // memoryFileInfos have no status of sorting
              val sortedFileInfo = partitionsSorter.getSortedDiskFileInfo(
                shuffleKey,
                sortFileInfo.fileName,
                sortFileInfo.fileInfo,
                sortFileInfo.startIndex,
                sortFileInfo.endIndex)

              if (sortedFileInfo != null) {
                sortFileInfo.fileInfo = sortedFileInfo
                sortFileInfo.status = SortFileInfo.SORT_FINISHED
              } else {
                if (currentTime - sortFileInfo.startTime > conf.workerPartitionSorterSortPartitionTimeout) {
                  sortFileInfo.status = SortFileInfo.SORT_FAILED
                  sortFileInfo.error = s"Sort file $fileRequest timeout."
                }
              }
            }

            val batchId =
              shuffleKey + "-" + sortFileInfo.client.getChannel.id() + "-" + sortFileInfo.rpcRequestId
            if (sortFileInfo.status == SortFileInfo.SORT_FINISHED) {
              val pbStreamHandlerOpt = getPbStreamHandlerOpt(
                sortFileInfo.fileInfo,
                sortFileInfo.client,
                shuffleKey,
                sortFileInfo.fileName,
                sortFileInfo.startIndex,
                sortFileInfo.endIndex,
                sortFileInfo.readLocalShuffle)

              if (sortFileInfo.fileIndex == -1) {
                replyStreamHandler(
                  sortFileInfo.client,
                  sortFileInfo.rpcRequestId,
                  pbStreamHandlerOpt.getStreamHandler,
                  sortFileInfo.isLegacy)
              } else {
                val batchStreamHandlerOptMap = pbOpenStreamListResponseMap.get(batchId)
                batchStreamHandlerOptMap.put(sortFileInfo.fileIndex, pbStreamHandlerOpt)
                val sortingCnt = batchSortingCnt.get(batchId)
                batchSortingCnt.put(batchId, sortingCnt - 1)
              }
              fileInfoIterator.remove()
            } else if (sortFileInfo.status == SortFileInfo.SORT_FAILED) {
              val error = makeException(shuffleKey, sortFileInfo)
              if (sortFileInfo.fileIndex == -1) {
                handleRpcIOException(
                  sortFileInfo.client,
                  sortFileInfo.rpcRequestId,
                  shuffleKey,
                  sortFileInfo.fileName,
                  error,
                  sortFileInfo.callback)
              } else {
                val failedStreamHandlerOpt =
                  PbStreamHandlerOpt.newBuilder().setStatus(StatusCode.OPEN_STREAM_FAILED.getValue)
                    .setErrorMsg(error.getMessage).build()
                val batchStreamHandlerOptMap = pbOpenStreamListResponseMap.get(batchId)
                batchStreamHandlerOptMap.put(sortFileInfo.fileIndex, failedStreamHandlerOpt)
                val sortingCnt = batchSortingCnt.get(batchId)
                batchSortingCnt.put(batchId, sortingCnt - 1)
              }
              workerSource.incCounter(WorkerSource.OPEN_STREAM_FAIL_COUNT)
              fileInfoIterator.remove()
            }

            // return pbOpenStreamListResponse for openStreamListRequest
            if (sortFileInfo.fileIndex != -1 && batchSortingCnt.containsKey(
                batchId) && batchSortingCnt.get(batchId) <= 0) {
              val pbOpenStreamListResponse = PbOpenStreamListResponse.newBuilder()
              val batchStreamHandlerOptMap = pbOpenStreamListResponseMap.get(batchId)
              batchStreamHandlerOptMap.asScala.toList.sortBy(_._1).map {
                case (_, pbStreamHandlerOpt) =>
                  pbOpenStreamListResponse.addStreamHandlerOpt(pbStreamHandlerOpt)
              }
              sortFileInfo.client.getChannel.writeAndFlush(new RpcResponse(
                sortFileInfo.rpcRequestId,
                new NioManagedBuffer(new TransportMessage(
                  MessageType.BATCH_OPEN_STREAM_RESPONSE,
                  pbOpenStreamListResponse.build().toByteArray).toByteBuffer)))
              pbOpenStreamListResponseMap.remove(batchId)
              batchSortingCnt.remove(batchId)
            }
          }
        } catch {
          case e: Exception =>
            logError(
              s"Check sort files error, shuffleKey: $shuffleKey, fileId: $fileRequest, error: ${e.getMessage}")
            fileInfoIterator.remove()
        }
      }
      if (!sortFilesInfos.containsKey(shuffleKey)) {
        logWarning(s"Shuffle $shuffleKey expired when checkSortFiles.")
        sortFileInfoIterator.remove()
      }
    }

    def makeException(shuffleKey: String, sortFileInfo: SortFileInfo): CelebornIOException = {
      val msg =
        s"Read file shuffleKey-fileName: ${shuffleKey}-${sortFileInfo.fileName} error from ${NettyUtils.getRemoteAddress(
          sortFileInfo.client.getChannel)}, Exception: ${sortFileInfo.error}."
      new CelebornIOException(msg)
    }
  }

  private def getPbStreamHandlerOpt(
      fileInfo: FileInfo,
      client: TransportClient,
      shuffleKey: String,
      fileName: String,
      startIndex: Int,
      endIndex: Int,
      readLocalShuffle: Boolean = false): PbStreamHandlerOpt = {

    val streamId = chunkStreamManager.nextStreamId()
    val meta = fileInfo.getReduceFileMeta
    val streamHandler =
      if (readLocalShuffle && !fileInfo.isInstanceOf[MemoryFileInfo]) {
        chunkStreamManager.registerStream(
          streamId,
          shuffleKey,
          fileName)
        makeStreamHandler(
          streamId,
          meta.getNumChunks,
          meta.getChunkOffsets,
          fileInfo.asInstanceOf[DiskFileInfo].getFilePath)
      } else fileInfo match {
        case info: DiskFileInfo if info.isHdfs =>
          chunkStreamManager.registerStream(
            streamId,
            shuffleKey,
            fileName)
          makeStreamHandler(streamId, numChunks = 0)
        case info: DiskFileInfo if info.isS3 =>
          chunkStreamManager.registerStream(
            streamId,
            shuffleKey,
            fileName)
          makeStreamHandler(streamId, numChunks = 0)
        case _ =>
          val managedBuffer = fileInfo match {
            case df: DiskFileInfo =>
              new FileChunkBuffers(df, transportConf)
            case mf: MemoryFileInfo =>
              new MemoryChunkBuffers(mf)
          }
          val fetchTimeMetric =
            fileInfo match {
              case info: DiskFileInfo =>
                storageManager.getFetchTimeMetric(info.getFile)
              case _ =>
                null
            }
          chunkStreamManager.registerStream(
            streamId,
            shuffleKey,
            managedBuffer,
            fileName,
            fetchTimeMetric)
          if (meta.getNumChunks == 0)
            logDebug(s"StreamId $streamId, fileName $fileName, mapRange " +
              s"[$startIndex-$endIndex] is empty. Received from client channel " +
              s"${NettyUtils.getRemoteAddress(client.getChannel)}")
          else logDebug(
            s"StreamId $streamId, fileName $fileName, numChunks ${meta.getNumChunks}, " +
              s"mapRange [$startIndex-$endIndex]. Received from client channel " +
              s"${NettyUtils.getRemoteAddress(client.getChannel)}")
          makeStreamHandler(
            streamId,
            meta.getNumChunks)
      }

    workerSource.incCounter(WorkerSource.OPEN_STREAM_SUCCESS_COUNT)
    workerSource.stopTimer(WorkerSource.OPEN_STREAM_TIME, shuffleKey)
    PbStreamHandlerOpt.newBuilder().setStreamHandler(streamHandler)
      .setStatus(StatusCode.SUCCESS.getValue)
      .build()
  }

  private def makeStreamHandler(
      streamId: Long,
      numChunks: Int,
      offsets: util.List[java.lang.Long] = null,
      filepath: String = ""): PbStreamHandler = {
    val pbStreamHandlerBuilder = PbStreamHandler.newBuilder.setStreamId(streamId).setNumChunks(
      numChunks)
    if (offsets != null) {
      pbStreamHandlerBuilder.addAllChunkOffsets(offsets)
    }
    if (filepath.nonEmpty) {
      pbStreamHandlerBuilder.setFullPath(filepath)
    }
    pbStreamHandlerBuilder.build()
  }

  private def replyStreamHandler(
      client: TransportClient,
      requestId: Long,
      pbStreamHandler: PbStreamHandler,
      isLegacy: Boolean): Unit = {
    if (isLegacy) {
      client.getChannel.writeAndFlush(new RpcResponse(
        requestId,
        new NioManagedBuffer(new StreamHandle(
          pbStreamHandler.getStreamId,
          pbStreamHandler.getNumChunks).toByteBuffer)))
    } else {
      client.getChannel.writeAndFlush(new RpcResponse(
        requestId,
        new NioManagedBuffer(new TransportMessage(
          MessageType.STREAM_HANDLER,
          pbStreamHandler.toByteArray).toByteBuffer)))
    }
  }

  private def handleRpcIOException(
      client: TransportClient,
      requestId: Long,
      shuffleKey: String,
      fileName: String,
      ioe: IOException,
      rpcCallback: RpcResponseCallback): Unit = {
    // if open stream rpc failed, this IOException actually should be FileNotFoundException
    // we wrapper this IOException(Other place may have other exception like FileCorruptException) unify to
    // PartitionUnRetryableException for reader can give up this partition and choose to regenerate the partition data
    logError(
      s"Read file: $fileName with shuffleKey: $shuffleKey error from ${NettyUtils.getRemoteAddress(client.getChannel)}",
      ioe)
    workerSource.incCounter(WorkerSource.OPEN_STREAM_FAIL_COUNT)
    workerSource.stopTimer(WorkerSource.OPEN_STREAM_TIME, shuffleKey)
    handleRpcException(client, requestId, ioe, rpcCallback)
  }

  private def handleRpcException(
      client: TransportClient,
      requestId: Long,
      ioe: IOException,
      rpcResponseCallback: RpcResponseCallback): Unit = {
    rpcResponseCallback.onFailure(ExceptionUtils.wrapIOExceptionToUnRetryable(ioe))
  }

  def handleEndStreamFromClient(client: TransportClient, streamId: Long): Unit = {
    handleEndStreamFromClient(client, streamId, StreamType.CreditStream)
  }

  def handleEndStreamFromClient(
      client: TransportClient,
      streamId: Long,
      streamType: StreamType): Unit = {
    streamType match {
      case StreamType.ChunkStream =>
        val streamState = chunkStreamManager.getStreamState(streamId)
        val (shuffleKey, fileName) = (streamState.shuffleKey, streamState.fileName)
        workerSource.recordAppActiveConnection(client, shuffleKey)
        getRawFileInfo(shuffleKey, fileName).closeStream(streamId)
      case StreamType.CreditStream =>
        val shuffleKey = creditStreamManager.getStreamShuffleKey(streamId)
        if (shuffleKey != null) {
          workerSource.recordAppActiveConnection(
            client,
            shuffleKey)
          creditStreamManager.notifyStreamEndByClient(streamId)
        }
      case _ =>
        logError(s"Received a PbBufferStreamEnd message with unknown type $streamType")
    }
  }

  def handleReadAddCredit(client: TransportClient, credit: Int, streamId: Long): Unit = {
    val shuffleKey = creditStreamManager.getStreamShuffleKey(streamId)
    if (shuffleKey != null) {
      workerSource.recordAppActiveConnection(
        client,
        shuffleKey)
      creditStreamManager.addCredit(credit, streamId)
    }
  }

  def handleNotifyRequiredSegment(
      client: TransportClient,
      requiredSegmentId: Int,
      streamId: Long,
      subPartitionId: Int): Unit = {
    // process NotifyRequiredSegment request here, the MapPartitionDataReader will send data if the segment buffer is available.
    logDebug(
      s"Handle NotifyRequiredSegment with streamId: $streamId, requiredSegmentId: $requiredSegmentId")
    val shuffleKey = creditStreamManager.getStreamShuffleKey(streamId)
    if (shuffleKey != null) {
      workerSource.recordAppActiveConnection(
        client,
        shuffleKey)
      creditStreamManager.notifyRequiredSegment(requiredSegmentId, streamId, subPartitionId)
    }
  }

  def handleChunkFetchRequest(
      client: TransportClient,
      streamChunkSlice: StreamChunkSlice,
      req: RequestMessage): Unit = {
    lazy val remoteAddr = NettyUtils.getRemoteAddress(client.getChannel)
    logDebug(s"Received req from ${remoteAddr}" +
      s" to fetch block $streamChunkSlice")

    workerSource.recordAppActiveConnection(
      client,
      chunkStreamManager.getShuffleKeyAndFileName(streamChunkSlice.streamId)._1)

    maxChunkBeingTransferred.foreach { threshold =>
      val chunksBeingTransferred = chunkStreamManager.chunksBeingTransferred // take high cpu usage
      if (chunksBeingTransferred > threshold) {
        val message = "Worker is too busy. The number of chunks being transferred " +
          s"$chunksBeingTransferred exceeds ${MAX_CHUNKS_BEING_TRANSFERRED.key} " +
          s"${Utils.bytesToString(threshold)}."
        logError(message)
        workerSource.incCounter(WorkerSource.FETCH_CHUNK_FAIL_COUNT)
        client.getChannel.writeAndFlush(new ChunkFetchFailure(streamChunkSlice, message))
        return
      }
    }

    val reqStr = req.toString
    workerSource.startTimer(WorkerSource.FETCH_CHUNK_TIME, reqStr)
    val fetchTimeMetric = chunkStreamManager.getFetchTimeMetric(streamChunkSlice.streamId)
    val fetchBeginTime = System.nanoTime()
    try {
      val buf = chunkStreamManager.getChunk(
        streamChunkSlice.streamId,
        streamChunkSlice.chunkIndex,
        streamChunkSlice.offset,
        streamChunkSlice.len)
      chunkStreamManager.chunkBeingSent(streamChunkSlice.streamId)
      client.getChannel.writeAndFlush(new ChunkFetchSuccess(streamChunkSlice, buf))
        .addListener(new GenericFutureListener[Future[_ >: Void]] {
          override def operationComplete(future: Future[_ >: Void]): Unit = {
            if (future.isSuccess) {
              if (log.isDebugEnabled) {
                logDebug(
                  s"Sending ChunkFetchSuccess to $remoteAddr succeeded, chunk $streamChunkSlice")
              }
              workerSource.incCounter(WorkerSource.FETCH_CHUNK_SUCCESS_COUNT)
            } else {
              logWarning(
                s"Sending ChunkFetchSuccess to $remoteAddr failed, chunk $streamChunkSlice",
                future.cause())
              workerSource.incCounter(WorkerSource.FETCH_CHUNK_FAIL_COUNT)
            }
            chunkStreamManager.chunkSent(streamChunkSlice.streamId)
            if (fetchTimeMetric != null) {
              fetchTimeMetric.update(System.nanoTime() - fetchBeginTime)
            }
            workerSource.stopTimer(WorkerSource.FETCH_CHUNK_TIME, reqStr)
          }
        })
    } catch {
      case e: Exception =>
        logError(
          s"Error opening block $streamChunkSlice for request from " +
            NettyUtils.getRemoteAddress(client.getChannel),
          e)
        workerSource.incCounter(WorkerSource.FETCH_CHUNK_FAIL_COUNT)
        client.getChannel.writeAndFlush(new ChunkFetchFailure(
          streamChunkSlice,
          Throwables.getStackTraceAsString(e)))
        workerSource.stopTimer(WorkerSource.FETCH_CHUNK_TIME, reqStr)
    }
  }

  override def checkRegistered: Boolean = registered.exists(_.get)

  /** Invoked when the channel associated with the given client is active. */
  override def channelActive(client: TransportClient): Unit = {
    logDebug(s"channel active ${client.getSocketAddress}")
    workerSource.connectionActive(client)
    super.channelActive(client)
  }

  override def channelInactive(client: TransportClient): Unit = {
    workerSource.connectionInactive(client)
    creditStreamManager.connectionTerminated(client.getChannel)
    logDebug(s"channel inactive ${client.getSocketAddress}")
  }

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    logWarning(s"exception caught ${client.getSocketAddress}", cause)
  }

  def cleanupExpiredShuffleKey(expiredShuffleKeys: util.HashSet[String]): Unit = {
    chunkStreamManager.cleanupExpiredShuffleKey(expiredShuffleKeys)
    // maybe null when running unit test.
    if (partitionsSorter != null) {
      partitionsSorter.cleanupExpiredShuffleKey(expiredShuffleKeys)
    }
  }

  def setPartitionsSorter(partitionFilesSorter: PartitionFilesSorter): Unit = {
    this.partitionsSorter = partitionFilesSorter
  }

  def setSortFilesInfos(sortFilesInfos: ConcurrentHashMap[
    String,
    ConcurrentHashMap[String, SortFileInfo]]): Unit = {
    this.sortFilesInfos = sortFilesInfos
  }

  def setSortFileChecker(sortFileChecker: ScheduledExecutorService): Unit = {
    this.sortFileChecker = sortFileChecker
    this.sortFileChecker.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          getAndReplySortedFiles()
        }
      },
      0,
      workerSortFilesCheckInterval,
      TimeUnit.MILLISECONDS)
  }

}
