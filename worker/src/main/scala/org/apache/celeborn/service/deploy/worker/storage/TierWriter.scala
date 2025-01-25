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

package org.apache.celeborn.service.deploy.worker.storage

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters.asScalaBufferConverter

import io.netty.buffer.{ByteBuf, CompositeByteBuf}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskFileInfo, FileInfo, MemoryFileInfo}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.common.util.FileChannelUtils
import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler
import org.apache.celeborn.service.deploy.worker.WorkerSource
import org.apache.celeborn.service.deploy.worker.congestcontrol.{CongestionController, UserCongestionControlContext}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

abstract class TierWriterBase(
    val conf: CelebornConf,
    val metaHandler: PartitionMetaHandler,
    val numPendingWrites: AtomicInteger,
    val notifier: FlushNotifier,
    val fileInfo: FileInfo,
    val source: AbstractSource,
    val storageType: StorageInfo.Type,
    val filename: String,
    val shuffleKey: String,
    val storageManager: StorageManager) extends Logging {
  val metricsCollectCriticalEnabled: Boolean = conf.metricsCollectCriticalEnabled
  val flushLock = new AnyRef
  val WAIT_INTERVAL_MS = 5

  var flushBuffer: CompositeByteBuf = _
  var writerCloseTimeoutMs: Long = conf.workerWriterCloseTimeoutMs
  var flusherBufferSize = 0L
  var chunkSize: Long = conf.shuffleChunkSize

  @volatile var closed = false
  @volatile var destroyed = false

  takeBuffer()

  def write(buf: ByteBuf): Unit = {
    ensureNotClosed()
    if (notifier.hasException) return

    flushLock.synchronized {
      metaHandler.beforeWrite(buf)
      ensureNotClosed()
      writerInternal(buf)
    }

    metaHandler.afterWrite(buf.readableBytes())
    numPendingWrites.decrementAndGet()
  }

  protected def writerInternal(buf: ByteBuf): Unit

  def needEvict(): Boolean

  def evict(file: TierWriterBase): Unit

  def swapFlushBuffer(inputBuffer: CompositeByteBuf): Unit = {
    if (flushBuffer != null) {
      returnBuffer(false)
    }
    flushBuffer = inputBuffer
  }

  def close(evict: Boolean = false): Long = {
    try {
      ensureNotClosed()
      waitOnNoPending(numPendingWrites, false)
      closed = true
      finalFlush()

      waitOnNoPending(notifier.numPendingFlushes, true)
      metaHandler.afterClose()
    } finally {
      returnBuffer(false)
      try {
        closeStreams()
      } catch {
        case e: IOException =>
          logWarning(s"close file writer ${this} failed", e)
      }
    }
    if (!evict) {
      notifyFileCommitted()
    }

    fileInfo.getFileLength
  }

  def ensureNotClosed(): Unit = {
    if (closed) {
      val msg = getFileAlreadyClosedMsg
      logWarning(msg)
      throw new AlreadyClosedException(msg)
    }
  }

  def getFileAlreadyClosedMsg: String = {
    s"PartitionDataWriter has already closed! File name:${filename}"
  }

  // this method is not used in memory tier writer
  def notifyFileCommitted(): Unit = {}

  // this method is not used in memory tier writer
  def finalFlush(): Unit = {}

  @throws[IOException]
  protected def waitOnNoPending(counter: AtomicInteger, failWhenTimeout: Boolean): Unit = {
    var waitTime = writerCloseTimeoutMs
    while (counter.get > 0 && waitTime > 0) {
      try {
        notifier.checkException()
        TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS)
      } catch {
        case e: InterruptedException =>
          val ioe = new IOException(e)
          notifier.setException(ioe)
          throw ioe
      }
      waitTime -= WAIT_INTERVAL_MS
    }
    if (counter.get > 0 && failWhenTimeout) {
      val ioe = new IOException("Wait pending actions timeout.")
      notifier.setException(ioe)
      throw ioe
    }
    notifier.checkException()
  }

  def closeStreams(): Unit

  def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask

  def flush(finalFlush: Boolean, rebuildChunkOffsets: Boolean = false): Unit = {
    if (flushBuffer != null) {
      val numBytes = flushBuffer.readableBytes()
      var flushTask: FlushTask = null
      if (numBytes != 0) {
        if (rebuildChunkOffsets) {
          val dupBuf = flushBuffer.retainedDuplicate()
          // this flusher buffer is from memory tier writer, so that we can not keep the buffer
          flushTask = genFlushTask(finalFlush, false)
          if (numBytes > chunkSize) {
            val headerBuf = ByteBuffer.allocate(16)
            while (dupBuf.isReadable) {
              headerBuf.rewind
              dupBuf.readBytes(headerBuf)
              val batchHeader = headerBuf.array
              val compressedSize = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 12)
              dupBuf.skipBytes(compressedSize)
            }
            dupBuf.release
          } else metaHandler.afterFlush(numBytes)
        } else {
          notifier.checkException()
          // if a flush buffer is larger than the chunk size, it might contain data of multiple chunks
          flushTask = genFlushTask(finalFlush, true)
        }
      }
      // task won't be null in real workloads unless it is a memory file
      // task will be null in UT to check chunk size and offset
      if (flushTask != null) {
        addFlushTask(flushTask)
        flushBuffer = null
        if (!rebuildChunkOffsets) {
          metaHandler.afterFlush(numBytes)
        }
        if (!finalFlush) {
          takeBuffer()
        }
      }
    }

  }

  def takeBuffer() = {
    var metricsName: String = null
    var fileAbsPath: String = null
    if (metricsCollectCriticalEnabled) {
      metricsName = WorkerSource.TAKE_BUFFER_TIME
      fileAbsPath = fileInfo.getFilePath
      source.startTimer(metricsName, fileAbsPath)
    }

    flushLock.synchronized {
      flushBuffer = takeBufferInternal()
    }

    if (metricsCollectCriticalEnabled) source.stopTimer(metricsName, fileAbsPath)
  }

  def addFlushTask(task: FlushTask): Unit

  def takeBufferInternal(): CompositeByteBuf

  def destroy(ioException: IOException): Unit = {
    if (!closed) {
      closed = true
      if (!notifier.hasException) {
        notifier.setException(ioException)
      }
      metaHandler.beforeDestroy()
      returnBuffer(true)
      closeResource()
    }

    if (!destroyed) {
      destroyed = true
      cleanLocalOrDfsFiles()
    }
  }

  def returnBuffer(destroy: Boolean): Unit = {
    flushLock.synchronized {
      returnBufferInternal(destroy)
    }
  }

  def closeResource(): Unit = {}

  def cleanLocalOrDfsFiles(): Unit = {}

  def returnBufferInternal(destroy: Boolean): Unit

}

class MemoryTierWriter(
    conf: CelebornConf,
    metaHandler: PartitionMetaHandler,
    numPendingWriters: AtomicInteger,
    notifier: FlushNotifier,
    source: AbstractSource,
    fileInfo: MemoryFileInfo,
    storageType: StorageInfo.Type,
    partitionDataWriterContext: PartitionDataWriterContext,
    storageManager: StorageManager)
  extends TierWriterBase(
    conf,
    metaHandler,
    numPendingWriters,
    notifier,
    fileInfo,
    source,
    storageType,
    partitionDataWriterContext.getPartitionLocation.getFileName,
    partitionDataWriterContext.getShuffleKey,
    storageManager) {

  val memoryFileStorageMaxFileSize: Long = conf.workerMemoryFileStorageMaxFileSize

  override def needEvict(): Boolean = {
    flushBuffer.readableBytes() > memoryFileStorageMaxFileSize && storageManager.localOrDfsStorageAvailable
  }

  override def evict(file: TierWriterBase): Unit = {
    flushLock.synchronized {
      // swap tier writer's flush buffer to memory tier writer's
      // and handle its release
      file.swapFlushBuffer(flushBuffer)
      file.flush(false, true)
      val numBytes = flushBuffer.readableBytes()
      MemoryManager.instance.releaseMemoryFileStorage(numBytes)
      MemoryManager.instance.incrementDiskBuffer(numBytes)
      storageManager.unregisterMemoryPartitionWriterAndFileInfo(fileInfo, shuffleKey, filename)
      storageManager.evictedFileCount.incrementAndGet
    }
  }

  // Memory file won't produce flush task
  override def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask = null

  override def writerInternal(buf: ByteBuf): Unit = {
    buf.retain()
    try {
      flushBuffer.addComponent(true, buf)
    } catch {
      case oom: OutOfMemoryError =>
        MemoryManager.instance.releaseMemoryFileStorage(buf.readableBytes())
        throw oom
    }
    // memory tier writer will not flush
    // add the bytes into flusher buffer is flush completed
    val numBytes = buf.readableBytes()
    metaHandler.afterFlush(numBytes)
    MemoryManager.instance().incrementMemoryFileStorage(numBytes)
  }

  override def closeStreams(): Unit = {
    flushBuffer.consolidate()
    fileInfo.setBuffer(flushBuffer)
  }

  override def takeBufferInternal(): CompositeByteBuf = {
    storageManager.storageBufferAllocator.compositeBuffer(Integer.MAX_VALUE)
  }

  override def returnBufferInternal(destroy: Boolean): Unit = {
    if (destroy && flushBuffer != null) {
      flushBuffer.removeComponents(0, flushBuffer.numComponents)
      flushBuffer.release
    }
  }

  override def addFlushTask(task: FlushTask): Unit = {
    // memory tier write does not need flush tasks
  }
}

class LocalTierWriter(
    conf: CelebornConf,
    metaHandler: PartitionMetaHandler,
    numPendingWrites: AtomicInteger,
    notifier: FlushNotifier,
    flusher: Flusher,
    source: AbstractSource,
    diskFileInfo: DiskFileInfo,
    storageType: StorageInfo.Type,
    partitionDataWriterContext: PartitionDataWriterContext,
    storageManager: StorageManager)
  extends TierWriterBase(
    conf,
    metaHandler,
    numPendingWrites,
    notifier,
    diskFileInfo,
    source,
    storageType,
    partitionDataWriterContext.getPartitionLocation.getFileName,
    partitionDataWriterContext.getShuffleKey,
    storageManager) {
  flusherBufferSize = conf.workerFlusherBufferSize
  val flushWorkerIndex = flusher.getWorkerIndex
  val userCongestionControlContext: UserCongestionControlContext =
    if (CongestionController.instance != null)
      CongestionController.instance.getUserCongestionContext(
        partitionDataWriterContext.getUserIdentifier)
    else
      null

  private val channel: FileChannel =
    FileChannelUtils.createWritableFileChannel(diskFileInfo.getFilePath);

  override def needEvict: Boolean = {
    false
  }

  override def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask = {
    notifier.numPendingFlushes.incrementAndGet()
    new LocalFlushTask(flushBuffer, channel, notifier, true)
  }

  override def writerInternal(buf: ByteBuf): Unit = {
    val numBytes = buf.readableBytes()
    val flushBufferReadableBytes = flushBuffer.readableBytes
    if (flushBufferReadableBytes != 0 && flushBufferReadableBytes + numBytes >= flusherBufferSize) {
      flush(false)
    }
    buf.retain()
    try {
      flushBuffer.addComponent(true, buf)
      MemoryManager.instance.incrementDiskBuffer(numBytes)
      if (userCongestionControlContext != null)
        userCongestionControlContext.updateProduceBytes(numBytes)
    } catch {
      case oom: OutOfMemoryError =>
        buf.release()
        MemoryManager.instance().releaseDiskBuffer(numBytes)
        throw oom;
    }
  }

  override def evict(file: TierWriterBase): Unit = ???

  override def finalFlush(): Unit = {
    if (flushBuffer != null && flushBuffer.readableBytes() > 0) {
      flush(true)
    }
  }

  override def closeStreams(): Unit = {
    // local disk file won't need to close streams
  }

  override def notifyFileCommitted(): Unit =
    storageManager.notifyFileInfoCommitted(shuffleKey, filename, diskFileInfo)

  override def closeResource(): Unit = {
    try if (channel != null) channel.close()
    catch {
      case e: IOException =>
        logWarning(
          s"Close channel failed for file ${diskFileInfo.getFilePath} caused by ${e.getMessage}.")
    }
  }

  override def cleanLocalOrDfsFiles(): Unit = {
    diskFileInfo.deleteAllFiles(null)
  }

  override def takeBufferInternal(): CompositeByteBuf = {
    flusher.takeBuffer()
  }

  override def returnBufferInternal(destroy: Boolean): Unit = {
    if (flushBuffer != null) {
      flusher.returnBuffer(flushBuffer, true)
      flushBuffer = null
    }
  }

  override def addFlushTask(task: FlushTask): Unit = {
    if (!flusher.addTask(task, writerCloseTimeoutMs, flushWorkerIndex)) {
      val e = new IOException("Add flush task timeout.")
      notifier.setException(e)
      throw e
    }
  }
}

class DfsTierWriter(
    conf: CelebornConf,
    metaHandler: PartitionMetaHandler,
    numPendingWrites: AtomicInteger,
    notifier: FlushNotifier,
    flusher: Flusher,
    source: AbstractSource,
    hdfsFileInfo: DiskFileInfo,
    storageType: StorageInfo.Type,
    partitionDataWriterContext: PartitionDataWriterContext,
    storageManager: StorageManager)
  extends TierWriterBase(
    conf,
    metaHandler,
    numPendingWrites,
    notifier,
    hdfsFileInfo,
    source,
    storageType,
    partitionDataWriterContext.getPartitionLocation.getFileName,
    partitionDataWriterContext.getShuffleKey,
    storageManager) {
  flusherBufferSize = conf.workerHdfsFlusherBufferSize
  val flushWorkerIndex = flusher.getWorkerIndex
  val hadoopFs = StorageManager.hadoopFs.get(storageType)
  var deleted = false
  var s3MultipartUploadHandler: MultipartUploadHandler = null
  var partNumber: Int = 1

  this.flusherBufferSize =
    if (hdfsFileInfo.isS3()) {
      conf.workerS3FlusherBufferSize
    } else {
      conf.workerHdfsFlusherBufferSize
    }

  try {
    hadoopFs.create(hdfsFileInfo.getDfsPath, true).close()
    if (hdfsFileInfo.isS3) {
      val configuration = hadoopFs.getConf
      val s3AccessKey = configuration.get("fs.s3a.access.key")
      val s3SecretKey = configuration.get("fs.s3a.secret.key")
      val s3EndpointRegion = configuration.get("fs.s3a.endpoint.region")

      val uri = hadoopFs.getUri
      val bucketName = uri.getHost
      val index = hdfsFileInfo.getFilePath.indexOf(bucketName)
      val key = hdfsFileInfo.getFilePath.substring(index + bucketName.length + 1)

      this.s3MultipartUploadHandler = TierWriterHelper.getS3MultipartUploadHandler(
        bucketName,
        s3AccessKey,
        s3SecretKey,
        s3EndpointRegion,
        key,
        conf.s3MultiplePartUploadMaxRetries)
      s3MultipartUploadHandler.startUpload()
    }
  } catch {
    case _: IOException =>
      try
      // If create file failed, wait 10 ms and retry
      Thread.sleep(10)
      catch {
        case ex: InterruptedException =>
          throw new RuntimeException(ex)
      }
      hadoopFs.create(hdfsFileInfo.getDfsPath, true).close()
  }

  override def needEvict: Boolean = {
    false
  }

  override def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask = {
    notifier.numPendingFlushes.incrementAndGet()
    if (hdfsFileInfo.isHdfs) {
      new HdfsFlushTask(flushBuffer, hdfsFileInfo.getDfsPath(), notifier, true)
    } else {
      val flushTask = new S3FlushTask(
        flushBuffer,
        notifier,
        false,
        s3MultipartUploadHandler,
        partNumber,
        finalFlush)
      partNumber = partNumber + 1
      flushTask
    }
  }

  override def writerInternal(buf: ByteBuf): Unit = {
    val numBytes = buf.readableBytes()
    val flushBufferReadableBytes = flushBuffer.readableBytes
    if (flushBufferReadableBytes != 0 && flushBufferReadableBytes + numBytes >= flusherBufferSize) {
      flush(false)
    }
    buf.retain()
    try {
      flushBuffer.addComponent(true, buf)
      MemoryManager.instance.incrementDiskBuffer(numBytes)
    } catch {
      case oom: OutOfMemoryError =>
        buf.release()
        MemoryManager.instance().releaseDiskBuffer(numBytes)
        throw oom;
    }
  }

  override def evict(file: TierWriterBase): Unit = ???

  override def finalFlush(): Unit = {
    if (flushBuffer != null && flushBuffer.readableBytes() > 0) {
      flush(true)
    }
  }

  override def closeStreams(): Unit = {
    if (hadoopFs.exists(hdfsFileInfo.getDfsPeerWriterSuccessPath)) {
      hadoopFs.delete(hdfsFileInfo.getDfsPath, false)
      deleted = true
    } else {
      hadoopFs.create(hdfsFileInfo.getDfsWriterSuccessPath).close()
      val indexOutputStream = hadoopFs.create(hdfsFileInfo.getDfsIndexPath)
      indexOutputStream.writeInt(hdfsFileInfo.getReduceFileMeta.getChunkOffsets.size)
      for (offset <- hdfsFileInfo.getReduceFileMeta.getChunkOffsets.asScala) {
        indexOutputStream.writeLong(offset)
      }
      indexOutputStream.close()
    }
  }

  override def notifyFileCommitted(): Unit =
    storageManager.notifyFileInfoCommitted(shuffleKey, filename, hdfsFileInfo)

  override def closeResource(): Unit = {}

  override def cleanLocalOrDfsFiles(): Unit = {
    hdfsFileInfo.deleteAllFiles(hadoopFs)
  }

  override def takeBufferInternal(): CompositeByteBuf = {
    flusher.takeBuffer()
  }

  override def returnBufferInternal(destroy: Boolean): Unit = {
    if (flushBuffer != null) {
      flusher.returnBuffer(flushBuffer, true)
      flushBuffer = null
    }
  }

  override def addFlushTask(task: FlushTask): Unit = {
    if (!flusher.addTask(task, writerCloseTimeoutMs, flushWorkerIndex)) {
      val e = new IOException("Add flush task timeout.")
      notifier.setException(e)
      throw e
    }
  }
}
