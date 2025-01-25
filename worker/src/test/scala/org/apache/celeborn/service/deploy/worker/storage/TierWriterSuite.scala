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
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import io.netty.buffer.UnpooledByteBufAllocator
import org.mockito.Mockito
import org.mockito.MockitoSugar.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{DiskFileInfo, MemoryFileInfo, ReduceFileMeta}
import org.apache.celeborn.common.network.util.{NettyUtils, TransportConf}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.service.deploy.worker.WorkerSource
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class TierWriterSuite extends AnyFunSuite with BeforeAndAfterEach {

  private def prepareMemoryWriter: MemoryTierWriter = {
    val celebornConf = new CelebornConf()
    celebornConf.set("celeborn.worker.memoryFileStorage.maxFileSize", "80k")
    val reduceFileMeta = new ReduceFileMeta(celebornConf.shuffleChunkSize)
    val userIdentifier = UserIdentifier("`aa`.`bb`")
    val memoryFileInfo = new MemoryFileInfo(userIdentifier, false, reduceFileMeta)
    val numPendingWriters = new AtomicInteger()
    val flushNotifier = new FlushNotifier()

    val SPLIT_THRESHOLD = 256 * 1024 * 1024L
    val splitMode = PartitionSplitMode.HARD

    val writerContext = new PartitionDataWriterContext(
      SPLIT_THRESHOLD,
      splitMode,
      false,
      new PartitionLocation(
        1,
        0,
        "host",
        1111,
        1112,
        1113,
        1114,
        PartitionLocation.Mode.PRIMARY,
        null),
      "app1-1",
      1,
      userIdentifier,
      PartitionType.REDUCE,
      false)

    val source = new WorkerSource(celebornConf)

    val storageManager: StorageManager = Mockito.mock(classOf[StorageManager])
    val transConf = new TransportConf("shuffle", new CelebornConf)
    val allocator = NettyUtils.getByteBufAllocator(transConf, source, false)
    val evictedFileCount = new AtomicLong()
    when(storageManager.storageBufferAllocator).thenReturn(allocator)
    when(storageManager.localOrDfsStorageAvailable).thenReturn(true)
    when(storageManager.evictedFileCount).thenReturn(evictedFileCount)

    MemoryManager.initialize(celebornConf, storageManager, null)

    val tierMemoryWriter = new MemoryTierWriter(
      celebornConf,
      new ReducePartitionMetaHandler(celebornConf.shuffleRangeReadFilterEnabled, memoryFileInfo),
      numPendingWriters,
      flushNotifier,
      source,
      memoryFileInfo,
      StorageInfo.Type.MEMORY,
      writerContext,
      storageManager)
    tierMemoryWriter
  }

  test("test memory tier writer case1") {

    val memoryTierWriter: MemoryTierWriter = prepareMemoryWriter

    val buf1 = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(buf1)
    assert(memoryTierWriter.fileInfo.getFileLength === 1024)

    val needEvict = memoryTierWriter.needEvict()
    assert(needEvict === false)

    for (i <- 2 to 80) {
      memoryTierWriter.numPendingWrites.incrementAndGet()
      memoryTierWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
      assert(memoryTierWriter.fileInfo.getFileLength === 1024 * i)
    }

    // 8 MB is lesser than the evict threshold
    assert(memoryTierWriter.needEvict() === false)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(WriterUtils.generateSparkFormatData(
      UnpooledByteBufAllocator.DEFAULT,
      0))

    assert(memoryTierWriter.needEvict() === true)

    val filelen = memoryTierWriter.close()
    assert(filelen === 81 * 1024)

    assert(memoryTierWriter.closed === true)

    try {
      memoryTierWriter.write((WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0)))
      // expect already closed exception here
      assert(false)
    } catch {
      case e: AlreadyClosedException =>
        assert(true)
    }

  }

  test("test memory tier writer case2") {

    val memoryTierWriter: MemoryTierWriter = prepareMemoryWriter

    val buf1 = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(buf1)
    assert(memoryTierWriter.fileInfo.getFileLength === 1024)

    val needEvict = memoryTierWriter.needEvict()
    assert(needEvict === false)

    memoryTierWriter.destroy(new IOException("test"))
    assert(memoryTierWriter.flushBuffer.refCnt() === 0)

    try {
      memoryTierWriter.write((WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0)))
      // expect already closed exception here
      assert(false)
    } catch {
      case e: AlreadyClosedException =>
        assert(true)
    }

  }

  private def prepareLocalTierWriter(rangeFilter: Boolean): LocalTierWriter = {
    val celebornConf = new CelebornConf()
    celebornConf.set("celeborn.worker.memoryFileStorage.maxFileSize", "80k")
    celebornConf.set("celeborn.client.shuffle.rangeReadFilter.enabled", rangeFilter.toString)
    val reduceFileMeta = new ReduceFileMeta(celebornConf.shuffleChunkSize)
    val userIdentifier = UserIdentifier("`aa`.`bb`")
    val tmpFile = Files.createTempFile("celeborn", "local-test").toString
    val diskFileInfo =
      new DiskFileInfo(userIdentifier, false, reduceFileMeta, tmpFile, StorageInfo.Type.HDD)
    val numPendingWriters = new AtomicInteger()
    val flushNotifier = new FlushNotifier()
    val source = new WorkerSource(celebornConf)

    val writerContext = new PartitionDataWriterContext(
      1024 * 1024 * 1024,
      PartitionSplitMode.SOFT,
      false,
      new PartitionLocation(
        1,
        0,
        "host",
        1111,
        1112,
        1113,
        1114,
        PartitionLocation.Mode.PRIMARY,
        null),
      "app1-1",
      1,
      userIdentifier,
      PartitionType.REDUCE,
      false)

    val flusher = new LocalFlusher(
      source,
      DeviceMonitor.EmptyMonitor(),
      1,
      NettyUtils.getByteBufAllocator(new TransportConf("test", celebornConf), null, true),
      256,
      "disk1",
      StorageInfo.Type.HDD,
      null)
    val storageManager: StorageManager = Mockito.mock(classOf[StorageManager])

    MemoryManager.initialize(celebornConf, storageManager, null)

    new LocalTierWriter(
      celebornConf,
      new ReducePartitionMetaHandler(celebornConf.shuffleRangeReadFilterEnabled, diskFileInfo),
      numPendingWriters,
      flushNotifier,
      flusher,
      source,
      diskFileInfo,
      StorageInfo.Type.HDD,
      writerContext,
      storageManager)
  }

  test("test memory tier evict to local tier writer") {
    val memoryTierWriter: MemoryTierWriter = prepareMemoryWriter
    val localTierWriter = prepareLocalTierWriter(true)

    val buf1 = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(buf1)
    assert(memoryTierWriter.fileInfo.getFileLength === 1024)

    val needEvict = memoryTierWriter.needEvict()
    assert(needEvict === false)

    for (i <- 2 to 80) {
      memoryTierWriter.numPendingWrites.incrementAndGet()
      memoryTierWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
      assert(memoryTierWriter.fileInfo.getFileLength === 1024 * i)
    }

    // 8 MB is lesser than the evict threshold
    assert(memoryTierWriter.needEvict() === false)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(WriterUtils.generateSparkFormatData(
      UnpooledByteBufAllocator.DEFAULT,
      0))

    assert(memoryTierWriter.needEvict() === true)
    memoryTierWriter.evict(localTierWriter)

    localTierWriter.close()
    assert(localTierWriter.fileInfo.getFileLength === 82944)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets.size() == 2)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getLastChunkOffset == 82944)
  }

  test("test local tier writer with range filter on ") {
    val localTierWriter = prepareLocalTierWriter(true)
    for (i <- 1 to 10) {
      localTierWriter.numPendingWrites.incrementAndGet()
      localTierWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
    }

    localTierWriter.close()

    assert(10240 === localTierWriter.fileInfo.getFileLength)
    assert(
      localTierWriter.metaHandler.asInstanceOf[ReducePartitionMetaHandler].mapIdBitMap.isDefined)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets.size() == 2)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getLastChunkOffset == 10240)
  }

  test("test local tier writer with range filter off ") {
    val localTierWriter = prepareLocalTierWriter(false)
    for (i <- 1 to 10) {
      localTierWriter.numPendingWrites.incrementAndGet()
      localTierWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
    }

    localTierWriter.close()

    assert(10240 === localTierWriter.fileInfo.getFileLength)
    assert(
      !localTierWriter.metaHandler.asInstanceOf[ReducePartitionMetaHandler].mapIdBitMap.isDefined)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets.size() == 2)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getLastChunkOffset == 10240)

  }
}
