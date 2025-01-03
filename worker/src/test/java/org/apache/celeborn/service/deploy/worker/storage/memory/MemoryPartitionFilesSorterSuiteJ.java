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

package org.apache.celeborn.service.deploy.worker.storage.memory;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.meta.MemoryFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.util.PushDataHeaderUtils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;
import org.apache.celeborn.service.deploy.worker.storage.PartitionDataWriter;
import org.apache.celeborn.service.deploy.worker.storage.PartitionFilesSorter;
import org.apache.celeborn.service.deploy.worker.storage.StorageManager;

public class MemoryPartitionFilesSorterSuiteJ {

  private final Random random = new Random();
  private MemoryFileInfo fileInfo;
  private PartitionDataWriter partitionDataWriter;
  private final UserIdentifier userIdentifier = new UserIdentifier("mock-tenantId", "mock-name");

  private static final int MAX_MAP_ID = 50;

  public long[] prepare(int mapCount) {
    long[] partitionSize = new long[MAX_MAP_ID];
    byte[] batchHeader = new byte[PushDataHeaderUtils.BATCH_HEADER_SIZE];
    fileInfo = new MemoryFileInfo(userIdentifier, true, new ReduceFileMeta(8 * 1024 * 1024));

    AbstractSource source = Mockito.mock(AbstractSource.class);
    ByteBufAllocator allocator =
        NettyUtils.getSharedByteBufAllocator(new CelebornConf(), source, false);
    CompositeByteBuf buffer = allocator.compositeBuffer();
    fileInfo.setBuffer(buffer);

    Map<Integer, Integer> batchIds = new HashMap<>();

    for (int i = 0; i < mapCount; i++) {
      int mapId = random.nextInt(MAX_MAP_ID);
      int currentAttemptId = 0;
      int batchId =
          batchIds.compute(
              mapId,
              (k, v) -> {
                if (v == null) {
                  v = 0;
                } else {
                  v++;
                }
                return v;
              });
      // [63.9k, 192k + 63.9k]
      int dataSize = random.nextInt(192 * 1024) + 65525;
      byte[] mockedData = new byte[dataSize];
      PushDataHeaderUtils.buildDataHeader(
          batchHeader, mapId, currentAttemptId, batchId, dataSize, true);
      ByteBuffer buf1 = ByteBuffer.wrap(batchHeader);
      while (buf1.hasRemaining()) {
        buffer.writeBytes(buf1);
      }
      random.nextBytes(mockedData);
      ByteBuffer buf2 = ByteBuffer.wrap(mockedData);
      while (buf2.hasRemaining()) {
        buffer.writeBytes(buf2);
      }
      partitionSize[mapId] = partitionSize[mapId] + batchHeader.length + mockedData.length;
    }
    long originFileLen = buffer.readableBytes();
    fileInfo.getReduceFileMeta().addChunkOffset(originFileLen);
    fileInfo.updateBytesFlushed((int) originFileLen);

    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE().key(), "0.8");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE().key(), "0.9");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_RESUME().key(), "0.5");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_READ_BUFFER().key(), "0.1");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_MEMORY_FILE_STORAGE().key(), "0.1");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL().key(), "10");
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_REPORT_INTERVAL().key(), "10");
    conf.set(CelebornConf.WORKER_READBUFFER_ALLOCATIONWAIT().key(), "10ms");

    StorageManager storageManager = Mockito.mock(StorageManager.class);
    Mockito.when(storageManager.storageBufferAllocator()).thenAnswer(a -> allocator);
    MemoryManager.initialize(conf, storageManager, null);
    partitionDataWriter = Mockito.mock(PartitionDataWriter.class);
    when(partitionDataWriter.getMemoryFileInfo()).thenAnswer(i -> fileInfo);

    return partitionSize;
  }

  private void check(int mapCount, int startMapIndex, int endMapIndex) throws IOException {
    long[] partitionSize = prepare(mapCount);
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.SHUFFLE_CHUNK_SIZE().key(), "8m");
    PartitionFilesSorter partitionFilesSorter =
        new PartitionFilesSorter(MemoryManager.instance(), conf, new WorkerSource(conf));
    FileInfo info =
        partitionFilesSorter.getSortedFileInfo(
            "application-1",
            "",
            partitionDataWriter.getMemoryFileInfo(),
            startMapIndex,
            endMapIndex);
    long totalSizeToFetch = 0;
    for (int i = startMapIndex; i < endMapIndex; i++) {
      totalSizeToFetch += partitionSize[i];
    }
    long numChunks = totalSizeToFetch / conf.shuffleChunkSize() + 1;
    Assert.assertTrue(
        0 < ((ReduceFileMeta) info.getFileMeta()).getNumChunks()
            && ((ReduceFileMeta) info.getFileMeta()).getNumChunks() <= numChunks);
    long actualTotalChunkSize =
        ((ReduceFileMeta) info.getFileMeta()).getLastChunkOffset()
            - ((ReduceFileMeta) info.getFileMeta()).getChunkOffsets().get(0);
    Assert.assertEquals(totalSizeToFetch, actualTotalChunkSize);
    fileInfo.getBuffer().release();
  }

  @Test
  public void testSortMemoryShuffleFile() throws IOException {
    int startMapIndex = random.nextInt(5);
    int endMapIndex = startMapIndex + random.nextInt(5) + 5;
    check(1000, startMapIndex, endMapIndex);
  }
}
