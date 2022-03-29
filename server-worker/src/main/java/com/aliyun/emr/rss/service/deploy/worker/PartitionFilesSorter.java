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

package com.aliyun.emr.rss.service.deploy.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.VM;
import sun.nio.ch.DirectBuffer;

import com.aliyun.emr.rss.common.network.server.FileInfo;
import com.aliyun.emr.rss.common.network.server.MemoryTracker;
import com.aliyun.emr.rss.common.unsafe.Platform;
import com.aliyun.emr.rss.common.util.ThreadUtils;

public class PartitionFilesSorter {
  private static Logger logger = LoggerFactory.getLogger(PartitionFilesSorter.class);
  public static final String SORTED_SUFFIX = ".sorted";
  public static final String INDEX_SUFFIX = ".index";

  private final ConcurrentHashMap<String, Set<String>> sortedShuffleFiles =
    new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<String>> sortingShuffleFiles =
    new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Map<String, Map<Integer, List<ShuffleBlockInfo>>>>
    cachedIndexMaps = new ConcurrentHashMap<>();
  private final LinkedBlockingQueue<FileSorter> shuffleSortTaskDeque = new LinkedBlockingQueue<>();
  protected final long sortTimeout;
  protected final long fetchChunkSize;
  protected final double maxSingleSortFileRatio;
  protected final long maxSingleFileInMemSize;
  protected final long reserveMemoryForOffHeapSort;

  private final ExecutorService fileSorterExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "worker-file-sorter-execute", Math.max(Runtime.getRuntime().availableProcessors(), 8), 120);
  private final Thread fileSorterSchedulerThread;

  PartitionFilesSorter(MemoryTracker memoryTracker, long sortTimeOut, long fetchChunkSize,
    double maxSingleSortFileRatio, long reserveMemoryForOffHeapSort) {
    this.sortTimeout = sortTimeOut;
    this.fetchChunkSize = fetchChunkSize;
    this.maxSingleSortFileRatio = maxSingleSortFileRatio;
    this.maxSingleFileInMemSize = (long) (maxSingleSortFileRatio * VM.maxDirectMemory());
    this.reserveMemoryForOffHeapSort = reserveMemoryForOffHeapSort;

    fileSorterSchedulerThread = new Thread(() -> {
      try {
        while (true) {
          FileSorter task = shuffleSortTaskDeque.take();
          if (task.inMemSort()) {
            memoryTracker.reserveSortMemory(task.getOriginFileLen());
          } else {
            memoryTracker.reserveSortMemory(reserveMemoryForOffHeapSort);
          }
          while (!memoryTracker.sortMemoryReady()) {
            Thread.sleep(20);
          }
          fileSorterExecutors.submit(() -> {
            task.sort();
            if (task.inMemSort()) {
              memoryTracker.releaseSortMemory(task.getOriginFileLen());
            } else {
              memoryTracker.releaseSortMemory(reserveMemoryForOffHeapSort);
            }
          });
        }
      } catch (InterruptedException e) {
        logger.warn("sort file failed, detail :", e);
      }
    });
    fileSorterSchedulerThread.start();
  }

  public FileInfo openStream(String shuffleKey, String fileName, FileWriter fileWriter,
    int startMapIndex, int endMapIndex) {
    if (endMapIndex == Integer.MAX_VALUE) {
      return new FileInfo(fileWriter.getFile(), fileWriter.getChunkOffsets());
    } else {
      logger.debug("read shuffle {} file {} startMapIndex {} endMapIndex {}", shuffleKey,
        fileWriter.getFile().getAbsolutePath(), startMapIndex, endMapIndex);
      String fileId = shuffleKey + "-" + fileName;

      Set<String> sorted =
        sortedShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());
      Set<String> sorting =
        sortingShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());

      boolean sortInMem = fileWriter.getFileLength() < maxSingleFileInMemSize;
      String sortedFileName = fileWriter.getFile().getAbsolutePath() + SORTED_SUFFIX;
      String indexFileName = fileWriter.getFile().getAbsolutePath() + INDEX_SUFFIX;

      if (sorted.contains(fileId)) {
        return resolve(shuffleKey, fileId, sortedFileName, indexFileName,
          startMapIndex, endMapIndex);
      }

      synchronized (sorting) {
        if (!sorting.contains(fileId)) {
          FileSorter fileSorter = new FileSorter(fileWriter.getFile(), fileWriter.getFileLength(),
            sortInMem, fileId, shuffleKey);
          sorting.add(fileId);
          try {
            shuffleSortTaskDeque.put(fileSorter);
          } catch (InterruptedException e) {
            logger.info("scheduler thread is interrupted means worker is shutting down.");
            return null;
          }

          long sortStartTime = System.currentTimeMillis();
          while (fileSorter.sortStatus.equals(SortStatus.success)) {
            try {
              Thread.sleep(50);
              if (System.currentTimeMillis() - sortStartTime > sortTimeout) {
                logger.error("waiting file {} to sort timeout", fileId);
                return null;
              }
              if (fileSorter.sortStatus.equals(SortStatus.failure)) {
                return null;
              }
            } catch (InterruptedException e) {
              logger.info("scheduler thread is interrupted means worker is shutting down.");
              return null;
            }
          }
        }
      }

      if (sorted.contains(fileId)) {
        return resolve(shuffleKey, fileId, sortedFileName, indexFileName,
          startMapIndex, endMapIndex);
      }

      return null;
    }
  }

  public void cleanup(HashSet<String> expirtedShuffleKeys) {
    for (String expiredShuffleKey : expirtedShuffleKeys) {
      sortingShuffleFiles.remove(expiredShuffleKey);
      sortedShuffleFiles.remove(expiredShuffleKey);
      cachedIndexMaps.remove(expiredShuffleKey);
    }
  }

  public void close() {
    fileSorterSchedulerThread.interrupt();
    fileSorterExecutors.shutdownNow();
    cachedIndexMaps.clear();
  }

  protected void writeIndex(Map<Integer, List<ShuffleBlockInfo>> indexMap, String indexFileName)
    throws IOException {
    File indexFile = new File(indexFileName);
    FileChannel indexFileChannel = new FileOutputStream(indexFile).getChannel();

    int indexSize = 0;
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : indexMap.entrySet()) {
      indexSize += 8;
      indexSize += entry.getValue().size() * 8;
    }

    ByteBuffer indexBuf = ByteBuffer.allocateDirect(indexSize);
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : indexMap.entrySet()) {
      int mapId = entry.getKey();
      List<ShuffleBlockInfo> list = entry.getValue();
      indexBuf.putInt(mapId);
      indexBuf.putInt(list.size());
      list.forEach(info -> {
        indexBuf.putInt(info.offset);
        indexBuf.putInt(info.length);
      });
    }

    indexBuf.flip();
    indexFileChannel.write(indexBuf);
    indexFileChannel.close();
    ((DirectBuffer) indexBuf).cleaner().clean();
  }

  protected Map<Integer, List<ShuffleBlockInfo>> readIndex(ByteBuffer indexBuf) {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();
    while (indexBuf.hasRemaining()) {
      int mapId = indexBuf.getInt();
      int count = indexBuf.getInt();
      List<ShuffleBlockInfo> blockInfos = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        int offset = indexBuf.getInt();
        int length = indexBuf.getInt();
        ShuffleBlockInfo info = new ShuffleBlockInfo();
        info.offset = offset;
        info.length = length;
        blockInfos.add(info);
      }
      indexMap.put(mapId, blockInfos);
    }
    return indexMap;
  }

  protected void readFully(FileChannel channel, ByteBuffer buffer, String filePath)
    throws IOException {
    while (buffer.hasRemaining()) {
      if (-1 == channel.read(buffer)) {
        throw new IOException("Unexpected EOF, file name : " + filePath);
      }
    }
  }

  private int transferFully(FileChannel originChannel, FileChannel targetChannel,
    int offset, int length) throws IOException {
    int transferedSize = 0;
    while (transferedSize != length) {
      transferedSize += originChannel.transferTo(offset + transferedSize,
        length - transferedSize, targetChannel);
    }
    return transferedSize;
  }

  ArrayList<Long> getChunkOffsets(int startMapIndex, int endMapIndex, String sortedFileName,
    Map<Integer, List<ShuffleBlockInfo>> indexMap) {
    ArrayList<Long> sortedChunkOffset = new ArrayList<>();
    ShuffleBlockInfo lastBlock = null;
    logger.debug("Refresh offsets for file {} , startMapIndex {} endMapIndex {}",
      sortedFileName, startMapIndex, endMapIndex);
    for (int i = startMapIndex; i < endMapIndex; i++) {
      List<ShuffleBlockInfo> blockInfos = indexMap.get(i);
      if (blockInfos != null) {
        for (ShuffleBlockInfo info : blockInfos) {
          if (sortedChunkOffset.size() == 0) {
            sortedChunkOffset.add((long) info.offset);
          }
          if (info.offset - sortedChunkOffset.get(sortedChunkOffset.size() - 1) > fetchChunkSize) {
            sortedChunkOffset.add((long) info.offset);
          }
          lastBlock = info;
        }
      }
    }
    if (lastBlock != null) {
      long endChunkOffset = lastBlock.length + lastBlock.offset;
      if (!sortedChunkOffset.contains(endChunkOffset)) {
        sortedChunkOffset.add(endChunkOffset);
      }
    }
    return sortedChunkOffset;
  }

  public FileInfo resolve(String shuffleKey, String fileId, String sortedFileName,
    String indexFileName, int startMapIndex, int endMapIndex) {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = null;
    if (cachedIndexMaps.containsKey(shuffleKey) &&
          cachedIndexMaps.get(shuffleKey).containsKey(fileId)) {
      indexMap = cachedIndexMaps.get(shuffleKey).get(fileId);
    } else {
      try (FileInputStream indexStream = new FileInputStream(indexFileName)) {
        File indexFile = new File(indexFileName);
        int indexSize = (int) indexFile.length();
        ByteBuffer indexBuf = ByteBuffer.allocateDirect(indexSize);
        readFully(indexStream.getChannel(), indexBuf, indexFileName);
        indexBuf.rewind();
        indexMap = readIndex(indexBuf);
        Map<String, Map<Integer, List<ShuffleBlockInfo>>> cacheMap =
          cachedIndexMaps.computeIfAbsent(shuffleKey, v -> new ConcurrentHashMap<>());
        cacheMap.put(fileId, indexMap);
      } catch (Exception e) {
        logger.error("Read sorted shuffle file error , detail : ", e);
        return null;
      }
    }
    return new FileInfo(new File(sortedFileName),
      getChunkOffsets(startMapIndex, endMapIndex, sortedFileName, indexMap));
  }

  class ShuffleBlockInfo {
    protected int offset;
    protected int length;
  }

  class FileSorter {
    private final File originFile;
    private final String sortedFileName;
    private final String indexFileName;
    private final long originFileLen;
    private final String fileId;
    private final String shuffleKey;
    private final boolean inMemSort;
    private SortStatus sortStatus = SortStatus.sorting;

    FileSorter(File originFile, long originFileLen, boolean inMemSort,
      String fileId, String shuffleKey) {
      this.originFile = originFile;
      this.sortedFileName = originFile.getAbsolutePath() + SORTED_SUFFIX;
      this.indexFileName = originFile.getAbsolutePath() + INDEX_SUFFIX;
      this.originFileLen = originFileLen;
      this.inMemSort = inMemSort;
      this.fileId = fileId;
      this.shuffleKey = shuffleKey;
    }

    public void sort() {
      try (FileChannel originFileChannel = new FileInputStream(originFile).getChannel();
           FileChannel sortedFileChannel = new FileOutputStream(sortedFileName).getChannel();) {
        int batchHeaderLen = 16;
        byte[] batchHeader = new byte[batchHeaderLen];

        Map<Integer, List<ShuffleBlockInfo>> originShuffleBlockInfos = new TreeMap<>();
        Map<Integer, List<ShuffleBlockInfo>> sortedBlockInfoMap = new HashMap<>();

        ByteBuffer headerBuf = null;
        ByteBuffer originFileBuf = null;
        if (inMemSort) {
          originFileBuf = ByteBuffer.allocateDirect((int) originFileLen);
          readFully(originFileChannel, originFileBuf, originFile.getAbsolutePath());
          originFileChannel.close();
          originFileBuf.flip();
        } else {
          headerBuf = ByteBuffer.allocate(batchHeaderLen);
        }

        int index = 0;
        while (index != originFileLen) {
          final int blockStartIndex = index;
          if (inMemSort) {
            originFileBuf.get(batchHeader);
          } else {
            readFully(originFileChannel, headerBuf, originFile.getAbsolutePath());
            batchHeader = headerBuf.array();
            headerBuf.rewind();
          }

          // header is 4 integers: mapId, attemptId, nextBatchId, compressedBlockTotalSize
          int mapId = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET);
          final int compressedSize = Platform.getInt(batchHeader,
            Platform.BYTE_ARRAY_OFFSET + 12);

          List<ShuffleBlockInfo> singleMapIdShuffleBlockList =
            originShuffleBlockInfos.computeIfAbsent(mapId,
              v -> new ArrayList<>());
          ShuffleBlockInfo blockInfo = new ShuffleBlockInfo();
          blockInfo.offset = blockStartIndex;
          blockInfo.length = compressedSize + 16;
          singleMapIdShuffleBlockList.add(blockInfo);

          index += batchHeaderLen + compressedSize;
          if (inMemSort) {
            originFileBuf.position(index);
          } else {
            originFileChannel.position(index);
          }
        }

        int fileIndex = 0;
        for (Map.Entry<Integer, List<ShuffleBlockInfo>>
               originBlockInfoEntry : originShuffleBlockInfos.entrySet()) {
          int mapId = originBlockInfoEntry.getKey();
          List<ShuffleBlockInfo> originShuffleBlocks = originBlockInfoEntry.getValue();
          List<ShuffleBlockInfo> sortedShuffleBlocks = new ArrayList<>();
          for (ShuffleBlockInfo blockInfo : originShuffleBlocks) {
            int offset = blockInfo.offset;
            int length = blockInfo.length;
            ShuffleBlockInfo sortedBlock = new ShuffleBlockInfo();
            sortedBlock.offset = fileIndex;
            sortedBlock.length = length;
            sortedShuffleBlocks.add(sortedBlock);
            if (inMemSort) {
              originFileBuf.limit(offset + length);
              originFileBuf.position(offset);
              fileIndex += sortedFileChannel.write(originFileBuf.slice());
            } else {
              fileIndex += transferFully(originFileChannel, sortedFileChannel, offset, length);
            }
          }
          sortedBlockInfoMap.put(mapId, sortedShuffleBlocks);
        }
        if (inMemSort) {
          ((DirectBuffer) originFileBuf).cleaner().clean();
        }

        sortedFileChannel.close();
        if (!originFile.delete()) {
          logger.warn("clean origin file failed, origin file is : {}",
            originFile.getAbsolutePath());
        }
        writeIndex(sortedBlockInfoMap, indexFileName);
        sortedShuffleFiles.get(shuffleKey).add(fileId);
        sortStatus = SortStatus.success;
        logger.debug("sort complete for {} {}", shuffleKey, originFile.getName());
      } catch (Exception e) {
        logger.error("sort shuffle file {} error", originFile.getName(), e);
        sortStatus = SortStatus.failure;
      } finally {
        sortingShuffleFiles.get(shuffleKey).remove(fileId);
      }
    }

    public long getOriginFileLen() {
      return originFileLen;
    }

    public boolean inMemSort() {
      return inMemSort;
    }
  }

  enum SortStatus {
    sorting,
    success,
    failure;
  }
}
