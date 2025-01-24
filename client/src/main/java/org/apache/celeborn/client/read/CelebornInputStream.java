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

package org.apache.celeborn.client.read;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import scala.Tuple2;

import com.github.luben.zstd.ZstdException;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.buffer.ByteBuf;
import net.jpountz.lz4.LZ4Exception;
import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ClientUtils;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.compress.Decompressor;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.protocol.*;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.ExceptionMaker;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.write.PushFailedBatch;

public abstract class CelebornInputStream extends InputStream {
  private static final Logger logger = LoggerFactory.getLogger(CelebornInputStream.class);

  public static CelebornInputStream create(
      CelebornConf conf,
      TransportClientFactory clientFactory,
      String shuffleKey,
      ArrayList<PartitionLocation> locations,
      ArrayList<PbStreamHandler> streamHandlers,
      int[] attempts,
      Map<String, Set<PushFailedBatch>> failedBatchSetMap,
      Map<String, Pair<Integer, Integer>> chunksRange,
      int attemptNumber,
      long taskId,
      int startMapIndex,
      int endMapIndex,
      ConcurrentHashMap<String, Long> fetchExcludedWorkers,
      ShuffleClient shuffleClient,
      int appShuffleId,
      int shuffleId,
      int partitionId,
      ExceptionMaker exceptionMaker,
      MetricsCallback metricsCallback)
      throws IOException {
    if (locations == null || locations.isEmpty()) {
      return emptyInputStream;
    } else {
      // if startMapIndex > endMapIndex, means partition is skew partition and read by Celeborn
      // implementation.
      // locations will split to sub-partitions with startMapIndex size.
      boolean readSkewPartitionWithoutMapRange =
          ClientUtils.readSkewPartitionWithoutMapRange(conf, startMapIndex, endMapIndex);
      if (readSkewPartitionWithoutMapRange) {
        return new CelebornInputStreamImpl(
            conf,
            clientFactory,
            shuffleKey,
            locations,
            streamHandlers,
            attempts,
            failedBatchSetMap,
            attemptNumber,
            taskId,
            chunksRange,
            fetchExcludedWorkers,
            shuffleClient,
            appShuffleId,
            shuffleId,
            partitionId,
            exceptionMaker,
            true,
            metricsCallback);
      } else {
        return new CelebornInputStreamImpl(
            conf,
            clientFactory,
            shuffleKey,
            locations,
            streamHandlers,
            attempts,
            failedBatchSetMap,
            attemptNumber,
            taskId,
            startMapIndex,
            endMapIndex,
            /*partitionLocationToChunkRange = */ null,
            fetchExcludedWorkers,
            shuffleClient,
            appShuffleId,
            shuffleId,
            partitionId,
            exceptionMaker,
            false,
            metricsCallback);
      }
    }
  }

  public static CelebornInputStream empty() {
    return emptyInputStream;
  }

  private static final CelebornInputStream emptyInputStream =
      new CelebornInputStream() {
        @Override
        public int read() throws IOException {
          return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          return -1;
        }

        @Override
        public int totalPartitionsToRead() {
          return 0;
        }

        @Override
        public int partitionsRead() {
          return 0;
        }
      };

  public abstract int totalPartitionsToRead();

  public abstract int partitionsRead();

  private static final class CelebornInputStreamImpl extends CelebornInputStream {
    private static final Random RAND = new Random();

    private final CelebornConf conf;
    private final TransportClientFactory clientFactory;
    private final String shuffleKey;
    private ArrayList<PartitionLocation> locations;
    private ArrayList<PbStreamHandler> streamHandlers;
    private int[] attempts;
    private final int attemptNumber;
    private final long taskId;
    private final int startMapIndex;
    private final int endMapIndex;
    private final Map<String, Pair<Integer, Integer>> partitionLocationToChunkRange;

    private Map<Integer, Set<Integer>> batchesRead = new HashMap<>();

    private final Map<String, Set<PushFailedBatch>> failedBatches;

    private byte[] compressedBuf;
    private byte[] rawDataBuf;
    private Decompressor decompressor;

    private ByteBuf currentChunk;
    private boolean firstChunk = true;
    private PartitionReader currentReader;
    private final int fetchChunkMaxRetry;
    private int fetchChunkRetryCnt = 0;
    int retryWaitMs;
    private int fileIndex;
    private int position;
    private int limit;

    private MetricsCallback callback;

    // mapId, attemptId, batchId, size
    private final int BATCH_HEADER_SIZE = 4 * 4;
    private final byte[] sizeBuf = new byte[BATCH_HEADER_SIZE];
    private LongAdder skipCount = new LongAdder();
    private final boolean rangeReadFilter;
    private final boolean enabledReadLocalShuffle;
    private final String localHostAddress;

    private boolean shuffleCompressionEnabled;
    private long fetchExcludedWorkerExpireTimeout;
    private ConcurrentHashMap<String, Long> fetchExcludedWorkers;

    private boolean containLocalRead = false;
    private ShuffleClient shuffleClient;
    private int appShuffleId;
    private int shuffleId;
    private int partitionId;
    private ExceptionMaker exceptionMaker;
    private boolean closed = false;

    private final boolean readSkewPartitionWithoutMapRange;

    CelebornInputStreamImpl(
        CelebornConf conf,
        TransportClientFactory clientFactory,
        String shuffleKey,
        ArrayList<PartitionLocation> locations,
        ArrayList<PbStreamHandler> streamHandlers,
        int[] attempts,
        Map<String, Set<PushFailedBatch>> failedBatchSet,
        int attemptNumber,
        long taskId,
        Map<String, Pair<Integer, Integer>> partitionLocationToChunkRange,
        ConcurrentHashMap<String, Long> fetchExcludedWorkers,
        ShuffleClient shuffleClient,
        int appShuffleId,
        int shuffleId,
        int partitionId,
        ExceptionMaker exceptionMaker,
        boolean splitSkewPartitionWithoutMapRange,
        MetricsCallback metricsCallback)
        throws IOException {
      this(
          conf,
          clientFactory,
          shuffleKey,
          locations,
          streamHandlers,
          attempts,
          failedBatchSet,
          attemptNumber,
          taskId,
          /*startMapIndex = */ -1,
          /*endMapIndex = */ -1,
          partitionLocationToChunkRange,
          fetchExcludedWorkers,
          shuffleClient,
          appShuffleId,
          shuffleId,
          partitionId,
          exceptionMaker,
          splitSkewPartitionWithoutMapRange,
          metricsCallback);
    }

    CelebornInputStreamImpl(
        CelebornConf conf,
        TransportClientFactory clientFactory,
        String shuffleKey,
        ArrayList<PartitionLocation> locations,
        ArrayList<PbStreamHandler> streamHandlers,
        int[] attempts,
        Map<String, Set<PushFailedBatch>> failedBatchSet,
        int attemptNumber,
        long taskId,
        int startMapIndex,
        int endMapIndex,
        Map<String, Pair<Integer, Integer>> partitionLocationToChunkRange,
        ConcurrentHashMap<String, Long> fetchExcludedWorkers,
        ShuffleClient shuffleClient,
        int appShuffleId,
        int shuffleId,
        int partitionId,
        ExceptionMaker exceptionMaker,
        boolean readSkewPartitionWithoutMapRange,
        MetricsCallback metricsCallback)
        throws IOException {
      this.conf = conf;
      this.clientFactory = clientFactory;
      this.shuffleKey = shuffleKey;
      this.locations = locations;
      if (streamHandlers != null && streamHandlers.size() == locations.size()) {
        this.streamHandlers = streamHandlers;
      }
      this.attempts = attempts;
      this.attemptNumber = attemptNumber;
      this.taskId = taskId;
      this.startMapIndex = startMapIndex;
      this.endMapIndex = endMapIndex;
      this.partitionLocationToChunkRange = partitionLocationToChunkRange;
      this.rangeReadFilter = conf.shuffleRangeReadFilterEnabled();
      this.enabledReadLocalShuffle = conf.enableReadLocalShuffleFile();
      this.localHostAddress = Utils.localHostName(conf);
      this.shuffleCompressionEnabled =
          !conf.shuffleCompressionCodec().equals(CompressionCodec.NONE);
      this.fetchExcludedWorkerExpireTimeout = conf.clientFetchExcludedWorkerExpireTimeout();
      this.failedBatches = failedBatchSet;
      this.readSkewPartitionWithoutMapRange = readSkewPartitionWithoutMapRange;
      this.fetchExcludedWorkers = fetchExcludedWorkers;

      if (conf.clientPushReplicateEnabled()) {
        fetchChunkMaxRetry = conf.clientFetchMaxRetriesForEachReplica() * 2;
      } else {
        fetchChunkMaxRetry = conf.clientFetchMaxRetriesForEachReplica();
      }
      this.retryWaitMs = conf.networkIoRetryWaitMs(TransportModuleConstants.DATA_MODULE);
      this.callback = metricsCallback;
      this.exceptionMaker = exceptionMaker;
      this.partitionId = partitionId;
      this.appShuffleId = appShuffleId;
      this.shuffleId = shuffleId;
      this.shuffleClient = shuffleClient;

      boolean chunkPrefetchEnabled = conf.clientChunkPrefetchEnabled();
      moveToNextReader(chunkPrefetchEnabled);
      if (chunkPrefetchEnabled) {
        init();
        firstChunk = false;
      }
    }

    private boolean skipLocation(int startMapIndex, int endMapIndex, PartitionLocation location) {
      if (!rangeReadFilter) {
        return false;
      }
      if (endMapIndex == Integer.MAX_VALUE) {
        return false;
      }
      RoaringBitmap bitmap = location.getMapIdBitMap();
      if (bitmap == null && location.hasPeer()) {
        bitmap = location.getPeer().getMapIdBitMap();
      }
      for (int i = startMapIndex; i < endMapIndex; i++) {
        if (bitmap.contains(i)) {
          return false;
        }
      }
      return true;
    }

    private Tuple2<PartitionLocation, PbStreamHandler> nextReadableLocation() {
      int locationCount = locations.size();
      if (fileIndex >= locationCount) {
        return null;
      }
      PartitionLocation currentLocation = locations.get(fileIndex);
      // if pushShuffleFailureTrackingEnabled is true, should not skip location
      while ((readSkewPartitionWithoutMapRange
              && !partitionLocationToChunkRange.containsKey(currentLocation.getUniqueId()))
          || (!readSkewPartitionWithoutMapRange
              && skipLocation(startMapIndex, endMapIndex, currentLocation))) {
        skipCount.increment();
        fileIndex++;
        if (fileIndex == locationCount) {
          return null;
        }
        currentLocation = locations.get(fileIndex);
      }

      fetchChunkRetryCnt = 0;

      return new Tuple2(
          currentLocation, streamHandlers == null ? null : streamHandlers.get(fileIndex));
    }

    private void moveToNextReader(boolean fetchChunk) throws IOException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      Tuple2<PartitionLocation, PbStreamHandler> currentLocation = nextReadableLocation();
      if (currentLocation == null) {
        return;
      }
      currentReader = createReaderWithRetry(currentLocation._1, currentLocation._2);
      fileIndex++;
      while (!currentReader.hasNext()) {
        currentReader.close();
        currentReader = null;
        currentLocation = nextReadableLocation();
        if (currentLocation == null) {
          return;
        }
        currentReader = createReaderWithRetry(currentLocation._1, currentLocation._2);
        fileIndex++;
      }
      if (fetchChunk) {
        currentChunk = getNextChunk();
      }
    }

    private boolean isExcluded(PartitionLocation location) {
      Long timestamp = fetchExcludedWorkers.get(location.hostAndFetchPort());
      if (timestamp == null) {
        return false;
      } else if (System.currentTimeMillis() - timestamp > fetchExcludedWorkerExpireTimeout) {
        fetchExcludedWorkers.remove(location.hostAndFetchPort());
        return false;
      } else if (location.getPeer() != null) {
        Long peerTimestamp = fetchExcludedWorkers.get(location.getPeer().hostAndFetchPort());
        // To avoid both replicate locations is excluded, if peer add to excluded list earlier,
        // change to try peer.
        if (peerTimestamp == null || peerTimestamp < timestamp) {
          return true;
        } else {
          return false;
        }
      } else {
        return true;
      }
    }

    private PartitionReader createReaderWithRetry(
        PartitionLocation location, PbStreamHandler pbStreamHandler) throws IOException {
      // For the first time, the location will be selected according to attemptNumber
      if (fetchChunkRetryCnt == 0
          && attemptNumber % 2 == 1
          && location.hasPeer()
          && !readSkewPartitionWithoutMapRange) {
        location = location.getPeer();
        logger.debug("Read peer {} for attempt {}.", location, attemptNumber);
      }
      Exception lastException = null;
      while (fetchChunkRetryCnt < fetchChunkMaxRetry) {
        try {
          if (isExcluded(location)) {
            throw new CelebornIOException("Fetch data from excluded worker! " + location);
          }
          return createReader(location, pbStreamHandler, fetchChunkRetryCnt, fetchChunkMaxRetry);
        } catch (Exception e) {
          lastException = e;
          shuffleClient.excludeFailedFetchLocation(location.hostAndFetchPort(), e);
          fetchChunkRetryCnt++;
          if (location.hasPeer() && !readSkewPartitionWithoutMapRange) {
            // fetchChunkRetryCnt % 2 == 0 means both replicas have been tried,
            // so sleep before next try.
            if (fetchChunkRetryCnt % 2 == 0) {
              Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
            }
            logger.warn(
                "CreatePartitionReader failed {}/{} times for location {}, change to peer",
                fetchChunkRetryCnt,
                fetchChunkMaxRetry,
                location,
                e);
            location = location.getPeer();
          } else {
            logger.warn(
                "CreatePartitionReader failed {}/{} times for location {}, retry the same location",
                fetchChunkRetryCnt,
                fetchChunkMaxRetry,
                location,
                e);
            Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
          }
        }
      }
      throw new CelebornIOException("createPartitionReader failed! " + location, lastException);
    }

    private ByteBuf getNextChunk() throws IOException {
      while (fetchChunkRetryCnt < fetchChunkMaxRetry) {
        try {
          if (isExcluded(currentReader.getLocation())) {
            throw new CelebornIOException(
                "Fetch data from excluded worker! " + currentReader.getLocation());
          }
          return currentReader.next();
        } catch (Exception e) {
          shuffleClient.excludeFailedFetchLocation(
              currentReader.getLocation().hostAndFetchPort(), e);
          fetchChunkRetryCnt++;
          currentReader.close();
          if (fetchChunkRetryCnt == fetchChunkMaxRetry) {
            logger.warn("Fetch chunk fail exceeds max retry {}", fetchChunkRetryCnt, e);
            throw new CelebornIOException(
                "Fetch chunk failed for "
                    + fetchChunkRetryCnt
                    + " times for location "
                    + currentReader.getLocation(),
                e);
          } else {
            if (currentReader.getLocation().hasPeer() && !readSkewPartitionWithoutMapRange) {
              logger.warn(
                  "Fetch chunk failed {}/{} times for location {}, change to peer",
                  fetchChunkRetryCnt,
                  fetchChunkMaxRetry,
                  currentReader.getLocation(),
                  e);
              // fetchChunkRetryCnt % 2 == 0 means both replicas have been tried,
              // so sleep before next try.
              if (fetchChunkRetryCnt % 2 == 0) {
                Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
              }
              currentReader = createReaderWithRetry(currentReader.getLocation().getPeer(), null);
            } else {
              logger.warn(
                  "Fetch chunk failed {}/{} times for location {}",
                  fetchChunkRetryCnt,
                  fetchChunkMaxRetry,
                  currentReader.getLocation(),
                  e);
              Uninterruptibles.sleepUninterruptibly(retryWaitMs, TimeUnit.MILLISECONDS);
              currentReader = createReaderWithRetry(currentReader.getLocation(), null);
            }
          }
        }
      }
      throw new CelebornIOException("Fetch chunk failed! " + currentReader.getLocation());
    }

    private PartitionReader createReader(
        PartitionLocation location,
        PbStreamHandler pbStreamHandler,
        int fetchChunkRetryCnt,
        int fetchChunkMaxRetry)
        throws IOException, InterruptedException {
      logger.debug("Create reader for location {}", location);

      StorageInfo storageInfo = location.getStorageInfo();

      int startChunkIndex = -1;
      int endChunkIndex = -1;
      if (partitionLocationToChunkRange != null) {
        Pair<Integer, Integer> chunkRange =
            partitionLocationToChunkRange.get(location.getUniqueId());
        startChunkIndex = chunkRange.getLeft();
        endChunkIndex = chunkRange.getRight();
      }
      switch (storageInfo.getType()) {
        case HDD:
        case SSD:
        case MEMORY:
          if (enabledReadLocalShuffle
              && location.getHost().equals(localHostAddress)
              && storageInfo.getType() != StorageInfo.Type.MEMORY) {
            logger.debug("Read local shuffle file {}", localHostAddress);
            containLocalRead = true;
            return new LocalPartitionReader(
                conf, shuffleKey, location, clientFactory, startMapIndex, endMapIndex, callback);
          } else {
            return new WorkerPartitionReader(
                conf,
                shuffleKey,
                location,
                pbStreamHandler,
                clientFactory,
                startMapIndex,
                endMapIndex,
                fetchChunkRetryCnt,
                fetchChunkMaxRetry,
                callback,
                startChunkIndex,
                endChunkIndex);
          }
        case S3:
        case HDFS:
          return new DfsPartitionReader(
              conf, shuffleKey, location, clientFactory, startMapIndex, endMapIndex, callback);
        default:
          throw new CelebornIOException(
              String.format("Unknown storage info %s to read location %s", storageInfo, location));
      }
    }

    @Override
    public int read() throws IOException {
      if (position < limit) {
        int b = rawDataBuf[position];
        position++;
        return b & 0xFF;
      }

      if (!fillBuffer()) {
        return -1;
      }

      if (position >= limit) {
        return read();
      } else {
        int b = rawDataBuf[position];
        position++;
        return b & 0xFF;
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }

      int readBytes = 0;
      while (readBytes < len) {
        while (position >= limit) {
          if (!fillBuffer()) {
            return readBytes > 0 ? readBytes : -1;
          }
        }

        int bytesToRead = Math.min(limit - position, len - readBytes);
        System.arraycopy(rawDataBuf, position, b, off + readBytes, bytesToRead);
        position += bytesToRead;
        readBytes += bytesToRead;
      }

      return readBytes;
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        int locationsCount = locations.size();
        logger.debug(
            "AppShuffleId {}, shuffleId {}, partitionId {}, total location count {}, read {}, skip {}",
            appShuffleId,
            shuffleId,
            partitionId,
            locationsCount,
            locationsCount - skipCount.sum(),
            skipCount.sum());
        if (currentChunk != null) {
          logger.debug("Release chunk {}", currentChunk);
          currentChunk.release();
          currentChunk = null;
        }
        if (currentReader != null) {
          logger.debug("Closing reader");
          currentReader.close();
          currentReader = null;
        }
        if (containLocalRead) {
          ShuffleClient.printReadStats(logger);
        }

        compressedBuf = null;
        rawDataBuf = null;
        batchesRead = null;
        locations = null;
        attempts = null;
        decompressor = null;
        fetchExcludedWorkers = null;

        closed = true;
      }
    }

    private boolean moveToNextChunk() throws IOException {
      if (currentChunk != null) {
        currentChunk.release();
      }
      currentChunk = null;
      if (currentReader.hasNext()) {
        currentChunk = getNextChunk();
        return true;
      } else if (fileIndex < locations.size()) {
        moveToNextReader(true);
        return currentReader != null;
      }
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      return false;
    }

    private void init() {
      int bufferSize = conf.clientFetchBufferSize();

      if (shuffleCompressionEnabled) {
        int headerLen = Decompressor.getCompressionHeaderLength(conf);
        bufferSize += headerLen;
        compressedBuf = new byte[bufferSize];
        decompressor = Decompressor.getDecompressor(conf);
      }
      rawDataBuf = new byte[bufferSize];
    }

    private boolean fillBuffer() throws IOException {
      try {
        if (firstChunk && currentReader != null) {
          init();
          currentChunk = getNextChunk();
          firstChunk = false;
        }
        if (currentChunk == null) {
          close();
          return false;
        }

        PushFailedBatch failedBatch = new PushFailedBatch(-1, -1, -1);
        boolean hasData = false;
        while (currentChunk.isReadable() || moveToNextChunk()) {
          currentChunk.readBytes(sizeBuf);
          int mapId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET);
          int attemptId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 4);
          int batchId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 8);
          int size = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 12);

          if (shuffleCompressionEnabled) {
            if (size > compressedBuf.length) {
              compressedBuf = new byte[size];
            }

            currentChunk.readBytes(compressedBuf, 0, size);
          } else {
            if (size > rawDataBuf.length) {
              rawDataBuf = new byte[size];
            }

            currentChunk.readBytes(rawDataBuf, 0, size);
          }

          // de-duplicate
          if (attemptId == attempts[mapId]) {
            if (readSkewPartitionWithoutMapRange) {
              Set<PushFailedBatch> failedBatchSet =
                  this.failedBatches.get(currentReader.getLocation().getUniqueId());
              if (null != failedBatchSet) {
                failedBatch.setMapId(mapId);
                failedBatch.setAttemptId(attemptId);
                failedBatch.setBatchId(batchId);
                if (failedBatchSet.contains(failedBatch)) {
                  logger.warn("Skip duplicated batch: {}.", failedBatch);
                  continue;
                }
              }
            }
            if (!batchesRead.containsKey(mapId)) {
              Set<Integer> batchSet = new HashSet<>();
              batchesRead.put(mapId, batchSet);
            }
            Set<Integer> batchSet = batchesRead.get(mapId);
            if (!batchSet.contains(batchId)) {
              batchSet.add(batchId);
              callback.incBytesRead(BATCH_HEADER_SIZE + size);
              if (shuffleCompressionEnabled) {
                // decompress data
                int originalLength = decompressor.getOriginalLen(compressedBuf);
                if (rawDataBuf.length < originalLength) {
                  rawDataBuf = new byte[originalLength];
                }
                limit = decompressor.decompress(compressedBuf, rawDataBuf, 0);
              } else {
                limit = size;
              }
              position = 0;
              hasData = true;
              break;
            } else {
              logger.debug(
                  "Skip duplicated batch: mapId {}, attemptId {}, batchId {}.",
                  mapId,
                  attemptId,
                  batchId);
            }
          }
        }

        return hasData;
      } catch (LZ4Exception | ZstdException | IOException e) {
        logger.error(
            "Failed to fill buffer from chunk. AppShuffleId {}, shuffleId {}, partitionId {}, location {}",
            appShuffleId,
            shuffleId,
            partitionId,
            Optional.ofNullable(currentReader).map(PartitionReader::getLocation).orElse(null),
            e);
        IOException ioe;
        if (e instanceof IOException) {
          ioe = (IOException) e;
        } else {
          ioe = new IOException(e);
        }
        if (exceptionMaker != null) {
          if (shuffleClient.reportShuffleFetchFailure(appShuffleId, shuffleId, taskId)) {
            /*
             * [[ExceptionMaker.makeException]], for spark applications with celeborn.client.spark.fetch.throwsFetchFailure enabled will result in creating
             * a FetchFailedException; and that will make the TaskContext as failed with shuffle fetch issues - see SPARK-19276 for more.
             * Given this, Celeborn can wrap the FetchFailedException with our CelebornIOException
             */
            ioe =
                new CelebornIOException(
                    exceptionMaker.makeFetchFailureException(
                        appShuffleId, shuffleId, partitionId, e));
          }
        }
        throw ioe;
      } catch (Exception e) {
        logger.error(
            "Failed to fill buffer from chunk. AppShuffleId {}, shuffleId {}, partitionId {}, location {}",
            appShuffleId,
            shuffleId,
            partitionId,
            Optional.ofNullable(currentReader).map(PartitionReader::getLocation).orElse(null),
            e);
        throw e;
      }
    }

    @Override
    public int totalPartitionsToRead() {
      return locations.size();
    }

    @Override
    public int partitionsRead() {
      return fileIndex;
    }
  }
}
