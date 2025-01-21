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

package org.apache.celeborn.service.deploy.worker.memory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.reflect.DynMethods;
import org.apache.celeborn.service.deploy.worker.storage.CreditStreamManager;
import org.apache.celeborn.service.deploy.worker.storage.PartitionDataWriter;
import org.apache.celeborn.service.deploy.worker.storage.StorageManager;

public class MemoryManager {
  private static final Logger logger = LoggerFactory.getLogger(MemoryManager.class);
  private static volatile MemoryManager _INSTANCE = null;
  @VisibleForTesting public long maxDirectMemory;
  private final long pausePushDataThreshold;
  private final long pauseReplicateThreshold;
  private final double resumeRatio;
  private final long maxSortMemory;
  private final int forceAppendPauseSpentTimeThreshold;
  private final List<MemoryPressureListener> memoryPressureListeners = new ArrayList<>();

  private final ScheduledExecutorService checkService =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-memory-manager-checker");

  private final ScheduledExecutorService reportService =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-memory-manager-reporter");

  private final ExecutorService actionService =
      ThreadUtils.newDaemonSingleThreadExecutor("worker-memory-manager-actor");

  private final AtomicBoolean trimInProcess = new AtomicBoolean(false);

  private final AtomicLong sortMemoryCounter = new AtomicLong(0);
  private final AtomicLong diskBufferCounter = new AtomicLong(0);
  private final LongAdder pausePushDataCounter = new LongAdder();
  private final LongAdder pausePushDataAndReplicateCounter = new LongAdder();
  public ServingState servingState = ServingState.NONE_PAUSED;
  private long pausePushDataStartTime = -1L;
  private long pausePushDataTime = 0L;
  private long pausePushDataAndReplicateStartTime = -1L;
  private long pausePushDataAndReplicateTime = 0L;
  private int trimCounter = 0;
  private volatile boolean isPaused = false;
  // For credit stream
  private final AtomicLong readBufferCounter = new AtomicLong(0);
  private long readBufferThreshold;
  private long readBufferTarget;
  private ReadBufferDispatcher readBufferDispatcher;
  private final List<ReadBufferTargetChangeListener> readBufferTargetChangeListeners =
      new ArrayList<>();
  private long lastNotifiedTarget = 0;
  private final ScheduledExecutorService readBufferTargetUpdateService =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
          "worker-memory-manager-read-buffer-target-updater");
  private CreditStreamManager creditStreamManager = null;

  private long memoryFileStorageThreshold;
  private final LongAdder memoryFileStorageCounter = new LongAdder();
  private final StorageManager storageManager;

  @VisibleForTesting
  public static MemoryManager initialize(CelebornConf conf) {
    return initialize(conf, null, null);
  }

  public static MemoryManager initialize(
      CelebornConf conf, StorageManager storageManager, AbstractSource source) {
    if (_INSTANCE == null) {
      _INSTANCE = new MemoryManager(conf, storageManager, source);
    }
    return _INSTANCE;
  }

  public void registerMemoryListener(MemoryPressureListener listener) {
    synchronized (memoryPressureListeners) {
      memoryPressureListeners.add(listener);
    }
  }

  public static MemoryManager instance() {
    return _INSTANCE;
  }

  private MemoryManager(CelebornConf conf, StorageManager storageManager, AbstractSource source) {
    double pausePushDataRatio = conf.workerDirectMemoryRatioToPauseReceive();
    double pauseReplicateRatio = conf.workerDirectMemoryRatioToPauseReplicate();
    this.resumeRatio = conf.workerDirectMemoryRatioToResume();
    double maxSortMemRatio = conf.workerPartitionSorterDirectMemoryRatioThreshold();
    double readBufferRatio = conf.workerDirectMemoryRatioForReadBuffer();
    double memoryFileStorageRatio = conf.workerDirectMemoryRatioForMemoryFilesStorage();
    long checkInterval = conf.workerDirectMemoryPressureCheckIntervalMs();
    long reportInterval = conf.workerDirectMemoryReportIntervalSecond();
    double readBufferTargetRatio = conf.readBufferTargetRatio();
    long readBufferTargetUpdateInterval = conf.readBufferTargetUpdateInterval();
    long readBufferTargetNotifyThreshold = conf.readBufferTargetNotifyThreshold();
    boolean aggressiveEvictModeEnabled = conf.workerMemoryFileStorageEictAggressiveModeEnabled();
    double evictRatio = conf.workerMemoryFileStorageEvictRatio();
    forceAppendPauseSpentTimeThreshold = conf.metricsWorkerForceAppendPauseSpentTimeThreshold();
    maxDirectMemory =
        DynMethods.builder("maxDirectMemory")
            .impl("jdk.internal.misc.VM") // for Java 10 and above
            .impl("sun.misc.VM") // for Java 9 and previous
            .buildStatic()
            .<Long>invoke();

    Preconditions.checkArgument(maxDirectMemory > 0);
    Preconditions.checkArgument(
        pauseReplicateRatio > pausePushDataRatio,
        String.format(
            "Invalid config, %s(%s) should be greater than %s(%s)",
            CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE().key(),
            pauseReplicateRatio,
            CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE().key(),
            pausePushDataRatio));
    Preconditions.checkArgument(pausePushDataRatio > resumeRatio);
    if (memoryFileStorageRatio > 0) {
      Preconditions.checkArgument(resumeRatio > (readBufferRatio + memoryFileStorageRatio));
    }

    maxSortMemory = ((long) (maxDirectMemory * maxSortMemRatio));
    pausePushDataThreshold = (long) (maxDirectMemory * pausePushDataRatio);
    pauseReplicateThreshold = (long) (maxDirectMemory * pauseReplicateRatio);
    readBufferThreshold = (long) (maxDirectMemory * readBufferRatio);
    readBufferTarget = (long) (readBufferThreshold * readBufferTargetRatio);
    memoryFileStorageThreshold = (long) (maxDirectMemory * memoryFileStorageRatio);

    checkService.scheduleWithFixedDelay(
        () -> {
          try {
            switchServingState();
          } catch (Exception e) {
            logger.error("Memory tracker check error", e);
          }
        },
        checkInterval,
        checkInterval,
        TimeUnit.MILLISECONDS);

    reportService.scheduleWithFixedDelay(
        () ->
            logger.info(
                "Direct memory usage: {}/{}, "
                    + "disk buffer size: {}, "
                    + "sort memory size: {}, "
                    + "read buffer size: {}, "
                    + "memory file storage size : {}",
                Utils.bytesToString(getNettyUsedDirectMemory()),
                Utils.bytesToString(maxDirectMemory),
                Utils.bytesToString(diskBufferCounter.get()),
                Utils.bytesToString(sortMemoryCounter.get()),
                Utils.bytesToString(readBufferCounter.get()),
                Utils.bytesToString(memoryFileStorageCounter.sum())),
        reportInterval,
        reportInterval,
        TimeUnit.SECONDS);

    if (readBufferThreshold > 0) {
      // if read buffer threshold is zero means that there will be no map data partitions
      readBufferDispatcher = new ReadBufferDispatcher(this, conf, source);
      readBufferTargetUpdateService.scheduleWithFixedDelay(
          () -> {
            try {
              if (creditStreamManager != null) {
                int mapDataPartitionCount = creditStreamManager.getActiveMapPartitionCount();
                if (mapDataPartitionCount > 0) {
                  long currentTarget =
                      (long) Math.ceil(readBufferTarget * 1.0 / mapDataPartitionCount);
                  if (Math.abs(lastNotifiedTarget - currentTarget)
                      > readBufferTargetNotifyThreshold) {
                    synchronized (readBufferTargetChangeListeners) {
                      logger.debug(
                          "read buffer target changed {} -> {} active map partition count {}",
                          lastNotifiedTarget,
                          currentTarget,
                          mapDataPartitionCount);
                      for (ReadBufferTargetChangeListener changeListener :
                          readBufferTargetChangeListeners) {
                        changeListener.onChange(currentTarget);
                      }
                      lastNotifiedTarget = currentTarget;
                    }
                  }
                }
              }
            } catch (Exception e) {
              logger.warn("Failed update buffer target", e);
            }
          },
          readBufferTargetUpdateInterval,
          readBufferTargetUpdateInterval,
          TimeUnit.MILLISECONDS);
    }

    this.storageManager = storageManager;
    if (memoryFileStorageThreshold > 0
        && storageManager != null
        && storageManager.localOrDfsStorageAvailable()) {
      ScheduledExecutorService memoryFileStorageService =
          ThreadUtils.newDaemonSingleThreadScheduledExecutor("memory-file-storage-checker");
      memoryFileStorageService.scheduleWithFixedDelay(
          () -> {
            try {
              if (shouldEvict(aggressiveEvictModeEnabled, evictRatio)) {
                List<PartitionDataWriter> memoryWriters =
                    new ArrayList<>(storageManager.memoryWriters().values());
                if (memoryWriters.isEmpty()) {
                  return;
                }
                logger.info("Start evicting {} memory file infos", memoryWriters.size());
                // always evict the largest memory file info first
                memoryWriters.sort(
                    Comparator.comparingLong(o -> o.getMemoryFileInfo().getFileLength()));
                Collections.reverse(memoryWriters);
                try {
                  for (PartitionDataWriter writer : memoryWriters) {
                    // this branch means that there is no memory pressure
                    if (!shouldEvict(aggressiveEvictModeEnabled, evictRatio)) {
                      break;
                    }
                    logger.debug("Evict writer {}", writer);
                    writer.evict(true);
                  }
                } catch (IOException e) {
                  logger.warn("Partition data writer evict failed", e);
                }
              }
            } catch (Exception e) {
              logger.error("Evict thread encounter error", e);
            }
          },
          checkInterval,
          checkInterval,
          TimeUnit.MILLISECONDS);
    }

    logger.info(
        "Memory tracker initialized with: "
            + "max direct memory: {}, pause push memory: {}, "
            + "pause replication memory: {},  "
            + "read buffer memory limit: {} target: {}, "
            + "memory shuffle storage limit: {}, "
            + "resume memory ratio: {}",
        Utils.bytesToString(maxDirectMemory),
        Utils.bytesToString(pausePushDataThreshold),
        Utils.bytesToString(pauseReplicateThreshold),
        Utils.bytesToString(readBufferThreshold),
        Utils.bytesToString(readBufferTarget),
        Utils.bytesToString(memoryFileStorageThreshold),
        resumeRatio);
  }

  public boolean shouldEvict(boolean aggressiveMemoryFileEvictEnabled, double evictRatio) {
    return servingState != ServingState.NONE_PAUSED
        && (aggressiveMemoryFileEvictEnabled
            || (memoryFileStorageCounter.sum() >= evictRatio * memoryFileStorageThreshold));
  }

  public ServingState currentServingState() {
    long memoryUsage = getMemoryUsage();
    // pause replicate threshold always greater than pause push data threshold
    // so when trigger pause replicate, pause both push and replicate
    if (memoryUsage > pauseReplicateThreshold) {
      isPaused = true;
      return ServingState.PUSH_AND_REPLICATE_PAUSED;
    }
    // trigger pause only push
    if (memoryUsage > pausePushDataThreshold) {
      isPaused = true;
      return ServingState.PUSH_PAUSED;
    }
    // trigger resume
    if (memoryUsage / (double) (maxDirectMemory) < resumeRatio) {
      isPaused = false;
      return ServingState.NONE_PAUSED;
    }
    // if isPaused and not trigger resume, then return pause push
    // wait for trigger resumeThreshold to resume state
    return isPaused ? ServingState.PUSH_PAUSED : ServingState.NONE_PAUSED;
  }

  @VisibleForTesting
  protected void switchServingState() {
    ServingState lastState = servingState;
    servingState = currentServingState();
    if (lastState == servingState) {
      if (servingState != ServingState.NONE_PAUSED) {
        logger.debug("Trigger action: TRIM");
        trimCounter += 1;
        // force to append pause spent time even we are in pause state
        if (trimCounter >= forceAppendPauseSpentTimeThreshold) {
          logger.debug(
              "Trigger action: TRIM for {} times, force to append pause spent time.", trimCounter);
          appendPauseSpentTime(lastState, servingState);
        }
        trimAllListeners();
      }
      return;
    }
    logger.info("Serving state transformed from {} to {}", lastState, servingState);
    switch (servingState) {
      case PUSH_PAUSED:
        if (lastState == ServingState.PUSH_AND_REPLICATE_PAUSED) {
          logger.info("Trigger action: RESUME REPLICATE");
          appendPauseSpentTime(lastState, servingState);
          memoryPressureListeners.forEach(
              memoryPressureListener ->
                  memoryPressureListener.onResume(TransportModuleConstants.REPLICATE_MODULE));
        } else if (lastState == ServingState.NONE_PAUSED) {
          logger.info("Trigger action: PAUSE PUSH");
          pausePushDataCounter.increment();
          pausePushDataStartTime = System.currentTimeMillis();
          memoryPressureListeners.forEach(
              memoryPressureListener ->
                  memoryPressureListener.onPause(TransportModuleConstants.PUSH_MODULE));
        }
        trimAllListeners();
        break;
      case PUSH_AND_REPLICATE_PAUSED:
        pausePushDataAndReplicateCounter.increment();
        long currentTime = System.currentTimeMillis();
        pausePushDataAndReplicateStartTime = currentTime;
        if (lastState == ServingState.NONE_PAUSED) {
          logger.info("Trigger action: PAUSE PUSH");
          pausePushDataCounter.increment();
          pausePushDataStartTime = currentTime;
          memoryPressureListeners.forEach(
              memoryPressureListener ->
                  memoryPressureListener.onPause(TransportModuleConstants.PUSH_MODULE));
        }
        logger.info("Trigger action: PAUSE REPLICATE");
        memoryPressureListeners.forEach(
            memoryPressureListener ->
                memoryPressureListener.onPause(TransportModuleConstants.REPLICATE_MODULE));
        trimAllListeners();
        break;
      case NONE_PAUSED:
        // resume from paused mode, append pause spent time
        appendPauseSpentTime(lastState, servingState);
        if (lastState == ServingState.PUSH_AND_REPLICATE_PAUSED) {
          logger.info("Trigger action: RESUME REPLICATE");
          memoryPressureListeners.forEach(
              memoryPressureListener ->
                  memoryPressureListener.onResume(TransportModuleConstants.REPLICATE_MODULE));
        }
        logger.info("Trigger action: RESUME PUSH");
        memoryPressureListeners.forEach(
            memoryPressureListener ->
                memoryPressureListener.onResume(TransportModuleConstants.PUSH_MODULE));
    }
  }

  public void trimAllListeners() {
    if (trimInProcess.compareAndSet(false, true)) {
      actionService.submit(
          () -> {
            try {
              // In current code, StorageManager will add into this before ChannelsLimiter,
              // so all behaviors of StorageManger will execute before ChannelsLimiter.
              memoryPressureListeners.forEach(MemoryPressureListener::onTrim);
            } finally {
              // MemoryManager uses this flag to avoid parallel trigger trim action,
              // We should make sure set this value back, otherwise it won't trigger trim action
              // again.
              trimInProcess.set(false);
            }
          });
    }
  }

  public void reserveSortMemory(long fileLen) {
    sortMemoryCounter.addAndGet(fileLen);
  }

  public boolean sortMemoryReady() {
    return maxSortMemory == 0
        || (servingState == ServingState.NONE_PAUSED && sortMemoryCounter.get() < maxSortMemory);
  }

  public void releaseSortMemory(long size) {
    synchronized (this) {
      if (sortMemoryCounter.get() - size < 0) {
        sortMemoryCounter.set(0);
      } else {
        sortMemoryCounter.addAndGet(-1L * size);
      }
    }
  }

  public void incrementDiskBuffer(int size) {
    diskBufferCounter.addAndGet(size);
  }

  public void releaseDiskBuffer(int size) {
    diskBufferCounter.addAndGet(size * -1);
  }

  public long getNettyUsedDirectMemory() {
    long usedDirectMemory = PlatformDependent.usedDirectMemory();
    assert usedDirectMemory != -1;
    return usedDirectMemory;
  }

  public long getMemoryUsage() {
    return getNettyUsedDirectMemory() + sortMemoryCounter.get();
  }

  public AtomicLong getSortMemoryCounter() {
    return sortMemoryCounter;
  }

  public AtomicLong getDiskBufferCounter() {
    return diskBufferCounter;
  }

  public long getReadBufferCounter() {
    return readBufferCounter.get();
  }

  public long getPausePushDataCounter() {
    return pausePushDataCounter.sum();
  }

  public void requestReadBuffers(ReadBufferRequest request) {
    readBufferDispatcher.addBufferRequest(request);
  }

  public void recycleReadBuffer(ByteBuf readBuf) {
    readBufferDispatcher.recycle(readBuf);
  }

  protected void changeReadBufferCounter(int delta) {
    readBufferCounter.addAndGet(delta);
  }

  @VisibleForTesting
  public boolean readBufferAvailable(int requiredBytes) {
    return readBufferCounter.get() + requiredBytes < readBufferThreshold;
  }

  public long getPausePushDataAndReplicateCounter() {
    return pausePushDataAndReplicateCounter.sum();
  }

  public long getAllocatedReadBuffers() {
    return readBufferDispatcher.getAllocatedReadBuffers();
  }

  public int dispatchRequestsLength() {
    return readBufferDispatcher.requestsLength();
  }

  public long getPausePushDataTime() {
    return pausePushDataTime;
  }

  public long getPausePushDataAndReplicateTime() {
    return pausePushDataAndReplicateTime;
  }

  private void appendPauseSpentTime(ServingState lastState, ServingState currentState) {
    long nextPauseStartTime = System.currentTimeMillis();
    if (lastState == ServingState.PUSH_AND_REPLICATE_PAUSED) {
      pausePushDataAndReplicateTime += nextPauseStartTime - pausePushDataAndReplicateStartTime;
      pausePushDataAndReplicateStartTime = nextPauseStartTime;
    }
    if (currentState == ServingState.NONE_PAUSED
            || (currentState == ServingState.PUSH_PAUSED && lastState == ServingState.PUSH_PAUSED)) {
      pausePushDataTime += nextPauseStartTime - pausePushDataStartTime;
      pausePushDataStartTime = nextPauseStartTime;
    }
    // reset
    trimCounter = 0;
  }

  public void addReadBufferTargetChangeListener(ReadBufferTargetChangeListener listener) {
    synchronized (readBufferTargetChangeListeners) {
      readBufferTargetChangeListeners.add(listener);
    }
  }

  public void removeReadBufferTargetChangeListener(ReadBufferTargetChangeListener listener) {
    synchronized (readBufferTargetChangeListeners) {
      readBufferTargetChangeListeners.remove(listener);
    }
  }

  public void setCreditStreamManager(CreditStreamManager creditStreamManager) {
    this.creditStreamManager = creditStreamManager;
  }

  public double workerMemoryUsageRatio() {
    return getMemoryUsage() / (double) (maxDirectMemory);
  }

  public long getMemoryFileStorageCounter() {
    return memoryFileStorageCounter.sum();
  }

  public boolean memoryFileStorageAvailable() {
    return memoryFileStorageCounter.sum() < memoryFileStorageThreshold;
  }

  public void incrementMemoryFileStorage(int bytes) {
    memoryFileStorageCounter.add(bytes);
  }

  public void releaseMemoryFileStorage(int bytes) {
    memoryFileStorageCounter.add(-1 * bytes);
  }

  public void close() {
    checkService.shutdown();
    reportService.shutdown();
    readBufferTargetUpdateService.shutdown();
    memoryPressureListeners.clear();
    actionService.shutdown();
    readBufferTargetChangeListeners.clear();
    readBufferDispatcher.close();
  }

  public ByteBufAllocator getStorageByteBufAllocator() {
    return storageManager.storageBufferAllocator();
  }

  @VisibleForTesting
  public static void reset() {
    _INSTANCE = null;
  }

  public interface MemoryPressureListener {
    void onPause(String moduleName);

    void onResume(String moduleName);

    void onTrim();
  }

  public interface ReadBufferTargetChangeListener {
    void onChange(long newMemoryTarget);
  }

  public enum ServingState {
    NONE_PAUSED,
    PUSH_AND_REPLICATE_PAUSED,
    PUSH_PAUSED
  }
}
