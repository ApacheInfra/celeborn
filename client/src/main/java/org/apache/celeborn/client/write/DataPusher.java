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

package org.apache.celeborn.client.write;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;

public class DataPusher {
  private static final Logger logger = LoggerFactory.getLogger(DataPusher.class);

  private final long WAIT_TIME_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

  private final LinkedBlockingQueue<PushTask> idleQueue;
  // partition -> PushTask Queue
  private final DataPushQueue dataPushQueue;
  private final ReentrantLock idleLock = new ReentrantLock();
  private final Condition idleFull = idleLock.newCondition();

  private final AtomicReference<IOException> exceptionRef = new AtomicReference<>();

  private final String appId;
  private final int shuffleId;
  private final int mapId;
  private final int attemptId;
  private final int numMappers;
  private final int numPartitions;
  private final ShuffleClient client;
  private final Consumer<Integer> afterPush;

  private volatile boolean terminated;
  private final LongAdder[] mapStatusLengths;
  private Thread pushThread;

  public DataPusher(
      String appId,
      int shuffleId,
      int mapId,
      int attemptId,
      long taskId,
      int numMappers,
      int numPartitions,
      CelebornConf conf,
      ShuffleClient client,
      Consumer<Integer> afterPush,
      LongAdder[] mapStatusLengths)
      throws IOException {
    final int pushQueueCapacity = conf.pushQueueCapacity();
    final int pushBufferMaxSize = conf.pushBufferMaxSize();

    idleQueue = new LinkedBlockingQueue<>(pushQueueCapacity);
    dataPushQueue =
        new DataPushQueue(
            conf, this, client, appId, shuffleId, mapId, attemptId, numMappers, numPartitions);

    for (int i = 0; i < pushQueueCapacity; i++) {
      try {
        idleQueue.put(new PushTask(pushBufferMaxSize));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CelebornIOException(e);
      }
    }

    this.appId = appId;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.client = client;
    this.afterPush = afterPush;
    this.mapStatusLengths = mapStatusLengths;

    pushThread =
        new Thread("DataPusher-" + taskId) {
          private void reclaimTask(PushTask task) throws InterruptedException {
            idleLock.lockInterruptibly();
            try {
              idleQueue.put(task);
              if (idleQueue.remainingCapacity() == 0) {
                idleFull.signal();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              exceptionRef.set(new CelebornIOException(e));
            } finally {
              idleLock.unlock();
            }
          }

          @Override
          public void run() {
            while (stillRunning()) {
              try {
                PushTask task = dataPushQueue.takePushTask();
                if (Objects.isNull(task)) {
                  continue;
                }
                pushData(task);
                reclaimTask(task);
              } catch (CelebornIOException e) {
                exceptionRef.set(e);
              } catch (InterruptedException | IOException e) {
                exceptionRef.set(new CelebornIOException(e));
              }
            }
          }
        };
    pushThread.setDaemon(true);
    pushThread.start();
  }

  public void addTask(int partitionId, byte[] buffer, int size) throws IOException {
    try {
      PushTask task = null;
      while (task == null) {
        checkException();
        task = idleQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
      }
      task.setSize(size);
      task.setPartitionId(partitionId);
      System.arraycopy(buffer, 0, task.getBuffer(), 0, size);
      while (!dataPushQueue.addPushTask(task)) {
        checkException();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException ioe = new CelebornIOException(e);
      exceptionRef.set(ioe);
      throw ioe;
    }
  }

  public void waitOnTermination() throws IOException {
    try {
      idleLock.lockInterruptibly();
      waitIdleQueueFullWithLock();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      exceptionRef.set(new CelebornIOException(e));
    }

    terminated = true;
    try {
      pushThread.join();
    } catch (InterruptedException ignored) {
      logger.info("Thread interrupted while joining pushThread");
    }
    idleQueue.clear();
    dataPushQueue.clear();
    checkException();
  }

  private void checkException() throws IOException {
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
  }

  private void pushData(PushTask task) throws IOException {
    int bytesWritten =
        client.pushData(
            appId,
            shuffleId,
            mapId,
            attemptId,
            task.getPartitionId(),
            task.getBuffer(),
            0,
            task.getSize(),
            numMappers,
            numPartitions);
    afterPush.accept(bytesWritten);
    mapStatusLengths[task.getPartitionId()].add(bytesWritten);
  }

  private void waitIdleQueueFullWithLock() {
    try {
      while (idleQueue.remainingCapacity() > 0 && exceptionRef.get() == null) {
        idleFull.await(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      exceptionRef.set(new CelebornIOException(e));
    } finally {
      idleLock.unlock();
    }
  }

  protected boolean stillRunning() {
    return !terminated && !Objects.nonNull(exceptionRef.get());
  }

  public DataPushQueue getDataPushQueue() {
    return dataPushQueue;
  }
}
