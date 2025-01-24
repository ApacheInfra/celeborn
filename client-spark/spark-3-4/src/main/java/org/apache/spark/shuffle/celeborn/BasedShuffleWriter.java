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

package org.apache.spark.shuffle.celeborn;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.storage.BlockManagerId;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;

public abstract class BasedShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

  protected static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();
  protected static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  protected final int PUSH_BUFFER_INIT_SIZE;
  protected final int PUSH_BUFFER_MAX_SIZE;
  protected final ShuffleDependency<K, V, C> dep;
  protected final Partitioner partitioner;
  protected final ShuffleWriteMetricsReporter writeMetrics;
  protected final int shuffleId;
  protected final int mapId;
  protected final int encodedAttemptId;
  protected final TaskContext taskContext;
  protected final ShuffleClient shuffleClient;
  protected final int numMappers;
  protected final int numPartitions;
  protected final OpenByteArrayOutputStream serBuffer;
  protected final SerializationStream serOutputStream;
  private final boolean unsafeRowFastWrite;

  protected final LongAdder[] mapStatusLengths;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true and
   * then call stop() with success = false if they get an exception, we want to make sure we don't
   * try deleting files, etc. twice.
   */
  private volatile boolean stopping = false;

  protected long peakMemoryUsedBytes = 0;
  protected long tmpRecordsWritten = 0;

  public BasedShuffleWriter(
      int shuffleId,
      CelebornShuffleHandle<K, V, C> handle,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics) {
    PUSH_BUFFER_INIT_SIZE = conf.clientPushBufferInitialSize();
    PUSH_BUFFER_MAX_SIZE = conf.clientPushBufferMaxSize();
    this.dep = handle.dependency();
    this.partitioner = dep.partitioner();
    this.writeMetrics = metrics;
    this.shuffleId = shuffleId;
    this.mapId = taskContext.partitionId();
    // [CELEBORN-1496] using the encoded attempt number instead of task attempt number
    this.encodedAttemptId = SparkCommonUtils.getEncodedAttemptNumber(taskContext);
    this.taskContext = taskContext;
    this.shuffleClient = client;
    this.numMappers = handle.numMappers();
    this.numPartitions = dep.partitioner().numPartitions();
    SerializerInstance serializer = dep.serializer().newInstance();
    serBuffer = new OpenByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
    unsafeRowFastWrite = conf.clientPushUnsafeRowFastWrite();

    mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
  }

  protected void doWrite(scala.collection.Iterator<Product2<K, V>> records)
      throws IOException, InterruptedException {
    if (canUseFastWrite()) {
      fastWrite0(records);
    } else if (dep.mapSideCombine()) {
      if (dep.aggregator().isEmpty()) {
        throw new UnsupportedOperationException(
            "When using map side combine, an aggregator must be specified.");
      }
      write0(dep.aggregator().get().combineValuesByKey(records, taskContext));
    } else {
      write0(records);
    }
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    boolean needCleanupPusher = true;
    try {
      doWrite(records);
      close();
      needCleanupPusher = false;
    } catch (InterruptedException e) {
      TaskInterruptedHelper.throwTaskKillException();
    } finally {
      if (needCleanupPusher) {
        cleanupPusher();
      }
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.empty();
      } else {
        stopping = true;
        if (success) {
          BlockManagerId bmId = SparkEnv.get().blockManager().shuffleServerId();
          MapStatus mapStatus =
              SparkUtils.createMapStatus(
                  bmId, SparkUtils.unwrap(mapStatusLengths), taskContext.taskAttemptId());
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.empty();
        }
      }
    } finally {
      shuffleClient.cleanup(shuffleId, mapId, encodedAttemptId);
    }
  }

  // Added in SPARK-32917, for Spark 3.2 and above
  @SuppressWarnings("MissingOverride")
  public long[] getPartitionLengths() {
    throw new UnsupportedOperationException(
        "Celeborn is not compatible with Spark push mode, please set spark.shuffle.push.enabled to false");
  }

  abstract void fastWrite0(scala.collection.Iterator iterator)
      throws IOException, InterruptedException;

  abstract void write0(scala.collection.Iterator iterator) throws IOException, InterruptedException;

  abstract void updatePeakMemoryUsed();

  abstract void cleanupPusher() throws IOException;

  abstract void closeWrite() throws IOException, InterruptedException;

  @VisibleForTesting
  boolean canUseFastWrite() {
    boolean keyIsPartitionId = false;
    if (unsafeRowFastWrite && dep.serializer() instanceof UnsafeRowSerializer) {
      // SPARK-39391 renames PartitionIdPassthrough's package
      String partitionerClassName = partitioner.getClass().getSimpleName();
      keyIsPartitionId = "PartitionIdPassthrough".equals(partitionerClassName);
    }
    return keyIsPartitionId;
  }

  /** Return the peak memory used so far, in bytes. */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  protected void pushGiantRecord(int partitionId, byte[] buffer, int numBytes) throws IOException {
    int bytesWritten =
        shuffleClient.pushData(
            shuffleId,
            mapId,
            encodedAttemptId,
            partitionId,
            buffer,
            0,
            numBytes,
            numMappers,
            numPartitions);
    mapStatusLengths[partitionId].add(bytesWritten);
    writeMetrics.incBytesWritten(bytesWritten);
  }

  /**
   * This method will push the remaining data and close these pushers. It's important, will send
   * Mapper End RPC to LifecycleManager to update the attempt of the corresponding task. We should
   * only call this method when the task is successfully completed.
   */
  protected void close() throws IOException, InterruptedException {
    long pushMergedDataTime = System.nanoTime();
    closeWrite();
    shuffleClient.pushMergedData(shuffleId, mapId, encodedAttemptId);
    writeMetrics.incWriteTime(System.nanoTime() - pushMergedDataTime);
    updateRecordsWrittenMetrics();

    long waitStartTime = System.nanoTime();
    shuffleClient.mapperEnd(shuffleId, mapId, encodedAttemptId, numMappers);
    writeMetrics.incWriteTime(System.nanoTime() - waitStartTime);
  }

  protected void updateRecordsWrittenMetrics() {
    writeMetrics.incRecordsWritten(tmpRecordsWritten);
    tmpRecordsWritten = 0;
  }
}
