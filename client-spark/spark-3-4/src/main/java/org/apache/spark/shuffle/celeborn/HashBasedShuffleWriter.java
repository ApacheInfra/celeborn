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
import java.util.concurrent.LinkedBlockingQueue;

import scala.Product2;

import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Private;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.write.DataPusher;
import org.apache.celeborn.client.write.PushTask;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.Utils;

@Private
public class HashBasedShuffleWriter<K, V, C> extends BasedShuffleWriter<K, V, C> {

  private static final Logger logger = LoggerFactory.getLogger(HashBasedShuffleWriter.class);

  private byte[][] sendBuffers;
  private int[] sendOffsets;
  private DataPusher dataPusher;
  private final SendBufferPool sendBufferPool;

  // In order to facilitate the writing of unit test code, ShuffleClient needs to be passed in as
  // parameters. By the way, simplify the passed parameters.
  public HashBasedShuffleWriter(
      int shuffleId,
      CelebornShuffleHandle<K, V, C> handle,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics,
      SendBufferPool sendBufferPool)
      throws IOException {
    super(shuffleId, handle, taskContext, conf, client, metrics);
    this.sendBufferPool = sendBufferPool;

    sendBuffers = sendBufferPool.acquireBuffer(numPartitions);
    sendOffsets = new int[numPartitions];

    try {
      LinkedBlockingQueue<PushTask> pushTaskQueue = sendBufferPool.acquirePushTaskQueue();
      dataPusher =
          new DataPusher(
              shuffleId,
              mapId,
              encodedAttemptId,
              taskContext.taskAttemptId(),
              numMappers,
              numPartitions,
              conf,
              shuffleClient,
              pushTaskQueue,
              writeMetrics::incBytesWritten,
              mapStatusLengths);
    } catch (InterruptedException e) {
      TaskInterruptedHelper.throwTaskKillException();
    }
  }

  @Override
  protected void fastWrite0(scala.collection.Iterator iterator)
      throws IOException, InterruptedException {
    final scala.collection.Iterator<Product2<Integer, UnsafeRow>> records = iterator;

    SQLMetric dataSize = SparkUtils.getDataSize((UnsafeRowSerializer) dep.serializer());
    while (records.hasNext()) {
      final Product2<Integer, UnsafeRow> record = records.next();
      final int partitionId = record._1();
      final UnsafeRow row = record._2();

      final int rowSize = row.getSizeInBytes();
      final int serializedRecordSize = 4 + rowSize;

      if (dataSize != null) {
        dataSize.add(rowSize);
      }

      if (serializedRecordSize > PUSH_BUFFER_MAX_SIZE) {
        byte[] giantBuffer = new byte[serializedRecordSize];
        Platform.putInt(giantBuffer, Platform.BYTE_ARRAY_OFFSET, Integer.reverseBytes(rowSize));
        Platform.copyMemory(
            row.getBaseObject(),
            row.getBaseOffset(),
            giantBuffer,
            Platform.BYTE_ARRAY_OFFSET + 4,
            rowSize);
        pushGiantRecord(partitionId, giantBuffer, serializedRecordSize);
      } else {
        int offset = getOrUpdateOffset(partitionId, serializedRecordSize);
        byte[] buffer = getOrCreateBuffer(partitionId);
        Platform.putInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset, Integer.reverseBytes(rowSize));
        Platform.copyMemory(
            row.getBaseObject(),
            row.getBaseOffset(),
            buffer,
            Platform.BYTE_ARRAY_OFFSET + offset + 4,
            rowSize);
        sendOffsets[partitionId] = offset + serializedRecordSize;
      }
      tmpRecordsWritten++;
    }
  }

  @Override
  protected void write0(scala.collection.Iterator iterator)
      throws IOException, InterruptedException {
    final scala.collection.Iterator<Product2<K, ?>> records = iterator;

    while (records.hasNext()) {
      final Product2<K, ?> record = records.next();
      final K key = record._1();
      final int partitionId = partitioner.getPartition(key);
      serBuffer.reset();
      serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
      serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
      serOutputStream.flush();

      final int serializedRecordSize = serBuffer.size();
      assert (serializedRecordSize > 0);

      if (serializedRecordSize > PUSH_BUFFER_MAX_SIZE) {
        pushGiantRecord(partitionId, serBuffer.getBuf(), serializedRecordSize);
      } else {
        int offset = getOrUpdateOffset(partitionId, serializedRecordSize);
        byte[] buffer = getOrCreateBuffer(partitionId);
        System.arraycopy(serBuffer.getBuf(), 0, buffer, offset, serializedRecordSize);
        sendOffsets[partitionId] = offset + serializedRecordSize;
      }
      tmpRecordsWritten++;
    }
  }

  @Override
  void updatePeakMemoryUsed() {
    // do nothing, hash shuffle writer always update this used peak memory
  }

  private byte[] getOrCreateBuffer(int partitionId) {
    byte[] buffer = sendBuffers[partitionId];
    if (buffer == null) {
      buffer = new byte[PUSH_BUFFER_INIT_SIZE];
      sendBuffers[partitionId] = buffer;
      peakMemoryUsedBytes += PUSH_BUFFER_INIT_SIZE;
    }
    return buffer;
  }

  private int getOrUpdateOffset(int partitionId, int serializedRecordSize)
      throws IOException, InterruptedException {
    int offset = sendOffsets[partitionId];
    byte[] buffer = getOrCreateBuffer(partitionId);
    while ((buffer.length - offset) < serializedRecordSize
        && buffer.length < PUSH_BUFFER_MAX_SIZE) {

      byte[] newBuffer = new byte[Math.min(buffer.length * 2, PUSH_BUFFER_MAX_SIZE)];
      peakMemoryUsedBytes += newBuffer.length - buffer.length;
      System.arraycopy(buffer, 0, newBuffer, 0, offset);
      sendBuffers[partitionId] = newBuffer;
      buffer = newBuffer;
    }

    if ((buffer.length - offset) < serializedRecordSize) {
      flushSendBuffer(partitionId, buffer, offset);
      updateRecordsWrittenMetrics();
      offset = 0;
    }
    return offset;
  }

  private void flushSendBuffer(int partitionId, byte[] buffer, int size)
      throws IOException, InterruptedException {
    long start = System.nanoTime();
    logger.debug("Flush buffer, size {}.", Utils.bytesToString(size));
    dataPusher.addTask(partitionId, buffer, size);
    writeMetrics.incWriteTime(System.nanoTime() - start);
  }

  @Override
  protected void closeWrite() throws IOException, InterruptedException {
    // here we wait for all the in-flight batches to return which sent by dataPusher thread
    dataPusher.waitOnTermination();
    sendBufferPool.returnPushTaskQueue(dataPusher.getAndResetIdleQueue());
    shuffleClient.prepareForMergeData(shuffleId, mapId, encodedAttemptId);
    // merge and push residual data to reduce network traffic
    // NB: since dataPusher thread have no in-flight data at this point,
    //     we now push merged data by task thread will not introduce any contention
    for (int i = 0; i < numPartitions; i++) {
      final int size = sendOffsets[i];
      if (size > 0) {
        mergeData(i, sendBuffers[i], 0, size);
        // free buffer
        sendBuffers[i] = null;
      }
    }
    sendBufferPool.returnBuffer(sendBuffers);
    sendBuffers = null;
    sendOffsets = null;
  }

  protected void mergeData(int partitionId, byte[] buffer, int offset, int length)
      throws IOException {
    int bytesWritten =
        shuffleClient.mergeData(
            shuffleId,
            mapId,
            encodedAttemptId,
            partitionId,
            buffer,
            offset,
            length,
            numMappers,
            numPartitions);
    mapStatusLengths[partitionId].add(bytesWritten);
    writeMetrics.incBytesWritten(bytesWritten);
  }

  @Override
  protected void cleanupPusher() throws IOException {
    try {
      dataPusher.waitOnTermination();
      sendBufferPool.returnPushTaskQueue(dataPusher.getAndResetIdleQueue());
    } catch (InterruptedException e) {
      TaskInterruptedHelper.throwTaskKillException();
    }
  }
}
