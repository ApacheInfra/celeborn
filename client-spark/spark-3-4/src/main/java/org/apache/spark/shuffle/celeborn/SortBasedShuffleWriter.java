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
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;

@Private
public class SortBasedShuffleWriter<K, V, C> extends BasedShuffleWriter<K, V, C> {

  private static final Logger logger = LoggerFactory.getLogger(SortBasedShuffleWriter.class);
  private final SendBufferPool sendBufferPool;

  private final SortBasedPusher pusher;

  public SortBasedShuffleWriter(
      int shuffleId,
      CelebornShuffleHandle<K, V, C> handle,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics,
      SendBufferPool sendBufferPool)
      throws IOException {
    this(shuffleId, handle, taskContext, conf, client, metrics, sendBufferPool, null);
  }

  // In order to facilitate the writing of unit test code, ShuffleClient needs to be passed in as
  // parameters. By the way, simplify the passed parameters.
  public SortBasedShuffleWriter(
      int shuffleId,
      CelebornShuffleHandle<K, V, C> handle,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics,
      SendBufferPool sendBufferPool,
      SortBasedPusher pusher) {
    super(shuffleId, handle, taskContext, conf, client, metrics);
    this.sendBufferPool = sendBufferPool;
    if (pusher == null) {
      this.pusher =
          new SortBasedPusher(
              taskContext.taskMemoryManager(),
              shuffleClient,
              taskContext,
              shuffleId,
              mapId,
              encodedAttemptId,
              taskContext.taskAttemptId(),
              numMappers,
              numPartitions,
              conf,
              writeMetrics::incBytesWritten,
              mapStatusLengths,
              conf.clientPushSortMemoryThreshold(),
              sendBufferPool);
    } else {
      this.pusher = pusher;
    }
  }

  @Override
  protected void updatePeakMemoryUsed() {
    long mem = pusher.getPeakMemoryUsedBytes();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  @Override
  protected void fastWrite0(scala.collection.Iterator iterator) throws IOException {
    final scala.collection.Iterator<Product2<Integer, UnsafeRow>> records = iterator;

    SQLMetric dataSize = SparkUtils.getDataSize((UnsafeRowSerializer) dep.serializer());
    while (records.hasNext()) {
      final Product2<Integer, UnsafeRow> record = records.next();
      final int partitionId = record._1();
      final UnsafeRow row = record._2();

      final int rowSize = row.getSizeInBytes();
      final int serializedRecordSize = 4 + rowSize;

      if (dataSize != null) {
        dataSize.add(serializedRecordSize);
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
        boolean success =
            pusher.insertRecord(
                row.getBaseObject(), row.getBaseOffset(), rowSize, partitionId, true);
        if (!success) {
          doPush();
          success =
              pusher.insertRecord(
                  row.getBaseObject(), row.getBaseOffset(), rowSize, partitionId, true);
          if (!success) {
            throw new CelebornIOException("Unable to push after switching pusher!");
          }
        }
      }
      tmpRecordsWritten++;
    }
  }

  private void doPush() throws IOException {
    long start = System.nanoTime();
    pusher.pushData(true);
    writeMetrics.incWriteTime(System.nanoTime() - start);
  }

  @Override
  protected void write0(scala.collection.Iterator iterator) throws IOException {
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
        boolean success =
            pusher.insertRecord(
                serBuffer.getBuf(),
                Platform.BYTE_ARRAY_OFFSET,
                serializedRecordSize,
                partitionId,
                false);
        if (!success) {
          doPush();
          success =
              pusher.insertRecord(
                  serBuffer.getBuf(),
                  Platform.BYTE_ARRAY_OFFSET,
                  serializedRecordSize,
                  partitionId,
                  false);
          if (!success) {
            throw new IOException("Unable to push after switching pusher!");
          }
        }
      }
      tmpRecordsWritten++;
    }
  }

  @Override
  protected void cleanupPusher() throws IOException {
    if (pusher != null) {
      pusher.close(false);
    }
  }

  @Override
  protected void closeWrite() throws IOException, InterruptedException {
    long pushStartTime = System.nanoTime();
    pusher.pushData(false);
    pusher.close(true);
    writeMetrics.incWriteTime(System.nanoTime() - pushStartTime);
  }
}
