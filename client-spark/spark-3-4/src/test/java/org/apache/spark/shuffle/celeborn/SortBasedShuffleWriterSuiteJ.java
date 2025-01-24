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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.ListBuffer;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkVersionUtil;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.UnifiedMemoryManager;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.celeborn.client.DummyShuffleClient;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.reflect.DynConstructors;

public class SortBasedShuffleWriterSuiteJ extends CelebornShuffleWriterSuiteBase {
  @Override
  protected ShuffleWriter<Integer, String> createShuffleWriter(
      CelebornShuffleHandle handle,
      TaskContext context,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics)
      throws IOException {
    return new SortBasedShuffleWriter<Integer, String, String>(
        SparkUtils.celebornShuffleId(client, handle, taskContext, true),
        handle,
        context,
        conf,
        client,
        metrics,
        SendBufferPool.get(4, 30, 60));
  }

  private SortBasedShuffleWriter<Integer, String, String> createShuffleWriterWithPusher(
      CelebornShuffleHandle handle,
      TaskContext context,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics,
      SortBasedPusher pusher)
      throws Exception {
    return new SortBasedShuffleWriter<Integer, String, String>(
        SparkUtils.celebornShuffleId(client, handle, taskContext, true),
        handle,
        context,
        conf,
        client,
        metrics,
        SendBufferPool.get(4, 30, 60),
        pusher);
  }

  private SortBasedPusher createSortBasedPusher(
      CelebornConf conf,
      File tempFile,
      int numPartitions,
      ShuffleWriteMetricsReporter metricsReporter)
      throws Exception {
    SparkConf sparkConf = new SparkConf(false).set("spark.buffer.pageSize", "32k");
    UnifiedMemoryManager unifiedMemoryManager = UnifiedMemoryManager.apply(sparkConf, 1);
    TaskMemoryManager taskMemoryManager = new TaskMemoryManager(unifiedMemoryManager, 0);

    final ShuffleClient client = new DummyShuffleClient(conf, tempFile);
    conf.set(CelebornConf.CLIENT_PUSH_SORT_USE_ADAPTIVE_MEMORY_THRESHOLD().key(), "true");
    LongAdder[] mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
    SortBasedPusher pusher =
        new SortBasedPusher(
            taskMemoryManager,
            /*shuffleClient=*/ client,
            /*taskContext=*/ taskContext,
            /*shuffleId=*/ 0,
            /*mapId=*/ 0,
            /*attemptNumber=*/ 0,
            /*taskAttemptId=*/ 0,
            /*numMappers=*/ 0,
            /*numPartitions=*/ numPartitions,
            conf,
            metricsReporter::incBytesWritten,
            mapStatusLengths,
            /*pushSortMemoryThreshold=*/ Utils.byteStringAsBytes("32K"),
            SendBufferPool.get(4, 30, 60));
    return pusher;
  }

  private String buildRecord(int size) {
    char[] record = new char[size];
    for (int i = 0; i < size; i++) {
      record[i] = 'a';
    }
    return new String(record);
  }

  private Iterator<Product2<Integer, UnsafeRow>> getUnsafeRowIterator(
      final int size, int recordSize, final AtomicInteger total, int numPartitions) {
    int current = 0;
    ListBuffer<Product2<Integer, UnsafeRow>> list = new ListBuffer<>();
    while (current < size) {
      int key = total.getAndIncrement();
      String value = buildRecord(recordSize);
      current += value.length();
      ListBuffer<Object> values = new ListBuffer<>();
      values.$plus$eq(UTF8String.fromString(value));

      InternalRow row = InternalRow.apply(values.toSeq());
      DataType[] types = new DataType[1];
      types[0] = StringType$.MODULE$;
      UnsafeRow unsafeRow = UnsafeProjection.create(types).apply(row);

      list.$plus$eq(new Tuple2<>(key % numPartitions, unsafeRow));
    }
    return list.toIterator();
  }

  @Test
  public void testAdaptiveMemoryThreshold() throws Exception {
    int numPartitions = 8;
    final UnsafeRowSerializer serializer = new UnsafeRowSerializer(1, null);
    final CelebornConf conf =
        new CelebornConf()
            .set(CelebornConf.CLIENT_PUSH_SORT_USE_ADAPTIVE_MEMORY_THRESHOLD().key(), "true")
            .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(), "32k");

    String PartitionIdPassthroughClazz;
    if (SparkVersionUtil.isGreaterThan(3, 3)) {
      PartitionIdPassthroughClazz = "org.apache.spark.PartitionIdPassthrough";
    } else {
      PartitionIdPassthroughClazz = "org.apache.spark.sql.execution.PartitionIdPassthrough";
    }
    DynConstructors.Ctor<Partitioner> partitionIdPassthroughCtor =
        DynConstructors.builder().impl(PartitionIdPassthroughClazz, int.class).build();
    final Partitioner partitioner = partitionIdPassthroughCtor.newInstance(numPartitions);
    Mockito.doReturn(partitioner).when(dependency).partitioner();
    Mockito.doReturn(serializer).when(dependency).serializer();

    final File tempFile = new File(tempDir, UUID.randomUUID().toString());
    final CelebornShuffleHandle<Integer, String, String> handle =
        new CelebornShuffleHandle<>(
            appId, host, port, userIdentifier, shuffleId, false, numMaps, dependency);
    final ShuffleClient client = new DummyShuffleClient(conf, tempFile);
    ((DummyShuffleClient) client).initReducePartitionMap(shuffleId, numPartitions, 1);
    File tempPusherFile = new File(tempDir, UUID.randomUUID().toString());
    SortBasedPusher pusher =
        createSortBasedPusher(conf, tempPusherFile, numPartitions, metrics.shuffleWriteMetrics());
    final SortBasedShuffleWriter<Integer, String, String> writer =
        createShuffleWriterWithPusher(
            handle, taskContext, conf, client, metrics.shuffleWriteMetrics(), pusher);
    AtomicInteger total = new AtomicInteger(0);
    System.out.println("time 1");
    Iterator iterator = getUnsafeRowIterator(32 * 1024, 8 * 1024, total, numPartitions);
    System.out.println(total.get());
    writer.doWrite(iterator);
    // given that the send buffer size is 32K and memory threshold is also 32K, with sortbasedwriter
    // we have pushed for 3 times (1 time per each existing partition),
    // so we will grow the threshold to 64K to mitigate too many pushes
    assertEquals(64 * 1024, pusher.getPushSortMemoryThreshold());
    System.out.println("time 2");
    iterator = getUnsafeRowIterator(32 * 1024, 8 * 1024, total, numPartitions);
    writer.doWrite(iterator);
    // no change on the threshold since we only pushed 32K more data
    assertEquals(64 * 1024, pusher.getPushSortMemoryThreshold());
    // write 32K more data will trigger the growth of threshold to 128K
    System.out.println("time 3");
    iterator = getUnsafeRowIterator(32 * 1024, 8 * 1024, total, numPartitions);
    writer.doWrite(iterator);
    assertEquals(128 * 1024, pusher.getPushSortMemoryThreshold());
    // some cleanup before the next test
    pusher.pushData(false);
    pusher.memoryThresholdManager.pushedCount = 0;
    pusher.memoryThresholdManager.pushedMemorySizeInBytes = 0;
    // now we insert 256K data but limited to 1 partition,
    // this time, we won't trigger the growing of memory threshold, because we have fully filled
    // the send buffer for each partition so that we are not "pushing data unnecessarily"
    System.out.println("time 4");
    iterator = getUnsafeRowIterator(256 * 1024, 8 * 1024, total, 1);
    writer.doWrite(iterator);
    assertEquals(128 * 1024, pusher.getPushSortMemoryThreshold());
  }
}
