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

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Private;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.execution.columnar.CelebornBatchBuilder;
import org.apache.spark.sql.execution.columnar.CelebornColumnarBatchBuilder;
import org.apache.spark.sql.execution.columnar.CelebornColumnarBatchCodeGenBuild;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;

@Private
public class ColumnarHashBasedShuffleWriter<K, V, C> extends HashBasedShuffleWriter<K, V, C> {

  private static final Logger logger =
      LoggerFactory.getLogger(ColumnarHashBasedShuffleWriter.class);

  private final int stageId;
  private final int shuffleId;
  private final CelebornBatchBuilder[] celebornBatchBuilders;
  private final StructType schema;
  private final Serializer depSerializer;
  private final boolean isColumnarShuffle;
  private final int columnarShuffleBatchSize;
  private final boolean columnarShuffleCodeGenEnabled;
  private final boolean columnarShuffleDictionaryEnabled;
  private final double columnarShuffleDictionaryMaxFactor;

  public ColumnarHashBasedShuffleWriter(
      int shuffleId,
      CelebornShuffleHandle<K, V, C> handle,
      TaskContext taskContext,
      CelebornConf conf,
      ShuffleClient client,
      ShuffleWriteMetricsReporter metrics,
      SendBufferPool sendBufferPool)
      throws IOException {
    super(shuffleId, handle, taskContext, conf, client, metrics, sendBufferPool);
    columnarShuffleBatchSize = conf.columnarShuffleBatchSize();
    columnarShuffleCodeGenEnabled = conf.columnarShuffleCodeGenEnabled();
    columnarShuffleDictionaryEnabled = conf.columnarShuffleDictionaryEnabled();
    columnarShuffleDictionaryMaxFactor = conf.columnarShuffleDictionaryMaxFactor();
    ShuffleDependency<?, ?, ?> shuffleDependency = handle.dependency();
    this.stageId = taskContext.stageId();
    this.shuffleId = shuffleDependency.shuffleId();
    this.schema = CustomShuffleDependencyUtils.getSchema(shuffleDependency);
    this.depSerializer = handle.dependency().serializer();
    this.celebornBatchBuilders =
        new CelebornBatchBuilder[handle.dependency().partitioner().numPartitions()];
    this.isColumnarShuffle = schema != null && CelebornBatchBuilder.supportsColumnarType(schema);
  }

  @Override
  protected void fastWrite0(scala.collection.Iterator iterator)
      throws IOException, InterruptedException {
    if (isColumnarShuffle) {
      logger.info("Fast columnar write of columnar shuffle {} for stage {}.", shuffleId, stageId);
      fastColumnarWrite0(iterator);
    } else {
      super.fastWrite0(iterator);
    }
  }

  private void fastColumnarWrite0(scala.collection.Iterator iterator) throws IOException {
    final scala.collection.Iterator<Product2<Integer, UnsafeRow>> records = iterator;

    SQLMetric dataSize = SparkUtils.getDataSize((UnsafeRowSerializer) depSerializer);
    while (records.hasNext()) {
      final Product2<Integer, UnsafeRow> record = records.next();
      final int partitionId = record._1();
      final UnsafeRow row = record._2();

      if (celebornBatchBuilders[partitionId] == null) {
        CelebornBatchBuilder columnBuilders;
        if (columnarShuffleCodeGenEnabled && !columnarShuffleDictionaryEnabled) {
          columnBuilders =
              new CelebornColumnarBatchCodeGenBuild().create(schema, columnarShuffleBatchSize);
        } else {
          columnBuilders =
              new CelebornColumnarBatchBuilder(
                  schema,
                  columnarShuffleBatchSize,
                  columnarShuffleDictionaryMaxFactor,
                  columnarShuffleDictionaryEnabled);
        }
        columnBuilders.newBuilders();
        celebornBatchBuilders[partitionId] = columnBuilders;
      }

      celebornBatchBuilders[partitionId].writeRow(row);
      if (celebornBatchBuilders[partitionId].getRowCnt() >= columnarShuffleBatchSize) {
        byte[] arr = celebornBatchBuilders[partitionId].buildColumnBytes();
        pushGiantRecord(partitionId, arr, arr.length);
        if (dataSize != null) {
          dataSize.add(arr.length);
        }
        celebornBatchBuilders[partitionId].newBuilders();
      }
      tmpRecordsWritten++;
    }
  }

  @Override
  protected void closeWrite() throws IOException, InterruptedException {
    if (canUseFastWrite() && isColumnarShuffle) {
      closeColumnarWrite();
    } else {
      super.closeWrite();
    }
  }

  private void closeColumnarWrite() throws IOException {
    SQLMetric dataSize = SparkUtils.getDataSize((UnsafeRowSerializer) depSerializer);
    for (int i = 0; i < celebornBatchBuilders.length; i++) {
      final CelebornBatchBuilder builders = celebornBatchBuilders[i];
      if (builders != null && builders.getRowCnt() > 0) {
        byte[] buffers = builders.buildColumnBytes();
        if (dataSize != null) {
          dataSize.add(buffers.length);
        }
        mergeData(i, buffers, 0, buffers.length);
        // free buffer
        celebornBatchBuilders[i] = null;
      }
    }
  }

  @VisibleForTesting
  public boolean isColumnarShuffle() {
    return isColumnarShuffle;
  }
}
