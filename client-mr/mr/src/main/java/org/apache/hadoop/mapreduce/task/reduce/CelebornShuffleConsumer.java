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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle.ShuffleError;
import org.apache.hadoop.util.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.read.CelebornInputStream;
import org.apache.celeborn.client.read.MetricsCallback;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.reflect.DynConstructors;
import org.apache.celeborn.reflect.DynMethods;
import org.apache.celeborn.util.HadoopUtils;

public class CelebornShuffleConsumer<K, V>
    implements ShuffleConsumerPlugin<K, V>, ExceptionReporter {
  private static final Logger logger = LoggerFactory.getLogger(CelebornShuffleConsumer.class);
  private JobConf mrJobConf;
  private MergeManager<K, V> merger;
  private Throwable throwable = null;
  private Progress copyPhase;
  private TaskStatus taskStatus;
  private org.apache.hadoop.mapreduce.TaskAttemptID reduceId;
  private TaskUmbilicalProtocol umbilical;
  private Reporter reporter;
  private ShuffleClientMetrics metrics;
  private Task reduceTask;
  private ShuffleClient shuffleClient;

  @Override
  public void init(Context<K, V> context) {

    reduceId = context.getReduceId();
    mrJobConf = context.getJobConf();
    JobConf celebornJobConf = new JobConf(HadoopUtils.MR_CELEBORN_CONF);

    umbilical = context.getUmbilical();
    reporter = context.getReporter();
    try {
      this.metrics = createMetrics(reduceId, mrJobConf);
    } catch (Exception e) {
      logger.error("Fatal error occurred, failed to get shuffle client metrics.", e);
      reportException(e);
    }
    copyPhase = context.getCopyPhase();
    taskStatus = context.getStatus();
    reduceTask = context.getReduceTask();

    String appId = celebornJobConf.get(HadoopUtils.MR_CELEBORN_APPLICATION_ID);
    String lmHost = celebornJobConf.get(HadoopUtils.MR_CELEBORN_LM_HOST);
    int lmPort = Integer.parseInt(celebornJobConf.get(HadoopUtils.MR_CELEBORN_LM_PORT));
    logger.info("Reducer initialized with celeborn {} {} {}", appId, lmHost, lmPort);
    CelebornConf celebornConf = HadoopUtils.fromYarnConf(mrJobConf);
    shuffleClient =
        ShuffleClient.get(
            appId,
            lmHost,
            lmPort,
            celebornConf,
            new UserIdentifier(
                celebornConf.userSpecificTenant(), celebornConf.userSpecificUserName()));
    this.merger =
        new MergeManagerImpl<>(
            reduceId,
            mrJobConf,
            context.getLocalFS(),
            context.getLocalDirAllocator(),
            reporter,
            context.getCodec(),
            context.getCombinerClass(),
            context.getCombineCollector(),
            context.getSpilledRecordsCounter(),
            context.getReduceCombineInputCounter(),
            context.getMergedMapOutputsCounter(),
            this,
            context.getMergePhase(),
            context.getMapOutputFile());
  }

  private ShuffleClientMetrics createMetrics(
      org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptID, JobConf jobConf)
      throws NoSuchMethodException {
    // for hadoop 3.1+ see MAPREDUCE-6861
    try {
      return DynMethods.builder("create")
          .impl(
              ShuffleClientMetrics.class,
              org.apache.hadoop.mapreduce.TaskAttemptID.class,
              JobConf.class)
          .buildStaticChecked()
          .invoke(taskAttemptID, jobConf);
    } catch (Exception e) {
      // ignore this exception because the createMetrics might use hadoop2
    }

    // for hadoop 3.1 see MAPREDUCE-6526
    try {
      return DynMethods.builder("create")
          .impl(ShuffleClientMetrics.class)
          .buildStaticChecked()
          .invoke(taskAttemptID, jobConf);
    } catch (Exception e) {
    }

    // for hadoop 2
    return DynConstructors.builder(ShuffleClientMetrics.class)
        .hiddenImpl(new Class[] {org.apache.hadoop.mapreduce.TaskAttemptID.class, JobConf.class})
        .buildChecked()
        .invoke(null, taskAttemptID, jobConf);
  }

  @Override
  public RawKeyValueIterator run() throws IOException {
    logger.info(
        "In reduce:{}, Celeborn mr client start to read shuffle data."
            + " Create inputstream with params: shuffleId 0 reduceId:{} attemptId:{}",
        reduceId,
        reduceId.getTaskID().getId(),
        reduceId.getId());

    MetricsCallback metricsCallback =
        new MetricsCallback() {
          @Override
          public void incBytesRead(long bytesRead) {}

          @Override
          public void incReadTime(long time) {}
        };

    CelebornInputStream shuffleInputStream =
        shuffleClient.readPartition(
            0,
            reduceId.getTaskID().getId(),
            reduceId.getId(),
            0,
            Integer.MAX_VALUE,
            metricsCallback);
    CelebornShuffleFetcher<K, V> shuffleReader =
        new CelebornShuffleFetcher(
            reduceId, taskStatus, merger, copyPhase, reporter, metrics, shuffleInputStream);
    shuffleReader.fetchAndMerge();

    copyPhase.complete();
    taskStatus.setPhase(TaskStatus.Phase.SORT);
    reduceTask.statusUpdate(umbilical);

    RawKeyValueIterator kvIter;
    try {
      kvIter = merger.close();
    } catch (Throwable e) {
      throw new ShuffleError("Error while doing final merge ", e);
    }

    logger.info("In reduce: {} Celeborn mr client read shuffle data complete", reduceId);

    return kvIter;
  }

  @Override
  public void close() {}

  @Override
  public void reportException(Throwable throwable) {
    if (this.throwable == null) {
      this.throwable = throwable;
    }
  }
}
