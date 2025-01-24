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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import scala.Int;

import org.apache.spark.*;
import org.apache.spark.internal.config.package$;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.rdd.DeterministicLevel;
import org.apache.spark.shuffle.*;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.ShuffleMode;
import org.apache.celeborn.reflect.DynMethods;

public class SparkShuffleManager implements ShuffleManager {

  private static final Logger logger = LoggerFactory.getLogger(SparkShuffleManager.class);

  private static final String sortShuffleManagerName =
      "org.apache.spark.shuffle.sort.SortShuffleManager";

  private final SparkConf conf;
  private final Boolean isDriver;
  private final CelebornConf celebornConf;
  private final int cores;
  // either be "{appId}_{appAttemptId}" or "{appId}"
  private String appUniqueId;

  private LifecycleManager lifecycleManager;
  private ShuffleClient shuffleClient;
  private volatile SortShuffleManager _sortShuffleManager;
  private final ConcurrentHashMap.KeySetView<Integer, Boolean> sortShuffleIds =
      ConcurrentHashMap.newKeySet();
  private final CelebornShuffleFallbackPolicyRunner fallbackPolicyRunner;

  private long sendBufferPoolCheckInterval;
  private long sendBufferPoolExpireTimeout;

  private ExecutorShuffleIdTracker shuffleIdTracker = new ExecutorShuffleIdTracker();

  public SparkShuffleManager(SparkConf conf, boolean isDriver) {
    SparkCommonUtils.validateAttemptConfig(conf);
    this.conf = conf;
    this.isDriver = isDriver;
    this.celebornConf = SparkUtils.fromSparkConf(conf);
    this.cores = executorCores(conf);
    this.fallbackPolicyRunner = new CelebornShuffleFallbackPolicyRunner(celebornConf);
    this.sendBufferPoolCheckInterval = celebornConf.clientPushSendBufferPoolExpireCheckInterval();
    this.sendBufferPoolExpireTimeout = celebornConf.clientPushSendBufferPoolExpireTimeout();
  }

  private SortShuffleManager sortShuffleManager() {
    if (_sortShuffleManager == null) {
      synchronized (this) {
        if (_sortShuffleManager == null) {
          _sortShuffleManager = SparkUtils.instantiateClass(sortShuffleManagerName, conf, isDriver);
        }
      }
    }
    return _sortShuffleManager;
  }

  private void initializeLifecycleManager(String appId) {
    // Only create LifecycleManager singleton in Driver. When register shuffle multiple times, we
    // need to ensure that LifecycleManager will only be created once. Parallelism needs to be
    // considered in this place, because if there is one RDD that depends on multiple RDDs
    // at the same time, it may bring parallel `register shuffle`, such as Join in Sql.
    if (isDriver && lifecycleManager == null) {
      synchronized (this) {
        if (lifecycleManager == null) {
          appUniqueId = celebornConf.appUniqueIdWithUUIDSuffix(appId);
          lifecycleManager = new LifecycleManager(appUniqueId, celebornConf);
          lifecycleManager.applicationCount().increment();
          lifecycleManager.registerCancelShuffleCallback(SparkUtils::cancelShuffle);
          if (celebornConf.clientStageRerunEnabled()) {
            MapOutputTrackerMaster mapOutputTracker =
                (MapOutputTrackerMaster) SparkEnv.get().mapOutputTracker();

            lifecycleManager.registerReportTaskShuffleFetchFailurePreCheck(
                taskId -> !SparkUtils.taskAnotherAttemptRunningOrSuccessful(taskId));
            SparkUtils.addSparkListener(new ShuffleFetchFailureReportTaskCleanListener());

            lifecycleManager.registerShuffleTrackerCallback(
                shuffleId -> mapOutputTracker.unregisterAllMapOutput(shuffleId));
          }
        }
      }
    }
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {
    // Note: generate app unique id at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    // This method may be called many times.
    String appId = SparkUtils.appUniqueId(dependency.rdd().context());
    initializeLifecycleManager(appId);

    lifecycleManager.shuffleCount().increment();
    if (fallbackPolicyRunner.applyFallbackPolicies(dependency, lifecycleManager)) {
      logger.warn("Fallback to SortShuffleManager!");
      sortShuffleIds.add(shuffleId);
      return sortShuffleManager().registerShuffle(shuffleId, numMaps, dependency);
    } else {
      lifecycleManager.registerAppShuffleDeterminate(
          shuffleId,
          !DeterministicLevel.INDETERMINATE()
              .equals(dependency.rdd().getOutputDeterministicLevel()));

      return new CelebornShuffleHandle<>(
          appUniqueId,
          lifecycleManager.getHost(),
          lifecycleManager.getPort(),
          lifecycleManager.getUserIdentifier(),
          shuffleId,
          celebornConf.clientStageRerunEnabled(),
          numMaps,
          dependency);
    }
  }

  @Override
  public boolean unregisterShuffle(int appShuffleId) {
    if (sortShuffleIds.remove(appShuffleId)) {
      return sortShuffleManager().unregisterShuffle(appShuffleId);
    }
    // For Spark driver side trigger unregister shuffle.
    if (lifecycleManager != null) {
      lifecycleManager.unregisterAppShuffle(appShuffleId, celebornConf.clientStageRerunEnabled());
    }
    // For Spark executor side cleanup shuffle related info.
    if (shuffleClient != null) {
      shuffleIdTracker.unregisterAppShuffleId(shuffleClient, appShuffleId);
    }
    return true;
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return sortShuffleManager().shuffleBlockResolver();
  }

  @Override
  public void stop() {
    if (shuffleClient != null) {
      shuffleClient.shutdown();
      ShuffleClient.reset();
      shuffleClient = null;
    }
    if (lifecycleManager != null) {
      lifecycleManager.stop();
      lifecycleManager = null;
    }
    if (_sortShuffleManager != null) {
      _sortShuffleManager.stop();
      _sortShuffleManager = null;
    }
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, int mapId, TaskContext context) {
    try {
      if (handle instanceof CelebornShuffleHandle) {
        @SuppressWarnings("unchecked")
        CelebornShuffleHandle<K, V, ?> h = ((CelebornShuffleHandle<K, V, ?>) handle);
        ShuffleClient client =
            ShuffleClient.get(
                h.appUniqueId(),
                h.lifecycleManagerHost(),
                h.lifecycleManagerPort(),
                celebornConf,
                h.userIdentifier(),
                h.extension());
        if (h.throwsFetchFailure()) {
          SparkUtils.addFailureListenerIfBarrierTask(client, context, h);
        }
        int shuffleId = SparkUtils.celebornShuffleId(client, h, context, true);
        shuffleIdTracker.track(h.shuffleId(), shuffleId);

        if (ShuffleMode.SORT.equals(celebornConf.shuffleWriterMode())) {
          return new SortBasedShuffleWriter<>(
              shuffleId,
              h.dependency(),
              h.numMaps(),
              context,
              celebornConf,
              client,
              SendBufferPool.get(cores, sendBufferPoolCheckInterval, sendBufferPoolExpireTimeout));
        } else if (ShuffleMode.HASH.equals(celebornConf.shuffleWriterMode())) {
          return new HashBasedShuffleWriter<>(
              shuffleId,
              h,
              mapId,
              context,
              celebornConf,
              client,
              SendBufferPool.get(cores, sendBufferPoolCheckInterval, sendBufferPoolExpireTimeout));
        } else {
          throw new UnsupportedOperationException(
              "Unrecognized shuffle write mode!" + celebornConf.shuffleWriterMode());
        }
      } else {
        checkUserClassPathFirst(handle);
        return sortShuffleManager().getWriter(handle, mapId, context);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle, int startPartition, int endPartition, TaskContext context) {
    if (handle instanceof CelebornShuffleHandle) {
      @SuppressWarnings("unchecked")
      CelebornShuffleHandle<K, ?, C> h = (CelebornShuffleHandle<K, ?, C>) handle;
      return new CelebornShuffleReader<>(
          h,
          startPartition,
          endPartition,
          0,
          Int.MaxValue(),
          context,
          celebornConf,
          shuffleIdTracker);
    }
    checkUserClassPathFirst(handle);
    return _sortShuffleManager.getReader(handle, startPartition, endPartition, context);
  }

  private int executorCores(SparkConf conf) {
    if (Utils.isLocalMaster(conf)) {
      // SparkContext.numDriverCores is package private.
      return DynMethods.builder("numDriverCores")
          .impl("org.apache.spark.SparkContext$", String.class)
          .build()
          .bind(SparkContext$.MODULE$)
          .invoke(conf.get("spark.master"));
    } else {
      return conf.getInt(SparkLauncher.EXECUTOR_CORES, 1);
    }
  }

  private void checkUserClassPathFirst(ShuffleHandle handle) {
    if ((Boolean) conf.get(package$.MODULE$.EXECUTOR_USER_CLASS_PATH_FIRST())
        && !Objects.equals(
            handle.getClass().getClassLoader(), CelebornShuffleHandle.class.getClassLoader())) {
      String key = package$.MODULE$.EXECUTOR_USER_CLASS_PATH_FIRST().key();
      Boolean defaultValue =
          (Boolean) package$.MODULE$.EXECUTOR_USER_CLASS_PATH_FIRST().defaultValue().get();
      logger.warn(
          "Detected {} (default is {}) is enabled, "
              + "it's highly recommended to disable it when use Celeborn as Remote Shuffle Service "
              + "to avoid falling back to vanilla Spark SortShuffleManager for ShuffleManger defined in user jar.",
          key,
          defaultValue);
    }
  }

  // for testing
  public LifecycleManager getLifecycleManager() {
    return this.lifecycleManager;
  }
}
