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

import org.apache.spark.*;
import org.apache.spark.internal.config.package$;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.rdd.DeterministicLevel;
import org.apache.spark.shuffle.*;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.ShuffleMode;
import org.apache.celeborn.reflect.DynMethods;

/**
 * In order to support Spark Stage resubmit with ShuffleReader FetchFails, Celeborn shuffleId has to
 * be distinguished from Spark shuffleId. Spark shuffleId is assigned at ShuffleDependency
 * construction time, and all Attempts of a Spark Stage have the same ShuffleId. When Celeborn
 * ShuffleReader fails to fetch shuffle data from worker and throws {@link FetchFailedException},
 * Spark DAGScheduler resubmits the failed ResultStage and corresponding ShuffleMapStage , but
 * Celeborn can't differentiate shuffle data from previous failed/resubmitted ShuffleMapStage with
 * the same shuffleId. Current solution takes Stage retry in account, and has LifecycleManager to
 * generate and track usage of spark shuffle id (appShuffleID) and Celeborn shuffle id (shuffle id).
 * Spark shuffle Reader/Write gets shuffleId from LifecycleManager with GetShuffleId RPC
 */
public class SparkShuffleManager implements ShuffleManager {

  private static final Logger logger = LoggerFactory.getLogger(SparkShuffleManager.class);

  private static final String SORT_SHUFFLE_MANAGER_NAME =
      "org.apache.spark.shuffle.sort.SortShuffleManager";

  private static final boolean COLUMNAR_SHUFFLE_CLASSES_PRESENT;

  static {
    boolean present;
    try {
      Class.forName(SparkUtils.COLUMNAR_HASH_BASED_SHUFFLE_WRITER_CLASS);
      Class.forName(SparkUtils.COLUMNAR_SHUFFLE_READER_CLASS);
      present = true;
    } catch (ClassNotFoundException e) {
      present = false;
    }
    COLUMNAR_SHUFFLE_CLASSES_PRESENT = present;
  }

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
    if (conf.getBoolean(SQLConf.LOCAL_SHUFFLE_READER_ENABLED().key(), true)) {
      logger.warn(
          "Detected {} (default is true) is enabled, it's highly recommended to disable it when "
              + "use Celeborn as Remote Shuffle Service to avoid performance degradation.",
          SQLConf.LOCAL_SHUFFLE_READER_ENABLED().key());
    }
    if ((Boolean) conf.get(package$.MODULE$.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED())) {
      String key = package$.MODULE$.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED().key();
      Boolean defaultValue =
          (Boolean) package$.MODULE$.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED().defaultValue().get();
      logger.warn(
          "Detected {} (default is {}) is enabled, "
              + "it's highly recommended to disable it when use Celeborn as Remote Shuffle Service "
              + "to avoid performance degradation.",
          key,
          defaultValue);
    }
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
          _sortShuffleManager =
              SparkUtils.instantiateClass(SORT_SHUFFLE_MANAGER_NAME, conf, isDriver);
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
          lifecycleManager.registerCancelShuffleCallback(SparkUtils::cancelShuffle);
          if (celebornConf.clientStageRerunEnabled()) {
            MapOutputTrackerMaster mapOutputTracker =
                (MapOutputTrackerMaster) SparkEnv.get().mapOutputTracker();

            lifecycleManager.registerShuffleTrackerCallback(
                shuffleId -> SparkUtils.unregisterAllMapOutput(mapOutputTracker, shuffleId));
          }
        }
      }
    }
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {
    // Note: generate app unique id at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    // This method may be called many times.
    String appId = SparkUtils.appUniqueId(dependency.rdd().context());
    initializeLifecycleManager(appId);

    lifecycleManager.shuffleCount().increment();
    if (fallbackPolicyRunner.applyFallbackPolicies(dependency, lifecycleManager)) {
      if (conf.getBoolean("spark.dynamicAllocation.enabled", false)
          && !conf.getBoolean("spark.shuffle.service.enabled", false)) {
        logger.error(
            "DRA is enabled but we fallback to vanilla Spark SortShuffleManager for "
                + "shuffle: {} due to fallback policy. It may cause block can not found when reducer "
                + "task fetch data.",
            shuffleId);
      } else {
        logger.warn("Fallback to vanilla Spark SortShuffleManager for shuffle: {}", shuffleId);
      }
      sortShuffleIds.add(shuffleId);
      return sortShuffleManager().registerShuffle(shuffleId, dependency);
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
          dependency.rdd().getNumPartitions(),
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
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    try {
      if (handle instanceof CelebornShuffleHandle) {
        @SuppressWarnings("unchecked")
        CelebornShuffleHandle<K, V, ?> h = ((CelebornShuffleHandle<K, V, ?>) handle);
        shuffleClient =
            ShuffleClient.get(
                h.appUniqueId(),
                h.lifecycleManagerHost(),
                h.lifecycleManagerPort(),
                celebornConf,
                h.userIdentifier(),
                h.extension());
        if (h.throwsFetchFailure()) {
          SparkUtils.addFailureListenerIfBarrierTask(shuffleClient, context, h);
        }
        int shuffleId = SparkUtils.celebornShuffleId(shuffleClient, h, context, true);
        shuffleIdTracker.track(h.shuffleId(), shuffleId);

        ShuffleMode shuffleMode = celebornConf.shuffleWriterMode();
        if (celebornConf.dynamicWriteModeEnabled()) {
          int partitionCount = h.dependency().partitioner().numPartitions();
          if (partitionCount > celebornConf.dynamicWriteModePartitionNumThreshold()) {
            logger.info(
                "Shuffle {} write mode is changed to SORT because "
                    + "partition count {} is greater than threshold {}",
                shuffleId,
                partitionCount,
                celebornConf.dynamicWriteModePartitionNumThreshold());
            shuffleMode = ShuffleMode.SORT;
          } else {
            logger.info(
                "Shuffle {} write mode is changed to HASH because "
                    + "partition count {} is less than threshold {}",
                shuffleId,
                partitionCount,
                celebornConf.dynamicWriteModePartitionNumThreshold());
            shuffleMode = ShuffleMode.HASH;
          }
        }

        if (ShuffleMode.SORT.equals(shuffleMode)) {
          return new SortBasedShuffleWriter<>(
              shuffleId,
              h,
              context,
              celebornConf,
              shuffleClient,
              metrics,
              SendBufferPool.get(cores, sendBufferPoolCheckInterval, sendBufferPoolExpireTimeout));
        } else if (ShuffleMode.HASH.equals(shuffleMode)) {
          SendBufferPool pool =
              SendBufferPool.get(cores, sendBufferPoolCheckInterval, sendBufferPoolExpireTimeout);
          if (COLUMNAR_SHUFFLE_CLASSES_PRESENT && celebornConf.columnarShuffleEnabled()) {
            logger.info("Creating columnar hash shuffle writer for shuffle {}", shuffleId);
            return SparkUtils.createColumnarHashBasedShuffleWriter(
                shuffleId, h, context, celebornConf, shuffleClient, metrics, pool);
          } else {
            return new HashBasedShuffleWriter<>(
                shuffleId, h, context, celebornConf, shuffleClient, metrics, pool);
          }
        } else {
          throw new UnsupportedOperationException(
              "Unrecognized shuffle write mode!" + celebornConf.shuffleWriterMode());
        }
      } else {
        checkUserClassPathFirst(handle);
        return sortShuffleManager().getWriter(handle, mapId, context, metrics);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Added in SPARK-32055, for Spark 3.1 and above
  @SuppressWarnings("MissingOverride")
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      return getCelebornShuffleReader(
          handle, startPartition, endPartition, startMapIndex, endMapIndex, context, metrics);
    }
    checkUserClassPathFirst(handle);
    return SparkUtils.getReader(
        sortShuffleManager(),
        handle,
        startPartition,
        endPartition,
        startMapIndex,
        endMapIndex,
        context,
        metrics);
  }

  // Marked as final in SPARK-32055, reserved for Spark 3.0
  @SuppressWarnings("MissingOverride")
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      return getCelebornShuffleReader(
          handle, startPartition, endPartition, 0, Integer.MAX_VALUE, context, metrics);
    }
    checkUserClassPathFirst(handle);
    return SparkUtils.getReader(
        sortShuffleManager(),
        handle,
        startPartition,
        endPartition,
        0,
        Integer.MAX_VALUE,
        context,
        metrics);
  }

  // Renamed to getReader in SPARK-32055, reserved for Spark 3.0
  public <K, C> ShuffleReader<K, C> getReaderForRange(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      return getCelebornShuffleReader(
          handle, startPartition, endPartition, startMapIndex, endMapIndex, context, metrics);
    }
    checkUserClassPathFirst(handle);
    return SparkUtils.getReader(
        sortShuffleManager(),
        handle,
        startPartition,
        endPartition,
        startMapIndex,
        endMapIndex,
        context,
        metrics);
  }

  public <K, C> ShuffleReader<K, C> getCelebornShuffleReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      int startMapIndex,
      int endMapIndex,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    CelebornShuffleHandle<K, ?, C> h = (CelebornShuffleHandle<K, ?, C>) handle;
    if (COLUMNAR_SHUFFLE_CLASSES_PRESENT && celebornConf.columnarShuffleEnabled()) {
      return SparkUtils.createColumnarShuffleReader(
          h,
          startPartition,
          endPartition,
          startMapIndex,
          endMapIndex,
          context,
          celebornConf,
          metrics,
          shuffleIdTracker);
    } else {
      return new CelebornShuffleReader<>(
          h,
          startPartition,
          endPartition,
          startMapIndex,
          endMapIndex,
          context,
          celebornConf,
          metrics,
          shuffleIdTracker);
    }
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
