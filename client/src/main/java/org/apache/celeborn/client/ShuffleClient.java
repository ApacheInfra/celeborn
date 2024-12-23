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

package org.apache.celeborn.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.read.CelebornInputStream;
import org.apache.celeborn.client.read.MetricsCallback;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.util.CelebornHadoopUtils;
import org.apache.celeborn.common.util.ExceptionMaker;
import org.apache.celeborn.common.write.PushState;

/**
 * ShuffleClient may be a process singleton, the specific PartitionLocation should be hidden in the
 * implementation
 */
public abstract class ShuffleClient {
  private static Logger logger = LoggerFactory.getLogger(ShuffleClient.class);
  private static volatile ShuffleClient _instance;
  private static volatile boolean initialized = false;
  private static volatile Map<StorageInfo.Type, FileSystem> hadoopFs;
  private static LongAdder totalReadCounter = new LongAdder();
  private static LongAdder localShuffleReadCounter = new LongAdder();

  // for testing
  public static void reset() {
    _instance = null;
    initialized = false;
    hadoopFs = null;
  }

  protected ShuffleClient() {}

  public static ShuffleClient get(
      String appUniqueId,
      String driverHost,
      int port,
      CelebornConf conf,
      UserIdentifier userIdentifier) {
    return ShuffleClient.get(appUniqueId, driverHost, port, conf, userIdentifier, null);
  }

  public static ShuffleClient get(
      String appUniqueId,
      String driverHost,
      int port,
      CelebornConf conf,
      UserIdentifier userIdentifier,
      byte[] extension) {
    if (null == _instance || !initialized) {
      synchronized (ShuffleClient.class) {
        if (null == _instance) {
          // During the execution of Spark tasks, each task may be interrupted due to speculative
          // tasks. If the Task is interrupted while obtaining the ShuffleClient and the
          // ShuffleClient is building a singleton, it may cause the LifecycleManagerEndpoint to not
          // be
          // assigned. An Executor will only construct a ShuffleClient singleton once. At this time,
          // when communicating with LifecycleManager, it will cause a NullPointerException.
          _instance = new ShuffleClientImpl(appUniqueId, conf, userIdentifier);
          _instance.setupLifecycleManagerRef(driverHost, port);
          _instance.setExtension(extension);
          initialized = true;
        } else if (!initialized) {
          _instance.shutdown();
          _instance = new ShuffleClientImpl(appUniqueId, conf, userIdentifier);
          _instance.setupLifecycleManagerRef(driverHost, port);
          _instance.setExtension(extension);
          initialized = true;
        }
      }
    }
    return _instance;
  }

  public static Map<StorageInfo.Type, FileSystem> getHadoopFs(CelebornConf conf) {
    if (null == hadoopFs) {
      synchronized (ShuffleClient.class) {
        if (null == hadoopFs) {
          try {
            hadoopFs = CelebornHadoopUtils.getHadoopFS(conf);
          } catch (Exception e) {
            logger.error("Celeborn initialize DFS failed.", e);
          }
        }
      }
    }
    return hadoopFs;
  }

  public static void incrementLocalReadCounter() {
    localShuffleReadCounter.increment();
    totalReadCounter.increment();
  }

  public static void incrementTotalReadCounter() {
    totalReadCounter.increment();
  }

  public static void printReadStats(Logger logger) {
    long totalReadCount = totalReadCounter.longValue();
    long localReadCount = localShuffleReadCounter.longValue();
    logger.info(
        "Current client read {}/{} (local/total) partitions, local read ratio {}",
        localReadCount,
        totalReadCount,
        String.format("%.2f", (localReadCount * 1.0d / totalReadCount) * 100));
  }

  public abstract void setupLifecycleManagerRef(String host, int port);

  public abstract void setupLifecycleManagerRef(RpcEndpointRef endpointRef);

  /**
   * @param extension Extension for shuffle client, it's a byte array. Used in derived shuffle
   *     client implementation.
   */
  public abstract void setExtension(byte[] extension);

  /**
   * Write data to a specific reduce partition
   *
   * @param shuffleId the unique shuffle id of the application
   * @param mapId the map id of the shuffle
   * @param attemptId the attempt id of the map task, i.e. speculative task or task rerun for Apache
   *     Spark
   * @param partitionId the partition id the data belongs to
   * @param data byte array containing data to be pushed
   * @param offset start position of data to be pushed
   * @param length length of data to be pushed
   * @param numMappers the number map tasks in the shuffle
   * @param numPartitions the number of partitions in the shuffle
   * @return bytes pushed
   * @throws IOException
   */
  public abstract int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException;

  public abstract void prepareForMergeData(int shuffleId, int mapId, int attemptId)
      throws IOException;

  public abstract int mergeData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException;

  public abstract void pushMergedData(int shuffleId, int mapId, int attemptId) throws IOException;

  // Report partition locations written by the completed map task of ReducePartition Shuffle Type
  public abstract void mapperEnd(int shuffleId, int mapId, int attemptId, int numMappers)
      throws IOException;

  // Report partition locations written by the completed map task of MapPartition Shuffle Type
  public abstract void mapPartitionMapperEnd(
      int shuffleId, int mapId, int attemptId, int numMappers, int partitionId) throws IOException;

  // Cleanup states of the map task
  public abstract void cleanup(int shuffleId, int mapId, int attemptId);

  public abstract ShuffleClientImpl.ReduceFileGroups updateFileGroup(int shuffleId, int partitionId)
      throws CelebornIOException;

  // Reduce side read partition which is deduplicated by mapperId+mapperAttemptNum+batchId, batchId
  // is a self-incrementing variable hidden in the implementation when sending data.
  /**
   * @param shuffleId the unique shuffle id of the application
   * @param partitionId the partition id to read from
   * @param attemptNumber the attempt id of reduce task, can be safely set to any value
   * @param startMapIndex the index of start map index of interested map range, set to 0 if you want
   *     to read all partition data
   * @param endMapIndex the index of end map index of interested map range, set to
   *     `Integer.MAX_VALUE` if you want to read all partition data
   * @param metricsCallback callback to report metrics
   * @return
   * @throws IOException
   */
  public CelebornInputStream readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex,
      MetricsCallback metricsCallback)
      throws IOException {
    return readPartition(
        shuffleId,
        shuffleId,
        partitionId,
        attemptNumber,
        startMapIndex,
        endMapIndex,
        null,
        null,
        null,
        null,
        metricsCallback);
  }

  public abstract CelebornInputStream readPartition(
      int shuffleId,
      int appShuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex,
      ExceptionMaker exceptionMaker,
      ArrayList<PartitionLocation> locations,
      ArrayList<PbStreamHandler> streamHandlers,
      int[] mapAttempts,
      MetricsCallback metricsCallback)
      throws IOException;

  public abstract boolean cleanupShuffle(int shuffleId);

  public abstract void shutdown();

  public abstract PartitionLocation registerMapPartitionTask(
      int shuffleId, int numMappers, int mapId, int attemptId, int partitionId) throws IOException;

  public abstract ConcurrentHashMap<Integer, PartitionLocation> getPartitionLocation(
      int shuffleId, int numMappers, int numPartitions) throws CelebornIOException;

  public abstract PushState getPushState(String mapKey);

  public abstract int getShuffleId(
      int appShuffleId, String appShuffleIdentifier, boolean isWriter, boolean isBarrierStage);

  /**
   * report shuffle data fetch failure to LifecycleManager for special handling, eg, shuffle status
   * cleanup for spark app. It must be a sync call and make sure the cleanup is done, otherwise,
   * incorrect shuffle data can be fetched in re-run tasks
   */
  public abstract boolean reportShuffleFetchFailure(int appShuffleId, int shuffleId);

  /**
   * Report barrier task failure. When any barrier task fails, all (shuffle) output for that stage
   * attempt is to be discarded, and spark will recompute the entire stage
   */
  public abstract boolean reportBarrierTaskFailure(int appShuffleId, String appShuffleIdentifier);

  public abstract TransportClientFactory getDataClientFactory();

  public abstract void excludeFailedFetchLocation(String hostAndFetchPort, Exception e);
}
