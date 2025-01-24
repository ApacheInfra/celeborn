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

package org.apache.celeborn.plugin.flink;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.NettyShuffleServiceFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;
import org.apache.flink.util.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.plugin.flink.fallback.ShuffleFallbackPolicy;
import org.apache.celeborn.plugin.flink.fallback.ShuffleFallbackPolicyRunner;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;

public class RemoteShuffleMaster implements ShuffleMaster<ShuffleDescriptor> {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleMaster.class);
  private final CelebornConf conf;
  private final ShuffleMasterContext shuffleMasterContext;
  // Flink JobId -> Celeborn register shuffleIds
  private final Map<JobID, Set<Integer>> jobShuffleIds = JavaUtils.newConcurrentHashMap();
  private final ConcurrentHashMap<JobID, String> jobFallbackPolicies =
      JavaUtils.newConcurrentHashMap();
  private String celebornAppId;
  private volatile LifecycleManager lifecycleManager;
  private final ShuffleTaskInfo shuffleTaskInfo = new ShuffleTaskInfo();
  private ShuffleResourceTracker shuffleResourceTracker;
  private final ScheduledExecutorService executor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
          "celeborn-client-remote-shuffle-master-executor");
  private final long lifecycleManagerTimestamp;
  private final NettyShuffleServiceFactory nettyShuffleServiceFactory;
  private volatile NettyShuffleMaster nettyShuffleMaster;

  public RemoteShuffleMaster(
      ShuffleMasterContext shuffleMasterContext,
      @Nullable NettyShuffleServiceFactory nettyShuffleServiceFactory) {
    Configuration configuration = shuffleMasterContext.getConfiguration();
    checkShuffleConfig(configuration);
    this.conf = FlinkUtils.toCelebornConf(configuration);
    this.shuffleMasterContext = shuffleMasterContext;
    this.lifecycleManagerTimestamp = System.currentTimeMillis();
    this.nettyShuffleServiceFactory = nettyShuffleServiceFactory;
  }

  @Override
  public void registerJob(JobShuffleContext context) {
    JobID jobID = context.getJobId();
    LOG.info("Register job: {}.", jobID);
    if (lifecycleManager == null) {
      synchronized (RemoteShuffleMaster.class) {
        if (lifecycleManager == null) {
          celebornAppId = FlinkUtils.toCelebornAppId(lifecycleManagerTimestamp, jobID);
          LOG.info("CelebornAppId: {}", celebornAppId);
          lifecycleManager = new LifecycleManager(celebornAppId, conf);
          this.shuffleResourceTracker = new ShuffleResourceTracker(executor, lifecycleManager);
        }
      }
    }

    lifecycleManager.applicationCount().increment();
    try {
      if (nettyShuffleServiceFactory != null) {
        Optional<ShuffleFallbackPolicy> shuffleFallbackPolicy =
            ShuffleFallbackPolicyRunner.getActivatedFallbackPolicy(context, conf, lifecycleManager);
        if (shuffleFallbackPolicy.isPresent()) {
          LOG.warn("Fallback to vanilla Flink NettyShuffleMaster for job: {}.", jobID);
          String jobFallbackPolicy = shuffleFallbackPolicy.get().getClass().getName();
          jobFallbackPolicies.put(jobID, jobFallbackPolicy);
          lifecycleManager.computeFallbackCounts(
              lifecycleManager.applicationFallbackCounts(), jobFallbackPolicy);
          nettyShuffleMaster().registerJob(context);
          return;
        }
      }
      Set<Integer> previousShuffleIds = jobShuffleIds.putIfAbsent(jobID, new HashSet<>());
      if (previousShuffleIds != null) {
        throw new RuntimeException("Duplicated registration job: " + jobID);
      }
      shuffleResourceTracker.registerJob(context);
    } catch (CelebornIOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void unregisterJob(JobID jobID) {
    LOG.info("Unregister job: {}.", jobID);
    if (jobFallbackPolicies.remove(jobID) != null) {
      nettyShuffleMaster().unregisterJob(jobID);
      return;
    }
    Set<Integer> shuffleIds = jobShuffleIds.remove(jobID);
    if (shuffleIds != null) {
      executor.execute(
          () -> {
            try {
              for (Integer shuffleId : shuffleIds) {
                lifecycleManager.unregisterShuffle(shuffleId);
                shuffleTaskInfo.removeExpiredShuffle(shuffleId);
              }
              shuffleResourceTracker.unRegisterJob(jobID);
            } catch (Throwable throwable) {
              LOG.error("Encounter an error when unregistering job: {}.", jobID, throwable);
            }
          });
    }
  }

  @Override
  public CompletableFuture<ShuffleDescriptor> registerPartitionWithProducer(
      JobID jobID, PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    return CompletableFuture.supplyAsync(
        () -> {
          lifecycleManager.shuffleCount().increment();
          String jobFallbackPolicy = jobFallbackPolicies.get(jobID);
          if (jobFallbackPolicy != null) {
            try {
              lifecycleManager.computeFallbackCounts(
                  lifecycleManager.shuffleFallbackCounts(), jobFallbackPolicy);
              return nettyShuffleMaster()
                  .registerPartitionWithProducer(jobID, partitionDescriptor, producerDescriptor)
                  .get();
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          } else {
            Set<Integer> shuffleIds = jobShuffleIds.get(jobID);
            if (shuffleIds == null) {
              throw new RuntimeException("Can not find job in lifecycleManager, job: " + jobID);
            }

            FlinkResultPartitionInfo resultPartitionInfo =
                new FlinkResultPartitionInfo(jobID, partitionDescriptor, producerDescriptor);
            ShuffleResourceDescriptor shuffleResourceDescriptor =
                shuffleTaskInfo.genShuffleResourceDescriptor(
                    resultPartitionInfo.getShuffleId(),
                    resultPartitionInfo.getTaskId(),
                    resultPartitionInfo.getAttemptId());

            synchronized (shuffleIds) {
              shuffleIds.add(shuffleResourceDescriptor.getShuffleId());
            }

            RemoteShuffleResource remoteShuffleResource =
                new RemoteShuffleResource(
                    lifecycleManager.getHost(),
                    lifecycleManager.getPort(),
                    lifecycleManagerTimestamp,
                    shuffleResourceDescriptor);

            shuffleResourceTracker.addPartitionResource(
                jobID,
                shuffleResourceDescriptor.getShuffleId(),
                shuffleResourceDescriptor.getPartitionId(),
                resultPartitionInfo.getResultPartitionId());

            return new RemoteShuffleDescriptor(
                celebornAppId,
                jobID,
                resultPartitionInfo.getShuffleId(),
                resultPartitionInfo.getResultPartitionId(),
                remoteShuffleResource);
          }
        },
        executor);
  }

  @Override
  public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
    executor.execute(
        () -> {
          if (shuffleDescriptor instanceof RemoteShuffleDescriptor) {
            try {
              RemoteShuffleDescriptor descriptor = (RemoteShuffleDescriptor) shuffleDescriptor;
              RemoteShuffleResource shuffleResource = descriptor.getShuffleResource();
              ShuffleResourceDescriptor resourceDescriptor =
                  shuffleResource.getMapPartitionShuffleDescriptor();
              LOG.debug("release partition resource: {}.", resourceDescriptor);
              lifecycleManager.releasePartition(
                  resourceDescriptor.getShuffleId(), resourceDescriptor.getPartitionId());
              shuffleResourceTracker.removePartitionResource(
                  descriptor.getJobId(),
                  resourceDescriptor.getShuffleId(),
                  resourceDescriptor.getPartitionId());
            } catch (Throwable throwable) {
              // it is not a problem if we failed to release the target data partition
              // because the session timeout mechanism will do the work for us latter
              LOG.debug("Failed to release data partition {}.", shuffleDescriptor, throwable);
            }
          } else if (shuffleDescriptor instanceof NettyShuffleDescriptor) {
            nettyShuffleMaster().releasePartitionExternally(shuffleDescriptor);
          } else {
            LOG.error(
                "Unsupported shuffle descriptor {}. Only supports {} and {}",
                shuffleDescriptor.getClass().getName(),
                RemoteShuffleDescriptor.class.getName(),
                NettyShuffleDescriptor.class.getName());
            shuffleMasterContext.onFatalError(
                new RuntimeException("Illegal shuffle descriptor type."));
          }
        });
  }

  @Override
  public MemorySize computeShuffleMemorySizeForTask(
      TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
    for (ResultPartitionType partitionType :
        taskInputsOutputsDescriptor.getPartitionTypes().values()) {
      boolean isBlockingShuffle = partitionType.isBlockingOrBlockingPersistentResultPartition();
      if (!isBlockingShuffle) {
        throw new RuntimeException(
            "Blocking result partition type expected but found " + partitionType);
      }
    }

    int numResultPartitions = taskInputsOutputsDescriptor.getSubpartitionNums().size();
    long numBytesPerPartition = conf.clientFlinkMemoryPerResultPartition();
    long numBytesForOutput = numBytesPerPartition * numResultPartitions;

    int numInputGates = taskInputsOutputsDescriptor.getInputChannelNums().size();
    long numBytesPerGate = conf.clientFlinkMemoryPerInputGate();
    long numBytesForInput = numBytesPerGate * numInputGates;

    LOG.debug(
        "Announcing number of bytes {} for output and {} for input.",
        numBytesForOutput,
        numBytesForInput);

    return new MemorySize(numBytesForInput + numBytesForOutput);
  }

  @Override
  public void close() throws Exception {
    try {
      jobFallbackPolicies.clear();
      jobShuffleIds.clear();
      LifecycleManager manager = lifecycleManager;
      if (null != manager) {
        manager.stop();
      }
      if (nettyShuffleMaster != null) {
        nettyShuffleMaster.close();
        nettyShuffleMaster = null;
      }
    } catch (Exception e) {
      LOG.warn("Encounter exception when shutdown: {}", e.getMessage(), e);
    }

    ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, executor);
  }

  /**
   * Checks the shuffle config given the Flink configuration.
   *
   * <p>The config option {@link ExecutionOptions#BATCH_SHUFFLE_MODE} should configure as {@link
   * BatchShuffleMode#ALL_EXCHANGES_BLOCKING}.
   *
   * @param configuration The Flink configuration with shuffle config.
   */
  private void checkShuffleConfig(Configuration configuration) {
    if (configuration.get(ExecutionOptions.BATCH_SHUFFLE_MODE)
        != BatchShuffleMode.ALL_EXCHANGES_BLOCKING) {
      throw new IllegalArgumentException(
          String.format(
              "The config option %s should configure as %s",
              ExecutionOptions.BATCH_SHUFFLE_MODE.key(),
              BatchShuffleMode.ALL_EXCHANGES_BLOCKING.name()));
    }
  }

  private NettyShuffleMaster nettyShuffleMaster() {
    if (nettyShuffleMaster == null) {
      synchronized (this) {
        if (nettyShuffleMaster == null) {
          nettyShuffleMaster = nettyShuffleServiceFactory.createShuffleMaster(shuffleMasterContext);
        }
      }
    }
    return nettyShuffleMaster;
  }

  @VisibleForTesting
  public LifecycleManager lifecycleManager() {
    return lifecycleManager;
  }

  @VisibleForTesting
  public ConcurrentHashMap<JobID, String> jobFallbackPolicies() {
    return jobFallbackPolicies;
  }
}
