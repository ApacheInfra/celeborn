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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import scala.Option;
import scala.Some;
import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.BarrierTaskContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.TaskContext;
import org.apache.spark.scheduler.DAGScheduler;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.scheduler.ShuffleMapStage;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.scheduler.TaskSchedulerImpl;
import org.apache.spark.scheduler.TaskSetManager;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.reflect.DynFields;

public class SparkUtils {
  private static final Logger logger = LoggerFactory.getLogger(SparkUtils.class);

  public static final String FETCH_FAILURE_ERROR_MSG = "Celeborn FetchFailure with shuffle id ";

  public static MapStatus createMapStatus(
      BlockManagerId loc, long[] uncompressedSizes, long[] uncompressedRecords) throws IOException {

    MapStatus$ status = MapStatus$.MODULE$;
    Class<?> clz = status.getClass();
    Method applyMethod = null;

    for (Method method : clz.getDeclaredMethods()) {
      if ("apply".equals(method.getName())) {
        applyMethod = method;
        break;
      }
    }

    if (applyMethod == null) {
      throw new IOException("Could not find apply method in MapStatus object.");
    }

    try {
      switch (applyMethod.getParameterCount()) {
        case 2:
          // spark 2 without adaptive execution
          return (MapStatus) applyMethod.invoke(status, loc, uncompressedSizes);
        case 3:
          // spark 2 with adaptive execution
          return (MapStatus)
              applyMethod.invoke(status, loc, uncompressedSizes, uncompressedRecords);
        default:
          throw new IllegalStateException(
              "Could not find apply method with correct parameter number in MapStatus object.");
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static SQLMetric getUnsafeRowSerializerDataSizeMetric(UnsafeRowSerializer serializer) {
    try {
      Field field = serializer.getClass().getDeclaredField("dataSize");
      field.setAccessible(true);
      return (SQLMetric) field.get(serializer);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      logger.warn("Failed to get dataSize metric, aqe won`t work properly.");
    }
    return null;
  }

  public static long[] unwrap(LongAdder[] adders) {
    int adderCounter = adders.length;
    long[] res = new long[adderCounter];
    for (int i = 0; i < adderCounter; i++) {
      res[i] = adders[i].longValue();
    }
    return res;
  }

  /** make celeborn conf from spark conf */
  public static CelebornConf fromSparkConf(SparkConf conf) {
    CelebornConf tmpCelebornConf = new CelebornConf();
    for (Tuple2<String, String> kv : conf.getAll()) {
      if (kv._1.startsWith("spark.celeborn.")) {
        tmpCelebornConf.set(kv._1.substring("spark.".length()), kv._2);
      }
    }
    return tmpCelebornConf;
  }

  public static String appUniqueId(SparkContext context) {
    if (context.applicationAttemptId().isDefined()) {
      return context.applicationId() + "_" + context.applicationAttemptId().get();
    } else {
      return context.applicationId();
    }
  }

  public static String getAppShuffleIdentifier(int appShuffleId, TaskContext context) {
    return appShuffleId + "-" + context.stageId() + "-" + context.stageAttemptNumber();
  }

  public static int celebornShuffleId(
      ShuffleClient client,
      CelebornShuffleHandle<?, ?, ?> handle,
      TaskContext context,
      Boolean isWriter) {
    if (handle.throwsFetchFailure()) {
      String appShuffleIdentifier = getAppShuffleIdentifier(handle.shuffleId(), context);
      return client.getShuffleId(
          handle.shuffleId(),
          appShuffleIdentifier,
          isWriter,
          context instanceof BarrierTaskContext);
    } else {
      return handle.shuffleId();
    }
  }

  // Create an instance of the class with the given name, possibly initializing it with our conf
  // Copied from SparkEnv
  public static <T> T instantiateClass(String className, SparkConf conf, Boolean isDriver) {
    @SuppressWarnings("unchecked")
    Class<T> cls = (Class<T>) Utils.classForName(className);
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      return cls.getConstructor(SparkConf.class, Boolean.TYPE).newInstance(conf, isDriver);
    } catch (ReflectiveOperationException roe1) {
      try {
        return cls.getConstructor(SparkConf.class).newInstance(conf);
      } catch (ReflectiveOperationException roe2) {
        try {
          return cls.getConstructor().newInstance();
        } catch (ReflectiveOperationException roe3) {
          throw new RuntimeException(roe3);
        }
      }
    }
  }

  // Adds a task failure listener which notifies lifecyclemanager when any
  // task fails for a barrier stage
  public static void addFailureListenerIfBarrierTask(
      ShuffleClient shuffleClient, TaskContext taskContext, CelebornShuffleHandle<?, ?, ?> handle) {

    if (!(taskContext instanceof BarrierTaskContext)) return;
    int appShuffleId = handle.shuffleId();
    String appShuffleIdentifier = SparkUtils.getAppShuffleIdentifier(appShuffleId, taskContext);

    BarrierTaskContext barrierContext = (BarrierTaskContext) taskContext;
    barrierContext.addTaskFailureListener(
        (context, error) -> {
          // whatever is the reason for failure, we notify lifecycle manager about the failure
          shuffleClient.reportBarrierTaskFailure(appShuffleId, appShuffleIdentifier);
        });
  }

  private static final DynFields.UnboundField shuffleIdToMapStage_FIELD =
      DynFields.builder().hiddenImpl(DAGScheduler.class, "shuffleIdToMapStage").build();

  public static void cancelShuffle(int shuffleId, String reason) {
    if (SparkContext$.MODULE$.getActive().nonEmpty()) {
      DAGScheduler scheduler = SparkContext$.MODULE$.getActive().get().dagScheduler();
      scala.collection.mutable.Map<Integer, ShuffleMapStage> shuffleIdToMapStageValue =
          (scala.collection.mutable.Map<Integer, ShuffleMapStage>)
              shuffleIdToMapStage_FIELD.bind(scheduler).get();
      Option<ShuffleMapStage> shuffleMapStage = shuffleIdToMapStageValue.get(shuffleId);
      if (shuffleMapStage.nonEmpty()) {
        scheduler.cancelStage(shuffleMapStage.get().id(), new Some<>(reason));
      }
    } else {
      logger.error("Can not get active SparkContext, skip cancelShuffle.");
    }
  }

  private static final DynFields.UnboundField<ConcurrentHashMap<Long, TaskSetManager>>
      TASK_ID_TO_TASK_SET_MANAGER_FIELD =
          DynFields.builder()
              .hiddenImpl(TaskSchedulerImpl.class, "taskIdToTaskSetManager")
              .defaultAlwaysNull()
              .build();
  private static final DynFields.UnboundField<scala.collection.mutable.HashMap<Long, TaskInfo>>
      TASK_INFOS_FIELD =
          DynFields.builder()
              .hiddenImpl(TaskSetManager.class, "taskInfos")
              .defaultAlwaysNull()
              .build();

  /**
   * TaskSetManager - it is not designed to be used outside the spark scheduler. Please be careful.
   */
  @VisibleForTesting
  protected static TaskSetManager getTaskSetManager(TaskSchedulerImpl taskScheduler, long taskId) {
    synchronized (taskScheduler) {
      ConcurrentHashMap<Long, TaskSetManager> taskIdToTaskSetManager =
          TASK_ID_TO_TASK_SET_MANAGER_FIELD.bind(taskScheduler).get();
      return taskIdToTaskSetManager.get(taskId);
    }
  }

  @VisibleForTesting
  protected static Tuple2<TaskInfo, List<TaskInfo>> getTaskAttempts(
      TaskSetManager taskSetManager, long taskId) {
    if (taskSetManager != null) {
      scala.Option<TaskInfo> taskInfoOption =
          TASK_INFOS_FIELD.bind(taskSetManager).get().get(taskId);
      if (taskInfoOption.isDefined()) {
        TaskInfo taskInfo = taskInfoOption.get();
        List<TaskInfo> taskAttempts =
            scala.collection.JavaConverters.asJavaCollectionConverter(
                    taskSetManager.taskAttempts()[taskInfo.index()])
                .asJavaCollection().stream()
                .collect(Collectors.toList());
        return Tuple2.apply(taskInfo, taskAttempts);
      } else {
        logger.error("Can not get TaskInfo for taskId: {}", taskId);
        return null;
      }
    } else {
      logger.error("Can not get TaskSetManager for taskId: {}", taskId);
      return null;
    }
  }

  protected static Map<String, Set<Long>> reportedStageShuffleFetchFailureTaskIds =
      JavaUtils.newConcurrentHashMap();

  protected static void removeStageReportedShuffleFetchFailureTaskIds(
      int stageId, int stageAttemptId) {
    reportedStageShuffleFetchFailureTaskIds.remove(stageId + "-" + stageAttemptId);
  }

  /**
   * Only used to check for the shuffle fetch failure task whether another attempt is running or
   * successful. If another attempt(excluding the reported shuffle fetch failure tasks in current
   * stage) is running or successful, return true. Otherwise, return false.
   */
  public static boolean taskAnotherAttemptRunningOrSuccessful(long taskId) {
    SparkContext sparkContext = SparkContext$.MODULE$.getActive().getOrElse(null);
    if (sparkContext == null) {
      logger.error("Can not get active SparkContext.");
      return false;
    }
    TaskSchedulerImpl taskScheduler = (TaskSchedulerImpl) sparkContext.taskScheduler();
    synchronized (taskScheduler) {
      TaskSetManager taskSetManager = getTaskSetManager(taskScheduler, taskId);
      if (taskSetManager != null) {
        int stageId = taskSetManager.stageId();
        int stageAttemptId = taskSetManager.taskSet().stageAttemptId();
        String stageUniqId = stageId + "-" + stageAttemptId;
        Set<Long> reportedStageTaskIds =
            reportedStageShuffleFetchFailureTaskIds.computeIfAbsent(
                stageUniqId, k -> new HashSet<>());
        reportedStageTaskIds.add(taskId);

        Tuple2<TaskInfo, List<TaskInfo>> taskAttempts = getTaskAttempts(taskSetManager, taskId);

        if (taskAttempts == null) return false;

        TaskInfo taskInfo = taskAttempts._1();
        for (TaskInfo ti : taskAttempts._2()) {
          if (ti.taskId() != taskId) {
            if (reportedStageTaskIds.contains(ti.taskId())) {
              logger.info(
                  "StageId={} index={} taskId={} attempt={} another attempt {} has reported shuffle fetch failure, ignore it.",
                  stageId,
                  taskInfo.index(),
                  taskId,
                  taskInfo.attemptNumber(),
                  ti.attemptNumber());
            } else if (ti.successful()) {
              logger.info(
                  "StageId={} index={} taskId={} attempt={} another attempt {} is successful.",
                  stageId,
                  taskInfo.index(),
                  taskId,
                  taskInfo.attemptNumber(),
                  ti.attemptNumber());
              return true;
            } else if (ti.running()) {
              logger.info(
                  "StageId={} index={} taskId={} attempt={} another attempt {} is running.",
                  stageId,
                  taskInfo.index(),
                  taskId,
                  taskInfo.attemptNumber(),
                  ti.attemptNumber());
              return true;
            }
          }
        }
        return false;
      } else {
        logger.error("Can not get TaskSetManager for taskId: {}", taskId);
        return false;
      }
    }
  }

  public static void addSparkListener(SparkListener listener) {
    SparkContext sparkContext = SparkContext$.MODULE$.getActive().getOrElse(null);
    if (sparkContext != null) {
      sparkContext.addSparkListener(listener);
    }
  }
}
