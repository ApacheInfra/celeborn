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

package org.apache.spark.shuffle.rss;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.LongAdder;

import scala.Tuple2;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.util.Utils;

public class SparkUtils {
  private static final Logger logger = LoggerFactory.getLogger(SparkUtils.class);

  public static MapStatus createMapStatus(
      BlockManagerId loc, long[] uncompressedSizes, long mapTaskId) {
    return MapStatus$.MODULE$.apply(loc, uncompressedSizes, mapTaskId);
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

  /** make rss conf from spark conf */
  public static RssConf fromSparkConf(SparkConf conf) {
    RssConf tmpRssConf = new RssConf();
    for (Tuple2<String, String> kv : conf.getAll()) {
      if (kv._1.startsWith("spark.rss.")) {
        tmpRssConf.set(kv._1.substring("spark.".length()), kv._2);
      }
    }
    return tmpRssConf;
  }

  public static String genNewAppId(SparkContext context) {
    return context
        .applicationAttemptId()
        .map(id -> context.applicationId() + "_" + id)
        .getOrElse(context::applicationId);
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

  // Invoke and return getReader method of SortShuffleManager
  @SuppressWarnings("unchecked")
  public static <K, C> ShuffleReader<K, C> invokeGetReaderMethod(
      String className,
      String methodName,
      SortShuffleManager sortShuffleManager,
      ShuffleHandle handle,
      Integer startMapIndex,
      Integer endMapIndex,
      Integer startPartition,
      Integer endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    Class<?> cls = Utils.classForName(className);
    try {
      Method method =
          cls.getMethod(
              methodName,
              ShuffleHandle.class,
              Integer.TYPE,
              Integer.TYPE,
              Integer.TYPE,
              Integer.TYPE,
              TaskContext.class,
              ShuffleReadMetricsReporter.class);
      return (ShuffleReader<K, C>)
          method.invoke(
              sortShuffleManager,
              handle,
              startMapIndex,
              endMapIndex,
              startPartition,
              endPartition,
              context,
              metrics);
    } catch (ReflectiveOperationException roe1) {
      try {
        Method method =
            cls.getMethod(
                methodName,
                ShuffleHandle.class,
                Integer.TYPE,
                Integer.TYPE,
                TaskContext.class,
                ShuffleReadMetricsReporter.class);
        return (ShuffleReader<K, C>)
            method.invoke(
                sortShuffleManager, handle, startPartition, endPartition, context, metrics);
      } catch (ReflectiveOperationException roe2) {
        throw new RuntimeException("Get getReader method failed.", roe2);
      }
    }
  }

  public static StructType getShuffleDependencySchema(ShuffleDependency<?, ?, ?> dep)
      throws IOException {
    try {
      Field field = dep.getClass().getDeclaredField("schema");
      field.setAccessible(true);
      return (StructType) field.get(dep);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IOException("Failed to get Schema, columnar shuffle won`t work properly.");
    }
  }
}
