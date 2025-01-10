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

package org.apache.spark.shuffle.celeborn

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.tests.spark.SparkTestBase

class SparkUtilsSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  test("check if fetch failure task another attempt is running or successful") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderFetchFailureGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      try {
        val sc = sparkSession.sparkContext
        val jobThread = new Thread {
          override def run(): Unit = {
            try {
              val value = Range(1, 10000).mkString(",")
              sc.parallelize(1 to 10000, 2)
                .map { i => (i, value) }
                .groupByKey(10)
                .mapPartitions { iter =>
                  Thread.sleep(3000)
                  iter
                }.collect()
            } catch {
              case _: InterruptedException =>
            }
          }
        }
        jobThread.start()

        val taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
        eventually(timeout(30.seconds), interval(0.milliseconds)) {
          assert(hook.executed.get() == true)
          val reportedTaskId =
            SparkUtils.reportedStageShuffleFetchFailureTaskIds.values().asScala.flatMap(
              _.asScala).head
          val taskSetManager = SparkUtils.getTaskSetManager(taskScheduler, reportedTaskId)
          assert(taskSetManager != null)
          assert(SparkUtils.getTaskAttempts(taskSetManager, reportedTaskId)._2.size() == 1)
          assert(!SparkUtils.taskAnotherAttemptRunningOrSuccessful(reportedTaskId))
        }

        sparkSession.sparkContext.cancelAllJobs()

        jobThread.interrupt()

        eventually(timeout(3.seconds), interval(100.milliseconds)) {
          assert(SparkUtils.reportedStageShuffleFetchFailureTaskIds.size() == 0)
        }
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("getTaskSetManager/getTaskAttempts test") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()

    try {
      val sc = sparkSession.sparkContext
      val jobThread = new Thread {
        override def run(): Unit = {
          try {
            sc.parallelize(1 to 100, 2)
              .repartition(1)
              .mapPartitions { iter =>
                Thread.sleep(3000)
                iter
              }.collect()
          } catch {
            case _: InterruptedException =>
          }
        }
      }
      jobThread.start()

      val taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
      eventually(timeout(3.seconds), interval(100.milliseconds)) {
        val taskId = 0
        val taskSetManager = SparkUtils.getTaskSetManager(taskScheduler, taskId)
        assert(taskSetManager != null)
        assert(SparkUtils.getTaskAttempts(taskSetManager, taskId)._2.size() == 1)
        assert(!SparkUtils.taskAnotherAttemptRunningOrSuccessful(taskId))
        assert(SparkUtils.reportedStageShuffleFetchFailureTaskIds.size() == 1)
      }

      sparkSession.sparkContext.cancelAllJobs()

      jobThread.interrupt()

      eventually(timeout(3.seconds), interval(100.milliseconds)) {
        assert(SparkUtils.reportedStageShuffleFetchFailureTaskIds.size() == 0)
      }
    } finally {
      sparkSession.stop()
    }
  }
}
