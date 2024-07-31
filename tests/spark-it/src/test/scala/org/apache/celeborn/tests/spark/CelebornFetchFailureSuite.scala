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

package org.apache.celeborn.tests.spark

import java.io.{File, IOException}
import java.util
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{BarrierTaskContext, ShuffleDependency, SparkConf, SparkContextHelper, SparkException, TaskContext}
import org.apache.spark.celeborn.ExceptionMakerHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContextHelper, TaskContext}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ShuffleManagerHook, SparkShuffleManager, SparkUtils, TestCelebornShuffleManager}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.junit.Assert
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.service.deploy.worker.Worker

class CelebornFetchFailureSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  var workerDirs: Seq[String] = Seq.empty

  override def createWorker(map: Map[String, String]): Worker = {
    val storageDir = createTmpDir()
    this.synchronized {
      workerDirs = workerDirs :+ storageDir
    }
    super.createWorker(map, storageDir)
  }

  class ShuffleReaderGetHook(conf: CelebornConf) extends ShuffleManagerHook {
    var executed: AtomicBoolean = new AtomicBoolean(false)
    val lock = new Object

    override def exec(
        handle: ShuffleHandle,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext): Unit = {
      if (executed.get() == true) return

      lock.synchronized {
        handle match {
          case h: CelebornShuffleHandle[_, _, _] => {
            val appUniqueId = h.appUniqueId
            val shuffleClient = ShuffleClient.get(
              h.appUniqueId,
              h.lifecycleManagerHost,
              h.lifecycleManagerPort,
              conf,
              h.userIdentifier,
              h.extension)
            val celebornShuffleId = SparkUtils.celebornShuffleId(shuffleClient, h, context, false)
            val allFiles = workerDirs.map(dir => {
              new File(s"$dir/celeborn-worker/shuffle_data/$appUniqueId/$celebornShuffleId")
            })
            val datafile = allFiles.filter(_.exists())
              .flatMap(_.listFiles().iterator).headOption
            datafile match {
              case Some(file) => file.delete()
              case None => throw new RuntimeException("unexpected, there must be some data file" +
                  s" under ${workerDirs.mkString(",")}")
            }
          }
          case _ => throw new RuntimeException("unexpected, only support RssShuffleHandle here")
        }
        executed.set(true)
      }
    }
  }

  test("celeborn spark integration test - Fetch Failure - Deterministic Stage") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
        .config("spark.celeborn.client.spark.shuffle.writer", "sort")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .config("spark.default.parallelism", "10")
        .config("spark.celeborn.test.client.push.mockRandomPushForStageRerun", true)
        .config("spark.celeborn.shuffle.chunk.size", "100k")
        .getOrCreate()

      // code
      val schema = new StructType().add("column1", StringType, nullable = true).add(
        "column2",
        IntegerType,
        nullable = true)

      val nums = new util.ArrayList[Row]
      for (i <- 0 until 100000) {
        nums.add(RowFactory.create(s"apple$i", new Integer(i)))
        nums.add(RowFactory.create(s"banana$i", new Integer(i)));
        nums.add(RowFactory.create(s"apple$i", new Integer(i)));
        nums.add(RowFactory.create(s"banana$i", new Integer(i)));
        nums.add(RowFactory.create(s"cherry$i", new Integer(i)));
      }

      val df = sparkSession.createDataFrame(nums, schema)
      val value = df.groupBy("column1").agg(sum("column2").as("sum")).select("column1", "sum")

      Assert.assertEquals(value.count(), 300000)
      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - Fetch Failure") {
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
      val hook = new ShuffleReaderGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      val value = Range(1, 10000).mkString(",")
      val tuples = sparkSession.sparkContext.parallelize(1 to 10000, 2)
        .map { i => (i, value) }.groupByKey(16).collect()

      // verify result
      assert(hook.executed.get() == true)
      assert(tuples.length == 10000)
      for (elem <- tuples) {
        assert(elem._2.mkString(",").equals(value))
      }

      val shuffleMgr = SparkContextHelper.env
        .shuffleManager
        .asInstanceOf[TestCelebornShuffleManager]
      val lifecycleManager = shuffleMgr.getLifecycleManager

      shuffleMgr.unregisterShuffle(0)
      assert(lifecycleManager.getUnregisterShuffleTime().containsKey(0))
      assert(lifecycleManager.getUnregisterShuffleTime().containsKey(1))

      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - unregister shuffle with throwsFetchFailure disabled") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "false")
        .getOrCreate()

      val value = Range(1, 10000).mkString(",")
      val tuples = sparkSession.sparkContext.parallelize(1 to 10000, 2)
        .map { i => (i, value) }.groupByKey(16).collect()

      // verify result
      assert(tuples.length == 10000)
      for (elem <- tuples) {
        assert(elem._2.mkString(",").equals(value))
      }

      val shuffleMgr = SparkContextHelper.env
        .shuffleManager
        .asInstanceOf[SparkShuffleManager]
      val lifecycleManager = shuffleMgr.getLifecycleManager

      shuffleMgr.unregisterShuffle(0)
      assert(lifecycleManager.getUnregisterShuffleTime().containsKey(0))

      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - Fetch Failure with multiple shuffle data") {
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
      val hook = new ShuffleReaderGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      import sparkSession.implicits._

      val df1 = Seq((1, "a"), (2, "b")).toDF("id", "data").groupBy("id").count()
      val df2 = Seq((2, "c"), (2, "d")).toDF("id", "data").groupBy("id").count()
      val tuples = df1.hint("merge").join(df2, "id").select("*").collect()

      // verify result
      assert(hook.executed.get() == true)
      val expect = "[2,1,2]"
      assert(tuples.head.toString().equals(expect))
      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - Fetch Failure with RDD reuse") {
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
      val hook = new ShuffleReaderGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      val sc = sparkSession.sparkContext
      val rdd1 = sc.parallelize(0 until 10000, 3).map(v => (v, v)).groupByKey()
      val rdd2 = sc.parallelize(0 until 10000, 2).map(v => (v, v)).groupByKey()
      val rdd3 = rdd1.map(v => (v._2, v._1))

      hook.executed.set(true)

      rdd1.count()
      rdd2.count()

      hook.executed.set(false)
      rdd3.count()
      hook.executed.set(false)
      rdd3.count()
      hook.executed.set(false)
      rdd3.count()
      hook.executed.set(false)
      rdd3.count()

      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - Fetch Failure with read write shuffles in one stage") {
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
      val hook = new ShuffleReaderGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      val sc = sparkSession.sparkContext
      val rdd1 = sc.parallelize(0 until 10000, 3).map(v => (v, v)).groupByKey()
      val rdd2 = rdd1.map(v => (v._2, v._1)).groupByKey()

      hook.executed.set(true)
      rdd1.count()

      hook.executed.set(false)
      rdd2.count()

      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - empty shuffle data") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
        .getOrCreate()

      sparkSession.sql("create table if not exists t_1 (a bigint) using parquet")
      sparkSession.sql("create table if not exists t_2 (a bigint) using parquet")
      sparkSession.sql("create table if not exists t_3 (a bigint) using parquet")
      val df1 = sparkSession.table("t_1")
      val df2 = sparkSession.table("t_2")
      val df3 = sparkSession.table("t_3")
      df1.union(df2).union(df3).count()

      sparkSession.stop()
    }
  }

  test(s"celeborn spark integration test - resubmit an unordered barrier stage with throwsFetchFailure enabled") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
      .config("spark.celeborn.client.push.buffer.max.size", 0)
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()

    try {
      val sc = sparkSession.sparkContext
      val rdd1 = sc
        .parallelize(0 until 10000, 2)
        .map(v => (v, v))
        .groupByKey()
        .barrier()
        .mapPartitions {
          it =>
            val context = BarrierTaskContext.get()
            if (context.stageAttemptNumber() == 0 && context.partitionId() == 0) {
              Thread.sleep(3000)
              throw new RuntimeException("failed")
            }
            if (context.stageAttemptNumber() > 0) {
              it.toBuffer.reverseIterator
            } else {
              it
            }
        }
      val rdd2 = rdd1.map(v => (v._2, v._1)).groupByKey()
      val result = rdd2.collect()
      result.foreach {
        elem =>
          assert(elem._1.size == elem._2.size)
      }
    } finally {
      sparkSession.stop()
    }
  }

  test(s"celeborn spark integration test - fetch failure in child of an unordered barrier stage with throwsFetchFailure enabled") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
      .config("spark.celeborn.client.push.buffer.max.size", 0)
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()

    try {
      val sc = sparkSession.sparkContext
      val inputGroupedRdd = sc
        .parallelize(0 until 10000, 2)
        .map(v => (v, v))
        .groupByKey()
      val rdd1 = inputGroupedRdd
        .barrier()
        .mapPartitions(it => it)
      val groupedRdd = rdd1.map(v => (v._2, v._1)).groupByKey()
      val appShuffleId = findAppShuffleId(groupedRdd)
      assert(findAppShuffleId(groupedRdd) != findAppShuffleId(inputGroupedRdd))
      val rdd2 = groupedRdd.mapPartitions { iter =>
        val context = TaskContext.get()
        if (context.stageAttemptNumber() == 0 && context.partitionId() == 0) {
          throw ExceptionMakerHelper.SHUFFLE_FETCH_FAILURE_EXCEPTION_MAKER.makeFetchFailureException(
            appShuffleId,
            -1,
            context.partitionId(),
            new IOException("forced"))
        }
        iter
      }
      val result = rdd2.collect()
      result.foreach {
        elem =>
          assert(elem._1.size == elem._2.size)
      }
    } finally {
      sparkSession.stop()
    }
  }

  test(s"celeborn spark integration test - resubmit a failed barrier stage across jobs") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
      .config("spark.celeborn.client.push.buffer.max.size", 0)
      .config("spark.stage.maxConsecutiveAttempts", "1")
      .config("spark.stage.maxAttempts", "1")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()

    // trigger failure
    CelebornFetchFailureSuite.triggerFailure.set(true)

    try {
      val sc = sparkSession.sparkContext
      val rdd1 = sc
        .parallelize(0 until 10000, 2)
        .map(v => (v, v))
        .groupByKey()
        .barrier()
        .mapPartitions {
          it =>
            val context = BarrierTaskContext.get()
            if (context.partitionId() == 0 && CelebornFetchFailureSuite.triggerFailure.get()) {
              Thread.sleep(3000)
              throw new RuntimeException("failed")
            }
            if (CelebornFetchFailureSuite.triggerFailure.get()) {
              it
            } else {
              it.toBuffer.reverseIterator
            }
        }
      val rdd2 = rdd1.map(v => (v._2, v._1)).groupByKey()
      assertThrows[SparkException] {
        rdd2.collect()
      }

      // Now, allow it to succeed
      CelebornFetchFailureSuite.triggerFailure.set(false)
      val result = rdd2.collect()
      result.foreach {
        elem =>
          assert(elem._1.size == elem._2.size)
      }
    } finally {
      sparkSession.stop()
    }
  }

  private def findAppShuffleId(rdd: RDD[_]): Int = {
    val deps = rdd.dependencies
    if (deps.size != 1 && !deps.head.isInstanceOf[ShuffleDependency[_, _, _]]) {
      throw new IllegalArgumentException("Expected an RDD with shuffle dependency: " + rdd)
    }

    deps.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
  }
}

object CelebornFetchFailureSuite {
  private val triggerFailure = new AtomicBoolean(false)
}
