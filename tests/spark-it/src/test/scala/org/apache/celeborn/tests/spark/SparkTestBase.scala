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

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.Random

import org.apache.spark.{SPARK_VERSION, SparkConf, TaskContext}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ShuffleManagerHook, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker

trait SparkTestBase extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll with BeforeAndAfterEach {

  val Spark3OrNewer = SPARK_VERSION >= "3.0"
  println(s"Spark version is $SPARK_VERSION, Spark3OrNewer: $Spark3OrNewer")

  private val sampleSeq = (1 to 78)
    .map(Random.alphanumeric)
    .toList
    .map(v => (v.toUpper, Random.nextInt(12) + 1))

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    setupMiniClusterWithRandomPorts(workerNum = 5)
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop Celeborn mini cluster")
    shutdownMiniCluster()
  }

  var workerDirs: Seq[String] = Seq.empty

  override def createWorker(map: Map[String, String]): Worker = {
    val storageDir = createTmpDir()
    this.synchronized {
      workerDirs = workerDirs :+ storageDir
    }
    super.createWorker(map, storageDir)
  }

  def updateSparkConf(sparkConf: SparkConf, mode: ShuffleMode): SparkConf = {
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set(
      "spark.shuffle.manager",
      "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
    sparkConf.set("spark.shuffle.useOldFetchProtocol", "true")
    sparkConf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    sparkConf.set("spark.shuffle.service.enabled", "false")
    sparkConf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    sparkConf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    sparkConf.set(s"spark.${MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
    sparkConf.set(s"spark.${SPARK_SHUFFLE_WRITER_MODE.key}", mode.toString)
    sparkConf
  }

  def combine(sparkSession: SparkSession): collection.Map[Char, (Int, Int)] = {
    val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 4)
    val resultWithOutCeleborn = inputRdd
      .combineByKey(
        (k: Int) => (k, 1),
        (acc: (Int, Int), v: Int) => (acc._1 + v, acc._2 + 1),
        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .collectAsMap()
    resultWithOutCeleborn
  }

  def repartition(sparkSession: SparkSession): collection.Map[Char, Int] = {
    val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 2)
    val result = inputRdd.repartition(8).reduceByKey((acc, v) => acc + v).collectAsMap()
    result
  }

  def groupBy(sparkSession: SparkSession): collection.Map[Char, String] = {
    val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 2)
    val result = inputRdd.groupByKey().sortByKey().collectAsMap()
    result.map(k => (k._1, k._2.toList.sorted.mkString(","))).toMap
  }

  def runsql(sparkSession: SparkSession): Map[String, Long] = {
    import sparkSession.implicits._
    val df = Seq(("fa", "fb"), ("fa1", "fb1"), ("fa2", "fb2"), ("fa3", "fb3")).toDF("fa", "fb")
    df.createOrReplaceTempView("tmp")
    val result = sparkSession.sql("select fa,count(fb) from tmp group by fa order by fa")
    val outMap = result.collect().map(row => row.getString(0) -> row.getLong(1)).toMap
    outMap
  }

  class ShuffleReaderFetchFailureGetHook(conf: CelebornConf) extends ShuffleManagerHook {
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
              .flatMap(_.listFiles().iterator).sortBy(_.getName).headOption
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
}
