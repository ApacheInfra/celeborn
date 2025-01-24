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

import com.google.common.collect.Sets
import org.apache.spark.{SparkConf, SparkContext, SparkContextHelper}
import org.apache.spark.shuffle.celeborn.SparkShuffleManager
import org.junit.Assert
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.common.write.PushFailedBatch
import org.apache.celeborn.service.deploy.worker.PushDataHandler

class PushFailedBatchSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    val workerConf = Map(
      CelebornConf.TEST_WORKER_PUSH_REPLICA_DATA_TIMEOUT.key -> "true")

    setupMiniClusterWithRandomPorts(workerConf = workerConf, workerNum = 4)
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
    PushDataHandler.pushPrimaryDataTimeoutTested.set(false)
    PushDataHandler.pushReplicaDataTimeoutTested.set(false)
    PushDataHandler.pushPrimaryMergeDataTimeoutTested.set(false)
    PushDataHandler.pushReplicaMergeDataTimeoutTested.set(false)
  }

  override protected def afterEach() {
    System.gc()
  }

  test("CELEBORN-1319: check failed batch info by making push timeout") {
    val sparkConf = new SparkConf()
      .set(s"spark.${CelebornConf.TEST_CLIENT_RETRY_REVIVE.key}", "false")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "true")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_DATA_TIMEOUT.key}", "3s")
      .set(
        s"spark.${CelebornConf.CLIENT_ADAPTIVE_OPTIMIZE_SKEWED_PARTITION_READ_ENABLED.key}",
        "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .setAppName("celeborn-1319")
      .setMaster("local[2]")
    updateSparkConf(sparkConf, ShuffleMode.HASH)
    val sc = new SparkContext(sparkConf)

    sc.parallelize(1 to 1, 1).repartition(1).map(i => i + 1).collect()

    val manager = SparkContextHelper.env
      .shuffleManager
      .asInstanceOf[SparkShuffleManager]
      .getLifecycleManager

    // only one batch failed due to push timeout, so shuffle id will be 0,
    // and PartitionLocation uniqueId will be 0-0
    val pushFailedBatch = manager.commitManager.getCommitHandler(0).getShuffleFailedBatches()
    assert(!pushFailedBatch.isEmpty)
    Assert.assertEquals(
      pushFailedBatch.get(0).get("0-0"),
      Sets.newHashSet(new PushFailedBatch(0, 0, 1)))

    sc.stop()
  }

}
