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

package org.apache.celeborn.service.deploy.master

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.{AbstractSource, Role}

class MasterSource(conf: CelebornConf) extends AbstractSource(conf, Role.MASTER) {
  override val sourceName = "master"

  import MasterSource._
  // add timers
  addTimer(OFFER_SLOTS_TIME)
  // start cleaner
  startCleaner()
}

object MasterSource {
  val WORKER_COUNT = "WorkerCount"

  val LOST_WORKER_COUNT = "LostWorkerCount"

  val EXCLUDED_WORKER_COUNT = "ExcludedWorkerCount"

  val SHUTDOWN_WORKER_COUNT = "ShutdownWorkerCount"

  val AVAILABLE_WORKER_COUNT = "AvailableWorkerCount"

  val DECOMMISSION_WORKER_COUNT = "DecommissionWorkerCount"

  val REGISTERED_SHUFFLE_COUNT = "RegisteredShuffleCount"
  val SHUFFLE_FALLBACK_COUNT = "ShuffleFallbackCount"
  // The total count including RegisteredShuffleCount(celeborn shuffle) and ShuffleFallbackCount(spark built-in shuffle).
  val SHUFFLE_TOTAL_COUNT = "ShuffleTotalCount"

  val RUNNING_APPLICATION_COUNT = "RunningApplicationCount"

  val IS_ACTIVE_MASTER = "IsActiveMaster"

  val PARTITION_SIZE = "PartitionSize"

  val ACTIVE_SHUFFLE_SIZE = "ActiveShuffleSize"

  val ACTIVE_SHUFFLE_FILE_COUNT = "ActiveShuffleFileCount"

  val OFFER_SLOTS_TIME = "OfferSlotsTime"

  val RATIS_APPLY_COMPLETED_INDEX = "RatisApplyCompletedIndex"

  // Capacity
  val DEVICE_CELEBORN_FREE_CAPACITY = "DeviceCelebornFreeBytes"
  val DEVICE_CELEBORN_TOTAL_CAPACITY = "DeviceCelebornTotalBytes"

  val UPDATE_RESOURCE_CONSUMPTION_TIME = "UpdateResourceConsumptionTime"
}
