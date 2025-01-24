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
package org.apache.celeborn.service.deploy.master.quota

import java.util.{Map => JMap}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.source.ResourceConsumptionSource
import org.apache.celeborn.common.metrics.source.ResourceConsumptionSource._
import org.apache.celeborn.common.protocol.message.ControlMessages.CheckQuotaResponse
import org.apache.celeborn.common.quota.{ResourceConsumption, StorageQuota}
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils, Utils}
import org.apache.celeborn.server.common.service.config.ConfigService
import org.apache.celeborn.service.deploy.master.MasterSource
import org.apache.celeborn.service.deploy.master.MasterSource.UPDATE_RESOURCE_CONSUMPTION_TIME
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager
import org.apache.celeborn.service.deploy.master.quota.QuotaStatus._

class QuotaManager(
    statusSystem: AbstractMetaManager,
    masterSource: MasterSource,
    resourceConsumptionSource: ResourceConsumptionSource,
    celebornConf: CelebornConf,
    configService: ConfigService) extends Logging {

  val userQuotaStatus: JMap[UserIdentifier, QuotaStatus] = JavaUtils.newConcurrentHashMap()
  val tenantQuotaStatus: JMap[String, QuotaStatus] = JavaUtils.newConcurrentHashMap()
  @volatile
  var clusterQuotaStatus: QuotaStatus = new QuotaStatus()
  val appQuotaStatus: JMap[String, QuotaStatus] = JavaUtils.newConcurrentHashMap()
  val userResourceConsumptionMap: JMap[UserIdentifier, ResourceConsumption] =
    JavaUtils.newConcurrentHashMap()
  private val quotaChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-quota-checker")
  quotaChecker.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        try {
          updateResourceConsumption()
        } catch {
          case t: Throwable => logError("Update user resource consumption failed.", t)
        }
      }
    },
    0L,
    celebornConf.masterResourceConsumptionInterval,
    TimeUnit.MILLISECONDS)

  def handleAppLost(appId: String): Unit = {
    appQuotaStatus.remove(appId)
  }

  def checkUserQuotaStatus(userIdentifier: UserIdentifier): CheckQuotaResponse = {
    val tenantStatus = tenantQuotaStatus.getOrDefault(userIdentifier.tenantId, QuotaStatus())
    val userStatus = userQuotaStatus.getOrDefault(userIdentifier, QuotaStatus())
    if (userStatus.exceed) {
      CheckQuotaResponse(false, userStatus.exceedReason)
    } else if (tenantStatus.exceed) {
      CheckQuotaResponse(false, tenantStatus.exceedReason)
    } else {
      CheckQuotaResponse(!clusterQuotaStatus.exceed, clusterQuotaStatus.exceedReason)
    }
  }

  def checkApplicationQuotaStatus(applicationId: String): CheckQuotaResponse = {
    val status = appQuotaStatus.getOrDefault(applicationId, QuotaStatus())
    CheckQuotaResponse(!status.exceed, status.exceedReason)
  }

  def getUserStorageQuota(user: UserIdentifier): StorageQuota = {
    Option(configService)
      .map(_.getTenantUserConfigFromCache(user.tenantId, user.name).getUserStorageQuota)
      .getOrElse(StorageQuota.DEFAULT_QUOTA)
  }

  def getTenantStorageQuota(tenantId: String): StorageQuota = {
    Option(configService)
      .map(_.getTenantConfigFromCache(tenantId).getTenantStorageQuota)
      .getOrElse(StorageQuota.DEFAULT_QUOTA)
  }

  def getClusterStorageQuota: StorageQuota = {
    Option(configService)
      .map(_.getSystemConfigFromCache.getClusterStorageQuota)
      .getOrElse(StorageQuota.DEFAULT_QUOTA)
  }

  private def interruptShuffleEnabled: Boolean = {
    Option(configService)
      .map(_.getSystemConfigFromCache.interruptShuffleEnabled())
      .getOrElse(celebornConf.quotaInterruptShuffleEnabled)
  }

  private def checkUserQuotaSpace(
      user: UserIdentifier,
      consumption: ResourceConsumption): QuotaStatus = {
    val quota = getUserStorageQuota(user)
    checkQuotaSpace(s"$USER_EXHAUSTED user: $user. ", consumption, quota)
  }

  private def checkTenantQuotaSpace(
      tenantId: String,
      consumption: ResourceConsumption): QuotaStatus = {
    val quota = getTenantStorageQuota(tenantId)
    checkQuotaSpace(s"$USER_EXHAUSTED tenant: $tenantId. ", consumption, quota)
  }

  private def checkClusterQuotaSpace(consumption: ResourceConsumption): QuotaStatus = {
    checkQuotaSpace(CLUSTER_EXHAUSTED, consumption, getClusterStorageQuota)
  }

  private def checkQuotaSpace(
      reason: String,
      consumption: ResourceConsumption,
      quota: StorageQuota): QuotaStatus = {
    val checkResults = Seq(
      checkQuota(
        consumption.diskBytesWritten,
        quota.diskBytesWritten,
        "DISK_BYTES_WRITTEN",
        Utils.bytesToString),
      checkQuota(
        consumption.diskFileCount,
        quota.diskFileCount,
        "DISK_FILE_COUNT",
        _.toString),
      checkQuota(
        consumption.hdfsBytesWritten,
        quota.hdfsBytesWritten,
        "HDFS_BYTES_WRITTEN",
        Utils.bytesToString),
      checkQuota(
        consumption.hdfsFileCount,
        quota.hdfsFileCount,
        "HDFS_FILE_COUNT",
        _.toString))
    val exceed = checkResults.foldLeft(false)(_ || _._1)
    val exceedReason =
      if (exceed) {
        s"$reason ${checkResults.foldLeft("")(_ + _._2)}"
      } else {
        ""
      }
    QuotaStatus(exceed, exceedReason)
  }

  private def checkQuota(
      value: Long,
      quota: Long,
      quotaType: String,
      format: Long => String): (Boolean, String) = {
    val exceed = quota > 0 && value >= quota
    var reason = ""
    if (exceed) {
      reason = s"$quotaType(${format(value)}) exceeds quota(${format(quota)}). "
      logWarning(reason)
    }
    (exceed, reason)
  }

  private def checkConsumptionExceeded(
      used: ResourceConsumption,
      threshold: StorageQuota): Boolean = {
    used.diskBytesWritten >= threshold.diskBytesWritten ||
    used.diskFileCount >= threshold.diskFileCount ||
    used.hdfsBytesWritten >= threshold.hdfsBytesWritten ||
    used.hdfsFileCount >= threshold.hdfsFileCount
  }

  def updateResourceConsumption(): Unit = {
    masterSource.sample(UPDATE_RESOURCE_CONSUMPTION_TIME, this.getClass.getSimpleName, Map.empty) {
      val clusterQuota = getClusterStorageQuota
      var clusterResourceConsumption = ResourceConsumption(0, 0, 0, 0)
      val activeUsers = mutable.Set[UserIdentifier]()

      val tenantResourceConsumption =
        statusSystem.availableWorkers.asScala.flatMap { workerInfo =>
          workerInfo.userResourceConsumption.asScala
        }.groupBy(_._1.tenantId).toSeq.map { case (tenantId, tenantConsumptionList) =>
          var tenantResourceConsumption = ResourceConsumption(0, 0, 0, 0)
          val userResourceConsumption =
            tenantConsumptionList.groupBy(_._1).map {
              case (userIdentifier, userConsumptionList) =>
                activeUsers.add(userIdentifier)
                // Step 1: Compute user consumption and set quota status.
                val resourceConsumptionList = userConsumptionList.map(_._2).toSeq
                val resourceConsumption = computeUserResourceConsumption(resourceConsumptionList)

                // Step 2: Update user resource consumption metrics.
                // For extract metrics
                userResourceConsumptionMap.put(userIdentifier, resourceConsumption)
                registerUserResourceConsumptionMetrics(userIdentifier)

                // Step 3: Expire user level exceeded app except already expired app
                clusterResourceConsumption = clusterResourceConsumption.add(resourceConsumption)
                tenantResourceConsumption = tenantResourceConsumption.add(resourceConsumption)
                val quotaStatus = checkUserQuotaSpace(userIdentifier, resourceConsumption)
                userQuotaStatus.put(userIdentifier, quotaStatus)
                if (interruptShuffleEnabled && quotaStatus.exceed) {
                  val subResourceConsumptions = computeSubConsumption(resourceConsumptionList)
                  // Compute expired size
                  val (expired, notExpired) = subResourceConsumptions.partition { case (app, _) =>
                    appQuotaStatus.containsKey(app)
                  }
                  val userConsumptions =
                    expired.values.foldLeft(resourceConsumption)(_.subtract(_))
                  expireApplication(
                    userConsumptions,
                    getUserStorageQuota(userIdentifier),
                    notExpired.toSeq,
                    USER_EXHAUSTED)
                  (Option(subResourceConsumptions), resourceConsumptionList)
                } else {
                  (None, resourceConsumptionList)
                }
            }

          val quotaStatus = checkTenantQuotaSpace(tenantId, tenantResourceConsumption)
          tenantQuotaStatus.put(tenantId, quotaStatus)
          // Expire tenant level exceeded app except already expired app
          if (interruptShuffleEnabled && quotaStatus.exceed) {
            val appConsumptions = userResourceConsumption.map {
              case (None, subConsumptionList) => computeSubConsumption(subConsumptionList)
              case (Some(subConsumptions), _) => subConsumptions
            }.flatMap(_.toSeq).toSeq

            // Compute nonExpired app total usage
            val (expired, notExpired) = appConsumptions.partition { case (app, _) =>
              appQuotaStatus.containsKey(app)
            }
            tenantResourceConsumption =
              expired.map(_._2).foldLeft(tenantResourceConsumption)(_.subtract(_))
            expireApplication(
              tenantResourceConsumption,
              getTenantStorageQuota(tenantId),
              notExpired,
              TENANT_EXHAUSTED)
            (Option(appConsumptions), tenantConsumptionList.map(_._2).toSeq)
          } else {
            (None, tenantConsumptionList.map(_._2).toSeq)
          }
        }

      // Clear expired users/tenant quota status
      clearQuotaStatus(activeUsers)

      // Expire cluster level exceeded app except already expired app
      clusterQuotaStatus = checkClusterQuotaSpace(clusterResourceConsumption)
      if (interruptShuffleEnabled && clusterQuotaStatus.exceed) {
        val appConsumptions = tenantResourceConsumption.map {
          case (None, subConsumptionList) => computeSubConsumption(subConsumptionList)
          case (Some(subConsumptions), _) => subConsumptions
        }.flatMap(_.toSeq).toSeq

        // Compute nonExpired app total usage
        val (expired, notExpired) = appConsumptions.partition { case (app, _) =>
          appQuotaStatus.containsKey(app)
        }
        clusterResourceConsumption =
          expired.map(_._2).foldLeft(clusterResourceConsumption)(_.subtract(_))
        expireApplication(clusterResourceConsumption, clusterQuota, notExpired, CLUSTER_EXHAUSTED)
      }
    }
  }

  private def expireApplication(
      used: ResourceConsumption,
      threshold: StorageQuota,
      appMap: Seq[(String, ResourceConsumption)],
      expireReason: String): Unit = {
    var nonExpired = used
    if (checkConsumptionExceeded(used, threshold)) {
      val sortedConsumption =
        appMap.sortBy(_._2)(Ordering.by((r: ResourceConsumption) =>
          (
            r.diskBytesWritten,
            r.diskFileCount,
            r.hdfsBytesWritten,
            r.hdfsFileCount)).reverse)
      for ((appId, consumption) <- sortedConsumption
        if checkConsumptionExceeded(nonExpired, threshold)) {
        val reason = s"$expireReason Used: ${consumption.simpleString}, Threshold: $threshold"
        appQuotaStatus.put(appId, QuotaStatus(exceed = true, reason))
        nonExpired = nonExpired.subtract(consumption)
      }
    }
  }

  private def computeUserResourceConsumption(
      consumptions: Seq[ResourceConsumption]): ResourceConsumption = {
    consumptions.foldRight(ResourceConsumption(0, 0, 0, 0))(_ add _)
  }

  private def computeSubConsumption(
      resourceConsumptionList: Seq[ResourceConsumption]): Map[String, ResourceConsumption] = {
    resourceConsumptionList.foldRight(Map.empty[String, ResourceConsumption]) {
      case (consumption, subConsumption) =>
        consumption.addSubResourceConsumptions(subConsumption)
    }
  }

  private def getResourceConsumption(userIdentifier: UserIdentifier): ResourceConsumption = {
    userResourceConsumptionMap.getOrDefault(userIdentifier, ResourceConsumption(0, 0, 0, 0))
  }

  private def registerUserResourceConsumptionMetrics(userIdentifier: UserIdentifier): Unit = {
    resourceConsumptionSource.addGauge(DISK_FILE_COUNT, userIdentifier.toMap) { () =>
      getResourceConsumption(userIdentifier).diskBytesWritten
    }
    resourceConsumptionSource.addGauge(DISK_BYTES_WRITTEN, userIdentifier.toMap) { () =>
      getResourceConsumption(userIdentifier).diskBytesWritten
    }
    resourceConsumptionSource.addGauge(HDFS_FILE_COUNT, userIdentifier.toMap) { () =>
      getResourceConsumption(userIdentifier).hdfsFileCount
    }
    resourceConsumptionSource.addGauge(HDFS_BYTES_WRITTEN, userIdentifier.toMap) { () =>
      getResourceConsumption(userIdentifier).hdfsBytesWritten
    }
  }

  private def clearQuotaStatus(activeUsers: mutable.Set[UserIdentifier]): Unit = {
    userQuotaStatus
      .keySet()
      .asScala
      .diff(activeUsers)
      .foreach(userQuotaStatus.remove)
    tenantQuotaStatus
      .keySet()
      .asScala
      .diff(activeUsers.map(_.tenantId).toSet)
      .foreach(tenantQuotaStatus.remove)
  }
}
