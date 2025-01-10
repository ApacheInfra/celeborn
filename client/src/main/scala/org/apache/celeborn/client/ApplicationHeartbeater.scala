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

package org.apache.celeborn.client

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}
import java.util.function.Consumer

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PbReviseLostShufflesResponse
import org.apache.celeborn.common.protocol.message.ControlMessages.{ApplicationLost, ApplicationLostResponse, CheckQuotaResponse, HeartbeatFromApplication, HeartbeatFromApplicationResponse, ReviseLostShuffles, ZERO_UUID}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{ThreadUtils, Utils}

class ApplicationHeartbeater(
    appId: String,
    conf: CelebornConf,
    masterClient: MasterClient,
    shuffleMetrics: () => (
        (Long, Long),
        (Long, Long, Map[String, java.lang.Long], Map[String, java.lang.Long])),
    workerStatusTracker: WorkerStatusTracker,
    registeredShuffles: ConcurrentHashMap.KeySetView[Int, java.lang.Boolean],
    cancelAllActiveStages: String => Unit) extends Logging {

  private var stopped = false
  private val reviseLostShuffles = conf.reviseLostShufflesEnabled

  // Use independent app heartbeat threads to avoid being blocked by other operations.
  private val appHeartbeatIntervalMs = conf.appHeartbeatIntervalMs
  private val applicationUnregisterEnabled = conf.applicationUnregisterEnabled
  private val appHeartbeatHandlerThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "celeborn-client-lifecycle-manager-app-heartbeater")
  private var appHeartbeat: ScheduledFuture[_] = _

  def start(): Unit = {
    appHeartbeat = appHeartbeatHandlerThread.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          try {
            require(masterClient != null, "When sending a heartbeat, client shouldn't be null.")
            val (
              (tmpTotalWritten, tmpTotalFileCount),
              (
                tmpShuffleCount,
                tmpApplicationCount,
                tmpShuffleFallbackCounts,
                tmpApplicationFallbackCounts)) = shuffleMetrics()
            logInfo("Send app heartbeat with " +
              s"written: ${Utils.bytesToString(tmpTotalWritten)}, file count: $tmpTotalFileCount, " +
              s"shuffle count: $tmpShuffleCount, shuffle fallback counts: $tmpShuffleFallbackCounts, " +
              s"application count: $tmpApplicationCount, application fallback counts: $tmpApplicationFallbackCounts")
            // UserResourceConsumption and DiskInfo are eliminated from WorkerInfo
            // during serialization of HeartbeatFromApplication
            val appHeartbeat =
              HeartbeatFromApplication(
                appId,
                tmpTotalWritten,
                tmpTotalFileCount,
                tmpShuffleCount,
                tmpApplicationCount,
                tmpShuffleFallbackCounts.asJava,
                tmpApplicationFallbackCounts.asJava,
                workerStatusTracker.getNeedCheckedWorkers().toList.asJava,
                ZERO_UUID,
                true)
            val response = requestHeartbeat(appHeartbeat)
            if (response.statusCode == StatusCode.SUCCESS) {
              logDebug("Successfully send app heartbeat.")
              workerStatusTracker.handleHeartbeatResponse(response)
              checkQuotaExceeds(response.checkQuotaResponse)
              // revise shuffle id if there are lost shuffles
              if (reviseLostShuffles) {
                val masterRecordedShuffleIds = response.registeredShuffles
                val localOnlyShuffles = new util.ArrayList[Integer]()
                registeredShuffles.forEach(new Consumer[Int] {
                  override def accept(key: Int): Unit = {
                    localOnlyShuffles.add(key)
                  }
                })
                localOnlyShuffles.removeAll(masterRecordedShuffleIds)
                if (!localOnlyShuffles.isEmpty) {
                  logWarning(
                    s"There are lost shuffle found ${StringUtils.join(localOnlyShuffles, ",")}, revise lost shuffles.")
                  val reviseLostShufflesResponse = masterClient.askSync(
                    ReviseLostShuffles.apply(appId, localOnlyShuffles, MasterClient.genRequestId()),
                    classOf[PbReviseLostShufflesResponse])
                  if (!reviseLostShufflesResponse.getSuccess) {
                    logWarning(
                      s"Revise lost shuffles failed. Error message :${reviseLostShufflesResponse.getMessage}")
                  } else {
                    logInfo("Revise lost shuffles succeed.")
                  }
                }
              }
            }
          } catch {
            case it: InterruptedException =>
              logWarning("Interrupted while sending app heartbeat.")
              Thread.currentThread().interrupt()
              throw it
            case t: Throwable =>
              logError("Error while send heartbeat", t)
          }
        }
      },
      0,
      appHeartbeatIntervalMs,
      TimeUnit.MILLISECONDS)
  }

  private def requestHeartbeat(message: HeartbeatFromApplication)
      : HeartbeatFromApplicationResponse = {
    try {
      masterClient.askSync[HeartbeatFromApplicationResponse](
        message,
        classOf[HeartbeatFromApplicationResponse])
    } catch {
      case e: Exception =>
        logError("AskSync HeartbeatFromApplication failed.", e)
        HeartbeatFromApplicationResponse(
          StatusCode.REQUEST_FAILED,
          List.empty.asJava,
          List.empty.asJava,
          List.empty.asJava,
          List.empty.asJava,
          CheckQuotaResponse(isAvailable = true, ""))
    }
  }

  private def unregisterApplication(): Unit = {
    try {
      // Then unregister Application
      val response = masterClient.askSync[ApplicationLostResponse](
        ApplicationLost(appId),
        classOf[ApplicationLostResponse])
      logInfo(s"Unregister Application $appId with response status: ${response.status}")
    } catch {
      case e: Exception =>
        logWarning("AskSync unRegisterApplication failed.", e)
    }
  }

  private def checkQuotaExceeds(response: CheckQuotaResponse): Unit = {
    if (conf.quotaInterruptShuffleEnabled && !response.isAvailable) {
      cancelAllActiveStages(response.reason)
    }
  }

  def stop(): Unit = {
    stopped.synchronized {
      if (!stopped) {
        // Stop appHeartbeat first
        logInfo(s"Stop Application heartbeat $appId")
        appHeartbeat.cancel(true)
        ThreadUtils.shutdown(appHeartbeatHandlerThread)
        if (applicationUnregisterEnabled) {
          unregisterApplication()
        }
        stopped = true
      }
    }
  }
}
