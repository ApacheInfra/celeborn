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

package org.apache.celeborn.service.deploy.worker

import java.util.{HashSet => JHashSet}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting
import io.netty.util.HashedWheelTimer

import org.apache.celeborn.common.RssConf
import org.apache.celeborn.common.RssConf._
import org.apache.celeborn.common.exception.RssException
import org.apache.celeborn.common.haclient.RssHARetryClient
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskInfo, PartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.metrics.source.{JVMCPUSource, JVMSource, RPCSource}
import org.apache.celeborn.common.network.TransportContext
import org.apache.celeborn.common.network.server.{ChannelsLimiter, MemoryTracker}
import org.apache.celeborn.common.protocol.{PbRegisterWorkerResponse, RpcNameConstants, TransportModuleConstants}
import org.apache.celeborn.common.protocol.message.ControlMessages
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.util.{ShutdownHookManager, ThreadUtils, Utils}
import org.apache.celeborn.server.common.{HttpService, Service}
import org.apache.celeborn.service.deploy.worker.storage.{PartitionFilesSorter, StorageManager}

private[celeborn] class Worker(
    override val conf: RssConf,
    val workerArgs: WorkerArguments)
  extends HttpService with Logging {

  @volatile private var stopped = false

  override def serviceName: String = Service.WORKER

  override val metricsSystem: MetricsSystem =
    MetricsSystem.createMetricsSystem(serviceName, conf, WorkerSource.ServletPath)

  val rpcEnv = RpcEnv.create(
    RpcNameConstants.WORKER_SYS,
    workerArgs.host,
    workerArgs.host,
    workerArgs.port.getOrElse(0),
    conf,
    Math.max(64, Runtime.getRuntime.availableProcessors()))

  private val host = rpcEnv.address.host
  private val rpcPort = rpcEnv.address.port
  Utils.checkHost(host)

  private val WORKER_SHUTDOWN_PRIORITY = 100
  val shutdown = new AtomicBoolean(false)
  private val gracefulShutdown = RssConf.workerGracefulShutdown(conf)
  assert(
    !gracefulShutdown || (gracefulShutdown &&
      RssConf.workerRPCPort(conf) != 0 && RssConf.fetchServerPort(conf) != 0 &&
      RssConf.pushServerPort(conf) != 0 && RssConf.replicateServerPort(conf) != 0),
    "If enable graceful shutdown, the worker should use stable server port.")

  val rpcSource = new RPCSource(conf, MetricsSystem.ROLE_WORKER)
  val workerSource = new WorkerSource(conf)
  metricsSystem.registerSource(workerSource)
  metricsSystem.registerSource(rpcSource)
  metricsSystem.registerSource(new JVMSource(conf, MetricsSystem.ROLE_WORKER))
  metricsSystem.registerSource(new JVMCPUSource(conf, MetricsSystem.ROLE_WORKER))

  val storageManager = new StorageManager(conf, workerSource)

  val memoryTracker = MemoryTracker.initialize(
    workerPausePushDataRatio(conf),
    workerPauseRepcaliteRatio(conf),
    workerResumeRatio(conf),
    partitionSortMaxMemoryRatio(conf),
    workerDirectMemoryPressureCheckIntervalMs(conf),
    workerDirectMemoryReportIntervalSecond(conf))
  memoryTracker.registerMemoryListener(storageManager)

  val partitionsSorter = new PartitionFilesSorter(memoryTracker, conf, workerSource)

  var controller = new Controller(rpcEnv, conf, metricsSystem)
  rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP, controller, Some(rpcSource))

  val pushDataHandler = new PushDataHandler()
  val (pushServer, pushClientFactory) = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt("rss.push.io.threads", storageManager.disksSnapshot().size * 2)
    val transportConf = Utils.fromRssConf(conf, TransportModuleConstants.PUSH_MODULE, numThreads)
    val pushServerLimiter = new ChannelsLimiter(TransportModuleConstants.PUSH_MODULE)
    val transportContext: TransportContext =
      new TransportContext(transportConf, pushDataHandler, closeIdleConnections, pushServerLimiter)
    (
      transportContext.createServer(RssConf.pushServerPort(conf)),
      transportContext.createClientFactory())
  }

  val replicateHandler = new PushDataHandler()
  private val replicateServer = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads =
      conf.getInt("rss.replicate.io.threads", storageManager.disksSnapshot().size * 2)
    val transportConf =
      Utils.fromRssConf(conf, TransportModuleConstants.REPLICATE_MODULE, numThreads)
    val replicateLimiter = new ChannelsLimiter(TransportModuleConstants.REPLICATE_MODULE)
    val transportContext: TransportContext =
      new TransportContext(transportConf, replicateHandler, closeIdleConnections, replicateLimiter)
    transportContext.createServer(RssConf.replicateServerPort(conf))
  }

  var fetchHandler: FetchHandler = _
  private val fetchServer = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt("rss.fetch.io.threads", storageManager.disksSnapshot().size * 2)
    val transportConf = Utils.fromRssConf(conf, TransportModuleConstants.FETCH_MODULE, numThreads)
    fetchHandler = new FetchHandler(transportConf)
    val transportContext: TransportContext =
      new TransportContext(transportConf, fetchHandler, closeIdleConnections)
    transportContext.createServer(RssConf.fetchServerPort(conf))
  }

  private val pushPort = pushServer.getPort
  private val fetchPort = fetchServer.getPort
  private val replicatePort = replicateServer.getPort

  assert(pushPort > 0)
  assert(fetchPort > 0)
  assert(replicatePort > 0)

  storageManager.updateDiskInfos()
  // WorkerInfo's diskInfos is a reference to storageManager.diskInfos
  val diskInfos = new ConcurrentHashMap[String, DiskInfo]()
  storageManager.disksSnapshot().foreach { case diskInfo =>
    diskInfos.put(diskInfo.mountPoint, diskInfo)
  }
  val workerInfo =
    new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort, diskInfos, controller.self)

  // whether this Worker registered to Master successfully
  val registered = new AtomicBoolean(false)

  val shuffleMapperAttempts = new ConcurrentHashMap[String, Array[Int]]()
  val partitionLocationInfo = new PartitionLocationInfo

  private val rssHARetryClient = new RssHARetryClient(rpcEnv, conf)

  // (workerInfo -> last connect timeout timestamp)
  val unavailablePeers = new ConcurrentHashMap[WorkerInfo, Long]()

  // Threads
  private val forwardMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
  private var sendHeartbeatTask: ScheduledFuture[_] = _
  private var checkFastfailTask: ScheduledFuture[_] = _
  val replicateThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-replicate-data",
    RssConf.workerReplicateNumThreads(conf))
  val commitThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "Worker-CommitFiles",
    RssConf.workerAsyncCommitFileThreads(conf))
  val asyncReplyPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("async-reply")
  val timer = new HashedWheelTimer()

  // Configs
  private val HEARTBEAT_MILLIS = RssConf.workerTimeoutMs(conf) / 4
  private val REPLICATE_FAST_FAIL_DURATION = RssConf.replicateFastFailDurationMs(conf)

  private val cleanTaskQueue = new LinkedBlockingQueue[JHashSet[String]]
  var cleaner: Thread = _

  workerSource.addGauge(
    WorkerSource.RegisteredShuffleCount,
    _ => workerInfo.getShuffleKeySet.size())
  workerSource.addGauge(WorkerSource.SlotsAllocated, _ => workerInfo.allocationsInLastHour())
  workerSource.addGauge(WorkerSource.SortMemory, _ => memoryTracker.getSortMemoryCounter.get())
  workerSource.addGauge(WorkerSource.SortingFiles, _ => partitionsSorter.getSortingCount)
  workerSource.addGauge(WorkerSource.SortedFiles, _ => partitionsSorter.getSortedCount)
  workerSource.addGauge(WorkerSource.SortedFileSize, _ => partitionsSorter.getSortedSize)
  workerSource.addGauge(WorkerSource.DiskBuffer, _ => memoryTracker.getDiskBufferCounter.get())
  workerSource.addGauge(WorkerSource.NettyMemory, _ => memoryTracker.getNettyMemoryCounter.get())
  workerSource.addGauge(WorkerSource.PausePushDataCount, _ => memoryTracker.getPausePushDataCounter)
  workerSource.addGauge(
    WorkerSource.PausePushDataAndReplicateCount,
    _ => memoryTracker.getPausePushDataAndReplicateCounter)

  private def heartBeatToMaster(): Unit = {
    val shuffleKeys = new JHashSet[String]
    shuffleKeys.addAll(partitionLocationInfo.shuffleKeySet)
    shuffleKeys.addAll(storageManager.shuffleKeySet())
    storageManager.updateDiskInfos()
    val diskInfos = storageManager.disksSnapshot()
    val response = rssHARetryClient.askSync[HeartbeatResponse](
      HeartbeatFromWorker(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        diskInfos,
        workerInfo.updateThenGetUserResourceConsumption(
          storageManager.userResourceConsumptionSnapshot().asJava),
        shuffleKeys),
      classOf[HeartbeatResponse])
    if (response.registered) {
      response.expiredShuffleKeys.asScala.foreach(shuffleKey => workerInfo.releaseSlots(shuffleKey))
      cleanTaskQueue.put(response.expiredShuffleKeys)
    } else {
      logError("Worker not registered in master, clean expired shuffle data and register again.")
      // Clean expired shuffle.
      cleanup(response.expiredShuffleKeys)
      try {
        registerWithMaster()
      } catch {
        case e: Throwable =>
          logError("Re-register worker failed after worker lost.", e)
          // Register to master failed then stop server
          System.exit(-1)
      }
    }
  }

  override def initialize(): Unit = {
    super.initialize()
    logInfo(s"Starting Worker $host:$pushPort:$fetchPort:$replicatePort" +
      s" with ${workerInfo.diskInfos} slots.")
    registerWithMaster()

    // start heartbeat
    sendHeartbeatTask = forwardMessageScheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          heartBeatToMaster()
        }
      },
      HEARTBEAT_MILLIS,
      HEARTBEAT_MILLIS,
      TimeUnit.MILLISECONDS)

    checkFastfailTask = forwardMessageScheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          unavailablePeers.entrySet().asScala.foreach { entry =>
            if (System.currentTimeMillis() - entry.getValue > REPLICATE_FAST_FAIL_DURATION) {
              unavailablePeers.remove(entry.getKey)
            }
          }
        }
      },
      0,
      REPLICATE_FAST_FAIL_DURATION,
      TimeUnit.MILLISECONDS)

    cleaner = new Thread("Cleaner") {
      override def run(): Unit = {
        while (true) {
          val expiredShuffleKeys = cleanTaskQueue.take()
          try {
            cleanup(expiredShuffleKeys)
          } catch {
            case e: Throwable =>
              logError("Cleanup failed", e)
          }
        }
      }
    }

    pushDataHandler.init(this)
    replicateHandler.init(this)
    fetchHandler.init(this)
    controller.init(this)

    cleaner.setDaemon(true)
    cleaner.start()

    logInfo("Worker started.")
    rpcEnv.awaitTermination()
  }

  override def close(): Unit = synchronized {
    if (!stopped) {
      logInfo("Stopping Worker.")

      if (sendHeartbeatTask != null) {
        sendHeartbeatTask.cancel(true)
        sendHeartbeatTask = null
      }
      if (checkFastfailTask != null) {
        checkFastfailTask.cancel(true)
        checkFastfailTask = null
      }
      forwardMessageScheduler.shutdownNow()
      replicateThreadPool.shutdownNow()
      commitThreadPool.shutdownNow()
      asyncReplyPool.shutdownNow()
      partitionsSorter.close()

      if (null != storageManager) {
        storageManager.close()
      }

      rssHARetryClient.close()
      replicateServer.close()
      fetchServer.close()

      super.close()

      logInfo("Worker is stopped.")
      stopped = true
    }
  }

  private def registerWithMaster(): Unit = {
    var registerTimeout = RssConf.registerWorkerTimeoutMs(conf)
    val interval = 2000
    while (registerTimeout > 0) {
      val resp =
        try {
          rssHARetryClient.askSync[PbRegisterWorkerResponse](
            ControlMessages.pbRegisterWorker(
              host,
              rpcPort,
              pushPort,
              fetchPort,
              replicatePort,
              workerInfo.diskInfos.asScala.toMap,
              workerInfo.updateThenGetUserResourceConsumption(
                storageManager.userResourceConsumptionSnapshot().asJava).asScala.toMap,
              RssHARetryClient.genRequestId()),
            classOf[PbRegisterWorkerResponse])
        } catch {
          case throwable: Throwable =>
            logWarning(
              s"Register worker to master failed, will retry after ${Utils.msDurationToString(interval)}",
              throwable)
            null
        }
      // Register successfully
      if (null != resp && resp.getSuccess) {
        registered.set(true)
        logInfo("Register worker successfully.")
        return
      }
      // Register failed, sleep and retry
      Thread.sleep(interval)
      registerTimeout = registerTimeout - interval
    }
    // If worker register still failed after retry, throw exception to stop worker process
    throw new RssException("Register worker failed.")
  }

  private def cleanup(expiredShuffleKeys: JHashSet[String]): Unit = synchronized {
    expiredShuffleKeys.asScala.foreach { shuffleKey =>
      partitionLocationInfo.getAllMasterLocations(shuffleKey).asScala.foreach { partition =>
        partition.asInstanceOf[WorkingPartition].getFileWriter.destroy()
      }
      partitionLocationInfo.getAllSlaveLocations(shuffleKey).asScala.foreach { partition =>
        partition.asInstanceOf[WorkingPartition].getFileWriter.destroy()
      }
      partitionLocationInfo.removeMasterPartitions(shuffleKey)
      partitionLocationInfo.removeSlavePartitions(shuffleKey)
      shuffleMapperAttempts.remove(shuffleKey)
      workerInfo.releaseSlots(shuffleKey)
      logInfo(s"Cleaned up expired shuffle $shuffleKey")
    }
    partitionsSorter.cleanup(expiredShuffleKeys)
    storageManager.cleanupExpiredShuffleKey(expiredShuffleKeys)
  }

  override def getWorkerInfo: String = workerInfo.toString()

  override def getThreadDump: String = Utils.getThreadDump()

  override def getHostnameList: String = throw new UnsupportedOperationException()

  override def getApplicationList: String = throw new UnsupportedOperationException()

  override def getShuffleList: String = {
    storageManager.shuffleKeySet().asScala.mkString("\n")
  }

  @VisibleForTesting
  def isRegistered(): Boolean = {
    registered.get()
  }

  ShutdownHookManager.get().addShutdownHook(
    new Thread(new Runnable {
      override def run(): Unit = {
        logInfo("Shutdown hook called.")
        shutdown.set(true)
        if (gracefulShutdown) {
          val interval = RssConf.checkSlotsFinishedInterval(conf)
          val timeout = RssConf.checkSlotsFinishedTimeoutMs(conf)
          var waitTimes = 0

          def waitTime: Long = waitTimes * interval

          while (!partitionLocationInfo.isEmpty && waitTime < timeout) {
            Thread.sleep(interval)
            waitTimes += 1
          }
          if (partitionLocationInfo.isEmpty) {
            logInfo(s"Waiting for all PartitionLocation released cost ${waitTime}ms.")
          } else {
            logWarning(s"Waiting for all PartitionLocation release cost ${waitTime}ms, " +
              s"unreleased PartitionLocation: \n$partitionLocationInfo")
          }
        }
        close()
      }
    }),
    WORKER_SHUTDOWN_PRIORITY)
}

private[deploy] object Worker extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new RssConf
    val workerArgs = new WorkerArguments(args, conf)
    // There are many entries for setting the master address, and we should unify the entries as
    // much as possible. Therefore, if the user manually specifies the address of the Master when
    // starting the Worker, we should set it in the parameters and automatically calculate what the
    // address of the Master should be used in the end.
    if (workerArgs.master != null) {
      conf.set("rss.master.address", RpcAddress.fromRssURL(workerArgs.master).toString)
    }

    val worker = new Worker(conf, workerArgs)
    worker.initialize()
  }
}
