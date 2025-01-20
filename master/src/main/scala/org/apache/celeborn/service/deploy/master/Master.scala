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

import java.io.IOException
import java.net.BindException
import java.util
import java.util.Collections
import java.util.concurrent.{ExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.ToLongFunction

import scala.collection.JavaConverters._
import scala.util.Random

import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.ratis.proto.RaftProtos
import org.apache.ratis.proto.RaftProtos.RaftPeerRole

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskInfo, WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.metrics.source.{JVMCPUSource, JVMSource, ResourceConsumptionSource, Role, SystemMiscSource, ThreadPoolSource}
import org.apache.celeborn.common.network.CelebornRackResolver
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.rpc.{RpcSecurityContextBuilder, ServerSaslContextBuilder}
import org.apache.celeborn.common.util.{CelebornHadoopUtils, JavaUtils, PbSerDeUtils, SignalUtils, ThreadUtils, Utils}
import org.apache.celeborn.server.common.{HttpService, Service}
import org.apache.celeborn.service.deploy.master.clustermeta.SingleMasterMetaManager
import org.apache.celeborn.service.deploy.master.clustermeta.ha.{HAHelper, HAMasterMetaManager, MetaHandler}
import org.apache.celeborn.service.deploy.master.quota.QuotaManager
import org.apache.celeborn.service.deploy.master.tags.TagsManager

private[celeborn] class Master(
    override val conf: CelebornConf,
    val masterArgs: MasterArguments)
  extends HttpService with RpcEndpoint with Logging {

  @volatile private var stopped = false

  override def serviceName: String = Service.MASTER

  override val metricsSystem: MetricsSystem =
    MetricsSystem.createMetricsSystem(serviceName, conf)
  // init and register master metrics
  private val resourceConsumptionSource =
    new ResourceConsumptionSource(conf, Role.MASTER)
  private val threadPoolSource = ThreadPoolSource(conf, Role.MASTER)
  private val masterSource = new MasterSource(conf)
  metricsSystem.registerSource(resourceConsumptionSource)
  metricsSystem.registerSource(masterSource)
  metricsSystem.registerSource(threadPoolSource)
  metricsSystem.registerSource(new JVMSource(conf, Role.MASTER))
  metricsSystem.registerSource(new JVMCPUSource(conf, Role.MASTER))
  metricsSystem.registerSource(new SystemMiscSource(conf, Role.MASTER))

  private val bindPreferIP: Boolean = conf.bindPreferIP
  private val authEnabled = conf.authEnabled
  private val haEnabled = conf.haEnabled
  private val secretRegistry = new MasterSecretRegistryImpl()
  private val sendApplicationMetaThreads = conf.masterSendApplicationMetaThreads
  // Send ApplicationMeta to workers
  private var sendApplicationMetaExecutor: ExecutorService = _
  private val masterPersistWorkerNetworkLocation = conf.masterPersistWorkerNetworkLocation
  PbSerDeUtils.setMasterPersistWorkerNetworkLocation(masterPersistWorkerNetworkLocation)

  if (conf.logCelebornConfEnabled) {
    logInfo(getConf)
  }

  override val rpcEnv: RpcEnv =
    if (!authEnabled) {
      RpcEnv.create(
        RpcNameConstants.MASTER_SYS,
        TransportModuleConstants.RPC_SERVICE_MODULE,
        masterArgs.host,
        masterArgs.port,
        conf,
        Math.max(64, Runtime.getRuntime.availableProcessors()),
        Role.MASTER,
        None,
        None)
    } else {
      val externalSecurityContext = new RpcSecurityContextBuilder()
        .withServerSaslContext(
          new ServerSaslContextBuilder()
            .withAddRegistrationBootstrap(true)
            .withSecretRegistry(secretRegistry).build()).build()
      logInfo(
        s"Secure port enabled ${masterArgs.port} for secured RPC.")
      RpcEnv.create(
        RpcNameConstants.MASTER_SYS,
        TransportModuleConstants.RPC_SERVICE_MODULE,
        masterArgs.host,
        masterArgs.port,
        conf,
        Math.max(64, Runtime.getRuntime.availableProcessors()),
        Role.MASTER,
        Some(externalSecurityContext),
        None)
    }
  metricsSystem.registerSource(rpcEnv.rpcSource())

  // Visible for testing
  private[master] var internalRpcEnvInUse: RpcEnv =
    if (!conf.internalPortEnabled) {
      rpcEnv
    } else {
      logInfo(
        s"Internal port enabled, using internal port ${masterArgs.internalPort} for internal RPC.")
      RpcEnv.create(
        RpcNameConstants.MASTER_INTERNAL_SYS,
        TransportModuleConstants.RPC_SERVICE_MODULE,
        masterArgs.host,
        masterArgs.internalPort,
        conf,
        Math.max(64, Runtime.getRuntime.availableProcessors()),
        Role.MASTER,
        None,
        None)
    }

  private val rackResolver = new CelebornRackResolver(conf)
  private[celeborn] val statusSystem =
    if (haEnabled) {
      val sys = new HAMasterMetaManager(internalRpcEnvInUse, conf, rackResolver)
      val handler = new MetaHandler(sys)
      try {
        handler.setUpMasterRatisServer(conf, masterArgs.masterClusterInfo.get)
      } catch {
        case ioe: IOException =>
          if (ioe.getCause.isInstanceOf[BindException]) {
            val msg = s"HA port ${sys.getRatisServer.getRaftPort} of Ratis Server is occupied, " +
              s"Master process will stop. Please refer to configuration doc to modify the HA port " +
              s"in config file for each node."
            logError(msg, ioe)
            System.exit(1)
          } else {
            logError("Face unexpected IO exception during staring Ratis server", ioe)
          }
      }
      sys
    } else {
      new SingleMasterMetaManager(internalRpcEnvInUse, conf, rackResolver)
    }
  secretRegistry.setMetadataHandler(statusSystem)

  // Threads
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-message-forwarder")
  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _
  private var checkForApplicationTimeOutTask: ScheduledFuture[_] = _
  private var checkForUnavailableWorkerTimeOutTask: ScheduledFuture[_] = _
  private var checkForDFSRemnantDirsTimeOutTask: ScheduledFuture[_] = _
  private val nonEagerHandler = ThreadUtils.newDaemonCachedThreadPool("master-noneager-handler", 64)

  // Config constants
  private val workerHeartbeatTimeoutMs = conf.workerHeartbeatTimeout
  private val appHeartbeatTimeoutMs = conf.appHeartbeatTimeoutMs
  private val workerUnavailableInfoExpireTimeoutMs = conf.workerUnavailableInfoExpireTimeout
  private val allowWorkerHostPattern = conf.allowWorkerHostPattern
  private val denyWorkerHostPattern = conf.denyWorkerHostPattern

  private val dfsExpireDirsTimeoutMS = conf.dfsExpireDirsTimeoutMS
  private val hasHDFSStorage = conf.hasHDFSStorage
  private val hasS3Storage = conf.hasS3Storage

  private val quotaManager = new QuotaManager(conf, configService)
  private val tagsManager = new TagsManager(Option(configService))
  private val masterResourceConsumptionInterval = conf.masterResourceConsumptionInterval
  private val userResourceConsumptions =
    JavaUtils.newConcurrentHashMap[UserIdentifier, (ResourceConsumption, Long)]()

  private val slotsAssignMaxWorkers = conf.masterSlotAssignMaxWorkers
  private val slotsAssignLoadAwareDiskGroupNum = conf.masterSlotAssignLoadAwareDiskGroupNum
  private val slotsAssignLoadAwareDiskGroupGradient =
    conf.masterSlotAssignLoadAwareDiskGroupGradient
  private val loadAwareFlushTimeWeight = conf.masterSlotAssignLoadAwareFlushTimeWeight
  private val loadAwareFetchTimeWeight = conf.masterSlotAssignLoadAwareFetchTimeWeight

  private val estimatedPartitionSizeUpdaterInitialDelay =
    conf.estimatedPartitionSizeUpdaterInitialDelay
  private val estimatedPartitionSizeForEstimationUpdateInterval =
    conf.estimatedPartitionSizeForEstimationUpdateInterval
  private val partitionSizeUpdateService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-partition-size-updater")
  partitionSizeUpdateService.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        executeWithLeaderChecker(
          null, {
            statusSystem.handleUpdatePartitionSize()
            logInfo(s"Cluster estimate partition size ${Utils.bytesToString(statusSystem.estimatedPartitionSize)}")
          })
      }
    },
    estimatedPartitionSizeUpdaterInitialDelay,
    estimatedPartitionSizeForEstimationUpdateInterval,
    TimeUnit.MILLISECONDS)
  private val slotsAssignPolicy = conf.masterSlotAssignPolicy

  private var hadoopFs: util.Map[StorageInfo.Type, FileSystem] = _
  masterSource.addGauge(MasterSource.REGISTERED_SHUFFLE_COUNT) { () =>
    statusSystem.registeredShuffleCount
  }
  masterSource.addGauge(MasterSource.WORKER_COUNT) { () => statusSystem.workersMap.size }
  masterSource.addGauge(MasterSource.LOST_WORKER_COUNT) { () => statusSystem.lostWorkers.size }
  masterSource.addGauge(MasterSource.EXCLUDED_WORKER_COUNT) { () =>
    statusSystem.excludedWorkers.size + statusSystem.manuallyExcludedWorkers.size
  }
  masterSource.addGauge(MasterSource.AVAILABLE_WORKER_COUNT) { () =>
    statusSystem.availableWorkers.size()
  }
  masterSource.addGauge(MasterSource.SHUTDOWN_WORKER_COUNT) { () =>
    statusSystem.shutdownWorkers.size
  }
  masterSource.addGauge(MasterSource.RUNNING_APPLICATION_COUNT) { () =>
    statusSystem.appHeartbeatTime.size
  }
  masterSource.addGauge(MasterSource.PARTITION_SIZE) { () => statusSystem.estimatedPartitionSize }
  masterSource.addGauge(MasterSource.ACTIVE_SHUFFLE_SIZE) { () =>
    statusSystem.workersMap.values().parallelStream()
      .mapToLong(new ToLongFunction[WorkerInfo]() {
        override def applyAsLong(value: WorkerInfo): Long =
          value.userResourceConsumption.values().parallelStream()
            .mapToLong(new ToLongFunction[ResourceConsumption]() {
              override def applyAsLong(value: ResourceConsumption): Long = value.diskBytesWritten
            }).sum()
      }).sum()
  }
  masterSource.addGauge(MasterSource.ACTIVE_SHUFFLE_FILE_COUNT) { () =>
    statusSystem.workersMap.values().parallelStream()
      .mapToLong(new ToLongFunction[WorkerInfo]() {
        override def applyAsLong(value: WorkerInfo): Long =
          value.userResourceConsumption.values().parallelStream()
            .mapToLong(new ToLongFunction[ResourceConsumption]() {
              override def applyAsLong(value: ResourceConsumption): Long = value.diskFileCount
            }).sum()
      }).sum()
  }

  masterSource.addGauge(MasterSource.SHUFFLE_TOTAL_COUNT) { () =>
    statusSystem.shuffleTotalCount.sum()
  }

  masterSource.addGauge(MasterSource.APPLICATION_TOTAL_COUNT) { () =>
    statusSystem.applicationTotalCount.sum()
  }

  masterSource.addGauge(MasterSource.SHUFFLE_FALLBACK_COUNT) { () =>
    statusSystem.shuffleFallbackCounts.values().asScala.map(_.longValue()).sum
  }

  masterSource.addGauge(MasterSource.APPLICATION_FALLBACK_COUNT) { () =>
    statusSystem.applicationFallbackCounts.values().asScala.map(_.longValue()).sum
  }

  masterSource.addGauge(MasterSource.DEVICE_CELEBORN_TOTAL_CAPACITY) { () =>
    statusSystem.workersMap.values().asScala.toList.map(_.totalSpace()).sum
  }

  masterSource.addGauge(MasterSource.DEVICE_CELEBORN_FREE_CAPACITY) { () =>
    statusSystem.availableWorkers.asScala.toList.map(_.totalActualUsableSpace()).sum
  }

  masterSource.addGauge(MasterSource.IS_ACTIVE_MASTER) { () => isMasterActive }

  masterSource.addGauge(MasterSource.DECOMMISSION_WORKER_COUNT) { () =>
    statusSystem.decommissionWorkers.size()
  }

  if (haEnabled) {
    masterSource.addGauge(MasterSource.RATIS_APPLY_COMPLETED_INDEX) { () =>
      getRatisApplyCompletedIndex
    }
  }

  private val threadsStarted: AtomicBoolean = new AtomicBoolean(false)
  rpcEnv.setupEndpoint(RpcNameConstants.MASTER_EP, this)
  // Visible for testing
  private[master] var internalRpcEndpoint: RpcEndpoint = _
  private var internalRpcEndpointRef: RpcEndpointRef = _
  if (conf.internalPortEnabled) {
    internalRpcEndpoint = new InternalRpcEndpoint(this, internalRpcEnvInUse, conf)
    internalRpcEndpointRef = internalRpcEnvInUse.setupEndpoint(
      RpcNameConstants.MASTER_INTERNAL_EP,
      internalRpcEndpoint)
  }

  // Maintains the mapping for the workers assigned to each application
  private val workersAssignedToApp
      : util.concurrent.ConcurrentHashMap[String, util.Set[WorkerInfo]] =
    JavaUtils.newConcurrentHashMap[String, util.Set[WorkerInfo]]()

  // start threads to check timeout for workers and applications
  override def onStart(): Unit = {
    if (!threadsStarted.compareAndSet(false, true)) {
      return
    }
    if (authEnabled) {
      sendApplicationMetaExecutor = ThreadUtils.newDaemonFixedThreadPool(
        sendApplicationMetaThreads,
        "send-application-meta")
    }

    checkForWorkerTimeOutTask = scheduleCheckTask(workerHeartbeatTimeoutMs, pbCheckForWorkerTimeout)
    checkForApplicationTimeOutTask =
      scheduleCheckTask(appHeartbeatTimeoutMs / 2, CheckForApplicationTimeOut)

    if (workerUnavailableInfoExpireTimeoutMs > 0) {
      checkForUnavailableWorkerTimeOutTask = scheduleCheckTask(
        workerUnavailableInfoExpireTimeoutMs / 2,
        CheckForWorkerUnavailableInfoTimeout)
    }

    if (hasHDFSStorage || hasS3Storage) {
      checkForDFSRemnantDirsTimeOutTask =
        scheduleCheckTask(dfsExpireDirsTimeoutMS, CheckForDFSExpiredDirsTimeout)
    }

  }

  private def scheduleCheckTask[T](timeoutMS: Long, message: T): ScheduledFuture[_] = {
    forwardMessageThread.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          self.send(message)
        }
      },
      timeoutMS,
      timeoutMS,
      TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    if (!threadsStarted.compareAndSet(true, false)) {
      return
    }
    logInfo("Stopping Celeborn Master.")
    Option(checkForWorkerTimeOutTask).foreach(_.cancel(true))
    Option(checkForUnavailableWorkerTimeOutTask).foreach(_.cancel(true))
    Option(checkForApplicationTimeOutTask).foreach(_.cancel(true))
    Option(checkForDFSRemnantDirsTimeOutTask).foreach(_.cancel(true))
    forwardMessageThread.shutdownNow()
    rackResolver.stop()
    if (authEnabled) {
      sendApplicationMetaExecutor.shutdownNow()
    }
    logInfo("Celeborn Master is stopped.")
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logDebug(s"Client $address got disassociated.")
  }

  def executeWithLeaderChecker[T](context: RpcCallContext, f: => T): Unit =
    if (HAHelper.checkShouldProcess(context, statusSystem, bindPreferIP)) {
      try {
        f
      } catch {
        case e: Exception =>
          HAHelper.sendFailure(context, HAHelper.getRatisServer(statusSystem), e, bindPreferIP)
      }
    }

  override def receive: PartialFunction[Any, Unit] = {
    case _: PbCheckForWorkerTimeout =>
      executeWithLeaderChecker(null, timeoutDeadWorkers())
    case CheckForWorkerUnavailableInfoTimeout =>
      executeWithLeaderChecker(null, timeoutWorkerUnavailableInfos())
    case CheckForApplicationTimeOut =>
      executeWithLeaderChecker(null, timeoutDeadApplications())
    case CheckForDFSExpiredDirsTimeout =>
      executeWithLeaderChecker(null, checkAndCleanExpiredAppDirsOnDFS())
    case pb: PbWorkerLost =>
      val host = pb.getHost
      val rpcPort = pb.getRpcPort
      val pushPort = pb.getPushPort
      val fetchPort = pb.getFetchPort
      val replicatePort = pb.getReplicatePort
      val requestId = pb.getRequestId
      logDebug(s"Received worker lost $host:$rpcPort:$pushPort:$fetchPort:$replicatePort.")
      executeWithLeaderChecker(
        null,
        handleWorkerLost(null, host, rpcPort, pushPort, fetchPort, replicatePort, requestId))
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case HeartbeatFromApplication(
          appId,
          totalWritten,
          fileCount,
          shuffleCount,
          applicationCount,
          shuffleFallbackCounts,
          applicationFallbackCounts,
          needCheckedWorkerList,
          requestId,
          shouldResponse) =>
      logDebug(s"Received heartbeat from app $appId")
      checkAuth(context, appId)
      executeWithLeaderChecker(
        context,
        handleHeartbeatFromApplication(
          context,
          appId,
          totalWritten,
          fileCount,
          shuffleCount,
          applicationCount,
          shuffleFallbackCounts,
          applicationFallbackCounts,
          needCheckedWorkerList,
          requestId,
          shouldResponse))

    case pbRegisterWorker: PbRegisterWorker =>
      val requestId = pbRegisterWorker.getRequestId
      val host = pbRegisterWorker.getHost
      val rpcPort = pbRegisterWorker.getRpcPort
      val pushPort = pbRegisterWorker.getPushPort
      val fetchPort = pbRegisterWorker.getFetchPort
      val replicatePort = pbRegisterWorker.getReplicatePort
      val internalPort = pbRegisterWorker.getInternalPort
      val networkLocation = pbRegisterWorker.getNetworkLocation
      val disks = pbRegisterWorker.getDisksList.asScala
        .map { pbDiskInfo => pbDiskInfo.getMountPoint -> PbSerDeUtils.fromPbDiskInfo(pbDiskInfo) }
        .toMap.asJava
      val userResourceConsumption =
        PbSerDeUtils.fromPbUserResourceConsumption(pbRegisterWorker.getUserResourceConsumptionMap)

      logDebug(s"Received RegisterWorker request $requestId, $host:$pushPort:$replicatePort" +
        s" $disks.")
      executeWithLeaderChecker(
        context,
        handleRegisterWorker(
          context,
          host,
          rpcPort,
          pushPort,
          fetchPort,
          replicatePort,
          internalPort,
          networkLocation,
          disks,
          userResourceConsumption,
          requestId))

    case ReleaseSlots(_, _, _, _, _) =>
      // keep it for compatible reason
      context.reply(ReleaseSlotsResponse(StatusCode.SUCCESS))

    case requestSlots @ RequestSlots(applicationId, _, _, _, _, _, _, _, _, _, _, _, _) =>
      logTrace(s"Received RequestSlots request $requestSlots.")
      checkAuth(context, applicationId)
      executeWithLeaderChecker(context, handleRequestSlots(context, requestSlots))

    case pb: PbBatchUnregisterShuffles =>
      val applicationId = pb.getAppId
      val shuffleIds = pb.getShuffleIdsList.asScala.toList
      val requestId = pb.getRequestId
      logDebug(s"Received BatchUnregisterShuffle request $requestId, $applicationId, $shuffleIds")
      checkAuth(context, applicationId)
      executeWithLeaderChecker(
        context,
        batchHandleUnregisterShuffles(context, applicationId, shuffleIds, requestId))

    case pb: PbUnregisterShuffle =>
      val applicationId = pb.getAppId
      val shuffleId = pb.getShuffleId
      val requestId = pb.getRequestId
      logDebug(s"Received UnregisterShuffle request $requestId, $applicationId, $shuffleId")
      checkAuth(context, applicationId)
      executeWithLeaderChecker(
        context,
        handleUnregisterShuffle(context, applicationId, shuffleId, requestId))

    case ApplicationLost(appId, requestId) =>
      logDebug(
        s"Received ApplicationLost request $requestId, $appId from ${context.senderAddress}.")
      checkAuth(context, appId)
      executeWithLeaderChecker(context, handleApplicationLost(context, appId, requestId))

    case HeartbeatFromWorker(
          host,
          rpcPort,
          pushPort,
          fetchPort,
          replicatePort,
          disks,
          userResourceConsumption,
          activeShuffleKey,
          highWorkload,
          workerStatus,
          requestId) =>
      logDebug(s"Received heartbeat from" +
        s" worker $host:$rpcPort:$pushPort:$fetchPort:$replicatePort with $disks.")
      executeWithLeaderChecker(
        context,
        handleHeartbeatFromWorker(
          context,
          host,
          rpcPort,
          pushPort,
          fetchPort,
          replicatePort,
          disks,
          userResourceConsumption,
          activeShuffleKey,
          highWorkload,
          workerStatus,
          requestId))

    case ReportWorkerUnavailable(failedWorkers: util.List[WorkerInfo], requestId: String) =>
      executeWithLeaderChecker(
        context,
        handleReportNodeUnavailable(context, failedWorkers, requestId))

    case ReportWorkerDecommission(workers: util.List[WorkerInfo], requestId: String) =>
      executeWithLeaderChecker(
        context,
        handleWorkerDecommission(context, workers, requestId))

    case pb: PbReviseLostShuffles =>
      executeWithLeaderChecker(
        context,
        handleReviseLostShuffle(context, pb.getAppId, pb.getLostShufflesList, pb.getRequestId))

    case pb: PbWorkerExclude =>
      val workersToAdd = new util.ArrayList[WorkerInfo](pb.getWorkersToAddList
        .asScala.map(PbSerDeUtils.fromPbWorkerInfo).toList.asJava)
      val workersToRemove = new util.ArrayList[WorkerInfo](pb.getWorkersToRemoveList
        .asScala.map(PbSerDeUtils.fromPbWorkerInfo).toList.asJava)
      executeWithLeaderChecker(
        context,
        handleWorkerExclude(
          context,
          workersToAdd,
          workersToRemove,
          pb.getRequestId))

    case pb: PbWorkerLost =>
      val host = pb.getHost
      val rpcPort = pb.getRpcPort
      val pushPort = pb.getPushPort
      val fetchPort = pb.getFetchPort
      val replicatePort = pb.getReplicatePort
      val requestId = pb.getRequestId
      logInfo(s"Received worker lost $host:$rpcPort:$pushPort:$fetchPort:$replicatePort.")
      executeWithLeaderChecker(
        context,
        handleWorkerLost(context, host, rpcPort, pushPort, fetchPort, replicatePort, requestId))

    case CheckQuota(userIdentifier) =>
      executeWithLeaderChecker(context, handleCheckQuota(userIdentifier, context))

    case _: PbCheckWorkersAvailable =>
      executeWithLeaderChecker(context, handleCheckWorkersAvailable(context))

    case pb: PbWorkerEventRequest =>
      val workers = new util.ArrayList[WorkerInfo](pb.getWorkersList
        .asScala.map(PbSerDeUtils.fromPbWorkerInfo).toList.asJava)
      executeWithLeaderChecker(
        context,
        handleWorkerEvent(
          pb.getRequestId,
          pb.getWorkerEventType.getNumber,
          workers,
          context))

    case pb: PbApplicationMetaRequest =>
      // This request is from a worker
      executeWithLeaderChecker(context, handleRequestForApplicationMeta(context, pb))

    case pb: PbRemoveWorkersUnavailableInfo =>
      val unavailableWorkers = new util.ArrayList[WorkerInfo](pb.getWorkerInfoList
        .asScala.map(PbSerDeUtils.fromPbWorkerInfo).toList.asJava)
      executeWithLeaderChecker(
        context,
        handleRemoveWorkersUnavailableInfos(context, unavailableWorkers, pb.getRequestId))
  }

  private def timeoutDeadWorkers(): Unit = {
    val currentTime = System.currentTimeMillis()
    // Need increase timeout deadline to avoid long time leader election period
    if (HAHelper.getWorkerTimeoutDeadline(statusSystem) > currentTime) {
      return
    }

    statusSystem.workersMap.values().asScala.foreach { worker =>
      if (worker.lastHeartbeat < currentTime - workerHeartbeatTimeoutMs
        && !statusSystem.workerLostEvents.contains(worker)) {
        logWarning(s"Worker ${worker.readableAddress()} timeout! Trigger WorkerLost event.")
        // trigger WorkerLost event
        self.send(WorkerLost(
          worker.host,
          worker.rpcPort,
          worker.pushPort,
          worker.fetchPort,
          worker.replicatePort,
          MasterClient.genRequestId()))
      }
    }
  }

  private def timeoutWorkerUnavailableInfos(): Unit = {
    val currentTime = System.currentTimeMillis()
    // Need increase timeout deadline to avoid long time leader election period
    if (HAHelper.getWorkerTimeoutDeadline(statusSystem) > currentTime) {
      return
    }

    val unavailableInfoTimeoutWorkers = statusSystem.lostWorkers.asScala.filter {
      case (_, lostTime) => currentTime - lostTime > workerUnavailableInfoExpireTimeoutMs
    }.keySet.toSeq

    if (!unavailableInfoTimeoutWorkers.isEmpty) {
      val handleResponse = removeWorkersUnavailableInfo(unavailableInfoTimeoutWorkers)
      logDebug(s"Remove unavailable info for workers response: $handleResponse")
    }
  }

  private def timeoutDeadApplications(): Unit = {
    val currentTime = System.currentTimeMillis()
    // Need increase timeout deadline to avoid long time leader election period
    if (HAHelper.getAppTimeoutDeadline(statusSystem) > currentTime) {
      return
    }
    statusSystem.appHeartbeatTime.asScala.foreach { case (appId, heartbeatTime) =>
      if (heartbeatTime < currentTime - appHeartbeatTimeoutMs) {
        logWarning(s"Application $appId timeout, trigger applicationLost event.")
        val requestId = MasterClient.genRequestId()
        handleApplicationLost(null, appId, requestId)
      }
    }
  }

  private def handleHeartbeatFromWorker(
      context: RpcCallContext,
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      disks: Seq[DiskInfo],
      userResourceConsumption: util.Map[UserIdentifier, ResourceConsumption],
      activeShuffleKeys: util.Set[String],
      highWorkload: Boolean,
      workerStatus: WorkerStatus,
      requestId: String): Unit = {
    val targetWorker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort)
    val registered = statusSystem.workersMap.containsKey(targetWorker.toUniqueId)
    if (!registered) {
      logWarning(s"Received heartbeat from unknown worker " +
        s"$host:$rpcPort:$pushPort:$fetchPort:$replicatePort.")
    } else {
      statusSystem.handleWorkerHeartbeat(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        disks.map { disk => disk.mountPoint -> disk }.toMap.asJava,
        userResourceConsumption,
        System.currentTimeMillis(),
        highWorkload,
        workerStatus,
        requestId)
    }

    val expiredShuffleKeys = new util.HashSet[String]
    activeShuffleKeys.asScala.foreach { shuffleKey =>
      val (appId, shuffleId) = Utils.splitShuffleKey(shuffleKey)
      val shuffleIds = statusSystem.registeredAppAndShuffles.get(appId)
      if (shuffleIds == null || !shuffleIds.contains(shuffleId)) {
        logWarning(
          s"Shuffle $shuffleKey expired on $host:$rpcPort:$pushPort:$fetchPort:$replicatePort.")
        expiredShuffleKeys.add(shuffleKey)
      }
    }
    logDebug(
      s"Shuffle ${expiredShuffleKeys.asScala.mkString("[", " ,", "]")} expired on ${targetWorker.toUniqueId}.")

    val workerEventInfo = statusSystem.workerEventInfos.get(targetWorker)
    if (workerEventInfo == null) {
      context.reply(HeartbeatFromWorkerResponse(
        expiredShuffleKeys,
        registered))
    } else {
      context.reply(HeartbeatFromWorkerResponse(
        expiredShuffleKeys,
        registered,
        workerEventInfo.getEventType))
    }
  }

  private def handleReviseLostShuffle(
      context: RpcCallContext,
      appId: String,
      lostShuffles: java.util.List[Integer],
      requestId: String) = {
    try {
      logInfo(s"Handle lost shuffles for ${appId} ${lostShuffles} ")
      statusSystem.handleReviseLostShuffles(appId, lostShuffles, requestId);
      if (context != null) {
        context.reply(ReviseLostShufflesResponse(true, ""))
      }
    } catch {
      case e: Exception =>
        context.reply(ReviseLostShufflesResponse(false, e.getMessage))
    }
  }

  private def handleWorkerExclude(
      context: RpcCallContext,
      workersToAdd: util.List[WorkerInfo],
      workersToRemove: util.List[WorkerInfo],
      requestId: String): Unit = {
    statusSystem.handleWorkerExclude(workersToAdd, workersToRemove, requestId)
    if (context != null) {
      context.reply(WorkerExcludeResponse(true))
    }
  }

  private def handleWorkerLost(
      context: RpcCallContext,
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      requestId: String): Unit = {
    val targetWorker = new WorkerInfo(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      -1,
      new util.HashMap[String, DiskInfo](),
      JavaUtils.newConcurrentHashMap[UserIdentifier, ResourceConsumption]())
    val worker: WorkerInfo = statusSystem.workersMap.get(targetWorker.toUniqueId)
    if (worker == null) {
      logWarning(s"Unknown worker $host:$rpcPort:$pushPort:$fetchPort:$replicatePort" +
        s" for WorkerLost handler!")
    } else {
      statusSystem.handleWorkerLost(host, rpcPort, pushPort, fetchPort, replicatePort, requestId)
    }
    if (context != null) {
      context.reply(WorkerLostResponse(true))
    }
  }

  def handleRegisterWorker(
      context: RpcCallContext,
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      internalPort: Int,
      networkLocation: String,
      disks: util.Map[String, DiskInfo],
      userResourceConsumption: util.Map[UserIdentifier, ResourceConsumption],
      requestId: String): Unit = {
    val workerToRegister =
      new WorkerInfo(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        internalPort,
        disks,
        userResourceConsumption)

    if (!workerHostAllowedToRegister(host)) {
      val msg =
        s"Worker ${workerToRegister.readableAddress} is not allowed to register due to host" +
          s" does not match the allow worker host pattern: ${allowWorkerHostPattern.orNull} " +
          s" or match the deny worker host pattern: ${denyWorkerHostPattern.orNull}."
      logError(msg)
      context.reply(RegisterWorkerResponse(false, msg))
      return
    }

    if (statusSystem.workersMap.containsKey(workerToRegister.toUniqueId)) {
      logWarning(s"Receive RegisterWorker while worker" +
        s" ${workerToRegister.toString()} already exists, re-register.")
      statusSystem.handleRegisterWorker(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        internalPort,
        networkLocation,
        disks,
        userResourceConsumption,
        requestId)
      context.reply(RegisterWorkerResponse(true, "Worker in snapshot, re-register."))
    } else if (statusSystem.workerLostEvents.contains(workerToRegister)) {
      logWarning(s"Receive RegisterWorker while worker $workerToRegister " +
        s"in workerLostEvents.")
      statusSystem.workerLostEvents.remove(workerToRegister)
      statusSystem.handleRegisterWorker(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        internalPort,
        networkLocation,
        disks,
        userResourceConsumption,
        requestId)
      context.reply(RegisterWorkerResponse(true, "Worker in workerLostEvents, re-register."))
    } else {
      statusSystem.handleRegisterWorker(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        internalPort,
        networkLocation,
        disks,
        userResourceConsumption,
        requestId)
      logInfo(s"Registered worker $workerToRegister.")
      context.reply(RegisterWorkerResponse(true, ""))
    }
  }

  @VisibleForTesting
  def workerHostAllowedToRegister(workerHost: String): Boolean = {

    val allow = allowWorkerHostPattern match {
      case Some(allowPattern) => allowPattern.pattern.matcher(workerHost).matches()
      case None => true
    }

    if (!allow) return false

    denyWorkerHostPattern match {
      case Some(denyPattern) => !denyPattern.pattern.matcher(workerHost).matches()
      case None => true
    }
  }

  def handleRequestSlots(context: RpcCallContext, requestSlots: RequestSlots): Unit = {
    val numReducers = requestSlots.partitionIdList.size()
    val shuffleKey = Utils.makeShuffleKey(requestSlots.applicationId, requestSlots.shuffleId)

    var availableWorkers = workersAvailable(requestSlots.excludedWorkerSet)
    if (conf.tagsEnabled) {
      availableWorkers = tagsManager.getTaggedWorkers(
        requestSlots.userIdentifier,
        requestSlots.tagsExpr,
        availableWorkers)
    }

    val numAvailableWorkers = availableWorkers.size()

    if (numAvailableWorkers == 0) {
      logError(s"Offer slots for $shuffleKey failed due to all workers are excluded!")
      context.reply(
        RequestSlotsResponse(StatusCode.WORKER_EXCLUDED, new WorkerResource(), requestSlots.packed))
    }

    val numWorkers = Math.min(
      Math.max(
        if (requestSlots.shouldReplicate) 2 else 1,
        if (requestSlots.maxWorkers <= 0) slotsAssignMaxWorkers
        else Math.min(slotsAssignMaxWorkers, requestSlots.maxWorkers)),
      numAvailableWorkers)
    val startIndex = Random.nextInt(numAvailableWorkers)
    val selectedWorkers = new util.ArrayList[WorkerInfo](numWorkers)
    selectedWorkers.addAll(availableWorkers.subList(
      startIndex,
      Math.min(numAvailableWorkers, startIndex + numWorkers)))
    if (startIndex + numWorkers > numAvailableWorkers) {
      selectedWorkers.addAll(availableWorkers.subList(
        0,
        startIndex + numWorkers - numAvailableWorkers))
    }
    // offer slots
    val slots =
      masterSource.sample(MasterSource.OFFER_SLOTS_TIME, s"offerSlots-${Random.nextInt()}") {
        statusSystem.workersMap.synchronized {
          if (slotsAssignPolicy == SlotsAssignPolicy.LOADAWARE) {
            SlotsAllocator.offerSlotsLoadAware(
              selectedWorkers,
              requestSlots.partitionIdList,
              requestSlots.shouldReplicate,
              requestSlots.shouldRackAware,
              slotsAssignLoadAwareDiskGroupNum,
              slotsAssignLoadAwareDiskGroupGradient,
              loadAwareFlushTimeWeight,
              loadAwareFetchTimeWeight,
              requestSlots.availableStorageTypes)
          } else {
            SlotsAllocator.offerSlotsRoundRobin(
              selectedWorkers,
              requestSlots.partitionIdList,
              requestSlots.shouldReplicate,
              requestSlots.shouldRackAware,
              requestSlots.availableStorageTypes)
          }
        }
      }

    if (log.isDebugEnabled()) {
      val distributions = SlotsAllocator.slotsToDiskAllocations(slots)
      logDebug(
        s"allocate slots for shuffle $shuffleKey ${slots.asScala.map(m => m._1.toUniqueId -> m._2)}" +
          s" distributions: ${distributions.asScala.map(m => m._1.toUniqueId -> m._2)}")
    }

    // reply false if offer slots failed
    if (slots == null || slots.isEmpty) {
      logError(s"Offer slots for $numReducers reducers of $shuffleKey failed!")
      context.reply(RequestSlotsResponse(
        StatusCode.SLOT_NOT_AVAILABLE,
        new WorkerResource(),
        requestSlots.packed))
      return
    }

    // register shuffle success, update status
    statusSystem.handleRequestSlots(
      shuffleKey,
      requestSlots.hostname,
      Utils.getSlotsPerDisk(slots.asInstanceOf[WorkerResource])
        .asScala.map { case (worker, slots) => worker.toUniqueId -> slots }.asJava,
      requestSlots.requestId)

    logInfo(s"Offer slots successfully for $numReducers reducers of $shuffleKey" +
      s" on ${slots.size()} workers.")

    val workersNotSelected = availableWorkers.asScala.filter(!slots.containsKey(_))
    val offerSlotsExtraSize = Math.min(conf.masterSlotAssignExtraSlots, workersNotSelected.size)
    if (offerSlotsExtraSize > 0) {
      var index = Random.nextInt(workersNotSelected.size)
      (1 to offerSlotsExtraSize).foreach(_ => {
        slots.put(
          workersNotSelected(index),
          (new util.ArrayList[PartitionLocation](), new util.ArrayList[PartitionLocation]()))
        index = (index + 1) % workersNotSelected.size
      })
      logInfo(s"Offered extra $offerSlotsExtraSize slots for $shuffleKey")
    }

    if (authEnabled) {
      pushApplicationMetaToWorkers(requestSlots, slots)
    }
    context.reply(RequestSlotsResponse(
      StatusCode.SUCCESS,
      slots.asInstanceOf[WorkerResource],
      requestSlots.packed))
  }

  def pushApplicationMetaToWorkers(
      requestSlots: RequestSlots,
      slots: util.Map[WorkerInfo, (util.List[PartitionLocation], util.List[PartitionLocation])])
      : Unit = {
    // Pass application registration information to the workers
    val pbApplicationMeta = PbApplicationMeta.newBuilder()
      .setAppId(requestSlots.applicationId)
      .setSecret(secretRegistry.getSecretKey(requestSlots.applicationId))
      .build()
    val transportMessage =
      new TransportMessage(MessageType.APPLICATION_META, pbApplicationMeta.toByteArray)
    val workerSet = workersAssignedToApp.computeIfAbsent(
      requestSlots.applicationId,
      new util.function.Function[String, util.Set[WorkerInfo]] {
        override def apply(key: String): util.Set[WorkerInfo] =
          util.Collections.newSetFromMap(JavaUtils.newConcurrentHashMap[
            WorkerInfo,
            java.lang.Boolean]())
      })
    slots.keySet().asScala.foreach { worker =>
      // The app meta info is send to a Worker only if it wasn't previously sent.
      if (workerSet.add(worker)) {
        sendApplicationMetaExecutor.submit(new Runnable {
          override def run(): Unit = {
            logDebug(s"Sending app registration info to ${worker.host}:${worker.internalPort}")
            internalRpcEnvInUse.setupEndpointRef(
              RpcAddress.apply(worker.host, worker.internalPort),
              RpcNameConstants.WORKER_INTERNAL_EP).send(transportMessage)
          }
        })
      }
    }
  }

  def handleUnregisterShuffle(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      requestId: String): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    statusSystem.handleUnRegisterShuffle(shuffleKey, requestId)
    logInfo(s"Unregister shuffle $shuffleKey")
    context.reply(UnregisterShuffleResponse(StatusCode.SUCCESS))
  }

  def batchHandleUnregisterShuffles(
      context: RpcCallContext,
      applicationId: String,
      shuffleIds: List[Integer],
      requestId: String): Unit = {
    val shuffleKeys =
      shuffleIds.map(shuffleId => Utils.makeShuffleKey(applicationId, shuffleId)).asJava
    statusSystem.handleBatchUnRegisterShuffles(shuffleKeys, requestId)
    logInfo(s"BatchUnregister shuffle $shuffleKeys")
    context.reply(BatchUnregisterShuffleResponse(StatusCode.SUCCESS, shuffleIds.asJava))
  }

  private def handleReportNodeUnavailable(
      context: RpcCallContext,
      failedWorkers: util.List[WorkerInfo],
      requestId: String): Unit = {
    logInfo(s"Receive ReportNodeFailure $failedWorkers, current excluded workers" +
      s"${statusSystem.excludedWorkers}")
    statusSystem.handleReportWorkerUnavailable(failedWorkers, requestId)
    context.reply(OneWayMessageResponse)
  }

  private def handleWorkerDecommission(
      context: RpcCallContext,
      workers: util.List[WorkerInfo],
      requestId: String): Unit = {
    logInfo(s"Receive ReportWorkerDecommission $workers, current decommission workers" +
      s"${statusSystem.excludedWorkers}")
    statusSystem.handleReportWorkerDecommission(workers, requestId)
    context.reply(OneWayMessageResponse)
  }

  def handleApplicationLost(context: RpcCallContext, appId: String, requestId: String): Unit = {
    nonEagerHandler.submit(new Runnable {
      override def run(): Unit = {
        workersAssignedToApp.remove(appId)
        statusSystem.handleAppLost(appId, requestId)
        logInfo(s"Removed application $appId")
        if (hasHDFSStorage || hasS3Storage) {
          checkAndCleanExpiredAppDirsOnDFS(appId)
        }
        if (context != null) {
          context.reply(ApplicationLostResponse(StatusCode.SUCCESS))
        }
      }
    })
  }

  private def checkAndCleanExpiredAppDirsOnDFS(expiredDir: String = ""): Unit = {
    if (hadoopFs == null) {
      try {
        hadoopFs = CelebornHadoopUtils.getHadoopFS(conf)
      } catch {
        case e: Exception =>
          logError("Celeborn initialize DFS failed.", e)
          throw e
      }
    }
    if (hasHDFSStorage) processDir(conf.hdfsDir, expiredDir)
    if (hasS3Storage) processDir(conf.s3Dir, expiredDir)
  }

  private def processDir(dfsDir: String, expiredDir: String): Unit = {
    val dfsWorkPath = new Path(dfsDir, conf.workerWorkingDir)
    hadoopFs.asScala.map(_._2).filter(_.exists(dfsWorkPath)).foreach { fs =>
      if (expiredDir.nonEmpty) {
        val dirToDelete = new Path(dfsWorkPath, expiredDir)
        // delete specific app dir on application lost
        CelebornHadoopUtils.deleteDFSPathOrLogError(fs, dirToDelete, true)
      } else {
        val iter = fs.listStatusIterator(dfsWorkPath)
        while (iter.hasNext && isMasterActive == 1) {
          val fileStatus = iter.next()
          if (!statusSystem.appHeartbeatTime.containsKey(fileStatus.getPath.getName)) {
            CelebornHadoopUtils.deleteDFSPathOrLogError(fs, fileStatus.getPath, true)
          }
        }

      }
    }
  }

  private def gaugeShuffleFallbackCounts(): Unit = {
    statusSystem.shuffleFallbackCounts.keySet().asScala.foreach { fallbackPolicy =>
      masterSource.addGauge(
        MasterSource.SHUFFLE_FALLBACK_COUNT,
        Map("fallbackPolicy" -> fallbackPolicy)) { () =>
        Option(statusSystem.shuffleFallbackCounts.get(fallbackPolicy)).getOrElse(0L)
      }
    }
  }

  private def handleHeartbeatFromApplication(
      context: RpcCallContext,
      appId: String,
      totalWritten: Long,
      fileCount: Long,
      shuffleCount: Long,
      applicationCount: Long,
      shuffleFallbackCounts: util.Map[String, java.lang.Long],
      applicationFallbackCounts: util.Map[String, java.lang.Long],
      needCheckedWorkerList: util.List[WorkerInfo],
      requestId: String,
      shouldResponse: Boolean): Unit = {
    statusSystem.handleAppHeartbeat(
      appId,
      totalWritten,
      fileCount,
      shuffleCount,
      applicationCount,
      shuffleFallbackCounts,
      applicationFallbackCounts,
      System.currentTimeMillis(),
      requestId)
    gaugeShuffleFallbackCounts()
    val unknownWorkers = needCheckedWorkerList.asScala.filterNot(w =>
      statusSystem.workersMap.containsKey(w.toUniqueId)).asJava
    if (shouldResponse) {
      // UserResourceConsumption and DiskInfo are eliminated from WorkerInfo
      // during serialization of HeartbeatFromApplicationResponse
      val appRelatedShuffles =
        statusSystem.registeredAppAndShuffles.getOrDefault(appId, Collections.emptySet())
      context.reply(HeartbeatFromApplicationResponse(
        StatusCode.SUCCESS,
        new util.ArrayList(
          (statusSystem.excludedWorkers.asScala ++ statusSystem.manuallyExcludedWorkers.asScala).asJava),
        unknownWorkers,
        new util.ArrayList[WorkerInfo](
          (statusSystem.shutdownWorkers.asScala ++ statusSystem.decommissionWorkers.asScala).asJava),
        new util.ArrayList(appRelatedShuffles),
        CheckQuotaResponse(isAvailable = true, "")))
    } else {
      context.reply(OneWayMessageResponse)
    }
  }

  private def handleRemoveWorkersUnavailableInfos(
      context: RpcCallContext,
      unavailableWorkers: util.List[WorkerInfo],
      requestId: String): Unit = {
    statusSystem.handleRemoveWorkersUnavailableInfo(unavailableWorkers, requestId)
    if (context != null) {
      context.reply(RemoveWorkersUnavailableInfoResponse(true))
    }
  }

  private def handleResourceConsumption(userIdentifier: UserIdentifier): ResourceConsumption = {
    val userResourceConsumption = computeUserResourceConsumption(userIdentifier)
    gaugeResourceConsumption(userIdentifier)
    userResourceConsumption
  }

  private def gaugeResourceConsumption(
      userIdentifier: UserIdentifier,
      applicationId: String = null): Unit = {
    val resourceConsumptionLabel =
      if (applicationId == null) userIdentifier.toMap
      else userIdentifier.toMap + (resourceConsumptionSource.applicationLabel -> applicationId)
    resourceConsumptionSource.addGauge(
      ResourceConsumptionSource.DISK_FILE_COUNT,
      resourceConsumptionLabel) { () =>
      computeResourceConsumption(userIdentifier, applicationId).diskFileCount
    }
    resourceConsumptionSource.addGauge(
      ResourceConsumptionSource.DISK_BYTES_WRITTEN,
      resourceConsumptionLabel) { () =>
      computeResourceConsumption(userIdentifier, applicationId).diskBytesWritten
    }
    if (hasHDFSStorage) {
      resourceConsumptionSource.addGauge(
        ResourceConsumptionSource.HDFS_FILE_COUNT,
        resourceConsumptionLabel) { () =>
        computeResourceConsumption(userIdentifier, applicationId).hdfsFileCount
      }
      resourceConsumptionSource.addGauge(
        ResourceConsumptionSource.HDFS_BYTES_WRITTEN,
        resourceConsumptionLabel) { () =>
        computeResourceConsumption(userIdentifier, applicationId).hdfsBytesWritten
      }
    }
  }

  private def computeResourceConsumption(
      userIdentifier: UserIdentifier,
      applicationId: String = null): ResourceConsumption = {
    val newResourceConsumption = computeUserResourceConsumption(userIdentifier)
    if (applicationId == null) {
      val current = System.currentTimeMillis()
      if (userResourceConsumptions.containsKey(userIdentifier)) {
        val resourceConsumptionAndUpdateTime = userResourceConsumptions.get(userIdentifier)
        if (current - resourceConsumptionAndUpdateTime._2 <= masterResourceConsumptionInterval) {
          return resourceConsumptionAndUpdateTime._1
        }
      }
      userResourceConsumptions.put(userIdentifier, (newResourceConsumption, current))
      newResourceConsumption
    } else {
      newResourceConsumption.subResourceConsumptions.get(applicationId)
    }
  }

  // TODO: Support calculate topN app resource consumption.
  private def computeUserResourceConsumption(
      userIdentifier: UserIdentifier): ResourceConsumption = {
    val resourceConsumption = statusSystem.workersMap.values().asScala.flatMap {
      workerInfo => workerInfo.userResourceConsumption.asScala.get(userIdentifier)
    }.foldRight(ResourceConsumption(0, 0, 0, 0))(_ add _)
    resourceConsumption
  }

  private[master] def handleCheckQuota(
      userIdentifier: UserIdentifier,
      context: RpcCallContext): Unit = {
    val userResourceConsumption = handleResourceConsumption(userIdentifier)
    if (conf.quotaEnabled) {
      val (isAvailable, reason) =
        quotaManager.checkQuotaSpaceAvailable(userIdentifier, userResourceConsumption)
      context.reply(CheckQuotaResponse(isAvailable, reason))
    } else {
      context.reply(CheckQuotaResponse(true, ""))
    }
  }

  private def handleCheckWorkersAvailable(context: RpcCallContext): Unit = {
    context.reply(CheckWorkersAvailableResponse(!statusSystem.availableWorkers.isEmpty))
  }

  private def handleWorkerEvent(
      requestId: String,
      workerEventTypeValue: Int,
      workers: util.List[WorkerInfo],
      context: RpcCallContext): Unit = {
    statusSystem.handleWorkerEvent(workerEventTypeValue, workers, requestId)
    context.reply(PbWorkerEventResponse.newBuilder().setSuccess(true).build())
  }

  private def workersAvailable(
      tmpExcludedWorkerList: Set[WorkerInfo] = Set.empty): util.List[WorkerInfo] = {
    if (tmpExcludedWorkerList.isEmpty) {
      new util.ArrayList[WorkerInfo](statusSystem.availableWorkers)
    } else {
      val availableWorkers = new util.HashSet(statusSystem.availableWorkers)
      tmpExcludedWorkerList.foreach(availableWorkers.remove)
      new util.ArrayList[WorkerInfo](availableWorkers)
    }
  }

  private def handleRequestForApplicationMeta(
      context: RpcCallContext,
      pb: PbApplicationMetaRequest): Unit = {
    val appId = pb.getAppId
    logDebug(s"Handling request for application meta info $appId.")
    val secret = secretRegistry.getSecretKey(appId)
    if (secret == null) {
      logWarning(s"Could not find the application meta of $appId.")
      context.sendFailure(new CelebornException(s"$appId is not registered."))
    } else {
      val pbApplicationMeta = PbApplicationMeta.newBuilder()
        .setAppId(appId)
        .setSecret(secret)
        .build()
      val transportMessage =
        new TransportMessage(MessageType.APPLICATION_META, pbApplicationMeta.toByteArray)
      context.reply(transportMessage)
    }
  }

  override def getMasterGroupInfo: String = {
    val sb = new StringBuilder
    sb.append("====================== Master Group INFO ==============================\n")
    sb.append(getMasterGroupInfoInternal)
    sb.toString()
  }

  private def getWorkers: String = {
    statusSystem.workersMap.values().asScala.mkString("\n")
  }

  override def handleWorkerEvent(
      workerEventType: WorkerEventType,
      workers: Seq[WorkerInfo]): HandleResponse = {
    val sb = new StringBuilder()
    try {
      val workerEventResponse = self.askSync[PbWorkerEventResponse](WorkerEventRequest(
        workers.asJava,
        workerEventType,
        MasterClient.genRequestId()))
      if (workerEventResponse.getSuccess) {
        sb.append(
          s"handle $workerEventType for ${workers.map(_.readableAddress()).mkString(",")} successfully")
      } else {
        sb.append(
          s"handle $workerEventType for ${workers.map(_.readableAddress).mkString(",")} failed")
      }
      workerEventResponse.getSuccess -> sb.toString()
    } catch {
      case e: Throwable =>
        val message =
          s"handle $workerEventType for ${workers.map(_.readableAddress()).mkString(
            ",")} failed, message: ${e.getMessage}"
        logError(message, e)
        sb.append(message)
        false -> sb.toString()
    }
  }

  override def getWorkerInfo: String = {
    val sb = new StringBuilder
    sb.append("====================== Workers Info in Master =========================\n")
    sb.append(getWorkers)
    sb.toString()
  }

  override def getLostWorkers: String = {
    val sb = new StringBuilder
    sb.append("======================= Lost Workers in Master ========================\n")
    statusSystem.lostWorkers.asScala.toSeq.sortBy(_._2).foreach { case (worker, time) =>
      sb.append(s"${worker.toUniqueId.padTo(50, " ").mkString}${Utils.formatTimestamp(time)}\n")
    }
    sb.toString()
  }

  override def getShutdownWorkers: String = {
    val sb = new StringBuilder
    sb.append("===================== Shutdown Workers in Master ======================\n")
    statusSystem.shutdownWorkers.asScala.foreach { worker =>
      sb.append(s"${worker.toUniqueId}\n")
    }
    sb.toString()
  }

  override def getDecommissionWorkers: String = {
    val sb = new StringBuilder
    sb.append("===================== Decommission Workers in Master ======================\n")
    statusSystem.decommissionWorkers.asScala.foreach { worker =>
      sb.append(s"${worker.toUniqueId}\n")
    }
    sb.toString()
  }

  override def getExcludedWorkers: String = {
    val sb = new StringBuilder
    sb.append("===================== Excluded Workers in Master ======================\n")
    (statusSystem.excludedWorkers.asScala ++ statusSystem.manuallyExcludedWorkers.asScala).foreach {
      worker => sb.append(s"${worker.toUniqueId}\n")
    }
    sb.toString()
  }

  override def getHostnameList: String = {
    val sb = new StringBuilder
    sb.append("================= LifecycleManager Hostname List ======================\n")
    statusSystem.hostnameSet.asScala.foreach { host =>
      sb.append(s"$host\n")
    }
    sb.toString()
  }

  override def getApplicationList: String = {
    val sb = new StringBuilder
    sb.append("================= LifecycleManager Application List ======================\n")
    statusSystem.appHeartbeatTime.asScala.toSeq.sortBy(_._2).foreach { case (appId, time) =>
      sb.append(s"${appId.padTo(40, " ").mkString}${Utils.formatTimestamp(time)}\n")
    }
    sb.toString()
  }

  override def getShuffleList: String = {
    val sb = new StringBuilder
    sb.append("======================= Shuffle Key List ============================\n")
    statusSystem.registeredAppAndShuffles.asScala.foreach { shuffleKey =>
      val appId = shuffleKey._1
      shuffleKey._2.asScala.foreach { id =>
        sb.append(s"$appId-${id}\n")
      }
    }
    sb.toString()
  }

  override def exclude(
      addWorkers: Seq[WorkerInfo],
      removeWorkers: Seq[WorkerInfo]): HandleResponse = {
    val sb = new StringBuilder
    val workerExcludeResponse = self.askSync[PbWorkerExcludeResponse](WorkerExclude(
      addWorkers.asJava,
      removeWorkers.asJava,
      MasterClient.genRequestId()))
    if (workerExcludeResponse.getSuccess) {
      sb.append(
        s"Excluded workers add ${addWorkers.map(_.readableAddress).mkString(
          ",")} and remove ${removeWorkers.map(_.readableAddress).mkString(",")} successfully.\n")
    } else {
      sb.append(
        s"Failed to Exclude workers add ${addWorkers.map(_.readableAddress).mkString(
          ",")} and remove ${removeWorkers.map(_.readableAddress).mkString(",")}.\n")
    }
    val unknownExcludedWorkers =
      (addWorkers ++ removeWorkers).filterNot(w =>
        statusSystem.workersMap.containsKey(w.toUniqueId))
    if (unknownExcludedWorkers.nonEmpty) {
      sb.append(
        s"Unknown workers ${unknownExcludedWorkers.map(_.readableAddress).mkString(",")}." +
          s" Currently total ${statusSystem.manuallyExcludedWorkers.size()}" +
          s" workers excluded manually in Master.")
    }
    workerExcludeResponse.getSuccess -> sb.toString()
  }

  def removeWorkersUnavailableInfo(unavailableWorkers: Seq[WorkerInfo]): HandleResponse = {
    val removeWorkersUnavailableInfoResponse =
      self.askSync[PbRemoveWorkersUnavailableInfoResponse](RemoveWorkersUnavailableInfo(
        unavailableWorkers.asJava,
        MasterClient.genRequestId()))
    if (removeWorkersUnavailableInfoResponse.getSuccess) {
      true -> s"Remove unavailable info for workers ${unavailableWorkers.map(_.readableAddress).mkString(",")} successfully."
    } else {
      false -> s"Failed to remove unavailable info for workers ${unavailableWorkers.map(
        _.readableAddress).mkString(",")}."
    }
  }

  private[master] def isMasterActive: Int = {
    // use int rather than bool for better monitoring on dashboard
    val isActive =
      if (haEnabled) {
        if (statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer.isLeader) {
          1
        } else {
          0
        }
      } else {
        1
      }
    isActive
  }

  private def getMasterGroupInfoInternal: String = {
    if (haEnabled) {
      val sb = new StringBuilder
      val groupInfo = statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer.getGroupInfo
      sb.append(s"group id: ${groupInfo.getGroup.getGroupId.getUuid}\n")

      def getLeader(roleInfo: RaftProtos.RoleInfoProto): RaftProtos.RaftPeerProto = {
        if (roleInfo == null) {
          return null
        }
        if (roleInfo.getRole == RaftPeerRole.LEADER) {
          return roleInfo.getSelf
        }
        val followerInfo = roleInfo.getFollowerInfo
        if (followerInfo == null) {
          return null
        }
        followerInfo.getLeaderInfo.getId
      }

      val leader = getLeader(groupInfo.getRoleInfoProto)
      if (leader == null) {
        sb.append("leader not found\n")
      } else {
        sb.append(s"leader info: ${leader.getId.toStringUtf8}(${leader.getAddress})\n\n")
      }
      sb.append(groupInfo.getCommitInfos)
      sb.append("\n")
      sb.toString()
    } else {
      "HA is not enabled"
    }
  }

  private def getRatisApplyCompletedIndex: Long = {
    val ratisServer = statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer
    if (ratisServer != null) {
      ratisServer.getMasterStateMachine.getLastAppliedTermIndex.getIndex
    } else {
      0
    }
  }

  override def getWorkerEventInfo(): String = {
    val sb = new StringBuilder
    sb.append("======================= Workers Event in Master ========================\n")
    statusSystem.workerEventInfos.asScala.foreach { case (worker, workerEventInfo) =>
      sb.append(s"${worker.toUniqueId.padTo(50, " ").mkString}$workerEventInfo\n")
    }
    sb.toString()
  }

  override def initialize(): Unit = {
    super.initialize()
    logInfo("Master started.")
    rpcEnv.awaitTermination()
    if (conf.internalPortEnabled) {
      internalRpcEnvInUse.awaitTermination()
    }
  }

  override def stop(exitKind: Int): Unit = synchronized {
    if (!stopped) {
      logInfo("Stopping Master")
      rpcEnv.stop(self)
      if (conf.internalPortEnabled) {
        internalRpcEnvInUse.stop(internalRpcEndpointRef)
      }
      super.stop(exitKind)
      logInfo("Master stopped.")
      stopped = true
    }
  }
}

private[deploy] object Master extends Logging {
  def main(args: Array[String]): Unit = {
    SignalUtils.registerLogger(log)
    val conf = new CelebornConf()
    val masterArgs = new MasterArguments(args, conf)
    try {
      val master = new Master(conf, masterArgs)
      master.initialize()
    } catch {
      case e: Throwable =>
        logError("Initialize master failed.", e)
        System.exit(-1)
    }
  }
}
