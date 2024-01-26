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

package org.apache.celeborn.service.deploy

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.util.Random

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.service.deploy.master.{Master, MasterArguments}
import org.apache.celeborn.service.deploy.worker.{Worker, WorkerArguments}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

trait MiniClusterFeature extends Logging {
  private def masterPort = Random.nextInt(65535 - 1200) + 1200

  private def workerPort = {
    var port = masterPort
    while (port == masterPort) {
      port = Random.nextInt(65535 - 1200) + 1200
    }
    port
  }

  def masterHttpPort = new AtomicInteger(masterPort)

  def workerHttpPort = new AtomicInteger(workerPort)
  var masterInfo: (Master, Thread) = _
  val workerInfos = new mutable.HashMap[Worker, Thread]()

  def runnerWrap[T](code: => T): Thread = new Thread(new Runnable {
    override def run(): Unit = {
      Utils.tryLogNonFatalError(code)
    }
  })

  def setupMiniClusterWithRandomPorts(
      masterConf: Option[Map[String, String]] = None,
      workerConf: Option[Map[String, String]] = None,
      workerNum: Int = 3): (Master, collection.Set[Worker]) = {
    var retryCount = 0
    var created = false
    var master: Master = null
    var workers: collection.Set[Worker] = null
    while (retryCount < 3 && !created) {
      try {
        val randomPort = Random.nextInt(65535 - 1200) + 1200
        val finalMasterConf = Map(
          s"${CelebornConf.MASTER_HOST.key}" -> "localhost",
          s"${CelebornConf.MASTER_PORT.key}" -> s"$randomPort",
          s"${CelebornConf.MASTER_ENDPOINTS.key}" -> s"localhost:$randomPort",
          s"${CelebornConf.CLIENT_SLOT_ASSIGN_MAX_WORKERS.key}" -> "10") ++
          masterConf.getOrElse(Map())
        val finalWorkerConf = Map(
          s"${CelebornConf.MASTER_ENDPOINTS.key}" -> s"localhost:$randomPort") ++
          workerConf.getOrElse(Map())
        logInfo(s"generated configuration $finalMasterConf")
        val (m, w) =
          setUpMiniCluster(masterConf = finalMasterConf, workerConf = finalWorkerConf, workerNum)
        master = m
        workers = w
        created = true
      } catch {
        case e: Exception =>
          if (retryCount < 3) {
            logError("failed to setup mini cluster, reached the max retry count")
            throw e
          } else {
            logError(s"failed to setup mini cluster, retrying (retry count: $retryCount")
            retryCount += 1
          }
      }
    }
    (master, workers)
  }

  def createTmpDir(): String = {
    val tmpDir = Files.createTempDirectory("celeborn-")
    logInfo(s"created temp dir: $tmpDir")
    tmpDir.toFile.deleteOnExit()
    tmpDir.toAbsolutePath.toString
  }

  private def createMaster(map: Map[String, String] = null): Master = {
    val conf = new CelebornConf()
    conf.set(CelebornConf.METRICS_ENABLED.key, "false")
    val httpPort = masterHttpPort.getAndIncrement()
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, s"$httpPort")
    logInfo(s"set ${CelebornConf.MASTER_HTTP_PORT.key} to $httpPort")
    if (map != null) {
      map.foreach(m => conf.set(m._1, m._2))
    }

    val masterArguments = new MasterArguments(Array(), conf)
    val master = new Master(conf, masterArguments)
    master.startHttpServer()

    Thread.sleep(5000L)
    master
  }

  def createWorker(map: Map[String, String] = null): Worker = {
    createWorker(map, createTmpDir())
  }

  def createWorker(map: Map[String, String], storageDir: String): Worker = {
    logInfo("start create worker for mini cluster")
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, storageDir)
    conf.set(CelebornConf.WORKER_DISK_MONITOR_ENABLED.key, "false")
    conf.set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
    conf.set(CelebornConf.WORKER_HTTP_PORT.key, s"${workerHttpPort.incrementAndGet()}")
    conf.set("celeborn.fetch.io.threads", "4")
    conf.set("celeborn.push.io.threads", "4")
    if (map != null) {
      map.foreach(m => conf.set(m._1, m._2))
    }
    logInfo("celeborn conf created")

    val workerArguments = new WorkerArguments(Array(), conf)
    logInfo("worker argument created")
    try {
      val worker = new Worker(conf, workerArguments)
      logInfo("worker created for mini cluster")
      worker
    } catch {
      case e: Exception =>
        logError("create worker failed, detail:", e)
        System.exit(-1)
        null
    }
  }

  private def setUpMiniCluster(
      masterConf: Map[String, String] = null,
      workerConf: Map[String, String] = null,
      workerNum: Int = 3): (Master, collection.Set[Worker]) = {
    val master = createMaster(masterConf)
    val masterThread = runnerWrap(master.rpcEnv.awaitTermination())
    masterThread.start()
    masterInfo = (master, masterThread)
    Thread.sleep(5000L)
    (1 to workerNum).foreach { _ =>
      val worker = createWorker(workerConf)
      val workerThread = runnerWrap(worker.initialize())
      workerThread.start()
      workerInfos.put(worker, workerThread)
    }

    var workerRegistrationRetryCount = 0
    var workerRegistrationDone = false
    while (workerRegistrationRetryCount < 3 && !workerRegistrationDone) {
      try {
        Thread.sleep(20000L)
        workerInfos.foreach { case (worker, _) => assert(worker.registered.get()) }
        workerRegistrationDone = true
      } catch {
        case ex: AssertionError =>
          if (workerRegistrationRetryCount < 3) {
            logWarning("worker registration cannot be done, retrying", ex)
            workerRegistrationRetryCount += 1
          } else {
            throw ex
          }
      }
    }
    (master, workerInfos.keySet)
  }

  def shutdownMiniCluster(): Unit = {
    // shutdown workers
    workerInfos.foreach {
      case (worker, _) =>
        worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
        worker.rpcEnv.shutdown()
    }

    // shutdown masters
    masterInfo._1.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    masterInfo._1.rpcEnv.shutdown()

    // interrupt threads
    Thread.sleep(5000)
    workerInfos.foreach {
      case (worker, thread) =>
        worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
        thread.interrupt()
    }
    workerInfos.clear()
    masterInfo._2.interrupt()
    MemoryManager.reset()
  }
}
