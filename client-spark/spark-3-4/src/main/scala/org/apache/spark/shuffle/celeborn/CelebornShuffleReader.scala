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

import java.io.IOException
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap, Set => JSet}
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.tuple.Pair
import org.apache.spark.{Aggregator, InterruptibleIterator, ShuffleDependency, TaskContext}
import org.apache.spark.celeborn.ExceptionMakerHelper
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader.streamCreatorPool
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

import org.apache.celeborn.client.{ClientUtils, ShuffleClient}
import org.apache.celeborn.client.ShuffleClientImpl.ReduceFileGroups
import org.apache.celeborn.client.read.{CelebornInputStream, MetricsCallback}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.{CelebornIOException, PartitionUnRetryAbleException}
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils, Utils}

class CelebornShuffleReader[K, C](
    handle: CelebornShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    startMapIndex: Int = 0,
    endMapIndex: Int = Int.MaxValue,
    context: TaskContext,
    conf: CelebornConf,
    metrics: ShuffleReadMetricsReporter,
    shuffleIdTracker: ExecutorShuffleIdTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  @VisibleForTesting
  val shuffleClient = ShuffleClient.get(
    handle.appUniqueId,
    handle.lifecycleManagerHost,
    handle.lifecycleManagerPort,
    conf,
    handle.userIdentifier,
    handle.extension)

  private val exceptionRef = new AtomicReference[IOException]
  private val throwsFetchFailure = handle.throwsFetchFailure
  private val encodedAttemptId = SparkCommonUtils.getEncodedAttemptNumber(context)

  override def read(): Iterator[Product2[K, C]] = {

    val serializerInstance = newSerializerInstance(dep)

    val shuffleId = SparkUtils.celebornShuffleId(shuffleClient, handle, context, false)
    shuffleIdTracker.track(handle.shuffleId, shuffleId)
    logDebug(
      s"get shuffleId $shuffleId for appShuffleId ${handle.shuffleId} attemptNum ${context.stageAttemptNumber()}")

    // Update the context task metrics for each record read.
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesWritten: Long): Unit = {
        metrics.incRemoteBytesRead(bytesWritten)
        metrics.incRemoteBlocksFetched(1)
      }

      override def incReadTime(time: Long): Unit =
        metrics.incFetchWaitTime(time)
    }

    if (streamCreatorPool == null) {
      CelebornShuffleReader.synchronized {
        if (streamCreatorPool == null) {
          streamCreatorPool = ThreadUtils.newDaemonCachedThreadPool(
            "celeborn-create-stream-thread",
            conf.readStreamCreatorPoolThreads,
            60)
        }
      }
    }

    val startTime = System.currentTimeMillis()
    val fetchTimeoutMs = conf.clientFetchTimeoutMs
    val localFetchEnabled = conf.enableReadLocalShuffleFile
    val localHostAddress = Utils.localHostName(conf)
    val shuffleKey = Utils.makeShuffleKey(handle.appUniqueId, shuffleId)
    var fileGroups: ReduceFileGroups = null
    try {
      // startPartition is irrelevant
      fileGroups = shuffleClient.updateFileGroup(shuffleId, startPartition)
    } catch {
      case ce @ (_: CelebornIOException | _: PartitionUnRetryAbleException) =>
        // if a task is interrupted, should not report fetch failure
        // if a task update file group timeout, should not report fetch failure
        checkAndReportFetchFailureForUpdateFileGroupFailure(shuffleId, ce)
      case e: Throwable => throw e
    }

    // host-port -> (TransportClient, PartitionLocation Array, PbOpenStreamList)
    val workerRequestMap = new JHashMap[
      String,
      (TransportClient, JArrayList[PartitionLocation], PbOpenStreamList.Builder)]()
    // partitionId -> (partition uniqueId -> chunkRange pair)
    val partitionId2ChunkRange = new JHashMap[Int, JMap[String, Pair[Integer, Integer]]]()

    val partitionId2PartitionLocations = new JHashMap[Int, JSet[PartitionLocation]]()

    var partCnt = 0

    // if startMapIndex > endMapIndex, means partition is skew partition and read by Celeborn implementation.
    // locations will split to sub-partitions with startMapIndex size.
    val splitSkewPartitionWithoutMapRange =
      ClientUtils.readSkewPartitionWithoutMapRange(conf, startMapIndex, endMapIndex)

    (startPartition until endPartition).foreach { partitionId =>
      if (fileGroups.partitionGroups.containsKey(partitionId)) {
        var locations = fileGroups.partitionGroups.get(partitionId)
        if (splitSkewPartitionWithoutMapRange) {
          val partitionLocation2ChunkRange = CelebornPartitionUtil.splitSkewedPartitionLocations(
            new JArrayList(locations),
            startMapIndex,
            endMapIndex)
          partitionId2ChunkRange.put(partitionId, partitionLocation2ChunkRange)
          // filter locations avoid OPEN_STREAM when split skew partition without map range
          val filterLocations = locations.asScala
            .filter { location =>
              null != partitionLocation2ChunkRange &&
              partitionLocation2ChunkRange.containsKey(location.getUniqueId)
            }
          locations = filterLocations.asJava
          partitionId2PartitionLocations.put(partitionId, locations)
        }

        locations.asScala.foreach { location =>
          partCnt += 1
          val hostPort = location.hostAndFetchPort
          if (!workerRequestMap.containsKey(hostPort)) {
            try {
              val client = shuffleClient.getDataClientFactory().createClient(
                location.getHost,
                location.getFetchPort)
              val pbOpenStreamList = PbOpenStreamList.newBuilder()
              pbOpenStreamList.setShuffleKey(shuffleKey)
              workerRequestMap.put(
                hostPort,
                (client, new JArrayList[PartitionLocation], pbOpenStreamList))
            } catch {
              case ex: Exception =>
                shuffleClient.excludeFailedFetchLocation(location.hostAndFetchPort, ex)
                logWarning(
                  s"Failed to create client for $shuffleKey-$partitionId from host: ${location.hostAndFetchPort}. " +
                    s"Shuffle reader will try its replica if exists.")
            }
          }
          workerRequestMap.get(hostPort) match {
            case (_, locArr, pbOpenStreamListBuilder) =>
              locArr.add(location)
              pbOpenStreamListBuilder.addFileName(location.getFileName)
                .addStartIndex(startMapIndex)
                .addEndIndex(endMapIndex)
              pbOpenStreamListBuilder.addReadLocalShuffle(
                localFetchEnabled && location.getHost.equals(localHostAddress))
            case _ =>
              logDebug(s"Empty client for host ${hostPort}")
          }
        }
      }
    }

    val locationStreamHandlerMap: ConcurrentHashMap[PartitionLocation, PbStreamHandler] =
      JavaUtils.newConcurrentHashMap()

    val futures = workerRequestMap.values().asScala.map { entry =>
      streamCreatorPool.submit(new Runnable {
        override def run(): Unit = {
          val (client, locArr, pbOpenStreamListBuilder) = entry
          val msg = new TransportMessage(
            MessageType.BATCH_OPEN_STREAM,
            pbOpenStreamListBuilder.build().toByteArray)
          val pbOpenStreamListResponse =
            try {
              val response = client.sendRpcSync(msg.toByteBuffer, fetchTimeoutMs)
              TransportMessage.fromByteBuffer(response).getParsedPayload[PbOpenStreamListResponse]
            } catch {
              case _: Exception => null
            }
          if (pbOpenStreamListResponse != null) {
            0 until locArr.size() foreach { idx =>
              val streamHandlerOpt = pbOpenStreamListResponse.getStreamHandlerOptList.get(idx)
              if (streamHandlerOpt.getStatus == StatusCode.SUCCESS.getValue) {
                locationStreamHandlerMap.put(locArr.get(idx), streamHandlerOpt.getStreamHandler)
              }
            }
          }
        }
      })
    }.toList
    // wait for all futures to complete
    futures.foreach(f => f.get())
    val end = System.currentTimeMillis()
    logInfo(s"BatchOpenStream for $partCnt cost ${end - startTime}ms")

    val streams = JavaUtils.newConcurrentHashMap[Integer, CelebornInputStream]()

    def createInputStream(partitionId: Int): Unit = {
      val locations =
        if (splitSkewPartitionWithoutMapRange) {
          partitionId2PartitionLocations.get(partitionId)
        } else {
          fileGroups.partitionGroups.get(partitionId)
        }

      val locationList =
        if (null == locations) {
          new JArrayList[PartitionLocation]()
        } else {
          new JArrayList[PartitionLocation](locations)
        }
      val streamHandlers =
        if (locations != null) {
          val streamHandlerArr = new JArrayList[PbStreamHandler](locationList.size)
          locationList.asScala.foreach { loc =>
            streamHandlerArr.add(locationStreamHandlerMap.get(loc))
          }
          streamHandlerArr
        } else null
      if (exceptionRef.get() == null) {
        try {
          val inputStream = shuffleClient.readPartition(
            shuffleId,
            handle.shuffleId,
            partitionId,
            encodedAttemptId,
            context.taskAttemptId(),
            startMapIndex,
            endMapIndex,
            if (throwsFetchFailure) ExceptionMakerHelper.SHUFFLE_FETCH_FAILURE_EXCEPTION_MAKER
            else null,
            locationList,
            streamHandlers,
            fileGroups.pushFailedBatches,
            partitionId2ChunkRange.get(partitionId),
            fileGroups.mapAttempts,
            metricsCallback)
          streams.put(partitionId, inputStream)
        } catch {
          case e: IOException =>
            logError(s"Exception caught when readPartition $partitionId!", e)
            exceptionRef.compareAndSet(null, e)
          case e: Throwable =>
            logError(s"Non IOException caught when readPartition $partitionId!", e)
            exceptionRef.compareAndSet(null, new CelebornIOException(e))
        }
      }
    }

    val inputStreamCreationWindow = conf.clientInputStreamCreationWindow
    (startPartition until Math.min(
      startPartition + inputStreamCreationWindow,
      endPartition)).foreach(partitionId => {
      streamCreatorPool.submit(new Runnable {
        override def run(): Unit = {
          createInputStream(partitionId)
        }
      })
    })

    val recordIter = (startPartition until endPartition).iterator.map(partitionId => {
      if (handle.numMappers > 0) {
        val startFetchWait = System.nanoTime()
        var inputStream: CelebornInputStream = streams.get(partitionId)
        while (inputStream == null) {
          if (exceptionRef.get() != null) {
            exceptionRef.get() match {
              case ce @ (_: CelebornIOException | _: PartitionUnRetryAbleException) =>
                handleFetchExceptions(handle.shuffleId, shuffleId, partitionId, ce)
              case e => throw e
            }
          }
          logInfo("inputStream is null, sleeping...")
          Thread.sleep(50)
          inputStream = streams.get(partitionId)
        }
        metricsCallback.incReadTime(
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait))
        // ensure inputStream is closed when task completes
        context.addTaskCompletionListener[Unit](_ => inputStream.close())

        // Advance the input creation window
        if (partitionId + inputStreamCreationWindow < endPartition) {
          streamCreatorPool.submit(new Runnable {
            override def run(): Unit = {
              createInputStream(partitionId + inputStreamCreationWindow)
            }
          })
        }

        (partitionId, inputStream)
      } else {
        (partitionId, CelebornInputStream.empty())
      }
    }).filter {
      case (_, inputStream) => inputStream != CelebornInputStream.empty()
    }.map { case (partitionId, inputStream) =>
      (partitionId, serializerInstance.deserializeStream(inputStream).asKeyValueIterator)
    }.flatMap { case (partitionId, iter) =>
      try {
        iter
      } catch {
        case e @ (_: CelebornIOException | _: PartitionUnRetryAbleException) =>
          handleFetchExceptions(handle.shuffleId, shuffleId, partitionId, e)
      }
    }

    val iterWithUpdatedRecordsRead =
      recordIter.map { record =>
        metrics.incRecordsRead(1)
        record
      }

    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      iterWithUpdatedRecordsRead,
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val resultIter: Iterator[Product2[K, C]] = {
      // Sort the output if there is a sort ordering defined.
      if (dep.keyOrdering.isDefined) {
        // Create an ExternalSorter to sort the data.
        val sorter: ExternalSorter[K, _, C] =
          if (dep.aggregator.isDefined) {
            if (dep.mapSideCombine) {
              new ExternalSorter[K, C, C](
                context,
                Option(new Aggregator[K, C, C](
                  identity,
                  dep.aggregator.get.mergeCombiners,
                  dep.aggregator.get.mergeCombiners)),
                ordering = Some(dep.keyOrdering.get),
                serializer = dep.serializer)
            } else {
              new ExternalSorter[K, Nothing, C](
                context,
                dep.aggregator.asInstanceOf[Option[Aggregator[K, Nothing, C]]],
                ordering = Some(dep.keyOrdering.get),
                serializer = dep.serializer)
            }
          } else {
            new ExternalSorter[K, C, C](
              context,
              ordering = Some(dep.keyOrdering.get),
              serializer = dep.serializer)
          }
        sorter.insertAll(interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]])
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener[Unit](_ => {
          sorter.stop()
        })
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      } else if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          // We are reading values that are already combined
          val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
          dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
        } else {
          // We don't know the value type, but also don't care -- the dependency *should*
          // have made sure its compatible w/ this aggregator, which will convert the value
          // type to the combined type C
          val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
          dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
        }
      } else {
        interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      }
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  @VisibleForTesting
  def checkAndReportFetchFailureForUpdateFileGroupFailure(
      celebornShuffleId: Int,
      ce: Throwable): Unit = {
    if (ce.getCause != null &&
      (ce.getCause.isInstanceOf[InterruptedException] || ce.getCause.isInstanceOf[
        TimeoutException])) {
      logWarning(s"fetch shuffle ${celebornShuffleId} timeout or interrupt", ce)
      throw ce
    } else {
      handleFetchExceptions(handle.shuffleId, celebornShuffleId, 0, ce)
    }
  }

  @VisibleForTesting
  def handleFetchExceptions(
      appShuffleId: Int,
      shuffleId: Int,
      partitionId: Int,
      ce: Throwable) = {
    if (throwsFetchFailure &&
      shuffleClient.reportShuffleFetchFailure(appShuffleId, shuffleId, context.taskAttemptId())) {
      logWarning(s"Handle fetch exceptions for ${shuffleId}-${partitionId}", ce)
      throw new FetchFailedException(
        null,
        appShuffleId,
        -1,
        -1,
        partitionId,
        SparkUtils.FETCH_FAILURE_ERROR_MSG + appShuffleId + "/" + shuffleId,
        ce)
    } else
      throw ce
  }

  def newSerializerInstance(dep: ShuffleDependency[K, _, C]): SerializerInstance = {
    dep.serializer.newInstance()
  }
}

object CelebornShuffleReader {
  var streamCreatorPool: ThreadPoolExecutor = null
}
