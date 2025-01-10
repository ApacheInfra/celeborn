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
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{Aggregator, InterruptibleIterator, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReader}
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader.streamCreatorPool
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.client.read.CelebornInputStream
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.{CelebornIOException, PartitionUnRetryAbleException}
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils}

class CelebornShuffleReader[K, C](
    handle: CelebornShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    startMapIndex: Int = 0,
    endMapIndex: Int = Int.MaxValue,
    context: TaskContext,
    conf: CelebornConf,
    shuffleIdTracker: ExecutorShuffleIdTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency
  private val shuffleClient = ShuffleClient.get(
    handle.appUniqueId,
    handle.lifecycleManagerHost,
    handle.lifecycleManagerPort,
    conf,
    handle.userIdentifier,
    handle.extension)

  private val exceptionRef = new AtomicReference[IOException]
  private val encodedAttemptId = SparkCommonUtils.getEncodedAttemptNumber(context)

  override def read(): Iterator[Product2[K, C]] = {

    val serializerInstance = dep.serializer.newInstance()

    val shuffleId = SparkUtils.celebornShuffleId(shuffleClient, handle, context, false)
    shuffleIdTracker.track(handle.shuffleId, shuffleId)
    logDebug(
      s"get shuffleId $shuffleId for appShuffleId ${handle.shuffleId} attemptNum ${context.stageAttemptNumber()}")

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesRead: Long): Unit =
        readMetrics.incRemoteBytesRead(bytesRead)

      override def incReadTime(time: Long): Unit =
        readMetrics.incFetchWaitTime(time)
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

    val streams = JavaUtils.newConcurrentHashMap[Integer, CelebornInputStream]()
    (startPartition until endPartition).map(partitionId => {
      streamCreatorPool.submit(new Runnable {
        override def run(): Unit = {
          if (exceptionRef.get() == null) {
            try {
              val inputStream = shuffleClient.readPartition(
                shuffleId,
                partitionId,
                encodedAttemptId,
                context.taskAttemptId(),
                startMapIndex,
                endMapIndex,
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
      })
    })

    val recordIter = (startPartition until endPartition).iterator.map(partitionId => {
      if (handle.numMaps > 0) {
        val startFetchWait = System.nanoTime()
        var inputStream: CelebornInputStream = streams.get(partitionId)
        while (inputStream == null) {
          if (exceptionRef.get() != null) {
            exceptionRef.get() match {
              case ce @ (_: CelebornIOException | _: PartitionUnRetryAbleException) =>
                if (handle.throwsFetchFailure &&
                  shuffleClient.reportShuffleFetchFailure(
                    handle.shuffleId,
                    shuffleId,
                    context.taskAttemptId())) {
                  throw new FetchFailedException(
                    null,
                    handle.shuffleId,
                    -1,
                    partitionId,
                    SparkUtils.FETCH_FAILURE_ERROR_MSG + shuffleId,
                    ce)
                } else
                  throw ce
              case e => throw e
            }
          }
          Thread.sleep(50)
          inputStream = streams.get(partitionId)
        }
        metricsCallback.incReadTime(
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait))
        // ensure inputStream is closed when task completes
        context.addTaskCompletionListener[Unit](_ => inputStream.close())
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
          if (handle.throwsFetchFailure &&
            shuffleClient.reportShuffleFetchFailure(
              handle.shuffleId,
              shuffleId,
              context.taskAttemptId())) {
            throw new FetchFailedException(
              null,
              handle.shuffleId,
              -1,
              partitionId,
              SparkUtils.FETCH_FAILURE_ERROR_MSG + shuffleId,
              e)
          } else
            throw e
      }
    }

    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
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
}

object CelebornShuffleReader {
  var streamCreatorPool: ThreadPoolExecutor = null
}
