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

package org.apache.celeborn.common.metrics.source

import java.util.{Map => JMap}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.codahale.metrics._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.{CelebornHistogram, CelebornTimer, MetricLabels, ResettableSlidingWindowReservoir}
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils, Utils}
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._

case class NamedCounter(
    name: String,
    counter: Counter,
    labels: Map[String, String],
    isApp: Boolean = false)
  extends MetricLabels

case class NamedGauge[T](
    name: String,
    gauge: Gauge[T],
    labels: Map[String, String],
    isApp: Boolean = false)
  extends MetricLabels

case class NamedMeter(name: String, meter: Meter, labels: Map[String, String])
  extends MetricLabels

case class NamedHistogram(name: String, histogram: Histogram, labels: Map[String, String])
  extends MetricLabels

case class NamedTimer(name: String, timer: Timer, labels: Map[String, String]) extends MetricLabels

abstract class AbstractSource(conf: CelebornConf, role: String)
  extends Source with Logging {
  override val metricRegistry = new MetricRegistry()

  val metricsSlidingWindowSize: Int = conf.metricsSlidingWindowSize

  val metricsSampleRate: Double = conf.metricsSampleRate

  val metricsCollectCriticalEnabled: Boolean = conf.metricsCollectCriticalEnabled

  val metricsCapacity: Int = conf.metricsCapacity

  val timerSupplier = new TimerSupplier(metricsSlidingWindowSize)

  val metricsCleaner: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-metrics-cleaner")

  val roleLabel: (String, String) = "role" -> role
  val instanceLabel: Map[String, String] = role match {
    case Role.MASTER =>
      Map("instance" -> s"${Utils.localHostName(conf)}:${conf.masterHttpPort}")
    case Role.WORKER =>
      Map("instance" -> s"${Utils.localHostName(conf)}:${conf.workerHttpPort}")
    case _ => Map.empty
  }
  val staticLabels: Map[String, String] = conf.metricsExtraLabels + roleLabel ++ instanceLabel
  val staticLabelsString: String = MetricLabels.labelString(staticLabels)

  val metricsAppEnabled: Boolean = conf.metricsAppEnabled

  val applicationLabel = "applicationId"

  val timerMetrics: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()

  protected val namedGauges: ConcurrentHashMap[String, NamedGauge[_]] =
    JavaUtils.newConcurrentHashMap[String, NamedGauge[_]]()

  protected val namedTimers
      : ConcurrentHashMap[String, (NamedTimer, ConcurrentHashMap[String, Long])] =
    JavaUtils.newConcurrentHashMap[String, (NamedTimer, ConcurrentHashMap[String, Long])]()

  protected val namedCounters: ConcurrentHashMap[String, NamedCounter] =
    JavaUtils.newConcurrentHashMap[String, NamedCounter]()

  protected val namedMeters: ConcurrentHashMap[String, NamedMeter] =
    JavaUtils.newConcurrentHashMap[String, NamedMeter]()

  def addTimerMetrics(namedTimer: NamedTimer): Unit = {
    val timerMetricsString = getTimerMetrics(namedTimer)
    timerMetrics.add(timerMetricsString)
  }

  def addGauge[T](
      name: String,
      labels: Map[String, String],
      gauge: Gauge[T],
      isAppMetrics: Boolean): Unit = {
    if (metricsAppEnabled || (!metricsAppEnabled && !isAppMetrics)) {
      // filter out non-number type gauges
      if (gauge.getValue.isInstanceOf[Number]) {
        namedGauges.putIfAbsent(
          metricNameWithCustomizedLabels(name, labels),
          NamedGauge(name, gauge, labels ++ staticLabels, isAppMetrics))
      } else {
        logWarning(
          s"Add gauge $name failed, the value type ${gauge.getValue.getClass} is not a number")
      }
    }
  }

  def addGauge[T](
      name: String,
      labels: Map[String, String],
      gauge: Gauge[T]): Unit = {
    addGauge(name, labels, gauge, false)
  }

  def addGauge[T](
      name: String,
      labels: JMap[String, String],
      gauge: Gauge[T]): Unit = {
    addGauge(name, labels.asScala.toMap, gauge)
  }

  def addGauge[T](
      name: String,
      labels: Map[String, String] = Map.empty,
      isAppMetrics: Boolean = false)(f: () => T): Unit = {
    addGauge(
      name,
      labels,
      metricRegistry.gauge(metricNameWithCustomizedLabels(name, labels), new GaugeSupplier[T](f)),
      isAppMetrics)
  }

  def addGauge[T](name: String, gauge: Gauge[T]): Unit = {
    addGauge(name, Map.empty[String, String], gauge)
  }

  def addMeter(
      name: String,
      labels: Map[String, String],
      meter: Meter): Unit = {
    namedMeters.putIfAbsent(
      metricNameWithCustomizedLabels(name, labels),
      NamedMeter(name, meter, labels ++ staticLabels))
  }

  def addMeter(
      name: String,
      labels: JMap[String, String],
      meter: Meter): Unit = {
    addMeter(name, labels.asScala.toMap, meter)
  }

  def addMeter(name: String, labels: Map[String, String] = Map.empty)(f: () => Long): Unit = {
    addMeter(
      name,
      labels,
      metricRegistry.meter(metricNameWithCustomizedLabels(name, labels), new MeterSupplier(f)))
  }

  def addMeter(name: String, meter: Meter): Unit = {
    addMeter(name, Map.empty[String, String], meter)
  }

  def addTimer(name: String): Unit = addTimer(name, Map.empty[String, String])

  def addTimer(name: String, labels: Map[String, String]): Unit = {
    val metricNameWithLabel = metricNameWithCustomizedLabels(name, labels)
    namedTimers.computeIfAbsent(
      metricNameWithLabel,
      (_: String) => {
        val namedTimer = NamedTimer(
          name,
          metricRegistry.timer(metricNameWithLabel, timerSupplier),
          labels ++ staticLabels)
        val values = JavaUtils.newConcurrentHashMap[String, Long]()
        (namedTimer, values)
      })
  }

  def addCounter(name: String): Unit = addCounter(name, Map.empty[String, String])

  def addCounter(name: String, labels: Map[String, String], isAppMetrics: Boolean = false): Unit = {
    if (metricsAppEnabled || (!metricsAppEnabled && !isAppMetrics)) {
      val metricNameWithLabel = metricNameWithCustomizedLabels(name, labels)
      namedCounters.putIfAbsent(
        metricNameWithLabel,
        NamedCounter(
          name,
          metricRegistry.counter(metricNameWithLabel),
          labels ++ staticLabels,
          isAppMetrics))
    }
  }

  def counters(): List[NamedCounter] = {
    namedCounters.values().asScala.toList
  }

  def gauges(): List[NamedGauge[_]] = {
    namedGauges.values().asScala.toList
  }

  def meters(): List[NamedMeter] = {
    namedMeters.values().asScala.toList
  }

  def histograms(): List[NamedHistogram] = {
    List.empty[NamedHistogram]
  }

  def timers(): List[NamedTimer] = {
    namedTimers.values().asScala.toList.map(_._1)
  }

  def getAndClearTimerMetrics(): List[String] = {
    timerMetrics.synchronized {
      var timerMetricsSize = timerMetrics.size()
      val timerMetricsList = ArrayBuffer[String]()
      while (timerMetricsSize > 0) {
        timerMetricsList.append(timerMetrics.poll())
        timerMetricsSize = timerMetricsSize - 1
      }
      timerMetricsList.toList
    }
  }

  def gaugeExists(name: String, labels: Map[String, String]): Boolean = {
    namedGauges.containsKey(metricNameWithCustomizedLabels(name, labels))
  }

  def needSample(): Boolean = {
    if (metricsSampleRate >= 1) {
      true
    } else if (metricsSampleRate <= 0) {
      false
    } else {
      Random.nextDouble() <= metricsSampleRate
    }
  }

  def removeCounter(name: String, labels: Map[String, String]): Unit = {
    namedCounters.remove(removeMetric(name, labels))
  }

  def removeGauge(name: String, labels: Map[String, String]): Unit = {
    namedGauges.remove(removeMetric(name, labels))
  }

  def removeMetric(name: String, labels: Map[String, String]): String = {
    val metricNameWithLabel = metricNameWithCustomizedLabels(name, labels)
    metricRegistry.remove(metricNameWithLabel)
    metricNameWithLabel
  }

  override def sample[T](metricsName: String, key: String)(f: => T): T = {
    sample(metricsName, key, Map.empty[String, String])(f)
  }

  def sample[T](metricsName: String, key: String, labels: Map[String, String])(f: => T): T = {
    val sample = needSample()
    var r: Any = null
    try {
      if (sample) {
        doStartTimer(metricsName, key, labels)
      }
      r = f
    } finally {
      if (sample) {
        doStopTimer(metricsName, key, labels)
      }
    }
    r.asInstanceOf[T]
  }

  override def startTimer(metricsName: String, key: String): Unit = {
    startTimer(metricsName, key, Map.empty[String, String])
  }

  def startTimer(metricsName: String, key: String, labels: Map[String, String]): Unit = {
    if (needSample()) {
      doStartTimer(metricsName, key, labels)
    }
  }

  override def stopTimer(metricsName: String, key: String): Unit = {
    stopTimer(metricsName, key, Map.empty[String, String])
  }

  def stopTimer(metricsName: String, key: String, labels: Map[String, String]): Unit = {
    doStopTimer(metricsName, key, labels)
  }

  def doStartTimer(metricsName: String, key: String, labels: Map[String, String]): Unit = {
    val metricNameWithLabel = metricNameWithCustomizedLabels(metricsName, labels)
    val pair = namedTimers.get(metricNameWithLabel)
    if (pair != null) {
      pair._2.put(key, System.nanoTime())
    } else {
      logWarning(s"Metric $metricNameWithLabel not found!")
    }
  }

  protected def doStopTimer(metricsName: String, key: String, labels: Map[String, String]): Unit = {
    val metricNameWithLabel = metricNameWithCustomizedLabels(metricsName, labels)
    try {
      val (namedTimer, map) = namedTimers.get(metricNameWithLabel)
      val startTime = Option(map.remove(key))
      startTime match {
        case Some(t) =>
          namedTimer.timer.update(System.nanoTime() - t, TimeUnit.NANOSECONDS)
          if (namedTimer.timer.getCount % metricsSlidingWindowSize == 0) {
            addTimerMetrics(namedTimer)
          }
        case None =>
      }
    } catch {
      case e: Exception =>
        logWarning(s"Exception encountered during stop timer of metric $metricNameWithLabel", e)
    }
  }

  def updateTimer(name: String, value: Long): Unit = {
    updateTimer(name, value, Map.empty[String, String])
  }

  def updateTimer(metricsName: String, value: Long, labels: Map[String, String]): Unit = {
    val metricNameWithLabel = metricNameWithCustomizedLabels(metricsName, labels)
    if (!namedTimers.containsKey(metricNameWithLabel)) {
      addTimer(metricsName, labels)
    }
    val (namedTimer, _) = namedTimers.get(metricNameWithLabel)
    namedTimer.timer.update(value, TimeUnit.NANOSECONDS)
  }

  def incCounter(metricsName: String): Unit = {
    incCounter(metricsName, 1)
  }

  override def incCounter(metricsName: String, incV: Long): Unit = {
    incCounter(metricsName, incV, Map.empty[String, String])
  }

  def incCounter(metricsName: String, incV: Long, labels: Map[String, String]): Unit = {
    val metricNameWithLabel = metricNameWithCustomizedLabels(metricsName, labels)
    val counter = namedCounters.get(metricNameWithLabel)
    if (counter != null) {
      counter.counter.inc(incV)
    } else {
      logWarning(s"Metric $metricNameWithLabel not found!")
    }
  }

  private def clearOldValues(map: ConcurrentHashMap[String, Long]): Unit = {
    if (map.size > 5000) {
      // remove values has existed more than 15 min
      // 50000 values may be 1MB more or less
      val threshTime = System.nanoTime() - 900000000000L
      val it = map.entrySet().iterator
      while (it.hasNext) {
        val entry = it.next()
        if (entry.getValue < threshTime) {
          it.remove()
        }
      }
    }
  }

  protected def startCleaner(): Unit = {
    val cleanTask: Runnable = new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        namedTimers.values.asScala.toArray.map(_._2).foreach(clearOldValues)
      }
    }
    metricsCleaner.scheduleWithFixedDelay(cleanTask, 10, 10, TimeUnit.MINUTES)
  }

  def getCounterMetrics(nc: NamedCounter): String = {
    val timestamp = System.currentTimeMillis
    val label = nc.labelString
    val str = s"${normalizeKey(nc.name)}Count$label ${nc.counter.getCount} $timestamp\n"
    str
  }

  def getGaugeMetrics(ng: NamedGauge[_]): String = {
    val timestamp = System.currentTimeMillis
    val sb = new StringBuilder
    val label = ng.labelString
    sb.append(s"${normalizeKey(ng.name)}Value$label ${ng.gauge.getValue} $timestamp\n")
    sb.toString()
  }

  def getMeterMetrics(nm: NamedMeter): String = {
    val timestamp = System.currentTimeMillis
    val sb = new StringBuilder
    val label = nm.labelString
    sb.append(s"${normalizeKey(nm.name)}Count$label ${nm.meter.getCount} $timestamp\n")
    sb.append(s"${normalizeKey(nm.name)}MeanRate$label ${nm.meter.getMeanRate} $timestamp\n")
    sb.append(
      s"${normalizeKey(nm.name)}OneMinuteRate$label ${nm.meter.getOneMinuteRate} $timestamp\n")
    sb.append(
      s"${normalizeKey(nm.name)}FiveMinuteRate$label ${nm.meter.getFiveMinuteRate} $timestamp\n")
    sb.append(
      s"${normalizeKey(nm.name)}FifteenMinuteRate$label ${nm.meter.getFifteenMinuteRate} $timestamp\n")
    sb.toString()
  }

  def getHistogramMetrics(nh: NamedHistogram): String = {
    val timestamp = System.currentTimeMillis
    val sb = new mutable.StringBuilder
    val snapshot = nh.histogram.getSnapshot
    val prefix = normalizeKey(nh.name)
    val label = nh.labelString
    sb.append(s"${prefix}Count$label ${nh.histogram.getCount} $timestamp\n")
    sb.append(s"${prefix}Max$label ${reportNanosAsMills(snapshot.getMax)} $timestamp\n")
    sb.append(s"${prefix}Mean$label ${reportNanosAsMills(snapshot.getMean)} $timestamp\n")
    sb.append(s"${prefix}Min$label ${reportNanosAsMills(snapshot.getMin)} $timestamp\n")
    sb.append(s"${prefix}50thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.getMedian)} $timestamp\n")
    sb.append(s"${prefix}75thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get75thPercentile)} $timestamp\n")
    sb.append(s"${prefix}95thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get95thPercentile)} $timestamp\n")
    sb.append(s"${prefix}98thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get98thPercentile)} $timestamp\n")
    sb.append(s"${prefix}99thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get99thPercentile)} $timestamp\n")
    sb.append(s"${prefix}999thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get999thPercentile)} $timestamp\n")
    sb.toString()
  }

  def getTimerMetrics(nt: NamedTimer): String = {
    val timestamp = System.currentTimeMillis
    val sb = new mutable.StringBuilder
    val snapshot = nt.timer.getSnapshot
    val prefix = normalizeKey(nt.name)
    val label = nt.labelString
    sb.append(s"${prefix}Count$label ${nt.timer.getCount} $timestamp\n")
    sb.append(s"${prefix}Max$label ${reportNanosAsMills(snapshot.getMax)} $timestamp\n")
    sb.append(s"${prefix}Mean$label ${reportNanosAsMills(snapshot.getMean)} $timestamp\n")
    sb.append(s"${prefix}Min$label ${reportNanosAsMills(snapshot.getMin)} $timestamp\n")
    sb.append(s"${prefix}50thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.getMedian)} $timestamp\n")
    sb.append(s"${prefix}75thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get75thPercentile)} $timestamp\n")
    sb.append(s"${prefix}95thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get95thPercentile)} $timestamp\n")
    sb.append(s"${prefix}98thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get98thPercentile)} $timestamp\n")
    sb.append(s"${prefix}99thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get99thPercentile)} $timestamp\n")
    sb.append(s"${prefix}999thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get999thPercentile)} $timestamp\n")
    sb.toString()
  }

  def getAllMetricsNum: Int = {
    val sum = timerMetrics.size() +
      namedTimers.size() +
      namedMeters.size() +
      namedGauges.size() +
      namedCounters.size()
    sum
  }

  override def getMetrics(): String = {
    var leftMetricsNum = metricsCapacity
    val sb = new mutable.StringBuilder
    leftMetricsNum = fillInnerMetricsSnapshot(getAndClearTimerMetrics(), leftMetricsNum, sb)
    leftMetricsNum = fillInnerMetricsSnapshot(timers(), leftMetricsNum, sb)
    leftMetricsNum = fillInnerMetricsSnapshot(histograms(), leftMetricsNum, sb)
    leftMetricsNum = fillInnerMetricsSnapshot(meters(), leftMetricsNum, sb)
    leftMetricsNum = fillInnerMetricsSnapshot(gauges(), leftMetricsNum, sb)
    leftMetricsNum = fillInnerMetricsSnapshot(counters(), leftMetricsNum, sb)
    if (leftMetricsNum <= 0) {
      logWarning(
        s"The number of metrics exceed the output metrics strings capacity! All metrics Num: $getAllMetricsNum")
    }
    sb.toString()
  }

  private def fillInnerMetricsSnapshot(
      metricList: List[AnyRef],
      leftNum: Int,
      sb: mutable.StringBuilder): Int = {
    if (leftNum <= 0) {
      return 0
    }
    val addList = metricList.take(leftNum)
    addList.foreach {
      case c: NamedCounter =>
        sb.append(getCounterMetrics(c))
      case g: NamedGauge[_] =>
        sb.append(getGaugeMetrics(g))
      case m: NamedMeter =>
        sb.append(getMeterMetrics(m))
      case h: NamedHistogram =>
        sb.append(getHistogramMetrics(h))
        h.asInstanceOf[CelebornHistogram].reservoir
          .asInstanceOf[ResettableSlidingWindowReservoir].reset()
      case t: NamedTimer =>
        sb.append(getTimerMetrics(t))
        t.timer.asInstanceOf[CelebornTimer].reservoir
          .asInstanceOf[ResettableSlidingWindowReservoir].reset()
      case s =>
        sb.append(s.toString)
    }
    leftNum - addList.size
  }

  override def destroy(): Unit = {
    metricsCleaner.shutdown()
    namedCounters.clear()
    namedGauges.clear()
    namedMeters.clear()
    namedTimers.clear()
    timerMetrics.clear()
    metricRegistry.removeMatching(new MetricFilter {
      override def matches(s: String, metric: Metric): Boolean = true
    })
  }

  protected def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
  }

  def reportNanosAsMills(value: Double): Double = {
    BigDecimal(value / 1000000).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  protected def metricNameWithCustomizedLabels(
      metricsName: String,
      labels: Map[String, String]): String = {
    if (labels.isEmpty) {
      metricsName + staticLabelsString
    } else {
      metricsName + MetricLabels.labelString(labels ++ staticLabels)
    }
  }
}

class TimerSupplier(val slidingWindowSize: Int)
  extends MetricRegistry.MetricSupplier[Timer] {
  override def newMetric(): Timer = {
    new CelebornTimer(new ResettableSlidingWindowReservoir(slidingWindowSize))
  }
}

class GaugeSupplier[T](f: () => T) extends MetricRegistry.MetricSupplier[Gauge[_]] {
  override def newMetric(): Gauge[T] = new Gauge[T] { override def getValue: T = f() }
}

class MeterSupplier(f: () => Long) extends MetricRegistry.MetricSupplier[Meter] {
  override def newMetric(): Meter = new Meter { override def getCount: Long = f() }
}
