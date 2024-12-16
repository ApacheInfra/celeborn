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

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class CelebornSourceSuite extends CelebornFunSuite {

  def createAbstractSource(
      conf: CelebornConf,
      extraLabels: String,
      role: String = "mock"): (String, List[String]) = {
    val mockSource = new AbstractSource(conf, role) {
      override def sourceName: String = "mockSource"
    }
    val user1 = Map("user" -> "user1")
    val user2 = Map("user" -> "user2")
    val user3 = Map("user" -> "user3")
    mockSource.addGauge("Gauge1") { () => 1000 }
    mockSource.addGauge("Gauge2", user1, true) { () => 2000 }
    mockSource.addCounter("Counter1", Map.empty[String, String], true)
    mockSource.addCounter("Counter2", user2, true)
    // test operation with and without label
    mockSource.incCounter("Counter1", 3000)
    mockSource.incCounter("Counter2", 4000, user2)
    mockSource.incCounter("Counter2", -4000, user2)
    mockSource.addTimer("Timer1")
    mockSource.addTimer("Timer2", user3)
    // ditto
    mockSource.startTimer("Timer1", "key1")
    mockSource.startTimer("Timer2", "key2", user3)
    Thread.sleep(10)
    mockSource.stopTimer("Timer1", "key1")
    mockSource.stopTimer("Timer2", "key2", user3)

    mockSource.timerMetrics.add("testTimerMetricsMap")

    val res = mockSource.getMetrics()
    var extraLabelsStr = extraLabels
    if (extraLabels.nonEmpty) {
      extraLabelsStr = extraLabels + ","
    }
    val instanceLabelStr =
      mockSource.instanceLabel.map(kv => s"""${kv._1}="${kv._2}",""").mkString(",")
    val exp1 = s"""metrics_Gauge1_Value{${extraLabelsStr}${instanceLabelStr}role="$role"} 1000"""
    val exp2 =
      s"""metrics_Gauge2_Value{${extraLabelsStr}${instanceLabelStr}role="$role",user="user1"} 2000"""
    val exp3 = s"""metrics_Counter1_Count{${extraLabelsStr}${instanceLabelStr}role="$role"} 3000"""
    val exp4 =
      s"""metrics_Counter2_Count{${extraLabelsStr}${instanceLabelStr}role="$role",user="user2"} 0"""
    val exp5 = s"""metrics_Timer1_Count{${extraLabelsStr}${instanceLabelStr}role="$role"} 1"""
    val exp6 =
      s"""metrics_Timer2_Count{${extraLabelsStr}${instanceLabelStr}role="$role",user="user3"} 1"""
    val exp7 = "testTimerMetricsMap"

    val expList = List[String](exp1, exp2, exp3, exp4, exp5, exp6, exp7)
    (res, expList)
  }

  def checkMetricsRes(res: String, labelList: List[String]): Unit = {
    labelList.foreach { exp =>
      assert(res.contains(exp))
    }
  }

  test("test getMetrics with customized label by conf") {
    val conf = new CelebornConf()
    val (resM, expsM) = createAbstractSource(conf, "", Role.MASTER)
    checkMetricsRes(resM, expsM)
    val (resW, expsW) = createAbstractSource(conf, "", Role.WORKER)
    checkMetricsRes(resW, expsW)

    // label's is normal
    conf.set(CelebornConf.METRICS_EXTRA_LABELS.key, "l1=v1,l2=v2,l3=v3")
    val extraLabels = """l1="v1",l2="v2",l3="v3""""
    val (res, exps) = createAbstractSource(conf, extraLabels)
    checkMetricsRes(res, exps)

    // labels' kv not correct
    assertThrows[IllegalArgumentException] {
      conf.set(CelebornConf.METRICS_EXTRA_LABELS.key, "l1=v1,l2=")
      val extraLabels2 = """l1="v1",l2="v2",l3="v3""""
      val (res2, exps2) = createAbstractSource(conf, extraLabels2)
      checkMetricsRes(res2, exps2)
    }

    // there are spaces in labels
    conf.set(CelebornConf.METRICS_EXTRA_LABELS.key, " l1 = v1, l2  =v2  ,l3 =v3  ")
    val extraLabels3 = """l1="v1",l2="v2",l3="v3""""
    val (res3, exps3) = createAbstractSource(conf, extraLabels3)
    checkMetricsRes(res3, exps3)
  }

  test("test getMetrics with full capacity and isAppEnable false") {
    val conf = new CelebornConf()

    // metrics won't contain appMetrics
    conf.set(CelebornConf.METRICS_APP_ENABLED.key, "false")
    conf.set(CelebornConf.METRICS_CAPACITY.key, "7")
    val (res1, exps1) = createAbstractSource(conf, "")
    List[Int](0, 4, 5, 6).foreach { i =>
      assert(res1.contains(exps1(i)))
    }
    List[Int](1, 2, 3).foreach { i =>
      assert(!res1.contains(exps1(i)))
    }

    // metrics contain appMetrics
    conf.set(CelebornConf.METRICS_APP_ENABLED.key, "true")
    conf.set(CelebornConf.METRICS_CAPACITY.key, "7")
    val (res2, exps2) = createAbstractSource(conf, "")
    checkMetricsRes(res2, exps2)
  }

  test("test getAndClearTimerMetrics in timerMetrics") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.METRICS_CAPACITY.key, "6")
    val role = "mock"
    val mockSource = new AbstractSource(conf, role) {
      override def sourceName: String = "mockSource"
    }
    val exp1 = "testTimerMetrics1"
    val exp2 = "testTimerMetrics2"
    mockSource.timerMetrics.add(exp1)
    val res1 = mockSource.getMetrics()
    mockSource.timerMetrics.add(exp2)
    val res2 = mockSource.getMetrics()

    assert(res1.contains(exp1) && !res1.contains(exp2))
    assert(res2.contains(exp2) && !res2.contains(exp1))
  }
}
