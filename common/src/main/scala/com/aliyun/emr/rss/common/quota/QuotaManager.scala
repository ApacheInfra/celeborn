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
package com.aliyun.emr.rss.common.quota

import java.util.concurrent.ConcurrentHashMap

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.UserIdentifier

abstract class QuotaManager(conf: RssConf) extends Logging {
  val userQuotas: ConcurrentHashMap[UserIdentifier, Quota] =
    new ConcurrentHashMap[UserIdentifier, Quota]()

  /**
   * Initialize user quota settings.
   */
  def initialize(): Unit

  /**
   * Method to refresh current user quota setting.
   */
  def refresh(): Unit

  def getQuota(userIdentifier: UserIdentifier): Quota = {
    userQuotas.getOrDefault(userIdentifier, new Quota())
  }
}

object QuotaManager extends Logging {
  def instantiate(conf: RssConf): QuotaManager = {
    val className = RssConf.quotaManagerClass(conf)
    logDebug(s"Creating quota manager $className")
    val clazz = Class.forName(
      className,
      true,
      Thread.currentThread().getContextClassLoader).asInstanceOf[Class[QuotaManager]]
    try {
      val ctor = clazz.getDeclaredConstructor(classOf[RssConf])
      val quotaManager = ctor.newInstance(conf)
      quotaManager.initialize()
      quotaManager
    } catch {
      case e: NoSuchMethodException =>
        logError(s"Falling to instantiate quota manager $className", e)
        throw e
    }
  }

}
