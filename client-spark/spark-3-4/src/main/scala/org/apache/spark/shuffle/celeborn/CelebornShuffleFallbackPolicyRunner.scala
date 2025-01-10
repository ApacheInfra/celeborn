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

import scala.collection.JavaConverters._

import org.apache.spark.ShuffleDependency

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.FallbackPolicy

class CelebornShuffleFallbackPolicyRunner(conf: CelebornConf) extends Logging {
  private val shuffleFallbackPolicy = conf.sparkShuffleFallbackPolicy
  private val shuffleFallbackPolicies =
    ShuffleFallbackPolicyFactory.getShuffleFallbackPolicies.asScala

  def getActivatedFallbackPolicy[K, V, C](
      dependency: ShuffleDependency[K, V, C],
      lifecycleManager: LifecycleManager): Option[ShuffleFallbackPolicy] = {
    val fallbackPolicy =
      shuffleFallbackPolicies.find(_.needFallback(dependency, conf, lifecycleManager))
    if (fallbackPolicy.isDefined && FallbackPolicy.NEVER.equals(shuffleFallbackPolicy)) {
      throw new CelebornIOException(
        "Fallback to spark built-in shuffle implementation is prohibited.")
    }
    fallbackPolicy
  }
}
