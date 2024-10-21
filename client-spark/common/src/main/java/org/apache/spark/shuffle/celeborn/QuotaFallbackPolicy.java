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

package org.apache.spark.shuffle.celeborn;

import org.apache.spark.ShuffleDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.message.ControlMessages.CheckQuotaResponse;

public class QuotaFallbackPolicy implements ShuffleFallbackPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(QuotaFallbackPolicy.class);

  public static final QuotaFallbackPolicy INSTANCE = new QuotaFallbackPolicy();

  /**
   * If celeborn cluster exceeds current user's quota, fallback to spark built-in shuffle
   * implementation.
   *
   * @param shuffleDependency The shuffle dependency of Spark.
   * @param celebornConf The configuration of Celeborn.
   * @param lifecycleManager The {@link LifecycleManager} of Celeborn.
   * @return Whether celeborn cluster has no available space for current user.
   */
  @Override
  public boolean needFallback(
      ShuffleDependency<?, ?, ?> shuffleDependency,
      CelebornConf celebornConf,
      LifecycleManager lifecycleManager) {
    if (!celebornConf.quotaEnabled()) {
      return false;
    }
    CheckQuotaResponse response = lifecycleManager.checkQuota();
    boolean needFallback = !response.isAvailable();
    if (needFallback) {
      LOG.warn(
          "Quota exceeds for current user {}. Because {}",
          lifecycleManager.getUserIdentifier(),
          response.reason());
    }
    return needFallback;
  }
}
