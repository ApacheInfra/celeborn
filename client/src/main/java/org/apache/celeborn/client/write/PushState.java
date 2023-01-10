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

package org.apache.celeborn.client.write;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.protocol.PartitionLocation;

public class PushState {
  private static final Logger logger = LoggerFactory.getLogger(PushState.class);

  private final int pushBufferMaxSize;
  public AtomicReference<IOException> exception = new AtomicReference<>();
  private final InFlightRequestTracker inFlightRequestTracker;

  public PushState(CelebornConf conf) {
    pushBufferMaxSize = conf.pushBufferMaxSize();
    inFlightRequestTracker = new InFlightRequestTracker(conf, this);
  }

  public void pushStarted(
      int batchId, ChannelFuture future, RpcResponseCallback callback, String hostAndPort) {
    InFlightRequestTracker.BatchInfo info =
        inFlightRequestTracker.getBatchIdSetByAddressPair(hostAndPort).get(batchId);
    // In rare cases info could be null. For example, a speculative task has one thread pushing,
    // and other thread retry-pushing. At time 1 thread 1 find StageEnded, then it cleans up
    // PushState, at the same time thread 2 pushes data and calles pushStarted,
    // at this time info will be null
    if (info != null) {
      info.pushTime = System.currentTimeMillis();
      info.channelFuture = future;
      info.callback = callback;
    }
  }

  public void cleanup() {
    inFlightRequestTracker.cleanup();
  }

  // key: ${master addr}-${slave addr} value: list of data batch
  public final ConcurrentHashMap<String, DataBatches> batchesMap = new ConcurrentHashMap<>();

  /**
   * Not thread-safe
   *
   * @param addressPair
   * @param loc
   * @param batchId
   * @param body
   * @return
   */
  public boolean addBatchData(String addressPair, PartitionLocation loc, int batchId, byte[] body) {
    DataBatches batches = batchesMap.computeIfAbsent(addressPair, (s) -> new DataBatches());
    batches.addDataBatch(loc, batchId, body);
    return batches.getTotalSize() > pushBufferMaxSize;
  }

  public DataBatches takeDataBatches(String addressPair) {
    return batchesMap.remove(addressPair);
  }

  public int nextBatchId() {
    return inFlightRequestTracker.nextBatchId();
  }

  public void addBatch(int batchId, String hostAndPushPort) {
    inFlightRequestTracker.addFlightBatches(batchId, hostAndPushPort);
  }

  public void removeBatch(int batchId, String hostAndPushPort) {
    inFlightRequestTracker.removeFlightBatches(batchId, hostAndPushPort);
  }

  public boolean limitMaxInFlight(String hostAndPushPort, int maxInFlight) throws IOException {
    return inFlightRequestTracker.limitMaxInFlight(hostAndPushPort, maxInFlight);
  }

  public boolean limitZeroInFlight() throws IOException {
    return inFlightRequestTracker.limitZeroInFlight();
  }
}
