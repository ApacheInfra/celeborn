/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.plugin.flink.tiered;

import static org.apache.celeborn.plugin.flink.utils.Utils.checkArgument;
import static org.apache.celeborn.plugin.flink.utils.Utils.checkState;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.DriverChangedException;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.plugin.flink.buffer.BufferHeader;
import org.apache.celeborn.plugin.flink.buffer.BufferPacker;
import org.apache.celeborn.plugin.flink.buffer.ReceivedNoHeaderBufferPacker;
import org.apache.celeborn.plugin.flink.readclient.FlinkShuffleClientImpl;
import org.apache.celeborn.plugin.flink.utils.BufferUtils;
import org.apache.celeborn.plugin.flink.utils.Utils;

public class CelebornTierProducerAgent implements TierProducerAgent {

  private static final Logger LOG = LoggerFactory.getLogger(CelebornTierProducerAgent.class);

  private final int numBuffersPerSegment;

  // The flink buffer size in bytes.
  private final int bufferSizeBytes;

  private final int numPartitions;

  private final int numSubPartitions;

  private final CelebornConf celebornConf;

  private final TieredStorageMemoryManager memoryManager;

  private final String applicationId;

  private final int shuffleId;

  private final int mapId;

  private final int attemptId;

  private final int partitionId;

  private final String lifecycleManagerHost;

  private final int lifecycleManagerPort;

  private final long lifecycleManagerTimestamp;

  private FlinkShuffleClientImpl flinkShuffleClient;

  private BufferPacker bufferPacker;

  private final int[] subPartitionSegmentIds;

  private final int[] subPartitionSegmentBuffers;

  private final int maxReviveTimes;

  private PartitionLocation partitionLocation;

  private boolean hasRegisteredShuffle;

  private int currentRegionIndex = 0;

  private int currentSubpartition = 0;

  private boolean hasSentHandshake = false;

  private boolean hasSentRegionStart = false;

  private volatile boolean isReleased;

  CelebornTierProducerAgent(
      CelebornConf conf,
      TieredStoragePartitionId partitionId,
      int numPartitions,
      int numSubPartitions,
      int numBytesPerSegment,
      int bufferSizeBytes,
      TieredStorageMemoryManager memoryManager,
      TieredStorageResourceRegistry resourceRegistry,
      List<TierShuffleDescriptor> shuffleDescriptors) {
    checkArgument(
        numBytesPerSegment >= bufferSizeBytes, "One segment should contain at least one buffer.");
    checkArgument(shuffleDescriptors.size() == 1, "There should be only one shuffle descriptor.");
    TierShuffleDescriptor descriptor = shuffleDescriptors.get(0);
    checkArgument(
        descriptor instanceof TierShuffleDescriptorImpl,
        "Wrong shuffle descriptor type " + descriptor.getClass());
    TierShuffleDescriptorImpl shuffleDesc = (TierShuffleDescriptorImpl) descriptor;

    this.numBuffersPerSegment = numBytesPerSegment / bufferSizeBytes;
    this.bufferSizeBytes = bufferSizeBytes;
    this.memoryManager = memoryManager;
    this.numPartitions = numPartitions;
    this.numSubPartitions = numSubPartitions;
    this.celebornConf = conf;
    this.subPartitionSegmentIds = new int[numSubPartitions];
    this.subPartitionSegmentBuffers = new int[numSubPartitions];
    this.maxReviveTimes = conf.clientPushMaxReviveTimes();

    this.applicationId = shuffleDesc.getCelebornAppId();
    this.shuffleId =
        shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getShuffleId();
    this.mapId = shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getMapId();
    this.attemptId =
        shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getAttemptId();
    this.partitionId =
        shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getPartitionId();
    this.lifecycleManagerHost = shuffleDesc.getShuffleResource().getLifecycleManagerHost();
    this.lifecycleManagerPort = shuffleDesc.getShuffleResource().getLifecycleManagerPort();
    this.lifecycleManagerTimestamp =
        shuffleDesc.getShuffleResource().getLifecycleManagerTimestamp();
    this.flinkShuffleClient = getShuffleClient();

    Arrays.fill(subPartitionSegmentIds, -1);
    Arrays.fill(subPartitionSegmentBuffers, 0);

    this.bufferPacker = new ReceivedNoHeaderBufferPacker(this::write);
    resourceRegistry.registerResource(partitionId, this::releaseResources);
    registerShuffle();
    try {
      handshake();
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  @Override
  public boolean tryStartNewSegment(
      TieredStorageSubpartitionId tieredStorageSubpartitionId, int segmentId, int minNumBuffers) {
    int subPartitionId = tieredStorageSubpartitionId.getSubpartitionId();
    checkState(
        segmentId >= subPartitionSegmentIds[subPartitionId], "Wrong segment id " + segmentId);
    subPartitionSegmentIds[subPartitionId] = segmentId;
    // If the start segment rpc is sent, the worker side will expect that
    // there must be at least one buffer will be written in the next moment.
    try {
      flinkShuffleClient.segmentStart(
          shuffleId, mapId, attemptId, subPartitionId, segmentId, partitionLocation);
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
    return true;
  }

  @Override
  public boolean tryWrite(
      TieredStorageSubpartitionId tieredStorageSubpartitionId,
      Buffer buffer,
      Object bufferOwner,
      int numRemainingConsecutiveBuffers) {
    // It should be noted that, unlike RemoteShuffleOutputGate#write, the received buffer contains
    // only
    // and does not have any remaining space for writing the celeborn header.

    int subPartitionId = tieredStorageSubpartitionId.getSubpartitionId();

    if (subPartitionSegmentBuffers[subPartitionId] + 1 + numRemainingConsecutiveBuffers
        >= numBuffersPerSegment) {
      // End the current segment if the segment buffer count reaches the threshold
      subPartitionSegmentBuffers[subPartitionId] = 0;
      try {
        bufferPacker.drain();
      } catch (InterruptedException e) {
        buffer.recycleBuffer();
        ExceptionUtils.rethrow(e, "Failed to process buffer.");
      }
      appendEndOfSegmentBuffer(subPartitionId);
      return false;
    }

    if (buffer.isBuffer()) {
      memoryManager.transferBufferOwnership(
          bufferOwner, CelebornTierFactory.getCelebornTierName(), buffer);
    }

    // write buffer to BufferPacker and record buffer count per subPartition per segment
    processBuffer(buffer, subPartitionId);
    subPartitionSegmentBuffers[subPartitionId]++;
    return true;
  }

  @Override
  public void close() {
    if (hasSentRegionStart) {
      regionFinish();
    }
    try {
      if (hasRegisteredShuffle && partitionLocation != null) {
        flinkShuffleClient.mapPartitionMapperEnd(
            shuffleId, mapId, attemptId, numPartitions, partitionLocation.getId());
      }
    } catch (Exception e) {
      Utils.rethrowAsRuntimeException(e);
    }
    bufferPacker.close();
    bufferPacker = null;
    flinkShuffleClient.cleanup(shuffleId, mapId, attemptId);
    flinkShuffleClient = null;
  }

  private void regionStartOrFinish(int subPartitionId) {
    // check whether the region should be started or finished
    regionStart();
    if (subPartitionId < currentSubpartition) {
      // if the consumed subPartitionId is out of order, it means that should the previous region
      // should be finished, and starting a new region.
      regionFinish();
      LOG.debug(
          "Check region finish sub partition id {} and start next region {}",
          subPartitionId,
          currentRegionIndex);
      regionStart();
    }
  }

  private void regionStart() {
    if (hasSentRegionStart) {
      return;
    }
    regionStartWithRevive();
  }

  private void regionStartWithRevive() {
    try {
      int remainingReviveTimes = maxReviveTimes;
      while (remainingReviveTimes-- > 0 && !hasSentRegionStart) {
        Optional<PartitionLocation> revivePartition =
            flinkShuffleClient.regionStart(
                shuffleId, mapId, attemptId, partitionLocation, currentRegionIndex, false);
        if (revivePartition.isPresent()) {
          LOG.info(
              "Revive at regionStart, currentTimes:{}, totalTimes:{} for shuffleId:{}, mapId:{}, "
                  + "attempId:{}, currentRegionIndex:{}, isBroadcast:{}, newPartition:{}, oldPartition:{}",
              remainingReviveTimes,
              maxReviveTimes,
              shuffleId,
              mapId,
              attemptId,
              currentRegionIndex,
              false,
              revivePartition,
              partitionLocation);
          partitionLocation = revivePartition.get();
          // For every revive partition, handshake should be sent firstly
          hasSentHandshake = false;
          handshake();
          if (numSubPartitions > 0) {
            for (int i = 0; i < numSubPartitions; i++) {
              flinkShuffleClient.segmentStart(
                  shuffleId, mapId, attemptId, i, subPartitionSegmentIds[i], partitionLocation);
            }
          }
        } else {
          hasSentRegionStart = true;
          currentSubpartition = 0;
        }
      }
      if (remainingReviveTimes == 0 && !hasSentRegionStart) {
        throw new RuntimeException(
            "After retry " + maxReviveTimes + " times, still failed to send regionStart");
      }
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  void regionFinish() {
    try {
      bufferPacker.drain();
      flinkShuffleClient.regionFinish(shuffleId, mapId, attemptId, partitionLocation);
      hasSentRegionStart = false;
      currentRegionIndex++;
    } catch (Exception e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  private void handshake() throws IOException {
    try {
      int remainingReviveTimes = maxReviveTimes;
      while (remainingReviveTimes-- > 0 && !hasSentHandshake) {
        // In the Flink hybrid shuffle integration strategy, the data buffer sent to the Celeborn
        // workers consists of two components: the Celeborn header and the data buffers.
        // In this scenario, the maximum byte size of the buffer received by the Celeborn worker is
        // equal to the sum of the Flink buffer size and the Celeborn header size.
        Optional<PartitionLocation> revivePartition =
            flinkShuffleClient.pushDataHandShake(
                shuffleId,
                mapId,
                attemptId,
                numSubPartitions,
                bufferSizeBytes + BufferUtils.HEADER_LENGTH,
                partitionLocation);
        // if remainingReviveTimes == 0 and revivePartition.isPresent(), there is no need to send
        // handshake again
        if (revivePartition.isPresent() && remainingReviveTimes > 0) {
          LOG.info(
              "Revive at handshake, currentTimes:{}, totalTimes:{} for shuffleId:{}, mapId:{}, "
                  + "attempId:{}, currentRegionIndex:{}, newPartition:{}, oldPartition:{}",
              remainingReviveTimes,
              maxReviveTimes,
              shuffleId,
              mapId,
              attemptId,
              currentRegionIndex,
              revivePartition,
              partitionLocation);
          partitionLocation = revivePartition.get();
          hasSentHandshake = false;
        } else {
          hasSentHandshake = true;
        }
      }
      if (remainingReviveTimes == 0 && !hasSentHandshake) {
        throw new RuntimeException(
            "After retry " + maxReviveTimes + " times, still failed to send handshake");
      }
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  private void releaseResources() {
    if (!isReleased) {
      isReleased = true;
    }
  }

  private void registerShuffle() {
    try {
      if (!hasRegisteredShuffle) {
        partitionLocation =
            flinkShuffleClient.registerMapPartitionTask(
                shuffleId, numPartitions, mapId, attemptId, partitionId, true);
        Utils.checkNotNull(partitionLocation);
        hasRegisteredShuffle = true;
      }
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  private void write(ByteBuf byteBuf, BufferHeader bufferHeader) {
    try {
      // create a composite buffer and write a header into it. This composite buffer will serve as
      // the result packed buffer.
      CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
      ByteBuf headerBuf = Unpooled.buffer(BufferUtils.HEADER_LENGTH);

      // write celeborn buffer header (subpartitionid(4) + attemptId(4) + nextBatchId(4) +
      // compressedsize)
      headerBuf.writeInt(bufferHeader.getSubPartitionId());
      headerBuf.writeInt(attemptId);
      headerBuf.writeInt(0);
      headerBuf.writeInt(
          byteBuf.readableBytes() + (BufferUtils.HEADER_LENGTH - BufferUtils.HEADER_LENGTH_PREFIX));

      // write flink buffer header (dataType(1) + isCompress(1) + size(4))
      headerBuf.writeByte(bufferHeader.getDataType().ordinal());
      headerBuf.writeBoolean(bufferHeader.isCompressed());
      headerBuf.writeInt(bufferHeader.getSize());

      // composite the headerBuf and data buffer together
      compositeByteBuf.addComponents(true, headerBuf, byteBuf);
      io.netty.buffer.ByteBuf wrappedBuffer =
          io.netty.buffer.Unpooled.wrappedBuffer(compositeByteBuf.nioBuffer());

      int numWritten =
          flinkShuffleClient.pushDataToLocation(
              shuffleId,
              mapId,
              attemptId,
              bufferHeader.getSubPartitionId(),
              wrappedBuffer,
              partitionLocation,
              compositeByteBuf::release);
      checkState(
          numWritten == byteBuf.readableBytes() + BufferUtils.HEADER_LENGTH, "Wrong written size.");
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  private void appendEndOfSegmentBuffer(int subPartitionId) {
    try {
      checkState(bufferPacker.isEmpty(), "BufferPacker is not empty");
      MemorySegment endSegmentMemorySegment =
          MemorySegmentFactory.wrap(
              EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE).array());
      Buffer endOfSegmentBuffer =
          new NetworkBuffer(
              endSegmentMemorySegment,
              FreeingBufferRecycler.INSTANCE,
              END_OF_SEGMENT,
              endSegmentMemorySegment.size());
      processBuffer(endOfSegmentBuffer, subPartitionId);
    } catch (Exception e) {
      ExceptionUtils.rethrow(e, "Failed to append end of segment event.");
    }
  }

  private void processBuffer(Buffer originBuffer, int subPartitionId) {
    try {
      regionStartOrFinish(subPartitionId);
      currentSubpartition = subPartitionId;

      Buffer buffer = originBuffer;
      if (originBuffer.isCompressed()) {
        // Flink hybrid shuffle will send a compressed buffer to tier. However, since we need to
        // write data to this buffer and the compressed buffer is read-only, we must create a
        // new Buffer object to the wrap origin buffer.
        NetworkBuffer networkBuffer =
            new NetworkBuffer(
                originBuffer.getMemorySegment(),
                originBuffer.getRecycler(),
                originBuffer.getDataType(),
                originBuffer.getSize());
        networkBuffer.writerIndex(originBuffer.asByteBuf().writerIndex());
        buffer = networkBuffer;
      }

      // set the buffer meta
      BufferUtils.setCompressedDataWithoutHeader(buffer, originBuffer);

      bufferPacker.process(buffer, subPartitionId);
    } catch (InterruptedException e) {
      originBuffer.recycleBuffer();
      ExceptionUtils.rethrow(e, "Failed to process buffer.");
    }
  }

  @VisibleForTesting
  FlinkShuffleClientImpl getShuffleClient() {
    try {
      return FlinkShuffleClientImpl.get(
          applicationId,
          lifecycleManagerHost,
          lifecycleManagerPort,
          lifecycleManagerTimestamp,
          celebornConf,
          null,
          bufferSizeBytes);
    } catch (DriverChangedException e) {
      // would generate a new attempt to retry output gate
      throw new RuntimeException(e.getMessage());
    }
  }
}
