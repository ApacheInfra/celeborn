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

package org.apache.celeborn.plugin.flink.readclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.Tuple2;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClientImpl;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.exception.DriverChangedException;
import org.apache.celeborn.common.exception.PartitionUnRetryAbleException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.PushData;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbChangeLocationPartitionInfo;
import org.apache.celeborn.common.protocol.PbChangeLocationResponse;
import org.apache.celeborn.common.protocol.PbPartitionLocation.Mode;
import org.apache.celeborn.common.protocol.PbPushDataHandShake;
import org.apache.celeborn.common.protocol.PbRegionFinish;
import org.apache.celeborn.common.protocol.PbRegionStart;
import org.apache.celeborn.common.protocol.PbSegmentStart;
import org.apache.celeborn.common.protocol.ReviveRequest;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.protocol.message.ControlMessages;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.util.CollectionUtils;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.PbSerDeUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.write.PushState;
import org.apache.celeborn.plugin.flink.network.FlinkTransportClientFactory;
import org.apache.celeborn.plugin.flink.network.ReadClientHandler;
import org.apache.celeborn.plugin.flink.network.TransportFrameDecoderWithBufferSupplier;

public class FlinkShuffleClientImpl extends ShuffleClientImpl {
  public static final Logger logger = LoggerFactory.getLogger(FlinkShuffleClientImpl.class);
  private static volatile FlinkShuffleClientImpl _instance;
  private static volatile boolean initialized = false;
  private FlinkTransportClientFactory flinkTransportClientFactory;
  private ReadClientHandler readClientHandler = new ReadClientHandler();
  private ConcurrentHashMap<String, TransportClient> currentClient =
      JavaUtils.newConcurrentHashMap();
  private long driverTimestamp;
  private final int BATCH_HEADER_SIZE = 4 * 4;

  private final TransportContext context;

  /** The buffer size bytes in flink, default value is 32KB. */
  private final int bufferSizeBytes;

  public static FlinkShuffleClientImpl get(
      String appUniqueId,
      String driverHost,
      int port,
      long driverTimestamp,
      CelebornConf conf,
      UserIdentifier userIdentifier)
      throws DriverChangedException {
    return get(
        appUniqueId,
        driverHost,
        port,
        driverTimestamp,
        conf,
        userIdentifier,
        TransportFrameDecoderWithBufferSupplier.DISABLE_LARGE_BUFFER_SPLIT_SIZE);
  }

  public static FlinkShuffleClientImpl get(
      String appUniqueId,
      String driverHost,
      int port,
      long driverTimestamp,
      CelebornConf conf,
      UserIdentifier userIdentifier,
      int bufferSizeBytes)
      throws DriverChangedException {
    if (null == _instance || !initialized || _instance.driverTimestamp < driverTimestamp) {
      synchronized (FlinkShuffleClientImpl.class) {
        if (null == _instance) {
          _instance =
              new FlinkShuffleClientImpl(
                  appUniqueId,
                  driverHost,
                  port,
                  driverTimestamp,
                  conf,
                  userIdentifier,
                  bufferSizeBytes);
          initialized = true;
        } else if (!initialized || _instance.driverTimestamp < driverTimestamp) {
          _instance.shutdown();
          _instance =
              new FlinkShuffleClientImpl(
                  appUniqueId,
                  driverHost,
                  port,
                  driverTimestamp,
                  conf,
                  userIdentifier,
                  bufferSizeBytes);
          initialized = true;
        }
      }
    }

    if (driverTimestamp < _instance.driverTimestamp) {
      String format = "Driver reinitialized or changed driverHost-port-driverTimestamp to %s-%s-%s";
      String message = String.format(format, driverHost, port, driverTimestamp);
      logger.warn(message);
      throw new DriverChangedException(message);
    }

    return _instance;
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (flinkTransportClientFactory != null) {
      flinkTransportClientFactory.close();
    }
    if (readClientHandler != null) {
      readClientHandler.close();
    }
  }

  public FlinkShuffleClientImpl(
      String appUniqueId,
      String driverHost,
      int port,
      long driverTimestamp,
      CelebornConf conf,
      UserIdentifier userIdentifier,
      int bufferSizeBytes) {
    super(appUniqueId, conf, userIdentifier);
    this.bufferSizeBytes = bufferSizeBytes;
    String module = TransportModuleConstants.DATA_MODULE;
    TransportConf dataTransportConf =
        Utils.fromCelebornConf(conf, module, conf.getInt("celeborn." + module + ".io.threads", 8));
    this.context =
        new TransportContext(
            dataTransportConf, readClientHandler, conf.clientCloseIdleConnections());
    this.setupLifecycleManagerRef(driverHost, port);
    this.driverTimestamp = driverTimestamp;
  }

  private void initializeTransportClientFactory() {
    if (null == flinkTransportClientFactory) {
      flinkTransportClientFactory =
          new FlinkTransportClientFactory(context, createBootstraps(), bufferSizeBytes);
    }
  }

  @Override
  public void setupLifecycleManagerRef(String host, int port) {
    super.setupLifecycleManagerRef(host, port);
    initializeTransportClientFactory();
  }

  @Override
  public void setupLifecycleManagerRef(RpcEndpointRef endpointRef) {
    super.setupLifecycleManagerRef(endpointRef);
    initializeTransportClientFactory();
  }

  public CelebornBufferStream readBufferedPartition(
      int shuffleId,
      int partitionId,
      int subPartitionIndexStart,
      int subPartitionIndexEnd,
      boolean isSegmentGranularityVisible)
      throws IOException {
    String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
    PartitionLocation[] partitionLocations =
        updateFileGroupAndGetLocations(shuffleId, partitionId, isSegmentGranularityVisible);
    if (partitionLocations.length == 0) {
      logger.error(
          "Shuffle data is empty for shuffle {} partitionId {} isSegmentGranularityVisible {}.",
          shuffleId,
          partitionId,
          isSegmentGranularityVisible);
      // TODO: in segment granularity visible senarios, when the downstream reduce tasks start early
      // than upstream map tasks, the shuffle
      // partition locations may be found empty, should retry until the upstream task started
      throw new PartitionUnRetryAbleException(
          String.format("Shuffle data lost for shuffle %d partition %d.", shuffleId, partitionId));
    } else {
      Arrays.sort(partitionLocations, Comparator.comparingInt(PartitionLocation::getEpoch));
      logger.debug(
          "readBufferedPartition shuffleKey:{} partitionid:{} partitionLocation:{}",
          shuffleKey,
          partitionId,
          partitionLocations);

      initializeTransportClientFactory();
      return CelebornBufferStream.create(
          this,
          flinkTransportClientFactory,
          shuffleKey,
          partitionLocations,
          subPartitionIndexStart,
          subPartitionIndexEnd,
          conf.pushDataTimeoutMs());
    }
  }

  /**
   * Update the reduce file groups and obtain the PartitionLocations of the target
   * shuffleId#partitionId. It is possible to return an empty array if the corresponding reduce file
   * groups are nonexistent, a scenario likely arising when downstream reduce tasks are start early
   * than upstream map tasks, e.g. Flink Hybrid Shuffle.
   */
  public PartitionLocation[] updateFileGroupAndGetLocations(
      int shuffleId, int partitionId, boolean isSegmentGranularityVisible) throws IOException {
    ReduceFileGroups fileGroups =
        updateFileGroup(shuffleId, partitionId, isSegmentGranularityVisible);
    if (CollectionUtils.isEmpty(fileGroups.partitionGroups)
        || !fileGroups.partitionGroups.containsKey(partitionId)) {
      return new PartitionLocation[0];
    } else {
      return fileGroups.partitionGroups.get(partitionId).toArray(new PartitionLocation[0]);
    }
  }

  @Override
  public ReduceFileGroups updateFileGroup(
      int shuffleId, int partitionId, boolean isSegmentGranularityVisible)
      throws CelebornIOException {
    ReduceFileGroups reduceFileGroups =
        reduceFileGroupsMap.computeIfAbsent(
                shuffleId, (id) -> Tuple2.apply(new ReduceFileGroups(), null))
            ._1;
    if (reduceFileGroups.partitionIds != null
        && reduceFileGroups.partitionIds.contains(partitionId)) {
      logger.debug(
          "use cached file groups for partition: {}", Utils.makeReducerKey(shuffleId, partitionId));
    } else {
      synchronized (reduceFileGroups) {
        if (reduceFileGroups.partitionIds != null
            && reduceFileGroups.partitionIds.contains(partitionId)) {
          logger.debug(
              "use cached file groups for partition: {}",
              Utils.makeReducerKey(shuffleId, partitionId));
        } else {
          // refresh file groups
          Tuple2<ReduceFileGroups, String> fileGroups =
              loadFileGroupInternal(shuffleId, isSegmentGranularityVisible);
          ReduceFileGroups newGroups = fileGroups._1;
          if (newGroups == null) {
            throw new CelebornIOException(
                loadFileGroupException(shuffleId, partitionId, fileGroups._2));
          } else if (!newGroups.partitionIds.contains(partitionId)) {
            throw new CelebornIOException(
                String.format(
                    "Shuffle data lost for shuffle %d partition %d.", shuffleId, partitionId));
          }
          reduceFileGroups.update(newGroups);
        }
      }
    }
    return reduceFileGroups;
  }

  public ReadClientHandler getReadClientHandler() {
    return readClientHandler;
  }

  public int pushDataToLocation(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      ByteBuf data,
      PartitionLocation location,
      Runnable closeCallBack)
      throws IOException {
    // mapKey
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);

    PushState pushState = getPushState(mapKey);

    // increment batchId
    final int nextBatchId = pushState.nextBatchId();
    int totalLength = data.readableBytes();
    data.markWriterIndex();
    data.writerIndex(0);
    data.writeInt(partitionId);
    data.writeInt(attemptId);
    data.writeInt(nextBatchId);
    data.writeInt(totalLength - BATCH_HEADER_SIZE);
    data.resetWriterIndex();
    logger.debug(
        "Do push data byteBuf size {} for app {} shuffle {} map {} attempt {} reduce {} batch {}.",
        totalLength,
        appUniqueId,
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        nextBatchId);
    // check limit
    limitMaxInFlight(mapKey, pushState, location.hostAndPushPort());

    // add inFlight requests
    pushState.addBatch(nextBatchId, location.hostAndPushPort());

    // build PushData request
    NettyManagedBuffer buffer = new NettyManagedBuffer(data);
    final String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
    PushData pushData = new PushData(PRIMARY_MODE, shuffleKey, location.getUniqueId(), buffer);

    // build callback
    RpcResponseCallback callback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer response) {
            pushState.removeBatch(nextBatchId, location.hostAndPushPort());
            logger.debug(
                "Push data byteBuf to {} success for shuffle {} map {} attemptId {} batch {}.",
                location.hostAndPushPort(),
                shuffleId,
                mapId,
                attemptId,
                nextBatchId);
          }

          @Override
          public void onFailure(Throwable e) {
            pushState.removeBatch(nextBatchId, location.hostAndPushPort());
            if (pushState.exception.get() != null) {
              return;
            }
            String errorMsg =
                String.format(
                    "Push data byteBuf to %s failed for shuffle %d map %d attempt %d batch %d.",
                    location.hostAndPushPort(), shuffleId, mapId, attemptId, nextBatchId);
            pushState.exception.compareAndSet(null, new CelebornIOException(errorMsg, e));
          }
        };
    // do push data
    try {
      TransportClient client = createClientWaitingInFlightRequest(location, mapKey, pushState);
      client.pushData(pushData, pushDataTimeout, callback, closeCallBack);
    } catch (Exception e) {
      logger.error(
          "Exception raised while pushing data byteBuf for shuffle {} map {} attempt {} partitionId {} batch {} location {}.",
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          nextBatchId,
          location,
          e);
      callback.onFailure(
          new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY, e));
    }
    return totalLength;
  }

  private TransportClient createClientWaitingInFlightRequest(
      PartitionLocation location, String mapKey, PushState pushState)
      throws IOException, InterruptedException {
    TransportClient client =
        dataClientFactory.createClient(
            location.getHost(), location.getPushPort(), location.getId());
    if (currentClient.get(mapKey) != client) {
      // make sure that messages have been sent by old client, in order to keep receiving data
      // orderly
      if (currentClient.get(mapKey) != null) {
        limitZeroInFlight(mapKey, pushState);
      }
      currentClient.put(mapKey, client);
    }
    return currentClient.get(mapKey);
  }

  public Optional<PartitionLocation> pushDataHandShake(
      int shuffleId,
      int mapId,
      int attemptId,
      int numPartitions,
      int bufferSize,
      PartitionLocation location)
      throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    final PushState pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState(conf));
    return retrySendMessage(
        () -> {
          String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
          logger.info(
              "PushDataHandShake shuffleKey {} attemptId {} locationId {}",
              shuffleKey,
              attemptId,
              location.getUniqueId());
          logger.debug("PushDataHandShake location {}", location);
          TransportClient client = createClientWaitingInFlightRequest(location, mapKey, pushState);
          ByteBuffer pushDataHandShakeResponse;
          try {
            pushDataHandShakeResponse =
                client.sendRpcSync(
                    new TransportMessage(
                            MessageType.PUSH_DATA_HAND_SHAKE,
                            PbPushDataHandShake.newBuilder()
                                .setMode(Mode.forNumber(PRIMARY_MODE))
                                .setShuffleKey(shuffleKey)
                                .setPartitionUniqueId(location.getUniqueId())
                                .setAttemptId(attemptId)
                                .setNumPartitions(numPartitions)
                                .setBufferSize(bufferSize)
                                .build()
                                .toByteArray())
                        .toByteBuffer(),
                    conf.pushDataTimeoutMs());
          } catch (IOException e) {
            // ioexeption revive
            return revive(shuffleId, mapId, attemptId, location);
          }
          if (pushDataHandShakeResponse.hasRemaining()
              && pushDataHandShakeResponse.get() == StatusCode.HARD_SPLIT.getValue()) {
            // if split then revive
            return revive(shuffleId, mapId, attemptId, location);
          }
          return Optional.empty();
        });
  }

  public Optional<PartitionLocation> regionStart(
      int shuffleId,
      int mapId,
      int attemptId,
      PartitionLocation location,
      int currentRegionIdx,
      boolean isBroadcast)
      throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    final PushState pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState(conf));
    return retrySendMessage(
        () -> {
          String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
          logger.info(
              "RegionStart for shuffle {} regionId {} attemptId {} locationId {}.",
              shuffleId,
              currentRegionIdx,
              attemptId,
              location.getUniqueId());
          logger.debug("RegionStart  for location {}.", location.toString());
          TransportClient client = createClientWaitingInFlightRequest(location, mapKey, pushState);
          ByteBuffer regionStartResponse;
          try {
            regionStartResponse =
                client.sendRpcSync(
                    new TransportMessage(
                            MessageType.REGION_START,
                            PbRegionStart.newBuilder()
                                .setMode(Mode.forNumber(PRIMARY_MODE))
                                .setShuffleKey(shuffleKey)
                                .setPartitionUniqueId(location.getUniqueId())
                                .setAttemptId(attemptId)
                                .setCurrentRegionIndex(currentRegionIdx)
                                .setIsBroadcast(isBroadcast)
                                .build()
                                .toByteArray())
                        .toByteBuffer(),
                    conf.pushDataTimeoutMs());
          } catch (IOException e) {
            // ioexeption revive
            return revive(shuffleId, mapId, attemptId, location);
          }

          if (regionStartResponse.hasRemaining()
              && regionStartResponse.get() == StatusCode.HARD_SPLIT.getValue()) {
            // if split then revive
            return revive(shuffleId, mapId, attemptId, location);
          }
          return Optional.empty();
        });
  }

  public Optional<PartitionLocation> revive(
      int shuffleId, int mapId, int attemptId, PartitionLocation location)
      throws CelebornIOException {
    Set<Integer> mapIds = new HashSet<>();
    mapIds.add(mapId);
    List<ReviveRequest> requests = new ArrayList<>();
    ReviveRequest req =
        new ReviveRequest(
            shuffleId,
            mapId,
            attemptId,
            location.getId(),
            location.getEpoch(),
            location,
            StatusCode.HARD_SPLIT);
    requests.add(req);
    PbChangeLocationResponse response =
        lifecycleManagerRef.askSync(
            ControlMessages.Revive$.MODULE$.apply(shuffleId, mapIds, requests),
            conf.clientRpcRequestPartitionLocationAskTimeout(),
            ClassTag$.MODULE$.apply(PbChangeLocationResponse.class));
    // per partitionKey only serve single PartitionLocation in Client Cache.
    PbChangeLocationPartitionInfo partitionInfo = response.getPartitionInfo(0);
    StatusCode respStatus = Utils.toStatusCode(partitionInfo.getStatus());
    if (StatusCode.SUCCESS.equals(respStatus)) {
      logger.debug("revive new partition:{}", partitionInfo.getPartition());
      return Optional.of(PbSerDeUtils.fromPbPartitionLocation(partitionInfo.getPartition()));
    } else {
      // throw exception
      logger.error(
          "Exception raised while reviving for shuffle {} map {} attemptId {} partition {} epoch {}.",
          shuffleId,
          mapId,
          attemptId,
          location.getId(),
          location.getEpoch());
      throw new CelebornIOException("RegionStart revive failed");
    }
  }

  public void regionFinish(int shuffleId, int mapId, int attemptId, PartitionLocation location)
      throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    final PushState pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState(conf));
    retrySendMessage(
        () -> {
          final String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
          logger.info(
              "RegionFinish for shuffle {} map {} attemptId {} locationId {}.",
              shuffleId,
              mapId,
              attemptId,
              location.getUniqueId());
          logger.debug("RegionFinish for location {}.", location);
          TransportClient client = createClientWaitingInFlightRequest(location, mapKey, pushState);
          client.sendRpcSync(
              new TransportMessage(
                      MessageType.REGION_FINISH,
                      PbRegionFinish.newBuilder()
                          .setMode(Mode.forNumber(PRIMARY_MODE))
                          .setShuffleKey(shuffleKey)
                          .setPartitionUniqueId(location.getUniqueId())
                          .setAttemptId(attemptId)
                          .build()
                          .toByteArray())
                  .toByteBuffer(),
              conf.pushDataTimeoutMs());
          return null;
        });
  }

  public void segmentStart(
      int shuffleId,
      int mapId,
      int attemptId,
      int subPartitionId,
      int segmentId,
      PartitionLocation location)
      throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    final PushState pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState(conf));
    retrySendMessage(
        () -> {
          final String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
          logger.debug(
              "SegmentStart for shuffle {} map {} attemptId {} locationId {} subpartitionId{} segmentId {}.",
              shuffleId,
              mapId,
              attemptId,
              location,
              subPartitionId,
              segmentId);
          TransportClient client = createClientWaitingInFlightRequest(location, mapKey, pushState);
          client.sendRpcSync(
              new TransportMessage(
                      MessageType.SEGMENT_START,
                      PbSegmentStart.newBuilder()
                          .setMode(Mode.forNumber(PRIMARY_MODE))
                          .setShuffleKey(shuffleKey)
                          .setPartitionUniqueId(location.getUniqueId())
                          .setAttemptId(attemptId)
                          .setSubPartitionId(subPartitionId)
                          .setSegmentId(segmentId)
                          .build()
                          .toByteArray())
                  .toByteBuffer(),
              conf.pushDataTimeoutMs());
          return null;
        });
  }

  @FunctionalInterface
  interface ThrowingExceptionSupplier<R, E extends Exception> {
    R get() throws E;
  }

  private <R> R retrySendMessage(ThrowingExceptionSupplier<R, Exception> supplier)
      throws IOException {

    int retryTimes = 0;
    boolean isSuccess = false;
    Exception currentException = null;
    R result = null;
    while (!Thread.currentThread().isInterrupted()
        && !isSuccess
        && retryTimes < conf.networkIoMaxRetries(TransportModuleConstants.PUSH_MODULE)) {
      logger.debug("RetrySendMessage  retry times {}.", retryTimes);
      try {
        result = supplier.get();
        isSuccess = true;
      } catch (Exception e) {
        currentException = e;
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        if (shouldRetry(e)) {
          retryTimes++;
          Uninterruptibles.sleepUninterruptibly(
              conf.networkIoRetryWaitMs(TransportModuleConstants.PUSH_MODULE),
              TimeUnit.MILLISECONDS);
        } else {
          break;
        }
      }
    }
    if (!isSuccess) {
      if (currentException instanceof IOException) {
        throw (IOException) currentException;
      } else {
        throw new CelebornIOException(currentException.getMessage(), currentException);
      }
    }
    return result;
  }

  private boolean shouldRetry(Throwable e) {
    boolean isIOException =
        e instanceof IOException
            || e instanceof TimeoutException
            || (e.getCause() != null && e.getCause() instanceof TimeoutException)
            || (e.getCause() != null && e.getCause() instanceof IOException)
            || (e instanceof RuntimeException
                && e.getMessage() != null
                && e.getMessage().startsWith(IOException.class.getName()));
    return isIOException;
  }

  @Override
  public void cleanup(int shuffleId, int mapId, int attemptId) {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    super.cleanup(shuffleId, mapId, attemptId);
    if (currentClient != null) {
      currentClient.remove(mapKey);
    }
  }

  public void setDataClientFactory(TransportClientFactory dataClientFactory) {
    this.dataClientFactory = dataClientFactory;
  }

  @Override
  @VisibleForTesting
  public TransportClientFactory getDataClientFactory() {
    initializeTransportClientFactory();
    return flinkTransportClientFactory;
  }
}
