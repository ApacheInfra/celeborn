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

package com.aliyun.emr.rss.common.protocol;

import java.io.Serializable;

import lombok.Builder;

import com.aliyun.emr.rss.common.meta.WorkerInfo;
@Builder
public class PartitionLocation implements Serializable {
  public enum Mode {
    Master(0), Slave(1);

    private final byte mode;

    Mode(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.mode = (byte) id;
    }

    public byte mode() { return mode; }
  }

  public enum StorageHint {
    NON_EXISTS, MEMORY, SSD, HDD, HDFS, OSS
  }

  public enum Type {
    REDUCE_PARTITION, MAP_PARTITION, MAPGROUP_REDUCE_PARTITION
  }

  public static PartitionLocation.Mode getMode(byte mode) {
    if (mode == 0) {
      return Mode.Master;
    } else {
      return Mode.Slave;
    }
  }

  public static String UNDEFINED_DISK = "UNDEFINED_DISK";

  private int reduceId;
  private int epoch;
  private String host;
  private int rpcPort;
  private int pushPort;
  private int fetchPort;
  private int replicatePort;
  private Mode mode;
  private PartitionLocation peer;
  private StorageHint storageHint;
  private String diskHint;

  public PartitionLocation(PartitionLocation loc) {
    this.reduceId = loc.reduceId;
    this.epoch = loc.epoch;
    this.host = loc.host;
    this.rpcPort = loc.rpcPort;
    this.pushPort = loc.pushPort;
    this.fetchPort = loc.fetchPort;
    this.replicatePort = loc.replicatePort;
    this.mode = loc.mode;
    this.peer = loc.peer;
    this.storageHint = loc.storageHint;
    this.diskHint = loc.diskHint;
  }

  public PartitionLocation(
    int reduceId,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode,
    String diskHint) {
    this(reduceId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort,
      mode, null, StorageHint.MEMORY, diskHint);
  }

  public PartitionLocation(
    int reduceId,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode,
    PartitionLocation peer) {
    this(reduceId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer,
      StorageHint.MEMORY, null);
  }

  public PartitionLocation(
    int reduceId,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode) {
    this(reduceId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, null,
      StorageHint.MEMORY, null);
  }

  public PartitionLocation(
    int reduceId,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode,
    PartitionLocation peer,
    StorageHint hint,
    String diskHint) {
    this.reduceId = reduceId;
    this.epoch = epoch;
    this.host = host;
    this.rpcPort = rpcPort;
    this.pushPort = pushPort;
    this.fetchPort = fetchPort;
    this.replicatePort = replicatePort;
    this.mode = mode;
    this.peer = peer;
    this.storageHint = hint;
    this.diskHint = diskHint;
  }

  public int getReduceId()
  {
    return reduceId;
  }

  public void setReduceId(int reduceId)
  {
    this.reduceId = reduceId;
  }

  public int getEpoch() {
    return epoch;
  }

  public void setEpoch(int epoch) {
    this.epoch = epoch;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPushPort() {
    return pushPort;
  }

  public void setPushPort(int pushPort) {
    this.pushPort = pushPort;
  }

  public int getFetchPort() {
    return fetchPort;
  }

  public void setFetchPort(int fetchPort) {
    this.fetchPort = fetchPort;
  }

  public String hostAndPorts() {
    return host + ":" + rpcPort + ":" + pushPort + ":" + fetchPort;
  }

  public String hostAndPushPort() {
    return host+":"+pushPort;
  }

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public PartitionLocation getPeer() {
    return peer;
  }

  public void setPeer(PartitionLocation peer) {
    this.peer = peer;
  }

  public String getUniqueId() {
    return reduceId + "-" + epoch;
  }

  public String getFileName() {
    return reduceId + "-" + epoch + "-" + mode.mode;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public int getReplicatePort() {
    return replicatePort;
  }

  public void setReplicatePort(int replicatePort) {
    this.replicatePort = replicatePort;
  }

  public StorageHint getStorageHint() {
    return storageHint;
  }

  public void setStorageHint(StorageHint storageHint) {
    this.storageHint = storageHint;
  }

  public String getDiskHint() {
    if (diskHint == null || diskHint.length() == 0) {
      return UNDEFINED_DISK;
    }
    return diskHint;
  }

  public void setDiskHint(String diskHint) {
    this.diskHint = diskHint;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionLocation)) {
      return false;
    }
    PartitionLocation o = (PartitionLocation) other;
    return reduceId == o.reduceId
        && epoch == o.epoch
        && host.equals(o.host)
        && rpcPort == o.rpcPort
        && pushPort == o.pushPort
        && fetchPort == o.fetchPort;
  }

  @Override
  public int hashCode() {
    return (reduceId + epoch + host + rpcPort + pushPort + fetchPort).hashCode();
  }

  @Override
  public String toString() {
    String peerAddress = "";
    if (peer != null) {
      peerAddress = peer.hostAndPorts();
    }

    return "PartitionLocation[" + reduceId + "-" + epoch + " " + host + ":" + rpcPort + ":" +
             pushPort + ":" + fetchPort + ":" + replicatePort + " Mode: " + mode +
             " peer: " + peerAddress + " storage hint:" + storageHint + "]";
  }

  public WorkerInfo getWorker() {
    return new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
  }

  public static PartitionLocation fromPbPartitionLocation(TransportMessages.PbPartitionLocation
                                                            pbPartitionLocation) {
    Mode mode = Mode.Master;
    if (pbPartitionLocation.getMode() == TransportMessages.PbPartitionLocation.Mode.Slave) {
      mode = Mode.Slave;
    }

    PartitionLocation partitionLocation = PartitionLocation.builder()
                                            .reduceId(pbPartitionLocation.getReduceId())
                                            .epoch(pbPartitionLocation.getEpoch())
                                            .host(pbPartitionLocation.getHost())
                                            .rpcPort(pbPartitionLocation.getRpcPort())
                                            .pushPort(pbPartitionLocation.getPushPort())
                                            .fetchPort(pbPartitionLocation.getFetchPort())
                                            .replicatePort(pbPartitionLocation.getReplicatePort())
                                            .mode(mode)
                                            .storageHint(
                                              PartitionLocation.StorageHint.
                                                values()[
                                                  pbPartitionLocation.getStorageHintOrdinal()])
                                            .diskHint(pbPartitionLocation.getDiskHint())
                                            .build();

    if (pbPartitionLocation.hasPeer()) {
      TransportMessages.PbPartitionLocation peerPb = pbPartitionLocation.getPeer();
      Mode peerMode = Mode.Master;
      if (peerPb.getMode() == TransportMessages.PbPartitionLocation.Mode.Slave) {
        peerMode = Mode.Slave;
      }
      PartitionLocation peerLocation = PartitionLocation.builder()
                                         .reduceId(peerPb.getReduceId())
                                         .epoch(peerPb.getEpoch())
                                         .host(peerPb.getHost())
                                         .rpcPort(peerPb.getRpcPort())
                                         .pushPort(peerPb.getPushPort())
                                         .fetchPort(peerPb.getFetchPort())
                                         .replicatePort(peerPb.getReplicatePort())
                                         .mode(peerMode)
                                         .storageHint(
                                           PartitionLocation.StorageHint.
                                             values()[
                                             peerPb.getStorageHintOrdinal()])
                                         .diskHint(peerPb.getDiskHint())
                                         .build();
      partitionLocation.setPeer(peerLocation);
    }

    return partitionLocation;
  }

  public static TransportMessages.PbPartitionLocation toPbPartitionLocation(PartitionLocation
                                                                              partitionLocation) {
    TransportMessages.PbPartitionLocation.Builder pbPartitionLocationBuilder = TransportMessages
                                                                                 .PbPartitionLocation.newBuilder();
    if (partitionLocation.mode == Mode.Master) {
      pbPartitionLocationBuilder.setMode(TransportMessages.PbPartitionLocation.Mode.Master);
    } else {
      pbPartitionLocationBuilder.setMode(TransportMessages.PbPartitionLocation.Mode.Slave);
    }
    pbPartitionLocationBuilder.setHost(partitionLocation.getHost())
      .setEpoch(partitionLocation.getEpoch())
      .setReduceId(partitionLocation.getReduceId())
      .setRpcPort(partitionLocation.getRpcPort())
      .setPushPort(partitionLocation.getPushPort())
      .setFetchPort(partitionLocation.getFetchPort())
      .setReplicatePort(partitionLocation.getReplicatePort())
      .setStorageHintOrdinal(partitionLocation.getStorageHint().ordinal())
      .setDiskHint(partitionLocation.getDiskHint());

    if (partitionLocation.getPeer() != null) {
      TransportMessages.PbPartitionLocation.Builder peerPbPartitionLocationBuilder =
        TransportMessages.PbPartitionLocation.newBuilder();
      if (partitionLocation.getPeer().mode == Mode.Master) {
        peerPbPartitionLocationBuilder.setMode(TransportMessages.PbPartitionLocation.Mode.Master);
      } else {
        peerPbPartitionLocationBuilder.setMode(TransportMessages.PbPartitionLocation.Mode.Slave);
      }
      peerPbPartitionLocationBuilder.setHost(partitionLocation.getPeer().getHost())
        .setEpoch(partitionLocation.getPeer().getEpoch())
        .setReduceId(partitionLocation.getPeer().getReduceId())
        .setRpcPort(partitionLocation.getPeer().getRpcPort())
        .setPushPort(partitionLocation.getPeer().getPushPort())
        .setFetchPort(partitionLocation.getPeer().getFetchPort())
        .setReplicatePort(
          partitionLocation.getPeer().getReplicatePort())
        .setStorageHintOrdinal(
          partitionLocation.getPeer().getStorageHint().ordinal())
        .setDiskHint(partitionLocation.getPeer().getDiskHint());
      pbPartitionLocationBuilder.setPeer(peerPbPartitionLocationBuilder.build());
    }

    return pbPartitionLocationBuilder.build();
  }
}
