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


package org.apache.celeborn.rest.v1.model;

import java.util.Objects;
import java.util.Arrays;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * WorkerInfoResponse
 */
@JsonPropertyOrder({
  WorkerInfoResponse.JSON_PROPERTY_HOST,
  WorkerInfoResponse.JSON_PROPERTY_RPC_PORT,
  WorkerInfoResponse.JSON_PROPERTY_PUSH_PORT,
  WorkerInfoResponse.JSON_PROPERTY_FETCH_PORT,
  WorkerInfoResponse.JSON_PROPERTY_REPLICATE_PORT,
  WorkerInfoResponse.JSON_PROPERTY_INTERNAL_PORT,
  WorkerInfoResponse.JSON_PROPERTY_SLOT_USED,
  WorkerInfoResponse.JSON_PROPERTY_LAST_HEARTBEAT_TIMESTAMP,
  WorkerInfoResponse.JSON_PROPERTY_HEARTBEAT_ELAPSED_SECONDS,
  WorkerInfoResponse.JSON_PROPERTY_DISK_INFOS,
  WorkerInfoResponse.JSON_PROPERTY_RESOURCE_CONSUMPTION,
  WorkerInfoResponse.JSON_PROPERTY_WORKER_REF,
  WorkerInfoResponse.JSON_PROPERTY_WORKER_STATE,
  WorkerInfoResponse.JSON_PROPERTY_WORKER_STATE_START_TIME,
  WorkerInfoResponse.JSON_PROPERTY_IS_REGISTERED,
  WorkerInfoResponse.JSON_PROPERTY_IS_SHUTDOWN,
  WorkerInfoResponse.JSON_PROPERTY_IS_DECOMMISSIONING,
  WorkerInfoResponse.JSON_PROPERTY_VERSION,
  WorkerInfoResponse.JSON_PROPERTY_COMPILE_INFO
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class WorkerInfoResponse {
  public static final String JSON_PROPERTY_HOST = "host";
  private String host;

  public static final String JSON_PROPERTY_RPC_PORT = "rpcPort";
  private Integer rpcPort;

  public static final String JSON_PROPERTY_PUSH_PORT = "pushPort";
  private Integer pushPort;

  public static final String JSON_PROPERTY_FETCH_PORT = "fetchPort";
  private Integer fetchPort;

  public static final String JSON_PROPERTY_REPLICATE_PORT = "replicatePort";
  private Integer replicatePort;

  public static final String JSON_PROPERTY_INTERNAL_PORT = "internalPort";
  private Integer internalPort;

  public static final String JSON_PROPERTY_SLOT_USED = "slotUsed";
  private Long slotUsed;

  public static final String JSON_PROPERTY_LAST_HEARTBEAT_TIMESTAMP = "lastHeartbeatTimestamp";
  private Long lastHeartbeatTimestamp;

  public static final String JSON_PROPERTY_HEARTBEAT_ELAPSED_SECONDS = "heartbeatElapsedSeconds";
  private Long heartbeatElapsedSeconds;

  public static final String JSON_PROPERTY_DISK_INFOS = "diskInfos";
  private Map<String, String> diskInfos = new HashMap<>();

  public static final String JSON_PROPERTY_RESOURCE_CONSUMPTION = "resourceConsumption";
  private Map<String, String> resourceConsumption = new HashMap<>();

  public static final String JSON_PROPERTY_WORKER_REF = "workerRef";
  private String workerRef;

  public static final String JSON_PROPERTY_WORKER_STATE = "workerState";
  private String workerState;

  public static final String JSON_PROPERTY_WORKER_STATE_START_TIME = "workerStateStartTime";
  private Long workerStateStartTime;

  public static final String JSON_PROPERTY_IS_REGISTERED = "isRegistered";
  private Boolean isRegistered;

  public static final String JSON_PROPERTY_IS_SHUTDOWN = "isShutdown";
  private Boolean isShutdown;

  public static final String JSON_PROPERTY_IS_DECOMMISSIONING = "isDecommissioning";
  private Boolean isDecommissioning;

  public static final String JSON_PROPERTY_VERSION = "version";
  private String version;

  public static final String JSON_PROPERTY_COMPILE_INFO = "compileInfo";
  private String compileInfo;

  public WorkerInfoResponse() {
  }

  public WorkerInfoResponse host(String host) {
    
    this.host = host;
    return this;
  }

  /**
   * The host of the worker.
   * @return host
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_HOST)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getHost() {
    return host;
  }


  @JsonProperty(JSON_PROPERTY_HOST)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setHost(String host) {
    this.host = host;
  }

  public WorkerInfoResponse rpcPort(Integer rpcPort) {
    
    this.rpcPort = rpcPort;
    return this;
  }

  /**
   * The rpc port of the worker.
   * @return rpcPort
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_RPC_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getRpcPort() {
    return rpcPort;
  }


  @JsonProperty(JSON_PROPERTY_RPC_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setRpcPort(Integer rpcPort) {
    this.rpcPort = rpcPort;
  }

  public WorkerInfoResponse pushPort(Integer pushPort) {
    
    this.pushPort = pushPort;
    return this;
  }

  /**
   * The push port of the worker.
   * @return pushPort
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_PUSH_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getPushPort() {
    return pushPort;
  }


  @JsonProperty(JSON_PROPERTY_PUSH_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setPushPort(Integer pushPort) {
    this.pushPort = pushPort;
  }

  public WorkerInfoResponse fetchPort(Integer fetchPort) {
    
    this.fetchPort = fetchPort;
    return this;
  }

  /**
   * The fetch port of the worker.
   * @return fetchPort
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_FETCH_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getFetchPort() {
    return fetchPort;
  }


  @JsonProperty(JSON_PROPERTY_FETCH_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setFetchPort(Integer fetchPort) {
    this.fetchPort = fetchPort;
  }

  public WorkerInfoResponse replicatePort(Integer replicatePort) {
    
    this.replicatePort = replicatePort;
    return this;
  }

  /**
   * The replicate port of the worker.
   * @return replicatePort
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_REPLICATE_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getReplicatePort() {
    return replicatePort;
  }


  @JsonProperty(JSON_PROPERTY_REPLICATE_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setReplicatePort(Integer replicatePort) {
    this.replicatePort = replicatePort;
  }

  public WorkerInfoResponse internalPort(Integer internalPort) {
    
    this.internalPort = internalPort;
    return this;
  }

  /**
   * The internal port of the worker.
   * @return internalPort
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_INTERNAL_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getInternalPort() {
    return internalPort;
  }


  @JsonProperty(JSON_PROPERTY_INTERNAL_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setInternalPort(Integer internalPort) {
    this.internalPort = internalPort;
  }

  public WorkerInfoResponse slotUsed(Long slotUsed) {
    
    this.slotUsed = slotUsed;
    return this;
  }

  /**
   * The slot used of the worker.
   * @return slotUsed
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_SLOT_USED)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getSlotUsed() {
    return slotUsed;
  }


  @JsonProperty(JSON_PROPERTY_SLOT_USED)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setSlotUsed(Long slotUsed) {
    this.slotUsed = slotUsed;
  }

  public WorkerInfoResponse lastHeartbeatTimestamp(Long lastHeartbeatTimestamp) {
    
    this.lastHeartbeatTimestamp = lastHeartbeatTimestamp;
    return this;
  }

  /**
   * The last heartbeat timestamp of the worker.
   * @return lastHeartbeatTimestamp
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_LAST_HEARTBEAT_TIMESTAMP)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getLastHeartbeatTimestamp() {
    return lastHeartbeatTimestamp;
  }


  @JsonProperty(JSON_PROPERTY_LAST_HEARTBEAT_TIMESTAMP)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLastHeartbeatTimestamp(Long lastHeartbeatTimestamp) {
    this.lastHeartbeatTimestamp = lastHeartbeatTimestamp;
  }

  public WorkerInfoResponse heartbeatElapsedSeconds(Long heartbeatElapsedSeconds) {
    
    this.heartbeatElapsedSeconds = heartbeatElapsedSeconds;
    return this;
  }

  /**
   * The elapsed seconds since the last heartbeat of the worker.
   * @return heartbeatElapsedSeconds
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_HEARTBEAT_ELAPSED_SECONDS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getHeartbeatElapsedSeconds() {
    return heartbeatElapsedSeconds;
  }


  @JsonProperty(JSON_PROPERTY_HEARTBEAT_ELAPSED_SECONDS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setHeartbeatElapsedSeconds(Long heartbeatElapsedSeconds) {
    this.heartbeatElapsedSeconds = heartbeatElapsedSeconds;
  }

  public WorkerInfoResponse diskInfos(Map<String, String> diskInfos) {
    
    this.diskInfos = diskInfos;
    return this;
  }

  public WorkerInfoResponse putDiskInfosItem(String key, String diskInfosItem) {
    if (this.diskInfos == null) {
      this.diskInfos = new HashMap<>();
    }
    this.diskInfos.put(key, diskInfosItem);
    return this;
  }

  /**
   * A map of disk name and disk info.
   * @return diskInfos
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_DISK_INFOS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Map<String, String> getDiskInfos() {
    return diskInfos;
  }


  @JsonProperty(JSON_PROPERTY_DISK_INFOS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setDiskInfos(Map<String, String> diskInfos) {
    this.diskInfos = diskInfos;
  }

  public WorkerInfoResponse resourceConsumption(Map<String, String> resourceConsumption) {
    
    this.resourceConsumption = resourceConsumption;
    return this;
  }

  public WorkerInfoResponse putResourceConsumptionItem(String key, String resourceConsumptionItem) {
    if (this.resourceConsumption == null) {
      this.resourceConsumption = new HashMap<>();
    }
    this.resourceConsumption.put(key, resourceConsumptionItem);
    return this;
  }

  /**
   * A map of identifier and resource consumption.
   * @return resourceConsumption
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_RESOURCE_CONSUMPTION)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Map<String, String> getResourceConsumption() {
    return resourceConsumption;
  }


  @JsonProperty(JSON_PROPERTY_RESOURCE_CONSUMPTION)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setResourceConsumption(Map<String, String> resourceConsumption) {
    this.resourceConsumption = resourceConsumption;
  }

  public WorkerInfoResponse workerRef(String workerRef) {
    
    this.workerRef = workerRef;
    return this;
  }

  /**
   * The reference of the worker.
   * @return workerRef
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_WORKER_REF)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getWorkerRef() {
    return workerRef;
  }


  @JsonProperty(JSON_PROPERTY_WORKER_REF)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setWorkerRef(String workerRef) {
    this.workerRef = workerRef;
  }

  public WorkerInfoResponse workerState(String workerState) {
    
    this.workerState = workerState;
    return this;
  }

  /**
   * The state of the worker.
   * @return workerState
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_WORKER_STATE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getWorkerState() {
    return workerState;
  }


  @JsonProperty(JSON_PROPERTY_WORKER_STATE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setWorkerState(String workerState) {
    this.workerState = workerState;
  }

  public WorkerInfoResponse workerStateStartTime(Long workerStateStartTime) {
    
    this.workerStateStartTime = workerStateStartTime;
    return this;
  }

  /**
   * The start time of the worker state.
   * @return workerStateStartTime
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_WORKER_STATE_START_TIME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getWorkerStateStartTime() {
    return workerStateStartTime;
  }


  @JsonProperty(JSON_PROPERTY_WORKER_STATE_START_TIME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setWorkerStateStartTime(Long workerStateStartTime) {
    this.workerStateStartTime = workerStateStartTime;
  }

  public WorkerInfoResponse isRegistered(Boolean isRegistered) {
    
    this.isRegistered = isRegistered;
    return this;
  }

  /**
   * The registration status of the worker.
   * @return isRegistered
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_IS_REGISTERED)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Boolean getIsRegistered() {
    return isRegistered;
  }


  @JsonProperty(JSON_PROPERTY_IS_REGISTERED)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setIsRegistered(Boolean isRegistered) {
    this.isRegistered = isRegistered;
  }

  public WorkerInfoResponse isShutdown(Boolean isShutdown) {
    
    this.isShutdown = isShutdown;
    return this;
  }

  /**
   * The shutdown status of the worker.
   * @return isShutdown
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_IS_SHUTDOWN)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Boolean getIsShutdown() {
    return isShutdown;
  }


  @JsonProperty(JSON_PROPERTY_IS_SHUTDOWN)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setIsShutdown(Boolean isShutdown) {
    this.isShutdown = isShutdown;
  }

  public WorkerInfoResponse isDecommissioning(Boolean isDecommissioning) {
    
    this.isDecommissioning = isDecommissioning;
    return this;
  }

  /**
   * The decommission status of the worker.
   * @return isDecommissioning
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_IS_DECOMMISSIONING)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Boolean getIsDecommissioning() {
    return isDecommissioning;
  }


  @JsonProperty(JSON_PROPERTY_IS_DECOMMISSIONING)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setIsDecommissioning(Boolean isDecommissioning) {
    this.isDecommissioning = isDecommissioning;
  }

  public WorkerInfoResponse version(String version) {
    
    this.version = version;
    return this;
  }

  /**
   * The version of the worker.
   * @return version
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_VERSION)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getVersion() {
    return version;
  }


  @JsonProperty(JSON_PROPERTY_VERSION)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setVersion(String version) {
    this.version = version;
  }

  public WorkerInfoResponse compileInfo(String compileInfo) {
    
    this.compileInfo = compileInfo;
    return this;
  }

  /**
   * The compile info of the worker.
   * @return compileInfo
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_COMPILE_INFO)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getCompileInfo() {
    return compileInfo;
  }


  @JsonProperty(JSON_PROPERTY_COMPILE_INFO)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setCompileInfo(String compileInfo) {
    this.compileInfo = compileInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerInfoResponse workerInfoResponse = (WorkerInfoResponse) o;
    return Objects.equals(this.host, workerInfoResponse.host) &&
        Objects.equals(this.rpcPort, workerInfoResponse.rpcPort) &&
        Objects.equals(this.pushPort, workerInfoResponse.pushPort) &&
        Objects.equals(this.fetchPort, workerInfoResponse.fetchPort) &&
        Objects.equals(this.replicatePort, workerInfoResponse.replicatePort) &&
        Objects.equals(this.internalPort, workerInfoResponse.internalPort) &&
        Objects.equals(this.slotUsed, workerInfoResponse.slotUsed) &&
        Objects.equals(this.lastHeartbeatTimestamp, workerInfoResponse.lastHeartbeatTimestamp) &&
        Objects.equals(this.heartbeatElapsedSeconds, workerInfoResponse.heartbeatElapsedSeconds) &&
        Objects.equals(this.diskInfos, workerInfoResponse.diskInfos) &&
        Objects.equals(this.resourceConsumption, workerInfoResponse.resourceConsumption) &&
        Objects.equals(this.workerRef, workerInfoResponse.workerRef) &&
        Objects.equals(this.workerState, workerInfoResponse.workerState) &&
        Objects.equals(this.workerStateStartTime, workerInfoResponse.workerStateStartTime) &&
        Objects.equals(this.isRegistered, workerInfoResponse.isRegistered) &&
        Objects.equals(this.isShutdown, workerInfoResponse.isShutdown) &&
        Objects.equals(this.isDecommissioning, workerInfoResponse.isDecommissioning) &&
        Objects.equals(this.version, workerInfoResponse.version) &&
        Objects.equals(this.compileInfo, workerInfoResponse.compileInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, rpcPort, pushPort, fetchPort, replicatePort, internalPort, slotUsed, lastHeartbeatTimestamp, heartbeatElapsedSeconds, diskInfos, resourceConsumption, workerRef, workerState, workerStateStartTime, isRegistered, isShutdown, isDecommissioning, version, compileInfo);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkerInfoResponse {\n");
    sb.append("    host: ").append(toIndentedString(host)).append("\n");
    sb.append("    rpcPort: ").append(toIndentedString(rpcPort)).append("\n");
    sb.append("    pushPort: ").append(toIndentedString(pushPort)).append("\n");
    sb.append("    fetchPort: ").append(toIndentedString(fetchPort)).append("\n");
    sb.append("    replicatePort: ").append(toIndentedString(replicatePort)).append("\n");
    sb.append("    internalPort: ").append(toIndentedString(internalPort)).append("\n");
    sb.append("    slotUsed: ").append(toIndentedString(slotUsed)).append("\n");
    sb.append("    lastHeartbeatTimestamp: ").append(toIndentedString(lastHeartbeatTimestamp)).append("\n");
    sb.append("    heartbeatElapsedSeconds: ").append(toIndentedString(heartbeatElapsedSeconds)).append("\n");
    sb.append("    diskInfos: ").append(toIndentedString(diskInfos)).append("\n");
    sb.append("    resourceConsumption: ").append(toIndentedString(resourceConsumption)).append("\n");
    sb.append("    workerRef: ").append(toIndentedString(workerRef)).append("\n");
    sb.append("    workerState: ").append(toIndentedString(workerState)).append("\n");
    sb.append("    workerStateStartTime: ").append(toIndentedString(workerStateStartTime)).append("\n");
    sb.append("    isRegistered: ").append(toIndentedString(isRegistered)).append("\n");
    sb.append("    isShutdown: ").append(toIndentedString(isShutdown)).append("\n");
    sb.append("    isDecommissioning: ").append(toIndentedString(isDecommissioning)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    compileInfo: ").append(toIndentedString(compileInfo)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

