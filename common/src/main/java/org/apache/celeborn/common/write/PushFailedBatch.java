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

package org.apache.celeborn.common.write;

import java.io.Serializable;

import com.google.common.base.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class PushFailedBatch implements Serializable {

  private int mapId;
  private int attemptId;
  private int batchId;

  public PushFailedBatch(int mapId, int attemptId, int batchId) {
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.batchId = batchId;
  }

  public int getMapId() {
    return mapId;
  }

  public void setMapId(int mapId) {
    this.mapId = mapId;
  }

  public int getAttemptId() {
    return attemptId;
  }

  public void setAttemptId(int attemptId) {
    this.attemptId = attemptId;
  }

  public int getBatchId() {
    return batchId;
  }

  public void setBatchId(int batchId) {
    this.batchId = batchId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PushFailedBatch)) {
      return false;
    }
    PushFailedBatch o = (PushFailedBatch) other;
    return mapId == o.mapId && attemptId == o.attemptId && batchId == o.batchId;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mapId, attemptId, batchId);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("mapId", mapId)
        .append("attemptId", attemptId)
        .append("batchId", batchId)
        .toString();
  }
}
