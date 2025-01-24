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

package org.apache.celeborn.common.protocol;

import java.io.Serializable;
import java.util.*;

public class StorageInfo implements Serializable {
  public enum Type {
    MEMORY(0),
    HDD(1),
    SSD(2),
    HDFS(3),
    OSS(4),
    S3(5);

    private final int value;

    Type(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  public static final Map<Integer, Type> typesMap = new HashMap<>();
  public static final Set<String> typeNames = new HashSet<>();
  public static final Map<String, Type> types = new HashMap<>();

  static {
    for (Type type : Type.values()) {
      typesMap.put(type.value, type);
      typeNames.add(type.name());
      types.put(type.name(), type);
    }
  }

  public static final int MEMORY_MASK = 0b1;
  public static final int LOCAL_DISK_MASK = 0b10;
  public static final int HDFS_MASK = 0b100;
  public static final int OSS_MASK = 0b1000;
  public static final int S3_MASK = 0b10000;
  public static final int ALL_TYPES_AVAILABLE_MASK = 0;

  // Default storage Type is MEMORY.
  private Type type = Type.MEMORY;
  private String mountPoint = "";
  // if a file is committed, field "finalResult" will be true
  private boolean finalResult = false;
  private String filePath;

  public int availableStorageTypes = 0;

  public long fileSize;

  public List<Long> chunkOffsets;

  public StorageInfo() {}

  public StorageInfo(Type type, boolean isFinal, String filePath) {
    this.type = type;
    this.finalResult = isFinal;
    this.filePath = filePath;
  }

  public StorageInfo(String mountPoint, StorageInfo.Type type, int availableStorageTypes) {
    this.mountPoint = mountPoint;
    this.type = type;
    this.availableStorageTypes = availableStorageTypes;
  }

  public StorageInfo(
      Type type,
      String mountPoint,
      boolean finalResult,
      String filePath,
      int availableStorageTypes) {
    this.type = type;
    this.mountPoint = mountPoint;
    this.finalResult = finalResult;
    this.filePath = filePath;
    this.availableStorageTypes = availableStorageTypes;
  }

  public StorageInfo(
      Type type,
      String mountPoint,
      boolean finalResult,
      String filePath,
      int availableStorageTypes,
      long fileSize,
      List<Long> chunkOffsets) {
    this.type = type;
    this.mountPoint = mountPoint;
    this.finalResult = finalResult;
    this.filePath = filePath;
    this.availableStorageTypes = availableStorageTypes;
    this.fileSize = fileSize;
    this.chunkOffsets = chunkOffsets;
  }

  public boolean isFinalResult() {
    return finalResult;
  }

  public void setFinalResult(boolean finalResult) {
    this.finalResult = finalResult;
  }

  public String getMountPoint() {
    return mountPoint;
  }

  public void setMountPoint(String mountPoint) {
    this.mountPoint = mountPoint;
  }

  public Type getType() {
    return type;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setChunkOffsets(List<Long> chunkOffsets) {
    this.chunkOffsets = chunkOffsets;
  }

  public List<Long> getChunkOffsets() {
    return this.chunkOffsets;
  }

  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
  }

  public long getFileSize() {
    return fileSize;
  }

  @Override
  public String toString() {
    return "StorageInfo{"
        + "type="
        + type
        + ", mountPoint='"
        + mountPoint
        + '\''
        + ", finalResult="
        + finalResult
        + ", filePath="
        + filePath
        + ", fileSize="
        + fileSize
        + ", chunkOffsets="
        + chunkOffsets
        + '}';
  }

  public boolean memoryAvailable() {
    return availableStorageTypes == ALL_TYPES_AVAILABLE_MASK
        || (availableStorageTypes & MEMORY_MASK) > 0;
  }

  public static boolean localDiskAvailable(int availableStorageTypes) {
    return availableStorageTypes == ALL_TYPES_AVAILABLE_MASK
        || (availableStorageTypes & LOCAL_DISK_MASK) > 0;
  }

  public boolean localDiskAvailable() {
    return StorageInfo.localDiskAvailable(availableStorageTypes);
  }

  public static boolean HDFSAvailable(int availableStorageTypes) {
    return availableStorageTypes == ALL_TYPES_AVAILABLE_MASK
        || (availableStorageTypes & HDFS_MASK) > 0;
  }

  public boolean HDFSAvailable() {
    return StorageInfo.HDFSAvailable(availableStorageTypes);
  }

  public static boolean HDFSOnly(int availableStorageTypes) {
    return availableStorageTypes == HDFS_MASK;
  }

  public boolean HDFSOnly() {
    return StorageInfo.HDFSOnly(availableStorageTypes);
  }

  public static boolean S3Only(int availableStorageTypes) {
    return availableStorageTypes == S3_MASK;
  }

  public static boolean OSSAvailable(int availableStorageTypes) {
    return availableStorageTypes == ALL_TYPES_AVAILABLE_MASK
        || (availableStorageTypes & OSS_MASK) > 0;
  }

  public static boolean S3Available(int availableStorageTypes) {
    return availableStorageTypes == ALL_TYPES_AVAILABLE_MASK
        || (availableStorageTypes & S3_MASK) > 0;
  }

  public boolean OSSAvailable() {
    return StorageInfo.OSSAvailable(availableStorageTypes);
  }

  public boolean S3Available() {
    return StorageInfo.S3Available(availableStorageTypes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StorageInfo that = (StorageInfo) o;
    return finalResult == that.finalResult
        && availableStorageTypes == that.availableStorageTypes
        && type == that.type
        && Objects.equals(mountPoint, that.mountPoint)
        && Objects.equals(filePath, that.filePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, mountPoint, finalResult, filePath, availableStorageTypes);
  }

  public static boolean validate(String typeStr) {
    return typeNames.contains(typeStr);
  }

  public static PbStorageInfo toPb(StorageInfo storageInfo) {
    String filePath = storageInfo.getFilePath();
    PbStorageInfo.Builder builder = PbStorageInfo.newBuilder();
    builder
        .setType(storageInfo.type.value)
        .setFinalResult(storageInfo.finalResult)
        .setMountPoint(storageInfo.mountPoint)
        .setAvailableStorageTypes(storageInfo.availableStorageTypes)
        .setFileSize(storageInfo.getFileSize());
    if (storageInfo.getChunkOffsets() != null) {
      builder.addAllChunkOffsets(storageInfo.getChunkOffsets());
    }
    if (filePath != null) {
      builder.setFilePath(filePath);
    }
    return builder.build();
  }

  public static StorageInfo fromPb(PbStorageInfo pbStorageInfo) {
    return new StorageInfo(
        typesMap.get(pbStorageInfo.getType()),
        pbStorageInfo.getMountPoint(),
        pbStorageInfo.getFinalResult(),
        pbStorageInfo.getFilePath(),
        pbStorageInfo.getAvailableStorageTypes(),
        pbStorageInfo.getFileSize(),
        pbStorageInfo.getChunkOffsetsList());
  }

  public static int getAvailableTypes(List<Type> types) {
    int ava = 0;
    for (Type type : types) {
      switch (type) {
        case MEMORY:
          ava = ava | MEMORY_MASK;
          break;
        case HDD:
        case SSD:
          ava = ava | LOCAL_DISK_MASK;
          break;
        case HDFS:
          ava = ava | HDFS_MASK;
          break;
        case OSS:
          ava = ava | OSS_MASK;
          break;
        case S3:
          ava = ava | S3_MASK;
          break;
      }
    }
    return ava;
  }

  public static Type fromStrToType(String typeStr) {
    return types.get(typeStr);
  }
}
