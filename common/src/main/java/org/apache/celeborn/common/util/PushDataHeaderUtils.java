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

package org.apache.celeborn.common.util;

import java.util.zip.CRC32;

import org.apache.celeborn.common.unsafe.Platform;

public class PushDataHeaderUtils {
  // Data Header Layout:
  // | mapId (4 bytes)                        |
  // | attemptId (4 bytes)                    |
  // | batchId with checksum flag (4 bytes)   |
  // | length with checksum length (4 bytes)  |
  // | checksum (4 bytes)                     |
  //
  // Fields description:
  // - mapId: Unique identifier for the map (4 bytes)
  // - attemptId: Identifier for the attempt (4 bytes)
  // - batchId with checksum flag:
  //   -- checksum flag: 1 bit (indicates if batchId has a checksum)
  //   -- batchId: 31 bits (always positive when represented as an integer)
  // - length with checksum length: total length of the data + 4 bytes for checksum
  // - checksum: Always positive integer (4 bytes)
  public static final int BATCH_HEADER_SIZE = 5 * 4;
  public static final int BATCH_HEADER_SIZE_WITHOUT_CHECKSUM = BATCH_HEADER_SIZE - 4;
  public static final int MAP_ID_OFFSET = Platform.BYTE_ARRAY_OFFSET;
  public static final int ATTEMPT_ID_OFFSET = Platform.BYTE_ARRAY_OFFSET + 4;
  public static final int BATCH_ID_OFFSET = Platform.BYTE_ARRAY_OFFSET + 8;
  public static final int LENGTH_OFFSET = Platform.BYTE_ARRAY_OFFSET + 12;
  public static final int CHECKSUM_OFFSET = Platform.BYTE_ARRAY_OFFSET + 16;
  public static final int POSITIVE_MASK = 0x7FFFFFFF;
  public static final int HIGHEST_1_BIT_FLAG_MASK = 0x80000000;

  public static void buildDataHeader(
      byte[] data, int mapId, int attemptId, int batchId, int length, boolean enableChecksum) {
    if (enableChecksum) {
      assert data.length >= BATCH_HEADER_SIZE;
      int batchIdWithChecksumFlag = batchIdWithChecksumFlag(batchId);
      int lengthWithChecksum = length + 4;
      Platform.putInt(data, MAP_ID_OFFSET, mapId);
      Platform.putInt(data, ATTEMPT_ID_OFFSET, attemptId);
      Platform.putInt(data, BATCH_ID_OFFSET, batchIdWithChecksumFlag);
      Platform.putInt(data, LENGTH_OFFSET, lengthWithChecksum);
      Platform.putInt(data, CHECKSUM_OFFSET, computeHeaderChecksum32(data));
    } else {
      assert data.length >= BATCH_HEADER_SIZE_WITHOUT_CHECKSUM;
      Platform.putInt(data, MAP_ID_OFFSET, mapId);
      Platform.putInt(data, ATTEMPT_ID_OFFSET, attemptId);
      Platform.putInt(data, BATCH_ID_OFFSET, batchId);
      Platform.putInt(data, LENGTH_OFFSET, length);
    }
  }

  public static int batchIdWithChecksumFlag(int batchId) {
    return batchId | HIGHEST_1_BIT_FLAG_MASK;
  }

  public static int batchIdWithoutChecksumFlag(int batchId) {
    return batchId & POSITIVE_MASK;
  }

  public static boolean hasChecksumFlag(byte[] data) {
    int batchId = Platform.getInt(data, BATCH_ID_OFFSET);
    return (batchId & HIGHEST_1_BIT_FLAG_MASK) != 0;
  }

  public static int getMapId(byte[] data) {
    return Platform.getInt(data, MAP_ID_OFFSET);
  }

  public static int getAttemptId(byte[] data) {
    return Platform.getInt(data, ATTEMPT_ID_OFFSET);
  }

  public static int getBatchId(byte[] data) {
    return batchIdWithoutChecksumFlag(Platform.getInt(data, BATCH_ID_OFFSET));
  }

  public static int getChecksumLength(byte[] data) {
    if (hasChecksumFlag(data)) {
      return 4;
    } else {
      return 0;
    }
  }

  public static int getDataLength(byte[] data) {
    return Platform.getInt(data, LENGTH_OFFSET) - getChecksumLength(data);
  }

  // lengthWithChecksumLength = Platform.getInt(data, LENGTH_OFFSET)
  public static int getTotalLengthWithHeader(byte[] data) {
    return BATCH_HEADER_SIZE_WITHOUT_CHECKSUM + Platform.getInt(data, LENGTH_OFFSET);
  }

  public static int computeHeaderChecksum32(byte[] data) {
    assert data.length >= BATCH_HEADER_SIZE_WITHOUT_CHECKSUM;
    CRC32 crc32 = new CRC32();
    crc32.update(data, 0, BATCH_HEADER_SIZE_WITHOUT_CHECKSUM);
    return ((int) crc32.getValue()) & POSITIVE_MASK;
  }

  public static boolean checkHeaderChecksum32(byte[] data) {
    assert data.length >= BATCH_HEADER_SIZE;
    int expectedChecksum = Platform.getInt(data, CHECKSUM_OFFSET);
    int currentChecksum = computeHeaderChecksum32(data);
    return currentChecksum == expectedChecksum;
  }
}
