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

import java.util.*;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public class CelebornPartitionUtilSuiteJ {
  @Test
  public void testSkewPartitionSplit() {

    ArrayList<PartitionLocation> locations = new ArrayList<>();
    for (int i = 0; i < 13; i++) {
      locations.add(genPartitionLocation(i, new Long[] {0L, 100L, 200L, 300L, 500L, 1000L}));
    }
    locations.add(genPartitionLocation(91, new Long[] {0L, 1L}));

    int subPartitionSize = 3;

    Map<String, Pair<Integer, Integer>> result1 =
        CelebornPartitionUtil.splitSkewedPartitionLocations(locations, subPartitionSize, 0);
    Map<String, Pair<Integer, Integer>> expectResult1 =
        genRanges(
            new Object[][] {
              {"0-0", 0, 4},
              {"0-1", 0, 4},
              {"0-10", 0, 4},
              {"0-11", 0, 4},
              {"0-12", 0, 2}
            });
    Assert.assertEquals(expectResult1, result1);

    Map<String, Pair<Integer, Integer>> result2 =
        CelebornPartitionUtil.splitSkewedPartitionLocations(locations, subPartitionSize, 1);
    Map<String, Pair<Integer, Integer>> expectResult2 =
        genRanges(
            new Object[][] {
              {"0-12", 3, 4},
              {"0-2", 0, 4},
              {"0-3", 0, 4},
              {"0-4", 0, 4},
              {"0-5", 0, 3}
            });
    Assert.assertEquals(expectResult2, result2);

    Map<String, Pair<Integer, Integer>> result3 =
        CelebornPartitionUtil.splitSkewedPartitionLocations(locations, subPartitionSize, 2);
    Map<String, Pair<Integer, Integer>> expectResult3 =
        genRanges(
            new Object[][] {
              {"0-5", 4, 4},
              {"0-6", 0, 4},
              {"0-7", 0, 4},
              {"0-8", 0, 4},
              {"0-9", 0, 4},
              {"0-91", 0, 0}
            });
    Assert.assertEquals(expectResult3, result3);
  }

  @Test
  public void testBoundary() {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    locations.add(genPartitionLocation(0, new Long[] {0L, 100L, 200L, 300L, 400L, 500L}));

    for (int i = 0; i < 5; i++) {
      Map<String, Pair<Integer, Integer>> result =
          CelebornPartitionUtil.splitSkewedPartitionLocations(locations, 5, i);
      Map<String, Pair<Integer, Integer>> expectResult = genRanges(new Object[][] {{"0-0", i, i}});
      Assert.assertEquals(expectResult, result);
    }
  }

  @Test
  public void testSplitStable() {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    for (int i = 0; i < 13; i++) {
      locations.add(genPartitionLocation(i, new Long[] {0L, 100L, 200L, 300L, 500L, 1000L}));
    }
    locations.add(genPartitionLocation(91, new Long[] {0L, 1L}));

    Collections.shuffle(locations);

    Map<String, Pair<Integer, Integer>> result =
        CelebornPartitionUtil.splitSkewedPartitionLocations(locations, 3, 0);
    Map<String, Pair<Integer, Integer>> expectResult =
        genRanges(
            new Object[][] {
              {"0-0", 0, 4},
              {"0-1", 0, 4},
              {"0-10", 0, 4},
              {"0-11", 0, 4},
              {"0-12", 0, 2}
            });
    Assert.assertEquals(expectResult, result);
  }

  private ArrayList<PartitionLocation> genPartitionLocations(Map<Integer, Long[]> epochToOffsets) {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    epochToOffsets.forEach(
        (epoch, offsets) -> {
          PartitionLocation location =
              new PartitionLocation(
                  0, epoch, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
          StorageInfo storageInfo =
              new StorageInfo(
                  StorageInfo.Type.HDD,
                  "mountPoint",
                  false,
                  "filePath",
                  StorageInfo.LOCAL_DISK_MASK,
                  1,
                  Arrays.asList(offsets));
          location.setStorageInfo(storageInfo);
          locations.add(location);
        });
    return locations;
  }

  private PartitionLocation genPartitionLocation(int epoch, Long[] offsets) {
    PartitionLocation location =
        new PartitionLocation(0, epoch, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
    StorageInfo storageInfo =
        new StorageInfo(
            StorageInfo.Type.HDD,
            "mountPoint",
            false,
            "filePath",
            StorageInfo.LOCAL_DISK_MASK,
            offsets[offsets.length - 1],
            Arrays.asList(offsets));
    location.setStorageInfo(storageInfo);
    return location;
  }

  private Map<String, Pair<Integer, Integer>> genRanges(Object[][] inputs) {
    Map<String, Pair<Integer, Integer>> ranges = new HashMap<>();
    for (Object[] idToChunkRange : inputs) {
      String uid = (String) idToChunkRange[0];
      Pair<Integer, Integer> range = Pair.of((int) idToChunkRange[1], (int) idToChunkRange[2]);
      ranges.put(uid, range);
    }
    return ranges;
  }
}
