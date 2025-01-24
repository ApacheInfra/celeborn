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

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class PushFailedBatchSuiteJ {

  @Test
  public void equalsReturnsTrueForIdenticalBatches() {
    PushFailedBatch batch1 = new PushFailedBatch(1, 2, 3);
    PushFailedBatch batch2 = new PushFailedBatch(1, 2, 3);
    Assert.assertEquals(batch1, batch2);
  }

  @Test
  public void equalsReturnsFalseForDifferentBatches() {
    PushFailedBatch batch1 = new PushFailedBatch(1, 2, 3);
    PushFailedBatch batch2 = new PushFailedBatch(4, 5, 6);
    Assert.assertNotEquals(batch1, batch2);
  }

  @Test
  public void hashCodeDiffersForDifferentBatches() {
    PushFailedBatch batch1 = new PushFailedBatch(1, 2, 3);
    PushFailedBatch batch2 = new PushFailedBatch(4, 5, 6);
    Assert.assertNotEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void hashCodeSameForIdenticalBatches() {
    PushFailedBatch batch1 = new PushFailedBatch(1, 2, 3);
    PushFailedBatch batch2 = new PushFailedBatch(1, 2, 3);
    Assert.assertEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void hashCodeIsConsistent() {
    PushFailedBatch batch = new PushFailedBatch(1, 2, 3);
    int hashCode1 = batch.hashCode();
    int hashCode2 = batch.hashCode();
    Assert.assertEquals(hashCode1, hashCode2);
  }

  @Test
  public void toStringReturnsExpectedFormat() {
    PushFailedBatch batch = new PushFailedBatch(1, 2, 3);
    String expected = "PushFailedBatch[mapId=1,attemptId=2,batchId=3]";
    Assert.assertEquals(expected, batch.toString());
  }

  @Test
  public void hashCodeAndEqualsWorkInSet() {
    Set<PushFailedBatch> set = new HashSet<>();
    PushFailedBatch batch1 = new PushFailedBatch(1, 2, 3);
    PushFailedBatch batch2 = new PushFailedBatch(1, 2, 3);
    set.add(batch1);
    Assert.assertTrue(set.contains(batch2));
  }
}
