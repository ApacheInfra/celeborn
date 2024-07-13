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

import org.apache.spark.BarrierTaskContext;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.util.TaskFailureListener;

import org.apache.celeborn.client.ShuffleClient;

public class BarrierHelper {

  public static void addFailureListenerIfBarrierTask(
      ShuffleClient shuffleClient, TaskContext context, ShuffleHandle handle) {

    if (!(context instanceof BarrierTaskContext)) return;
    BarrierTaskContext barrierContext = (BarrierTaskContext) context;

    barrierContext.addTaskFailureListener(
        new TaskFailureListener() {
          @Override
          public void onTaskFailure(TaskContext context, Throwable error) {
            // whatever is the reason for failure, we notify lifecycle manager about the failure
            shuffleClient.reportBarrierTaskFailure(
                context.stageId(), context.stageAttemptNumber(), handle.shuffleId());
          }
        });
  }
}
