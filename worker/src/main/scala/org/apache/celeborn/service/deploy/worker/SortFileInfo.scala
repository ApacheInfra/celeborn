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

package org.apache.celeborn.service.deploy.worker

import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.FileInfo
import org.apache.celeborn.common.network.client.{RpcResponseCallback, TransportClient}

class SortFileInfo(
    var fileInfo: FileInfo,
    var status: Int,
    var client: TransportClient,
    var fileName: String,
    var startIndex: Int,
    var endIndex: Int,
    var readLocalShuffle: Boolean,
    var startTime: Long,
    var rpcRequestId: Long,
    var isLegacy: Boolean,
    var callback: RpcResponseCallback,
    var userIdentifier: UserIdentifier,
    var sortedFilePath: String,
    var indexFilePath: String,
    var error: String,
    var fileIndex: Int)

object SortFileInfo {
  val SORT_NOTSTARTED: Int = 0
  val SORTING: Int = 1
  val SORT_FINISHED: Int = 2
  val SORT_FAILED: Int = 3
}
