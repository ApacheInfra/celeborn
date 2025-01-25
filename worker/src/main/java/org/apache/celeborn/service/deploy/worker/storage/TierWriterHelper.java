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

package org.apache.celeborn.service.deploy.worker.storage;

import org.apache.celeborn.reflect.DynConstructors;
import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler;

public class TierWriterHelper {
  public static MultipartUploadHandler getS3MultipartUploadHandler(
      String bucketName,
      String s3AccessKey,
      String s3SecretKey,
      String s3EndpointRegion,
      String key,
      int maxRetryies) {
    return (MultipartUploadHandler)
        DynConstructors.builder()
            .impl(
                "org.apache.celeborn.S3MultipartUploadHandler",
                String.class,
                String.class,
                String.class,
                String.class,
                String.class,
                Integer.class)
            .build()
            .newInstance(bucketName, s3AccessKey, s3SecretKey, s3EndpointRegion, key, maxRetryies);
  }
}
