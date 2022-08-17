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

package com.aliyun.emr.rss.service.deploy.worker;

import java.io.File;
import java.util.ArrayList;

public class LocalFileMeta {
  File file;
  long bytesFlushed = 0;
  ArrayList<Long> chunkOffsets = new ArrayList<>();

  public LocalFileMeta(
     File file,
     long bytesFlushed,
     ArrayList<Long> chunkOffsets) {
    this.file = file;
    this.bytesFlushed = bytesFlushed;
    this.chunkOffsets = chunkOffsets;
  }

  public LocalFileMeta(File file) {
    this.file = file;
    chunkOffsets.add(0L);
  }

  long getFileLength() {
    return bytesFlushed;
  }


  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  public long getBytesFlushed() {
    return bytesFlushed;
  }

  public void setBytesFlushed(long bytesFlushed) {
    this.bytesFlushed = bytesFlushed;
  }

  public ArrayList<Long> getChunkOffsets() {
    return chunkOffsets;
  }

  public void setChunkOffsets(ArrayList<Long> chunkOffsets) {
    this.chunkOffsets = chunkOffsets;
  }

  @Override
  public String toString() {
    return "LocalFileMeta{" +
        "file=" + file +
        ", bytesFlushed=" + bytesFlushed +
        ", chunkOffsets=" + chunkOffsets +
        '}';
  }
}
