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

package org.apache.celeborn.service.deploy.worker;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportResponseHandler;
import org.apache.celeborn.common.network.protocol.ChunkFetchSuccess;
import org.apache.celeborn.common.network.protocol.Message;
import org.apache.celeborn.common.network.protocol.OpenStream;
import org.apache.celeborn.common.network.protocol.RpcFailure;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.RpcResponse;
import org.apache.celeborn.common.network.protocol.StreamHandle;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbBufferStreamEnd;
import org.apache.celeborn.common.protocol.PbChunkFetchRequest;
import org.apache.celeborn.common.protocol.PbOpenStream;
import org.apache.celeborn.common.protocol.PbStreamChunkSlice;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StreamType;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.PushDataHeaderUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;
import org.apache.celeborn.service.deploy.worker.storage.PartitionFilesSorter;
import org.apache.celeborn.service.deploy.worker.storage.StorageManager;

public class FetchHandlerSuiteJ {

  private static final Logger LOG = LoggerFactory.getLogger(FetchHandlerSuiteJ.class);

  private static final CelebornConf conf = new CelebornConf();

  private static final Random random = new Random();

  private static final UserIdentifier userIdentifier =
      new UserIdentifier("mock-tenantId", "mock-name");
  private static final int MAX_MAP_ID = 50;
  private static final int DATA_SIZE_PER_BATCH =
      256 * 1024
          - PushDataHeaderUtils
              .BATCH_HEADER_SIZE; // 256k - PushDataHeaderUtils.BATCH_HEADER_SIZE byte

  public FileInfo prepare(int batchCountPerMap) throws IOException {
    byte[] batchHeader = new byte[PushDataHeaderUtils.BATCH_HEADER_SIZE];
    File shuffleFile = File.createTempFile("celeborn", UUID.randomUUID().toString());

    DiskFileInfo fileInfo = new DiskFileInfo(shuffleFile, userIdentifier, conf);
    FileOutputStream fileOutputStream = new FileOutputStream(shuffleFile);
    FileChannel channel = fileOutputStream.getChannel();
    Map<Integer, Integer> batchIds = new HashMap<>();
    ArrayList<Integer> mapIds = generateMapIds(batchCountPerMap);

    for (int mapId : mapIds) {
      int currentAttemptId = 0;
      int batchId =
          batchIds.compute(
              mapId,
              (k, v) -> {
                if (v == null) {
                  v = 0;
                } else {
                  v++;
                }
                return v;
              });
      byte[] mockedData = new byte[DATA_SIZE_PER_BATCH];
      PushDataHeaderUtils.buildDataHeader(
          batchHeader, mapId, currentAttemptId, batchId, DATA_SIZE_PER_BATCH, true);
      ByteBuffer buf1 = ByteBuffer.wrap(batchHeader);
      while (buf1.hasRemaining()) {
        channel.write(buf1);
      }
      random.nextBytes(mockedData);
      ByteBuffer buf2 = ByteBuffer.wrap(mockedData);
      while (buf2.hasRemaining()) {
        channel.write(buf2);
      }
    }
    long originFileLen = channel.size();
    // update original fileInfo chunk offsets
    for (long offset = conf.shuffleChunkSize();
        offset <= originFileLen;
        offset += conf.shuffleChunkSize()) {
      (fileInfo.getReduceFileMeta()).addChunkOffset(offset);
    }
    // update sorted fileInfo chunk offsets
    fileInfo.updateBytesFlushed(originFileLen);
    return fileInfo;
  }

  public void cleanup(FileInfo fileInfo) throws IOException {
    if (fileInfo != null) {
      // origin file
      JavaUtils.deleteRecursively(((DiskFileInfo) fileInfo).getFile());
      // sorted file
      JavaUtils.deleteRecursively(
          new File(((DiskFileInfo) fileInfo).getFile().getPath() + ".sorted"));
      // index file
      JavaUtils.deleteRecursively(
          new File(((DiskFileInfo) fileInfo).getFile().getPath() + ".index"));
    }
  }

  @BeforeClass
  public static void beforeAll() {
    MemoryManager.initialize(conf);
  }

  @AfterClass
  public static void afterAll() {
    MemoryManager.reset();
  }

  @Test
  public void testFetchOriginFile() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      PbStreamHandler streamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE);

      fetchChunkAndCheck(client, channel, fetchHandler, streamHandler);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testFetchSortFile() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      PbStreamHandler streamHandler = openStreamAndCheck(client, channel, fetchHandler, 5, 10);

      fetchChunkAndCheck(client, channel, fetchHandler, streamHandler);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testLegacyOpenStream() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      legacyOpenStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE);

    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testWorkerReadSortFileOnceOriginalFileBeDeleted() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      PbStreamHandler rangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 5, 10);
      checkOriginFileBeDeleted(fileInfo);
      PbStreamHandler nonRangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE);
      fetchChunkAndCheck(client, channel, fetchHandler, nonRangeReadStreamHandler);
      fetchChunkAndCheck(client, channel, fetchHandler, rangeReadStreamHandler);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testLocalReadSortFileOnceOriginalFileBeDeleted() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      // read local shuffle
      openStreamAndCheck(client, channel, fetchHandler, 5, 10, true);
      checkOriginFileBeDeleted(fileInfo);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testDoNotDeleteOriginalFileWhenNonRangeWorkerReadWorkInProgress() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      PbStreamHandler nonRangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE);
      PbStreamHandler rangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 5, 10);
      fetchChunkAndCheck(client, channel, fetchHandler, nonRangeReadStreamHandler);
      fetchChunkAndCheck(client, channel, fetchHandler, rangeReadStreamHandler);

      // non-range fetch finished.
      bufferStreamEnd(client, fetchHandler, nonRangeReadStreamHandler.getStreamId());
      checkOriginFileBeDeleted(fileInfo);
    } finally {
      cleanup(fileInfo);
    }
  }

  @Test
  public void testDoNotDeleteOriginalFileWhenNonRangeLocalReadWorkInProgress() throws IOException {
    FileInfo fileInfo = null;
    try {
      // total write size: 32 * 50 * 256k = 400m
      fileInfo = prepare(32);
      EmbeddedChannel channel = new EmbeddedChannel();
      TransportClient client = new TransportClient(channel, mock(TransportResponseHandler.class));
      FetchHandler fetchHandler = mockFetchHandler(fileInfo);

      // read local shuffle
      PbStreamHandler nonRangeReadStreamHandler =
          openStreamAndCheck(client, channel, fetchHandler, 0, Integer.MAX_VALUE, true);
      openStreamAndCheck(client, channel, fetchHandler, 5, 10);

      // non-range fetch finished.
      bufferStreamEnd(client, fetchHandler, nonRangeReadStreamHandler.getStreamId());
      checkOriginFileBeDeleted(fileInfo);
    } finally {
      cleanup(fileInfo);
    }
  }

  private FetchHandler mockFetchHandler(FileInfo fileInfo) {
    WorkerSource workerSource = mock(WorkerSource.class);
    TransportConf transportConf =
        Utils.fromCelebornConf(conf, TransportModuleConstants.FETCH_MODULE, 4);
    FetchHandler fetchHandler0 = new FetchHandler(conf, transportConf, workerSource);
    Worker worker = mock(Worker.class);
    PartitionFilesSorter partitionFilesSorter =
        new PartitionFilesSorter(MemoryManager.instance(), conf, workerSource);

    StorageManager storageManager = mock(StorageManager.class);
    Mockito.doReturn(storageManager).when(worker).storageManager();
    Mockito.doReturn(workerSource).when(worker).workerSource();
    Mockito.doReturn(partitionFilesSorter).when(worker).partitionsSorter();
    fetchHandler0.init(worker);
    FetchHandler fetchHandler = spy(fetchHandler0);
    Mockito.doReturn(fileInfo).when(fetchHandler).getRawFileInfo(anyString(), anyString());
    return fetchHandler;
  }

  private final String shuffleKey = "dummyShuffleKey-123";
  private final String fileName = "dummyFileName";
  private final long dummyRequestId = 0;

  @Deprecated
  private void legacyOpenStreamAndCheck(
      TransportClient client,
      EmbeddedChannel channel,
      FetchHandler fetchHandler,
      int startIndex,
      int endIndex)
      throws IOException {
    ByteBuffer openStreamByteBuffer =
        new OpenStream(shuffleKey, fileName, startIndex, endIndex).toByteBuffer();
    fetchHandler.receive(
        client,
        new RpcRequest(dummyRequestId, new NioManagedBuffer(openStreamByteBuffer)),
        createRpcResponseCallback(channel));
    RpcResponse result = channel.readOutbound();
    StreamHandle streamHandler = (StreamHandle) Message.decode(result.body().nioByteBuffer());
    if (endIndex == Integer.MAX_VALUE) {
      assertEquals(50, streamHandler.numChunks);
    } else {
      assertEquals(endIndex - startIndex, streamHandler.numChunks);
    }
  }

  private PbStreamHandler openStreamAndCheck(
      TransportClient client,
      EmbeddedChannel channel,
      FetchHandler fetchHandler,
      int startIndex,
      int endIndex)
      throws IOException {
    return openStreamAndCheck(client, channel, fetchHandler, startIndex, endIndex, false);
  }

  private PbStreamHandler openStreamAndCheck(
      TransportClient client,
      EmbeddedChannel channel,
      FetchHandler fetchHandler,
      int startIndex,
      int endIndex,
      Boolean readLocalShuffle)
      throws IOException {
    ByteBuffer openStreamByteBuffer =
        new TransportMessage(
                MessageType.OPEN_STREAM,
                PbOpenStream.newBuilder()
                    .setShuffleKey(shuffleKey)
                    .setFileName(fileName)
                    .setStartIndex(startIndex)
                    .setEndIndex(endIndex)
                    .setReadLocalShuffle(readLocalShuffle)
                    .build()
                    .toByteArray())
            .toByteBuffer();
    fetchHandler.receive(
        client,
        new RpcRequest(dummyRequestId, new NioManagedBuffer(openStreamByteBuffer)),
        createRpcResponseCallback(channel));
    RpcResponse result = channel.readOutbound();
    PbStreamHandler streamHandler =
        TransportMessage.fromByteBuffer(result.body().nioByteBuffer()).getParsedPayload();
    if (endIndex == Integer.MAX_VALUE) {
      assertEquals(50, streamHandler.getNumChunks());
    } else {
      assertEquals(endIndex - startIndex, streamHandler.getNumChunks());
    }
    return streamHandler;
  }

  private void fetchChunkAndCheck(
      TransportClient client,
      EmbeddedChannel channel,
      FetchHandler fetchHandler,
      PbStreamHandler streamHandler) {
    for (int chunkIndex = 0; chunkIndex < streamHandler.getNumChunks(); chunkIndex++) {
      fetchHandler.receive(
          client,
          new RpcRequest(
              TransportClient.requestId(),
              new NioManagedBuffer(
                  new TransportMessage(
                          MessageType.CHUNK_FETCH_REQUEST,
                          PbChunkFetchRequest.newBuilder()
                              .setStreamChunkSlice(
                                  PbStreamChunkSlice.newBuilder()
                                      .setStreamId(streamHandler.getStreamId())
                                      .setChunkIndex(chunkIndex)
                                      .setOffset(0)
                                      .setLen(Integer.MAX_VALUE))
                              .build()
                              .toByteArray())
                      .toByteBuffer())),
          createRpcResponseCallback(channel));
      ChunkFetchSuccess chunkFetchSuccess = channel.readOutbound();
      chunkFetchSuccess.body().retain();
      // chunk size 8m
      assertEquals(chunkFetchSuccess.body().size(), 8 * 1024 * 1024);
      chunkFetchSuccess.body().release();
    }
  }

  private void bufferStreamEnd(TransportClient client, FetchHandler fetchHandler, long streamId) {
    TransportMessage bufferStreamEnd =
        new TransportMessage(
            MessageType.BUFFER_STREAM_END,
            PbBufferStreamEnd.newBuilder()
                .setStreamId(streamId)
                .setStreamType(StreamType.ChunkStream)
                .build()
                .toByteArray());
    fetchHandler.receive(
        client,
        new RpcRequest(dummyRequestId, new NioManagedBuffer(bufferStreamEnd.toByteBuffer())),
        createRpcResponseCallback(client.getChannel()));
  }

  private void checkOriginFileBeDeleted(FileInfo fileInfo) {
    assertFalse(((DiskFileInfo) fileInfo).getFilePath().endsWith(".sorted"));
    long startTs = System.currentTimeMillis();
    boolean deleted = false;
    long timeout = 5 * 1000; // 5s
    while (!deleted) {
      deleted = !((DiskFileInfo) fileInfo).getFile().exists();
      if (System.currentTimeMillis() - startTs > timeout) {
        fail("Origin file was not deleted within the expected timeout of 5 seconds.");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("catch Exception when checking origin file states.", e);
      }
    }
    assertTrue(deleted);
  }

  private ArrayList<Integer> generateMapIds(int batchCountPerMap) {
    int mapCount = batchCountPerMap * MAX_MAP_ID;
    ArrayList<Integer> ids = new ArrayList<>(mapCount);
    for (int i = 0; i < mapCount; i++) {
      ids.add(i / batchCountPerMap);
    }
    Collections.shuffle(ids);
    return ids;
  }

  private RpcResponseCallback createRpcResponseCallback(Channel channel) {
    return new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        channel.writeAndFlush(new RpcResponse(dummyRequestId, new NioManagedBuffer(response)));
      }

      @Override
      public void onFailure(Throwable e) {
        channel.writeAndFlush(new RpcFailure(dummyRequestId, Throwables.getStackTraceAsString(e)));
      }
    };
  }
}
