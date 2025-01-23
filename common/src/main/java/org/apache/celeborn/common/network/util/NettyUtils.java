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

package org.apache.celeborn.common.network.util;

import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.ThreadPerTaskExecutor;
import io.netty.util.internal.PlatformDependent;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.util.JavaUtils;

/** Utilities for creating various Netty constructs based on whether we're using EPOLL or NIO. */
public class NettyUtils {
  private static final ByteBufAllocator[] _sharedByteBufAllocator = new ByteBufAllocator[2];
  private static final ConcurrentHashMap<String, Integer> allocatorsIndex =
      JavaUtils.newConcurrentHashMap();
  private static final List<PooledByteBufAllocator> pooledByteBufAllocators = new ArrayList<>();

  /** Creates a new ThreadFactory which prefixes each thread with the given name. */
  public static ThreadFactory createThreadFactory(String threadPoolPrefix) {
    return new DefaultThreadFactory(threadPoolPrefix, true);
  }

  public static EventLoopGroup createEventLoop(IOMode mode, int numThreads, String threadPrefix) {
    return createEventLoop(mode, numThreads, false, threadPrefix);
  }

  /** Creates a Netty EventLoopGroup based on the IOMode. */
  public static EventLoopGroup createEventLoop(
      IOMode mode, int numThreads, boolean conflictAvoidChooserEnable, String threadPrefix) {
    ThreadFactory threadFactory = createThreadFactory(threadPrefix);

    switch (mode) {
      case NIO:
        return conflictAvoidChooserEnable
            ? new NioEventLoopGroup(
                numThreads,
                new ThreadPerTaskExecutor(threadFactory),
                ConflictAvoidEventExecutorChooserFactory.INSTANCE,
                SelectorProvider.provider(),
                DefaultSelectStrategyFactory.INSTANCE)
            : new NioEventLoopGroup(numThreads, threadFactory);
      case EPOLL:
        return new EpollEventLoopGroup(numThreads, threadFactory);
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /** Returns the correct (client) SocketChannel class based on IOMode. */
  public static Class<? extends Channel> getClientChannelClass(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioSocketChannel.class;
      case EPOLL:
        return EpollSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /** Returns the correct ServerSocketChannel class based on IOMode. */
  public static Class<? extends ServerChannel> getServerChannelClass(IOMode mode) {
    switch (mode) {
      case NIO:
        return NioServerSocketChannel.class;
      case EPOLL:
        return EpollServerSocketChannel.class;
      default:
        throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
  }

  /** Returns the remote address on the channel or "&lt;unknown remote&gt;" if none exists. */
  public static String getRemoteAddress(Channel channel) {
    if (channel != null && channel.remoteAddress() != null) {
      return channel.remoteAddress().toString();
    }
    return "<unknown remote>";
  }

  /**
   * Create a ByteBufAllocator that respects the parameters
   *
   * @param pooled If true, create a PooledByteBufAllocator, otherwise UnpooledByteBufAllocator
   * @param allowDirectBufs If true and platform supports, allocate ByteBuf in direct memory,
   *     otherwise in heap memory.
   * @param allowCache If true, enable thread-local cache, it only take effect for
   *     PooledByteBufAllocator.
   * @param numCores Number of heap/direct arenas, 0 means use number of cpu cores, it only take
   *     effect for PooledByteBufAllocator.
   */
  private static ByteBufAllocator createByteBufAllocator(
      boolean pooled, boolean allowDirectBufs, boolean allowCache, int numCores) {
    if (pooled) {
      if (numCores == 0) {
        numCores = Runtime.getRuntime().availableProcessors();
      }
      return new PooledByteBufAllocator(
          allowDirectBufs && PlatformDependent.directBufferPreferred(),
          Math.min(PooledByteBufAllocator.defaultNumHeapArena(), numCores),
          Math.min(PooledByteBufAllocator.defaultNumDirectArena(), allowDirectBufs ? numCores : 0),
          PooledByteBufAllocator.defaultPageSize(),
          PooledByteBufAllocator.defaultMaxOrder(),
          allowCache ? PooledByteBufAllocator.defaultSmallCacheSize() : 0,
          allowCache ? PooledByteBufAllocator.defaultNormalCacheSize() : 0,
          allowCache && PooledByteBufAllocator.defaultUseCacheForAllThreads());
    } else {
      return new UnpooledByteBufAllocator(
          allowDirectBufs && PlatformDependent.directBufferPreferred());
    }
  }

  /**
   * Returns the lazily created shared pooled ByteBuf allocator for the specified allowCache
   * parameter value.
   */
  public static synchronized ByteBufAllocator getSharedByteBufAllocator(
      CelebornConf conf, AbstractSource source, boolean allowCache) {
    final int index = allowCache ? 0 : 1;
    if (_sharedByteBufAllocator[index] == null) {
      _sharedByteBufAllocator[index] =
          createByteBufAllocator(
              conf.networkMemoryAllocatorPooled(), true, allowCache, conf.networkAllocatorArenas());
      if (conf.networkMemoryAllocatorPooled()) {
        pooledByteBufAllocators.add((PooledByteBufAllocator) _sharedByteBufAllocator[index]);
      }
      if (source != null) {
        new NettyMemoryMetrics(
            _sharedByteBufAllocator[index],
            "shared-pool-" + index,
            conf.networkAllocatorVerboseMetric(),
            source,
            Collections.emptyMap());
      }
    }
    return _sharedByteBufAllocator[index];
  }

  public static ByteBufAllocator getByteBufAllocator(
      TransportConf conf, AbstractSource source, boolean allowCache) {
    return getByteBufAllocator(conf, source, allowCache, 0);
  }

  public static ByteBufAllocator getByteBufAllocator(
      TransportConf conf, AbstractSource source, boolean allowCache, int coreNum) {
    if (conf.getCelebornConf().networkShareMemoryAllocator()) {
      return getSharedByteBufAllocator(
          conf.getCelebornConf(),
          source,
          allowCache && conf.getCelebornConf().networkMemoryAllocatorAllowCache());
    }
    int arenas;
    if (coreNum != 0) {
      arenas = coreNum;
    } else {
      arenas = conf.getCelebornConf().networkAllocatorArenas();
    }
    ByteBufAllocator allocator =
        createByteBufAllocator(
            conf.getCelebornConf().networkMemoryAllocatorPooled(),
            conf.preferDirectBufs(),
            allowCache,
            arenas);
    if (conf.getCelebornConf().networkMemoryAllocatorPooled()) {
      pooledByteBufAllocators.add((PooledByteBufAllocator) allocator);
    }
    if (source != null) {
      String poolName = "default-netty-pool";
      Map<String, String> labels = new HashMap<>();
      String moduleName = conf.getModuleName();
      if (!moduleName.isEmpty()) {
        poolName = moduleName;
        int index = allocatorsIndex.compute(moduleName, (k, v) -> v == null ? 0 : v + 1);
        labels.put("allocatorIndex", String.valueOf(index));
      }
      new NettyMemoryMetrics(
          allocator,
          poolName,
          conf.getCelebornConf().networkAllocatorVerboseMetric(),
          source,
          labels);
    }
    return allocator;
  }

  public static List<PooledByteBufAllocator> getAllPooledByteBufAllocators() {
    return pooledByteBufAllocators;
  }
}
