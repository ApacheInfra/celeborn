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

package org.apache.celeborn.common.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.server.TransportChannelHandler;
import org.apache.celeborn.common.network.util.*;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * <p>The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for all
 * TransportClients.
 *
 * <p>TransportClients will be reused whenever possible.
 */
public class TransportClientFactory implements Closeable {

  /** A simple data structure to track the pool of clients between two peer nodes. */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final List<TransportClientBootstrap> clientBootstraps;
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /** Random number generator for picking connections between peers. */
  private final Random rand;

  private final int numConnectionsPerPeer;

  private final int connectTimeoutMs;
  private final int connectionTimeoutMs;

  private final int receiveBuf;

  private final int sendBuf;
  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  protected ByteBufAllocator allocator;
  private final int maxClientConnectRetries;
  private final int maxClientConnectRetryWaitTimeMs;

  public TransportClientFactory(
      TransportContext context, List<TransportClientBootstrap> clientBootstraps) {
    this.context = Preconditions.checkNotNull(context);
    TransportConf conf = context.getConf();
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    this.connectionPool = JavaUtils.newConcurrentHashMap();
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.connectTimeoutMs = conf.connectTimeoutMs();
    this.connectionTimeoutMs = conf.connectionTimeoutMs();
    this.receiveBuf = conf.receiveBuf();
    this.sendBuf = conf.sendBuf();
    this.rand = new Random();

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    logger.info("Module {} mode {} threads {}", conf.getModuleName(), ioMode, conf.clientThreads());
    this.workerGroup =
        NettyUtils.createEventLoop(
            ioMode,
            conf.clientThreads(),
            conf.conflictAvoidChooserEnable(),
            conf.getModuleName() + "-client");
    // Always disable thread-local cache when creating pooled ByteBuf allocator for TransportClients
    // because the ByteBufs are allocated by the event loop thread, but released by the executor
    // thread rather than the event loop thread. Those thread-local caches actually delay the
    // recycling of buffers, leading to larger memory usage.
    this.allocator =
        NettyUtils.getByteBufAllocator(conf, context.getSource(), false, conf.clientThreads());
    this.maxClientConnectRetries = conf.maxIORetries();
    this.maxClientConnectRetryWaitTimeMs = conf.ioRetryWaitTimeMs();
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * <p>We maintains an array of clients (size determined by
   * celeborn.$module.io.numConnectionsPerPeer) and randomly picks one to use. If no client was
   * previously created in the randomly selected spot, this function creates a new client and places
   * it there.
   *
   * <p>This blocks until a connection is successfully established and fully bootstrapped.
   *
   * <p>Concurrency: This method is safe to call from multiple threads.
   */
  public TransportClient createClient(String remoteHost, int remotePort, int partitionId)
      throws IOException, InterruptedException {
    return retryCreateClient(remoteHost, remotePort, partitionId, TransportFrameDecoder::new);
  }

  public TransportClient retryCreateClient(
      String remoteHost,
      int remotePort,
      int partitionId,
      Supplier<ChannelInboundHandlerAdapter> supplier)
      throws IOException, InterruptedException {
    int numTries = 0;
    while (numTries < maxClientConnectRetries) {
      try {
        return createClient(remoteHost, remotePort, partitionId, supplier.get());
      } catch (Exception e) {
        numTries++;
        logger.warn(
            "Retry create client, times {}/{} with error: {}",
            numTries,
            maxClientConnectRetries,
            e.getMessage(),
            e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        if (numTries == maxClientConnectRetries) {
          throw e;
        }

        Uninterruptibles.sleepUninterruptibly(
            maxClientConnectRetryWaitTimeMs, TimeUnit.MILLISECONDS);
      }
    }

    return null;
  }

  public TransportClient createClient(
      String remoteHost, int remotePort, int partitionId, ChannelInboundHandlerAdapter decoder)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we create a client.
    final InetSocketAddress unresolvedAddress =
        InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool =
        connectionPool.computeIfAbsent(
            unresolvedAddress, key -> new ClientPool(numConnectionsPerPeer));
    int clientIndex =
        partitionId < 0 ? rand.nextInt(numConnectionsPerPeer) : partitionId % numConnectionsPerPeer;
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      TransportChannelHandler handler =
          cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);
      if (handler != null) {
        synchronized (handler) {
          handler.getResponseHandler().updateTimeOfLastRequest();
        }
      }

      if (cachedClient.isActive()) {
        logger.debug(
            "Returning cached connection from {} to {}: {}",
            cachedClient.getChannel().localAddress(),
            cachedClient.getSocketAddress(),
            cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - preResolveHost);
    final String resolveMsg = resolvedAddress.isUnresolved() ? "failed" : "succeed";
    if (hostResolveTimeMs > 2000) {
      logger.warn(
          "DNS resolution {} for {} took {} ms", resolveMsg, resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace(
          "DNS resolution {} for {} took {} ms", resolveMsg, resolvedAddress, hostResolveTimeMs);
    }

    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.debug(
              "Returning cached connection from {} to {}: {}",
              cachedClient.getChannel().localAddress(),
              resolvedAddress,
              cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }
      clientPool.clients[clientIndex] = internalCreateClient(resolvedAddress, decoder);
      return clientPool.clients[clientIndex];
    }
  }

  public TransportClient createClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    return createClient(remoteHost, remotePort, -1);
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port. This
   * connection is not pooled.
   *
   * <p>As with {@link #createClient(String, int)}, this method is blocking.
   */
  private TransportClient internalCreateClient(
      InetSocketAddress address, ChannelInboundHandlerAdapter decoder)
      throws IOException, InterruptedException {
    Bootstrap bootstrap = new Bootstrap();
    bootstrap
        .group(workerGroup)
        .channel(socketChannelClass)
        // Disable Nagle's Algorithm since we don't want packets to wait
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
        .option(ChannelOption.ALLOCATOR, allocator);

    if (receiveBuf > 0) {
      bootstrap.option(ChannelOption.SO_RCVBUF, receiveBuf);
    }

    if (sendBuf > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, sendBuf);
    }

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    bootstrap.handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            TransportChannelHandler clientHandler = context.initializePipeline(ch, decoder, true);
            clientRef.set(clientHandler.getClient());
            channelRef.set(ch);
          }
        });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (connectTimeoutMs <= 0) {
      cf.await();
      assert cf.isDone();
      if (cf.isCancelled()) {
        throw new IOException(String.format("Connecting to %s cancelled", address));
      } else if (!cf.isSuccess()) {
        throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
      }
    } else if (!cf.await(connectTimeoutMs)) {
      throw new CelebornIOException(
          String.format("Connecting to %s timed out (%s ms)", address, connectTimeoutMs));
    } else if (cf.cause() != null) {
      throw new CelebornIOException(String.format("Failed to connect to %s", address), cf.cause());
    }
    if (context.sslEncryptionEnabled()) {
      final SslHandler sslHandler = cf.channel().pipeline().get(SslHandler.class);
      Future<Channel> future =
          sslHandler
              .handshakeFuture()
              .addListener(
                  new GenericFutureListener<Future<Channel>>() {
                    @Override
                    public void operationComplete(final Future<Channel> handshakeFuture) {
                      if (handshakeFuture.isSuccess()) {
                        logger.debug("successfully completed TLS handshake to {}", address);
                      } else {
                        logger.info(
                            "failed to complete TLS handshake to {}",
                            address,
                            handshakeFuture.cause());
                        cf.channel().close();
                      }
                    }
                  });
      if (!future.await(connectionTimeoutMs)) {
        cf.channel().close();
        throw new IOException(
            String.format("Failed to connect to %s within connection timeout", address));
      }
    }

    TransportClient client = clientRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
    long preBootstrap = System.nanoTime();
    logger.debug("Running bootstraps for {} ...", address);
    try {
      for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
        clientBootstrap.doBootstrap(client);
      }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
      long bootstrapTime = System.nanoTime() - preBootstrap;
      logger.error(
          "Exception while bootstrapping client after {}",
          Utils.nanoDurationToString(bootstrapTime),
          e);
      client.close();
      throw Throwables.propagate(e);
    }
    long postBootstrap = System.nanoTime();
    logger.debug(
        "Successfully created connection to {} after {} ({} spent in bootstraps)",
        address,
        Utils.nanoDurationToString(postBootstrap - preConnect),
        Utils.nanoDurationToString(postBootstrap - preBootstrap));

    return client;
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    // SPARK-19147
    if (workerGroup != null && !workerGroup.isShuttingDown()) {
      workerGroup.shutdownGracefully();
    }
  }

  public TransportContext getContext() {
    return context;
  }
}
