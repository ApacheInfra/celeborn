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

import java.io.File;

import io.netty.channel.epoll.Epoll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.TransportModuleConstants;

/** A central location that tracks all the settings we expose to users. */
public class TransportConf {

  private static final Logger logger = LoggerFactory.getLogger(TransportConf.class);

  private final CelebornConf celebornConf;

  private final String module;

  public TransportConf(String module, CelebornConf celebornConf) {
    this.module = module;
    this.celebornConf = celebornConf;
  }

  public String getModuleName() {
    return module;
  }

  /** IO mode: nio or epoll */
  public String ioMode() {
    return Epoll.isAvailable() ? celebornConf.networkIoMode(module) : IOMode.NIO.name();
  }

  /** If true, we will prefer allocating off-heap byte buffers within Netty. */
  public boolean preferDirectBufs() {
    return celebornConf.networkIoPreferDirectBufs(module);
  }

  /** Connect timeout in milliseconds. Default 10 secs. */
  public int connectTimeoutMs() {
    return celebornConf.networkIoConnectTimeoutMs(module);
  }

  /** Connection active timeout in milliseconds. Default 240 secs. */
  public int connectionTimeoutMs() {
    return celebornConf.networkIoConnectionTimeoutMs(module);
  }

  /** Number of concurrent connections between two nodes for fetching data. */
  public int numConnectionsPerPeer() {
    return celebornConf.networkIoNumConnectionsPerPeer(module);
  }

  /** Requested maximum length of the queue of incoming connections. Default 0 for no backlog. */
  public int backLog() {
    return celebornConf.networkIoBacklog(module);
  }

  /** Number of threads used in the server thread pool. Default to 0, which is 2x#cores. */
  public int serverThreads() {
    return celebornConf.networkIoServerThreads(module);
  }

  /** Number of threads used in the client thread pool. Default to 0, which is 2x#cores. */
  public int clientThreads() {
    return celebornConf.networkIoClientThreads(module);
  }

  /** * Whether to use conflict avoid EventExecutorChooser while creating transport client */
  public boolean conflictAvoidChooserEnable() {
    return celebornConf.networkIoConflictAvoidChooserEnable(module);
  }

  /**
   * Receive buffer size (SO_RCVBUF). Note: the optimal size for receive buffer and send buffer
   * should be latency * network_bandwidth. Assuming latency = 1ms, network_bandwidth = 10Gbps
   * buffer size should be ~ 1.25MB
   */
  public int receiveBuf() {
    return celebornConf.networkIoReceiveBuf(module);
  }

  /** Send buffer size (SO_SNDBUF). */
  public int sendBuf() {
    return celebornConf.networkIoSendBuf(module);
  }

  /**
   * Max number of times we will try IO exceptions (such as connection timeouts) per request. If set
   * to 0, we will not do any retries.
   */
  public int maxIORetries() {
    return celebornConf.networkIoMaxRetries(module);
  }

  /**
   * Time (in milliseconds) that we will wait in order to perform a retry after an IOException. Only
   * relevant if maxIORetries &gt; 0.
   */
  public int ioRetryWaitTimeMs() {
    return celebornConf.networkIoRetryWaitMs(module);
  }

  /**
   * Minimum size of a block that we should start using memory map rather than reading in through
   * normal IO operations. This prevents Celeborn from memory mapping very small blocks. In general,
   * memory mapping has high overhead for blocks close to or below the page size of the OS.
   */
  public int memoryMapBytes() {
    return celebornConf.networkIoMemoryMapBytes(module);
  }

  /**
   * Whether to initialize FileDescriptor lazily or not. If true, file descriptors are created only
   * when data is going to be transferred. This can reduce the number of open files.
   */
  public boolean lazyFileDescriptor() {
    return celebornConf.networkIoLazyFileDescriptor(module);
  }

  /**
   * Whether to track Netty memory detailed metrics. If true, the detailed metrics of Netty
   * PoolByteBufAllocator will be gotten, otherwise only general memory usage will be tracked.
   */
  public boolean verboseMetrics() {
    return celebornConf.networkIoVerboseMetrics(module);
  }

  public CelebornConf getCelebornConf() {
    return celebornConf;
  }

  public int pushDataTimeoutCheckerThreads() {
    return celebornConf.pushDataTimeoutCheckerThreads(module);
  }

  public long pushDataTimeoutCheckIntervalMs() {
    return celebornConf.pushDataTimeoutCheckInterval(module);
  }

  public int fetchDataTimeoutCheckerThreads() {
    return celebornConf.fetchDataTimeoutCheckerThreads(module);
  }

  public long fetchDataTimeoutCheckIntervalMs() {
    return celebornConf.fetchDataTimeoutCheckInterval(module);
  }

  public long clientHeartbeatInterval() {
    return celebornConf.clientHeartbeatInterval(module);
  }

  /** Timeout for a single round trip of sasl message exchange, in milliseconds. */
  public int saslTimeoutMs() {
    return celebornConf.networkIoSaslTimoutMs(module);
  }

  /** Whether authentication is enabled or not. */
  public boolean authEnabled() {
    return celebornConf.authEnabled();
  }

  /** Whether Secure (SSL/TLS) wire communication is enabled. */
  public boolean sslEnabled() {
    return celebornConf.sslEnabled(module);
  }

  /** SSL protocol (remember that SSLv3 was compromised) supported by Java */
  public String sslProtocol() {
    return celebornConf.sslProtocol(module);
  }

  /** A comma separated list of ciphers */
  public String[] sslRequestedCiphers() {
    return celebornConf.sslRequestedCiphers(module);
  }

  /** The key-store file; can be relative to the current directory */
  public File sslKeyStore() {
    return celebornConf.sslKeyStore(module);
  }

  /** The password to the key-store file */
  public String sslKeyStorePassword() {
    return celebornConf.sslKeyStorePassword(module);
  }

  /** The trust-store file; can be relative to the current directory */
  public File sslTrustStore() {
    return celebornConf.sslTrustStore(module);
  }

  /** The password to the trust-store file */
  public String sslTrustStorePassword() {
    return celebornConf.sslTrustStorePassword(module);
  }

  /**
   * If using a trust-store that that reloads its configuration is enabled. If true, when the
   * trust-store file on disk changes, it will be reloaded
   */
  public boolean sslTrustStoreReloadingEnabled() {
    return celebornConf.sslTrustStoreReloadingEnabled(module);
  }

  /** The interval, in milliseconds, the trust-store will reload its configuration */
  public int sslTrustStoreReloadIntervalMs() {
    return celebornConf.sslTrustStoreReloadIntervalMs(module);
  }

  /** Internal config: the max size when chunking the stream with SSL */
  public int maxSslEncryptedBlockSize() {
    return celebornConf.maxSslEncryptedBlockSize(module);
  }

  // suppressing to ensure clarity of code.
  @SuppressWarnings("RedundantIfStatement")
  public boolean sslEnabledAndKeysAreValid() {
    if (!sslEnabled()) {
      return false;
    }
    // It is not required to have a keyStore for client side connections - only server side
    // connectivity ... so transport conf's without keystore can be used in
    // client mode only.
    // In case it is specified, we check for its validity
    File keyStore = sslKeyStore();
    if (keyStore != null && !keyStore.exists()) {
      return false;
    }
    // It's fine for the trust store to be missing, we would default to trusting all.
    return true;
  }

  public boolean autoSslEnabled() {
    // auto_ssl_enable is supported only for RPC_LIFECYCLEMANAGER_MODULE
    if (!TransportModuleConstants.RPC_LIFECYCLEMANAGER_MODULE.equals(module)) return false;

    // auto ssl must be enabled, and there should be no keystore or trust store configured
    boolean autoSslEnabled = celebornConf.isAutoSslEnabled(module);

    if (!autoSslEnabled) return false;

    File keystore = celebornConf.sslKeyStore(module);
    File truststore = celebornConf.sslTrustStore(module);

    boolean shouldAutoSslEnabled = null == keystore && null == truststore;

    if (!shouldAutoSslEnabled) {
      logger.warn(
          "Auto ssl for {} disabled as keystore = {} or truststore = {} was configured",
          module,
          null != keystore ? keystore.getPath() : null,
          null != truststore ? truststore.getPath() : null);
    }

    return shouldAutoSslEnabled;
  }
}
