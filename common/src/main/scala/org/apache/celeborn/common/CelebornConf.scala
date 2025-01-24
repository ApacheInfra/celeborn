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

package org.apache.celeborn.common

import java.io.{File, IOException}
import java.util.{Collection => JCollection, Collections, HashMap => JHashMap, Locale, Map => JMap, UUID}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Try
import scala.util.matching.Regex

import io.netty.channel.epoll.Epoll

import org.apache.celeborn.common.authentication.AnonymousAuthenticationProviderImpl
import org.apache.celeborn.common.identity.{DefaultIdentityProvider, HadoopBasedIdentityProvider, IdentityProvider}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.internal.config._
import org.apache.celeborn.common.network.util.{ByteUnit, IOMode}
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.StorageInfo.Type
import org.apache.celeborn.common.protocol.StorageInfo.Type.{HDD, SSD}
import org.apache.celeborn.common.rpc.RpcTimeout
import org.apache.celeborn.common.util.{JavaUtils, Utils}

class CelebornConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  import CelebornConf._

  /** Create a CelebornConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = JavaUtils.newConcurrentHashMap[String, String]()

  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new CelebornConfigProvider(settings))
    _reader.bindEnv(new ConfigProvider {
      override def get(key: String): Option[String] = Option(getenv(key))
    })
    _reader
  }

  private def loadFromMap(props: Map[String, String], silent: Boolean): Unit =
    settings.synchronized {
      // Load any celeborn.* system properties
      for ((key, value) <- props if key.startsWith("celeborn.")) {
        set(key, value, silent)
      }
      this
    }

  if (loadDefaults) {
    loadFromMap(Utils.getSystemProperties, false)
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): CelebornConf = {
    set(key, value, false)
  }

  private[celeborn] def set(key: String, value: String, silent: Boolean): CelebornConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException(s"null value for $key")
    }
    if (!silent) {
      logDeprecationWarning(key)
    }
    requireDefaultValueOfRemovedConf(key, value)
    settings.put(key, value)
    this
  }

  private def warnIfInternalTransportModule(module: String, key: String): Unit = {
    if (INTERNAL_TRANSPORT_MODULES.contains(module)) {
      log.warn(s"$key configured for internal transport module $module, " +
        s"should be using ${INTERNAL_TRANSPORT_MODULES(module)} instead")
    }
  }

  private def getTransportConfContainsImpl[T](module: String, entry: ConfigEntry[T]): Boolean = {
    var currentModule = module

    while (null != currentModule) {
      val key = entry.key.replace("<module>", currentModule)
      val opt = getOption(key)
      if (opt.isDefined) {
        warnIfInternalTransportModule(currentModule, key)
        return true
      }

      currentModule = CelebornConf.TRANSPORT_MODULE_FALLBACKS.getOrElse(currentModule, null)
    }

    // Is there a fallback for the config entry itself ?
    entry match {
      case fallbackConfig: FallbackConfigEntry[T] =>
        contains(fallbackConfig)
      case _ =>
        false
    }
  }

  private def getTransportConfImpl[T](
      module: String,
      configEntry: ConfigEntry[T],
      converter: String => T,
      allowDefault: Boolean = true): Option[T] = {
    var currentModule = module

    while (null != currentModule) {
      val key = configEntry.key.replace("<module>", currentModule)
      val opt = getOption(key)
      if (opt.isDefined) {
        warnIfInternalTransportModule(currentModule, key)
        return opt.map(converter)
      }

      currentModule = CelebornConf.TRANSPORT_MODULE_FALLBACKS.getOrElse(currentModule, null)
    }

    // Is there a fallback for the config entry itself ?
    configEntry match {
      case fallbackConfig: FallbackConfigEntry[T] =>
        if (fallbackConfig.fallback.defaultValue.isDefined || contains(fallbackConfig)) {
          // We do not expect fallback key to be a transport conf
          assert(!fallbackConfig.fallback.key.contains("<module>"))
          Some(get(fallbackConfig.fallback))
        } else {
          // We return None if fallback config is defined, but it does not have a default
          None
        }
      case _ =>
        if (allowDefault) {
          Some(converter(configEntry.defaultValueString))
        } else {
          None
        }
    }
  }

  def setTransportConfIfMissing[T](
      module: String,
      configEntry: ConfigEntry[T],
      value: String): Unit = {

    if (getTransportConfContainsImpl(module, configEntry)) {
      return
    }

    val key = configEntry.key.replace(
      "<module>",
      // if this is an internal module. map it to the exposed module - else use the same
      INTERNAL_TRANSPORT_MODULES.getOrElse(module, module))
    set(key, value)
  }

  def set[T](entry: ConfigEntry[T], value: T): CelebornConf = {
    set(entry.key, entry.stringConverter(value))
    this
  }

  def set[T](entry: OptionalConfigEntry[T], value: T): CelebornConf = {
    set(entry.key, entry.rawStringConverter(value))
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): CelebornConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): CelebornConf = {
    requireDefaultValueOfRemovedConf(key, value)
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  def setIfMissing[T](entry: ConfigEntry[T], value: T): CelebornConf = {
    setIfMissing(entry.key, entry.stringConverter(value))
  }

  def setIfMissing[T](entry: OptionalConfigEntry[T], value: T): CelebornConf = {
    setIfMissing(entry.key, entry.rawStringConverter(value))
  }

  /** Remove a parameter from the configuration */
  def unset(key: String): CelebornConf = {
    settings.remove(key)
    this
  }

  def unset(entry: ConfigEntry[_]): CelebornConf = {
    unset(entry.key)
  }

  def clear(): Unit = {
    settings.clear()
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }

  /** Get a transport config */
  def getTransportConf(module: String, configEntry: ConfigEntry[String]): String = {
    getTransportConfImpl(module, configEntry, Predef.identity).get
  }

  /**
   * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then seconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException            If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key))
  }

  /**
   * Get a time parameter as seconds, falling back to a default if not set. If no
   * suffix is provided then seconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /**
   * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key))
  }

  /**
   * Get a time parameter as milliseconds, falling back to a default if not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /** Get time in ms for a transport config */
  def getTransportConfTimeAsMs(module: String, configEntry: ConfigEntry[Long]): Long = {
    getTransportConfImpl(module, configEntry, Utils.timeStringAsMs).get
  }

  /**
   * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then bytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set. If no
   * suffix is provided then bytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue + "B"))
  }

  /** Get a size parameter as bytes for a transport config */
  def getTransportConfSizeAsBytes(module: String, configEntry: ConfigEntry[Long]): Long = {
    getTransportConfImpl(module, configEntry, Utils.byteStringAsBytes).get
  }

  /**
   * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key))
  }

  /**
   * Get a size parameter as Kibibytes, falling back to a default if not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key))
  }

  /**
   * Get a size parameter as Mebibytes, falling back to a default if not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key))
  }

  /**
   * Get a size parameter as Gibibytes, falling back to a default if not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, settings))
  }

  /** Get an optional value, applying variable substitution. */
  private[celeborn] def getWithSubstitution(key: String): Option[String] = {
    getOption(key).map(reader.substitute)
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /**
   * Get all parameters that start with `prefix`
   */
  def getAllWithPrefix(prefix: String): Array[(String, String)] = {
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }

  /**
   * Get a parameter as an integer, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as an integer
   */
  def getInt(key: String, defaultValue: Int): Int = catchIllegalValue(key) {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as an integer for a transport config */
  def getTransportConfInt(module: String, configEntry: ConfigEntry[Int]): Int = {
    getTransportConfImpl(module, configEntry, Integer.parseInt).get
  }

  /**
   * Get a parameter as a long, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as a long
   */
  def getLong(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a double, falling back to a default if not ste
   * @throws NumberFormatException If the value cannot be interpreted as a double
   */
  def getDouble(key: String, defaultValue: Double): Double = catchIllegalValue(key) {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a boolean, falling back to a default if not set
   * @throws IllegalArgumentException If the value cannot be interpreted as a boolean
   */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = catchIllegalValue(key) {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Get a parameter as a long for a transport config */
  def getTransportConfBoolean(module: String, configEntry: ConfigEntry[Boolean]): Boolean = {
    getTransportConfImpl(module, configEntry, java.lang.Boolean.parseBoolean).get
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = {
    settings.containsKey(key) ||
    configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
  }

  private[celeborn] def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)

  private[celeborn] def getTransportConfContains[E](
      module: String,
      configEntry: ConfigEntry[E]): Boolean = {
    getTransportConfContainsImpl(module, configEntry)
  }

  /** Copy this object */
  override def clone: CelebornConf = {
    val cloned = new CelebornConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey, e.getValue, true)
    }
    cloned
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
   */
  private[celeborn] def getenv(name: String): String = System.getenv(name)

  /**
   * Wrapper method for get() methods which require some specific value format. This catches
   * any [[NumberFormatException]] or [[IllegalArgumentException]] and re-raises it with the
   * incorrectly configured key in the exception message.
   */
  private def catchIllegalValue[T](key: String)(getValue: => T): T = {
    try {
      getValue
    } catch {
      case e: NumberFormatException =>
        // NumberFormatException doesn't have a constructor that takes a cause for some reason.
        throw new NumberFormatException(s"Illegal value for config key $key: ${e.getMessage}")
          .initCause(e)
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Illegal value for config key $key: ${e.getMessage}", e)
    }
  }

  def dynamicConfigStoreBackend: Option[String] = get(DYNAMIC_CONFIG_STORE_BACKEND)
  def dynamicConfigRefreshInterval: Long = get(DYNAMIC_CONFIG_REFRESH_INTERVAL)
  def dynamicConfigStoreFsPath: Option[String] = get(DYNAMIC_CONFIG_STORE_FS_PATH)
  def dynamicConfigStoreDbFetchPageSize: Int = get(DYNAMIC_CONFIG_STORE_DB_FETCH_PAGE_SIZE)
  def dynamicConfigStoreDbHikariDriverClassName: String =
    get(DYNAMIC_CONFIG_STORE_DB_HIKARI_DRIVER_CLASS_NAME)
  def dynamicConfigStoreDbHikariJdbcUrl: String = get(DYNAMIC_CONFIG_STORE_DB_HIKARI_JDBC_URL)
  def dynamicConfigStoreDbHikariUsername: String = get(DYNAMIC_CONFIG_STORE_DB_HIKARI_USERNAME)
  def dynamicConfigStoreDbHikariPassword: String = get(DYNAMIC_CONFIG_STORE_DB_HIKARI_PASSWORD)
  def dynamicConfigStoreDbHikariConnectionTimeout: Long =
    get(DYNAMIC_CONFIG_STORE_DB_HIKARI_CONNECTION_TIMEOUT)
  def dynamicConfigStoreDbHikariIdleTimeout: Long = get(DYNAMIC_CONFIG_STORE_DB_HIKARI_IDLE_TIMEOUT)
  def dynamicConfigStoreDbHikariMaxLifetime: Long = get(DYNAMIC_CONFIG_STORE_DB_HIKARI_MAX_LIFETIME)
  def dynamicConfigStoreDbHikariMaximumPoolSize: Int =
    get(DYNAMIC_CONFIG_STORE_DB_HIKARI_MAXIMUM_POOL_SIZE)
  def dynamicConfigStoreDbHikariCustomConfigs: JMap[String, String] = {
    settings.asScala.filter(_._1.startsWith("celeborn.dynamicConfig.store.db.hikari")).toMap.asJava
  }

  // //////////////////////////////////////////////////////
  //                      Network                        //
  // //////////////////////////////////////////////////////
  def bindPreferIP: Boolean = get(NETWORK_BIND_PREFER_IP)
  def advertisePreferIP: Boolean = get(NETWORK_ADVERTISE_PREFER_IP)
  def bindWildcardAddress: Boolean = get(NETWORK_WILDCARD_ADDRESS_BIND)
  def portMaxRetries: Int = get(PORT_MAX_RETRY)
  def networkTimeout: RpcTimeout =
    new RpcTimeout(get(NETWORK_TIMEOUT).milli, NETWORK_TIMEOUT.key)
  def networkConnectTimeout: RpcTimeout =
    new RpcTimeout(get(NETWORK_CONNECT_TIMEOUT).milli, NETWORK_CONNECT_TIMEOUT.key)
  def rpcIoThreads: Option[Int] = get(RPC_IO_THREAD)
  def rpcConnectThreads: Int = get(RPC_CONNECT_THREADS)
  def rpcLookupTimeout: RpcTimeout =
    new RpcTimeout(get(RPC_LOOKUP_TIMEOUT).milli, RPC_LOOKUP_TIMEOUT.key)
  def rpcAskTimeout: RpcTimeout =
    new RpcTimeout(get(RPC_ASK_TIMEOUT).milli, RPC_ASK_TIMEOUT.key)
  def rpcInMemoryBoundedInboxCapacity(): Int = {
    get(RPC_INBOX_CAPACITY)
  }
  def rpcDispatcherNumThreads(availableCores: Int): Int = {
    val num = get(RPC_DISPATCHER_THREADS)
    if (num != 0) num else availableCores
  }
  def rpcDispatcherNumThreads(availableCores: Int, role: String): Int = {
    val num = getInt(
      RPC_ROLE_DISPATHER_THREADS.key.replace("<role>", role),
      rpcDispatcherNumThreads(availableCores))
    if (num != 0) num else availableCores
  }

  def rpcSlowThresholdNs(): Long = get(RPC_SLOW_THRESHOLD)
  def rpcSlowIntervalMs(): Long = get(RPC_SLOW_INTERVAL).getOrElse(-1)
  def rpcDumpIntervalMs(): Long = get(RPC_SUMMARY_DUMP_INTERVAL)

  def networkIoMode(module: String): String = {
    get(
      NETWORK_IO_MODE.key.replace("<module>", module),
      if (Epoll.isAvailable) IOMode.EPOLL.name() else IOMode.NIO.name())
  }

  def networkIoPreferDirectBufs(module: String): Boolean = {
    getTransportConfBoolean(module, NETWORK_IO_PREFER_DIRECT_BUFS)
  }

  def networkIoConnectTimeoutMs(module: String): Int = {
    getTransportConfTimeAsMs(module, NETWORK_IO_CONNECT_TIMEOUT).toInt
  }

  def networkIoConnectionTimeoutMs(module: String): Int = {
    getTransportConfTimeAsMs(module, NETWORK_IO_CONNECTION_TIMEOUT).toInt
  }

  def networkIoNumConnectionsPerPeer(module: String): Int = {
    getTransportConfInt(module, NETWORK_IO_NUM_CONNECTIONS_PER_PEER)
  }

  def networkIoBacklog(module: String): Int = {
    getTransportConfInt(module, NETWORK_IO_BACKLOG)
  }

  def networkIoServerThreads(module: String): Int = {
    getTransportConfInt(module, NETWORK_IO_SERVER_THREADS)
  }

  def networkIoClientThreads(module: String): Int = {
    getTransportConfInt(module, NETWORK_IO_CLIENT_THREADS)
  }

  def networkIoReceiveBuf(module: String): Int = {
    getTransportConfSizeAsBytes(module, NETWORK_IO_RECEIVE_BUFFER).toInt
  }

  def networkIoSendBuf(module: String): Int = {
    getTransportConfSizeAsBytes(module, NETWORK_IO_SEND_BUFFER).toInt
  }

  def networkIoMaxRetries(module: String): Int = {
    getTransportConfInt(module, NETWORK_IO_MAX_RETRIES)
  }

  def networkIoRetryWaitMs(module: String): Int = {
    getTransportConfTimeAsMs(module, NETWORK_IO_RETRY_WAIT).toInt
  }

  def networkIoMemoryMapBytes(module: String): Int = {
    getTransportConfSizeAsBytes(module, NETWORK_IO_STORAGE_MEMORY_MAP_THRESHOLD).toInt
  }

  def networkIoLazyFileDescriptor(module: String): Boolean = {
    getTransportConfBoolean(module, NETWORK_IO_LAZY_FD)
  }

  def networkIoVerboseMetrics(module: String): Boolean = {
    getTransportConfBoolean(module, NETWORK_VERBOSE_METRICS)
  }

  def networkShareMemoryAllocator: Boolean = get(NETWORK_MEMORY_ALLOCATOR_SHARE)

  def networkMemoryAllocatorAllowCache: Boolean =
    get(NETWORK_MEMORY_ALLOCATOR_ALLOW_CACHE)

  def networkMemoryAllocatorPooled: Boolean =
    get(NETWORK_MEMORY_ALLOCATOR_POOLED)

  def networkAllocatorArenas: Int = get(NETWORK_MEMORY_ALLOCATOR_ARENAS).getOrElse(Math.max(
    Runtime.getRuntime.availableProcessors(),
    2))

  def networkAllocatorVerboseMetric: Boolean = get(NETWORK_MEMORY_ALLOCATOR_VERBOSE_METRIC)

  def shuffleIoMaxChunksBeingTransferred: Option[Long] = {
    get(MAX_CHUNKS_BEING_TRANSFERRED)
  }

  def clientHeartbeatInterval(module: String): Long = {
    getTransportConfTimeAsMs(module, CHANNEL_HEARTBEAT_INTERVAL)
  }

  def pushDataTimeoutCheckerThreads(module: String): Int = {
    getTransportConfInt(module, PUSH_TIMEOUT_CHECK_THREADS)
  }

  def pushDataTimeoutCheckInterval(module: String): Long = {
    getTransportConfTimeAsMs(module, PUSH_TIMEOUT_CHECK_INTERVAL)
  }

  def fetchDataTimeoutCheckerThreads(module: String): Int = {
    getTransportConfInt(module, FETCH_TIMEOUT_CHECK_THREADS)
  }

  def fetchDataTimeoutCheckInterval(module: String): Long = {
    getTransportConfTimeAsMs(module, FETCH_TIMEOUT_CHECK_INTERVAL)
  }

  def maxDefaultNettyThreads: Int = get(MAX_DEFAULT_NETTY_THREADS)

  def networkIoSaslTimoutMs(module: String): Int = {
    getTransportConfTimeAsMs(module, NETWORK_IO_SASL_TIMEOUT).toInt
  }

  // //////////////////////////////////////////////////////
  //                      Master                         //
  // //////////////////////////////////////////////////////
  def masterSlotAssignPolicy: SlotsAssignPolicy =
    SlotsAssignPolicy.valueOf(get(MASTER_SLOT_ASSIGN_POLICY))
  def availableStorageTypes: Int = {
    val types = get(ACTIVE_STORAGE_TYPES).split(",").map(StorageInfo.Type.valueOf).toList
    StorageInfo.getAvailableTypes(types.asJava)
  }
  def hasHDFSStorage: Boolean =
    get(ACTIVE_STORAGE_TYPES).contains(StorageInfo.Type.HDFS.name()) && get(HDFS_DIR).isDefined
  def hasS3Storage: Boolean =
    get(ACTIVE_STORAGE_TYPES).contains(StorageInfo.Type.S3.name()) && get(S3_DIR).isDefined
  def masterSlotAssignLoadAwareDiskGroupNum: Int = get(MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM)
  def masterSlotAssignLoadAwareDiskGroupGradient: Double =
    get(MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_GRADIENT)
  def masterSlotAssignLoadAwareFlushTimeWeight: Double =
    get(MASTER_SLOT_ASSIGN_LOADAWARE_FLUSHTIME_WEIGHT)
  def masterSlotAssignLoadAwareFetchTimeWeight: Double =
    get(MASTER_SLOT_ASSIGN_LOADAWARE_FETCHTIME_WEIGHT)
  def masterSlotAssignExtraSlots: Int = get(MASTER_SLOT_ASSIGN_EXTRA_SLOTS)
  def masterSlotAssignMaxWorkers: Int = get(MASTER_SLOT_ASSIGN_MAX_WORKERS)
  def initialEstimatedPartitionSize: Long = get(ESTIMATED_PARTITION_SIZE_INITIAL_SIZE)
  def estimatedPartitionSizeUpdaterInitialDelay: Long =
    get(ESTIMATED_PARTITION_SIZE_UPDATE_INITIAL_DELAY)
  def estimatedPartitionSizeForEstimationUpdateInterval: Long =
    get(ESTIMATED_PARTITION_SIZE_UPDATE_INTERVAL)
  def masterResourceConsumptionInterval: Long = get(MASTER_RESOURCE_CONSUMPTION_INTERVAL)
  def clusterName: String = get(CLUSTER_NAME)

  // //////////////////////////////////////////////////////
  //               Address && HA && RATIS                //
  // //////////////////////////////////////////////////////
  def masterEndpoints: Array[String] = get(MASTER_ENDPOINTS).toArray

  def masterEndpointResolver: String = get(MASTER_ENDPOINTS_RESOLVER)

  def masterClientRpcAskTimeout: RpcTimeout =
    new RpcTimeout(get(MASTER_CLIENT_RPC_ASK_TIMEOUT).milli, MASTER_CLIENT_RPC_ASK_TIMEOUT.key)

  def masterClientMaxRetries: Int = get(MASTER_CLIENT_MAX_RETRIES)

  def masterHost: String = get(MASTER_HOST).replace("<localhost>", Utils.localHostName(this))

  def advertiseAddressMasterHost: String = get(MASTER_HOST)
    .replace("<localhost>", Utils.getHostName(this.advertisePreferIP))

  def masterHttpHost: String =
    get(MASTER_HTTP_HOST).replace("<localhost>", Utils.localHostName(this))

  def masterPort: Int = get(MASTER_PORT)

  def masterHttpPort: Int = get(MASTER_HTTP_PORT)

  def masterHttpMaxWorkerThreads: Int = get(MASTER_HTTP_MAX_WORKER_THREADS)

  def masterHttpStopTimeout: Long = get(MASTER_HTTP_STOP_TIMEOUT)

  def masterHttpIdleTimeout: Long = get(MASTER_HTTP_IDLE_TIMEOUT)

  def haEnabled: Boolean = get(HA_ENABLED)

  def haMasterNodeId: Option[String] = get(HA_MASTER_NODE_ID)

  def haMasterNodeIds: Array[String] = {
    def extractPrefix(original: String, stop: String): String = {
      val i = original.indexOf(stop)
      assert(i >= 0, s"$original does not contain $stop")
      original.substring(0, i)
    }

    val nodeConfPrefix = extractPrefix(HA_MASTER_NODE_HOST.key, "<id>")
    val nodeIds = getAllWithPrefix(nodeConfPrefix)
      .map(_._1)
      .filterNot(_.equals("id"))
      .map(k => extractPrefix(k, "."))
      .distinct

    // CELEBORN-638: compatible with `celeborn.ha.master.*`, expect to remove before 0.4.0
    val legacyNodeConfPrefix = extractPrefix(HA_MASTER_NODE_HOST.alternatives.head._1, "<id>")
    val legacyNodeIds = getAllWithPrefix(legacyNodeConfPrefix)
      .map(_._1)
      .filterNot(_.equals("id"))
      .map(k => extractPrefix(k, "."))
      .distinct
    (nodeIds ++ legacyNodeIds).distinct
  }

  def haMasterNodeHost(nodeId: String): String = {
    val key = HA_MASTER_NODE_HOST.key.replace("<id>", nodeId)
    val legacyKey = HA_MASTER_NODE_HOST.alternatives.head._1.replace("<id>", nodeId)
    get(key, get(legacyKey, Utils.localHostName(this)))
  }

  def haMasterNodePort(nodeId: String): Int = {
    val key = HA_MASTER_NODE_PORT.key.replace("<id>", nodeId)
    val legacyKey = HA_MASTER_NODE_PORT.alternatives.head._1.replace("<id>", nodeId)
    getInt(key, getInt(legacyKey, HA_MASTER_NODE_PORT.defaultValue.get))
  }

  def haMasterRatisHost(nodeId: String): String = {
    val key = HA_MASTER_NODE_RATIS_HOST.key.replace("<id>", nodeId)
    val legacyKey = HA_MASTER_NODE_RATIS_HOST.alternatives.head._1.replace("<id>", nodeId)
    get(key, get(legacyKey, haMasterNodeHost(nodeId)))
  }

  def haMasterRatisPort(nodeId: String): Int = {
    val key = HA_MASTER_NODE_RATIS_PORT.key.replace("<id>", nodeId)
    val legacyKey = HA_MASTER_NODE_RATIS_PORT.alternatives.head._1.replace("<id>", nodeId)
    getInt(key, getInt(legacyKey, HA_MASTER_NODE_RATIS_PORT.defaultValue.get))
  }

  def haMasterRatisRpcType: String = get(HA_MASTER_RATIS_RPC_TYPE)
  def haMasterRatisStorageDir: String = get(HA_MASTER_RATIS_STORAGE_DIR)
  def haMasterRatisStorageStartupOption: String = get(HA_MASTER_RATIS_STORAGE_STARTUP_OPTION)
  def haMasterRatisLogSegmentSizeMax: Long = get(HA_MASTER_RATIS_LOG_SEGMENT_SIZE_MAX)
  def haMasterRatisLogPreallocatedSize: Long = get(HA_MASTER_RATIS_LOG_PREALLOCATED_SIZE)
  def haMasterRatisLogWriteBufferSize: Long = get(HA_MASTER_RATIS_LOG_WRITE_BUFFER_SIZE)
  def haMasterRatisLogAppenderQueueNumElements: Int =
    get(HA_MASTER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS)
  def haMasterRatisLogAppenderQueueBytesLimit: Long =
    get(HA_MASTER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT)
  def haMasterRatisLogPurgeGap: Int = get(HA_MASTER_RATIS_LOG_PURGE_GAP)
  def haMasterRatisLogInstallSnapshotEnabled: Boolean =
    get(HA_MASTER_RATIS_LOG_INSTALL_SNAPSHOT_ENABLED)
  def haMasterRatisRpcRequestTimeout: Long = get(HA_MASTER_RATIS_RPC_REQUEST_TIMEOUT)
  def haMasterRatisRetryCacheExpiryTime: Long = get(HA_MASTER_RATIS_SERVER_RETRY_CACHE_EXPIRY_TIME)
  def haMasterRatisRpcTimeoutMin: Long = get(HA_MASTER_RATIS_RPC_TIMEOUT_MIN)
  def haMasterRatisRpcTimeoutMax: Long = get(HA_MASTER_RATIS_RPC_TIMEOUT_MAX)
  def haMasterRatisClientRpcTimeout: Long = get(HA_MASTER_RATIS_CLIENT_RPC_TIMEOUT)
  def haMasterRatisClientRpcWatchTimeout: Long = get(HA_MASTER_RATIS_CLIENT_RPC_WATCH_TIMEOUT)
  def haMasterRatisFirstElectionTimeoutMin: Long = get(HA_MASTER_RATIS_FIRSTELECTION_TIMEOUT_MIN)
  def haMasterRatisFirstElectionTimeoutMax: Long = get(HA_MASTER_RATIS_FIRSTELECTION_TIMEOUT_MAX)
  def hasMasterRatisLeaderElectionMemberMajorityAdd: Boolean =
    get(HA_MASTER_RATIS_LEADER_ELECTION_MEMBER_MAJORITY_ADD)
  def haMasterRatisNotificationNoLeaderTimeout: Long =
    get(HA_MASTER_RATIS_NOTIFICATION_NO_LEADER_TIMEOUT)
  def haMasterRatisRpcSlownessTimeout: Long = get(HA_MASTER_RATIS_RPC_SLOWNESS_TIMEOUT)
  def haMasterRatisRoleCheckInterval: Long = get(HA_MASTER_RATIS_ROLE_CHECK_INTERVAL)
  def haMasterRatisSnapshotAutoTriggerEnabled: Boolean =
    get(HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_ENABLED)
  def haMasterRatisSnapshotAutoTriggerThreshold: Long =
    get(HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD)
  def haMasterRatisSnapshotRetentionFileNum: Int = get(HA_MASTER_RATIS_SNAPSHOT_RETENTION_FILE_NUM)

  def masterPersistWorkerNetworkLocation: Boolean = get(MASTER_PERSIST_WORKER_NETWORK_LOCATION)
  def haRatisCustomConfigs: JMap[String, String] = {
    settings.asScala.filter(_._1.startsWith("celeborn.ratis")).toMap.asJava
  }
  def masterExcludeWorkerUnhealthyDiskRatioThreshold: Double =
    get(MASTER_EXCLUDE_WORKER_UNHEALTHY_DISK_RATIO_THRESHOLD)

  // //////////////////////////////////////////////////////
  //                      Worker                         //
  // //////////////////////////////////////////////////////
  def workerHttpHost: String =
    get(WORKER_HTTP_HOST).replace("<localhost>", Utils.localHostName(this))
  def workerHttpPort: Int = get(WORKER_HTTP_PORT)
  def workerHttpMaxWorkerThreads: Int = get(WORKER_HTTP_MAX_WORKER_THREADS)
  def workerHttpStopTimeout: Long = get(WORKER_HTTP_STOP_TIMEOUT)
  def workerHttpIdleTimeout: Long = get(WORKER_HTTP_IDLE_TIMEOUT)
  def workerRpcPort: Int = get(WORKER_RPC_PORT)
  def workerPushPort: Int = get(WORKER_PUSH_PORT)
  def workerFetchPort: Int = get(WORKER_FETCH_PORT)
  def workerReplicatePort: Int = get(WORKER_REPLICATE_PORT)
  def workerPushIoThreads: Option[Int] = get(WORKER_PUSH_IO_THREADS)
  def workerFetchIoThreads: Option[Int] = get(WORKER_FETCH_IO_THREADS)
  def workerReplicateIoThreads: Option[Int] = get(WORKER_REPLICATE_IO_THREADS)
  def registerWorkerTimeout: Long = get(WORKER_REGISTER_TIMEOUT)
  def workerWorkingDir: String = get(WORKER_WORKING_DIR)
  def workerCloseIdleConnections: Boolean = get(WORKER_CLOSE_IDLE_CONNECTIONS)
  def workerReplicateFastFailDuration: Long = get(WORKER_REPLICATE_FAST_FAIL_DURATION)
  def workerReplicateRandomConnectionEnabled: Boolean =
    get(WORKER_REPLICATE_RANDOM_CONNECTION_ENABLED)
  def workerCheckFileCleanMaxRetries: Int = get(WORKER_CHECK_FILE_CLEAN_MAX_RETRIES)
  def workerCheckFileCleanTimeout: Long = get(WORKER_CHECK_FILE_CLEAN_TIMEOUT)
  def workerHeartbeatTimeout: Long = get(WORKER_HEARTBEAT_TIMEOUT)
  def workerUnavailableInfoExpireTimeout: Long = get(WORKER_UNAVAILABLE_INFO_EXPIRE_TIMEOUT)
  def allowWorkerHostPattern: Option[Regex] = get(ALLOW_WORKER_HOST_PATTERN)
  def denyWorkerHostPattern: Option[Regex] = get(DENY_WORKER_HOST_PATTERN)

  def workerReplicateThreads: Int = get(WORKER_REPLICATE_THREADS)
  def workerCommitThreads: Int =
    if (hasHDFSStorage) Math.max(128, get(WORKER_COMMIT_THREADS)) else get(WORKER_COMMIT_THREADS)
  def workerCommitFilesCheckInterval: Long = get(WORKER_COMMIT_FILES_CHECK_INTERVAL)
  def workerCleanThreads: Int = get(WORKER_CLEAN_THREADS)
  def workerShuffleCommitTimeout: Long = get(WORKER_SHUFFLE_COMMIT_TIMEOUT)
  def maxPartitionSizeToEstimate: Long =
    get(ESTIMATED_PARTITION_SIZE_MAX_SIZE).getOrElse(partitionSplitMaximumSize * 2)
  def minPartitionSizeToEstimate: Long = get(ESTIMATED_PARTITION_SIZE_MIN_SIZE)
  def workerPartitionSorterSortPartitionTimeout: Long = get(WORKER_PARTITION_SORTER_SORT_TIMEOUT)
  def workerPartitionSorterPrefetchEnabled: Boolean =
    get(WORKER_PARTITION_SORTER_PREFETCH_ENABLED)
  def workerPartitionSorterShuffleBlockCompactionFactor: Double =
    get(WORKER_SHUFFLE_BLOCK_COMPACTION_FACTOR)
  def workerPartitionSorterReservedMemoryPerPartition: Long =
    get(WORKER_PARTITION_SORTER_RESERVED_MEMORY_PER_PARTITION)
  def workerPartitionSorterThreads: Int =
    get(WORKER_PARTITION_SORTER_THREADS).getOrElse(Runtime.getRuntime.availableProcessors)
  def workerPartitionSorterIndexCacheMaxWeight: Long =
    get(WORKER_PARTITION_SORTER_INDEX_CACHE_MAX_WEIGHT)
  def workerPartitionSorterIndexExpire: Long = get(WORKER_PARTITION_SORTER_INDEX_CACHE_EXPIRE)
  def workerPushHeartbeatEnabled: Boolean = get(WORKER_PUSH_HEARTBEAT_ENABLED)
  def workerPushMaxComponents: Int = get(WORKER_PUSH_COMPOSITEBUFFER_MAXCOMPONENTS)
  def workerFetchHeartbeatEnabled: Boolean = get(WORKER_FETCH_HEARTBEAT_ENABLED)
  def workerPartitionSplitEnabled: Boolean = get(WORKER_PARTITION_SPLIT_ENABLED)
  def workerActiveConnectionMax: Option[Long] = get(WORKER_ACTIVE_CONNECTION_MAX)
  def workerJvmProfilerEnabled: Boolean = get(WORKER_JVM_PROFILER_ENABLED)
  def workerJvmProfilerOptions: String = get(WORKER_JVM_PROFILER_OPTIONS)
  def workerJvmProfilerLocalDir: String = get(WORKER_JVM_PROFILER_LOCAL_DIR)
  def workerJvmQuakeEnabled: Boolean = get(WORKER_JVM_QUAKE_ENABLED)
  def workerJvmQuakeCheckInterval: Long = get(WORKER_JVM_QUAKE_CHECK_INTERVAL)
  def workerJvmQuakeRuntimeWeight: Double = get(WORKER_JVM_QUAKE_RUNTIME_WEIGHT)
  def workerJvmQuakeDumpEnabled: Boolean = get(WORKER_JVM_QUAKE_DUMP_ENABLED)
  def workerJvmQuakeDumpPath: String = get(WORKER_JVM_QUAKE_DUMP_PATH)

  def workerJvmQuakeDumpThreshold: Duration =
    getTimeAsMs(
      WORKER_JVM_QUAKE_DUMP_THRESHOLD.key,
      WORKER_JVM_QUAKE_DUMP_THRESHOLD.defaultValueString).microsecond
  def workerJvmQuakeKillThreshold: Duration =
    getTimeAsMs(
      WORKER_JVM_QUAKE_KILL_THRESHOLD.key,
      WORKER_JVM_QUAKE_KILL_THRESHOLD.defaultValueString).microsecond
  def workerJvmQuakeExitCode: Int = get(WORKER_JVM_QUAKE_EXIT_CODE)

  // //////////////////////////////////////////////////////
  //                 Metrics System                      //
  // //////////////////////////////////////////////////////
  def metricsConf: Option[String] = get(METRICS_CONF)
  def metricsSystemEnable: Boolean = get(METRICS_ENABLED)
  def metricsSampleRate: Double = get(METRICS_SAMPLE_RATE)
  def metricsSlidingWindowSize: Int = get(METRICS_SLIDING_WINDOW_SIZE)
  def metricsCollectCriticalEnabled: Boolean = get(METRICS_COLLECT_CRITICAL_ENABLED)
  def metricsCapacity: Int = get(METRICS_CAPACITY)
  def metricsExtraLabels: Map[String, String] =
    get(METRICS_EXTRA_LABELS).map(Utils.parseMetricLabels).toMap
  def metricsWorkerAppTopResourceConsumptionCount: Int =
    get(METRICS_WORKER_APP_TOP_RESOURCE_CONSUMPTION_COUNT)
  def metricsWorkerForceAppendPauseSpentTimeThreshold: Int =
    get(METRICS_WORKER_PAUSE_SPENT_TIME_FORCE_APPEND_THRESHOLD)
  def metricsJsonPrettyEnabled: Boolean = get(METRICS_JSON_PRETTY_ENABLED)

  // //////////////////////////////////////////////////////
  //                      Quota                         //
  // //////////////////////////////////////////////////////
  def quotaEnabled: Boolean = get(QUOTA_ENABLED)
  def quotaInterruptShuffleEnabled: Boolean = get(QUOTA_INTERRUPT_SHUFFLE_ENABLED)

  // //////////////////////////////////////////////////////
  //                      Identity                       //
  // //////////////////////////////////////////////////////
  def identityProviderClass: String = get(IDENTITY_PROVIDER)
  def userSpecificTenant: String = get(USER_SPECIFIC_TENANT)
  def userSpecificUserName: String = get(USER_SPECIFIC_USERNAME)

  // //////////////////////////////////////////////////////
  //                      Client                         //
  // //////////////////////////////////////////////////////
  def clientCloseIdleConnections: Boolean = get(CLIENT_CLOSE_IDLE_CONNECTIONS)
  def clientRegisterShuffleMaxRetry: Int = get(CLIENT_REGISTER_SHUFFLE_MAX_RETRIES)
  def clientRegisterShuffleRetryWaitMs: Long = get(CLIENT_REGISTER_SHUFFLE_RETRY_WAIT)
  def clientReserveSlotsRackAwareEnabled: Boolean = get(CLIENT_RESERVE_SLOTS_RACKAWARE_ENABLED)
  def clientReserveSlotsMaxRetries: Int = get(CLIENT_RESERVE_SLOTS_MAX_RETRIES)
  def clientReserveSlotsRetryWait: Long = get(CLIENT_RESERVE_SLOTS_RETRY_WAIT)
  def clientRequestCommitFilesMaxRetries: Int = get(CLIENT_COMMIT_FILE_REQUEST_MAX_RETRY)
  def clientCommitFilesIgnoreExcludedWorkers: Boolean = get(CLIENT_COMMIT_IGNORE_EXCLUDED_WORKERS)
  def clientShuffleDynamicResourceEnabled: Boolean =
    get(CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED)
  def clientShuffleDynamicResourceFactor: Double = get(CLIENT_SHUFFLE_DYNAMIC_RESOURCE_FACTOR)
  def appHeartbeatTimeoutMs: Long = get(APPLICATION_HEARTBEAT_TIMEOUT)
  def hdfsExpireDirsTimeoutMS: Long = get(HDFS_EXPIRE_DIRS_TIMEOUT)
  def dfsExpireDirsTimeoutMS: Long = get(DFS_EXPIRE_DIRS_TIMEOUT)
  def appHeartbeatIntervalMs: Long = get(APPLICATION_HEARTBEAT_INTERVAL)
  def applicationUnregisterEnabled: Boolean = get(APPLICATION_UNREGISTER_ENABLED)

  def clientCheckedUseAllocatedWorkers: Boolean = get(CLIENT_CHECKED_USE_ALLOCATED_WORKERS)
  def clientExcludedWorkerExpireTimeout: Long = get(CLIENT_EXCLUDED_WORKER_EXPIRE_TIMEOUT)
  def clientExcludeReplicaOnFailureEnabled: Boolean =
    get(CLIENT_EXCLUDE_PEER_WORKER_ON_FAILURE_ENABLED)
  def clientMrMaxPushData: Long = get(CLIENT_MR_PUSH_DATA_MAX)
  def clientApplicationUUIDSuffixEnabled: Boolean = get(CLIENT_APPLICATION_UUID_SUFFIX_ENABLED)

  def appUniqueIdWithUUIDSuffix(appId: String): String = {
    if (clientApplicationUUIDSuffixEnabled) {
      appId + "-" + UUID.randomUUID().toString.replaceAll("-", "")
    } else {
      appId
    }
  }

  // //////////////////////////////////////////////////////
  //               Shuffle Compression                   //
  // //////////////////////////////////////////////////////
  def shuffleCompressionCodec: CompressionCodec =
    CompressionCodec.valueOf(get(SHUFFLE_COMPRESSION_CODEC))
  def shuffleDecompressionLz4XXHashInstance: Option[String] =
    get(SHUFFLE_DECOMPRESSION_LZ4_XXHASH_INSTANCE)
  def shuffleCompressionZstdCompressLevel: Int = get(SHUFFLE_COMPRESSION_ZSTD_LEVEL)

  // //////////////////////////////////////////////////////
  //                Shuffle Client RPC                   //
  // //////////////////////////////////////////////////////
  def clientRpcCacheSize: Int = get(CLIENT_RPC_CACHE_SIZE)
  def clientRpcCacheConcurrencyLevel: Int = get(CLIENT_RPC_CACHE_CONCURRENCY_LEVEL)
  def clientRpcReserveSlotsRpcTimeout: RpcTimeout =
    new RpcTimeout(
      get(CLIENT_RESERVE_SLOTS_RPC_TIMEOUT).milli,
      CLIENT_RESERVE_SLOTS_RPC_TIMEOUT.key)

  def clientRpcRegisterShuffleAskTimeout: RpcTimeout =
    new RpcTimeout(
      get(CLIENT_RPC_REGISTER_SHUFFLE_ASK_TIMEOUT).milli,
      CLIENT_RPC_REGISTER_SHUFFLE_ASK_TIMEOUT.key)

  def clientRpcRequestPartitionLocationAskTimeout: RpcTimeout =
    new RpcTimeout(
      get(CLIENT_RPC_REQUEST_PARTITION_LOCATION_ASK_TIMEOUT).milli,
      CLIENT_RPC_REQUEST_PARTITION_LOCATION_ASK_TIMEOUT.key)

  def clientRpcGetReducerFileGroupAskTimeout: RpcTimeout =
    new RpcTimeout(
      get(CLIENT_RPC_GET_REDUCER_FILE_GROUP_ASK_TIMEOUT).milli,
      CLIENT_RPC_GET_REDUCER_FILE_GROUP_ASK_TIMEOUT.key)

  def clientRpcCommitFilesAskTimeout: RpcTimeout =
    new RpcTimeout(
      get(CLIENT_RPC_COMMIT_FILES_ASK_TIMEOUT).milli,
      CLIENT_RPC_COMMIT_FILES_ASK_TIMEOUT.key)

  // //////////////////////////////////////////////////////
  //               Shuffle Client Fetch                  //
  // //////////////////////////////////////////////////////
  def clientFetchTimeoutMs: Long = get(CLIENT_FETCH_TIMEOUT)
  def clientFetchBufferSize: Int = get(CLIENT_FETCH_BUFFER_SIZE).toInt
  def clientFetchMaxReqsInFlight: Int = get(CLIENT_FETCH_MAX_REQS_IN_FLIGHT)
  def clientFetchMaxRetriesForEachReplica: Int = get(CLIENT_FETCH_MAX_RETRIES_FOR_EACH_REPLICA)
  def clientStageRerunEnabled: Boolean = get(CLIENT_STAGE_RERUN_ENABLED)
  def clientFetchExcludeWorkerOnFailureEnabled: Boolean =
    get(CLIENT_FETCH_EXCLUDE_WORKER_ON_FAILURE_ENABLED)
  def clientFetchExcludedWorkerExpireTimeout: Long =
    get(CLIENT_FETCH_EXCLUDED_WORKER_EXPIRE_TIMEOUT)

  // //////////////////////////////////////////////////////
  //               Shuffle Client Push                   //
  // //////////////////////////////////////////////////////
  def clientPushReplicateEnabled: Boolean = get(CLIENT_PUSH_REPLICATE_ENABLED)
  def clientPushBufferInitialSize: Int = get(CLIENT_PUSH_BUFFER_INITIAL_SIZE).toInt
  def clientPushBufferMaxSize: Int = get(CLIENT_PUSH_BUFFER_MAX_SIZE).toInt
  def clientPushQueueCapacity: Int = get(CLIENT_PUSH_QUEUE_CAPACITY)
  def clientPushExcludeWorkerOnFailureEnabled: Boolean =
    get(CLIENT_PUSH_EXCLUDE_WORKER_ON_FAILURE_ENABLED)
  def clientPushMaxReqsInFlightPerWorker: Int = get(CLIENT_PUSH_MAX_REQS_IN_FLIGHT_PERWORKER)
  def clientPushMaxReqsInFlightTotal: Int = get(CLIENT_PUSH_MAX_REQS_IN_FLIGHT_TOTAL)
  def clientPushMaxReviveTimes: Int = get(CLIENT_PUSH_MAX_REVIVE_TIMES)
  def clientPushReviveInterval: Long = get(CLIENT_PUSH_REVIVE_INTERVAL)
  def clientPushReviveBatchSize: Int = get(CLIENT_PUSH_REVIVE_BATCHSIZE)
  def clientPushSortMemoryThreshold: Long = get(CLIENT_PUSH_SORT_MEMORY_THRESHOLD)
  def clientPushSortUseAdaptiveMemoryThreshold: Boolean =
    get(CLIENT_PUSH_SORT_USE_ADAPTIVE_MEMORY_THRESHOLD)
  def clientPushSortSmallPushTolerateFactor: Double =
    get(CLIENT_PUSH_SORT_SMALL_PUSH_TOLERATE_FACTOR)
  def clientPushSortMaxMemoryFactor: Double =
    get(CLIENT_PUSH_SORT_MAX_MEMORY_FACTOR)
  def clientPushSortRandomizePartitionIdEnabled: Boolean =
    get(CLIENT_PUSH_SORT_RANDOMIZE_PARTITION_ENABLED)
  def clientPushRetryThreads: Int = get(CLIENT_PUSH_RETRY_THREADS)
  def clientPushStageEndTimeout: Long = get(CLIENT_PUSH_STAGE_END_TIMEOUT)
  def clientPushUnsafeRowFastWrite: Boolean = get(CLIENT_PUSH_UNSAFEROW_FASTWRITE_ENABLED)
  def clientRpcCacheExpireTime: Long = get(CLIENT_RPC_CACHE_EXPIRE_TIME)
  def clientRpcSharedThreads: Int = get(CLIENT_RPC_SHARED_THREADS)
  def clientRpcMaxRetries: Int = get(CLIENT_RPC_MAX_RETIRES)
  def pushDataTimeoutMs: Long = get(CLIENT_PUSH_DATA_TIMEOUT)
  def clientPushLimitStrategy: String = get(CLIENT_PUSH_LIMIT_STRATEGY)
  def clientPushSlowStartInitialSleepTime: Long = get(CLIENT_PUSH_SLOW_START_INITIAL_SLEEP_TIME)
  def clientSlotAssignMaxWorkers: Int = get(CLIENT_SLOT_ASSIGN_MAX_WORKERS)
  def clientPushSlowStartMaxSleepMills: Long = get(CLIENT_PUSH_SLOW_START_MAX_SLEEP_TIME)
  def clientPushLimitInFlightTimeoutMs: Long =
    if (clientPushReplicateEnabled) {
      get(CLIENT_PUSH_LIMIT_IN_FLIGHT_TIMEOUT).getOrElse(
        pushDataTimeoutMs * clientPushMaxReviveTimes * 4)
    } else {
      get(CLIENT_PUSH_LIMIT_IN_FLIGHT_TIMEOUT).getOrElse(
        pushDataTimeoutMs * clientPushMaxReviveTimes * 2)
    }
  def clientPushLimitInFlightSleepDeltaMs: Long = get(CLIENT_PUSH_LIMIT_IN_FLIGHT_SLEEP_INTERVAL)
  def clientPushTakeTaskWaitIntervalMs: Long = get(CLIENT_PUSH_TAKE_TASK_WAIT_INTERVAL)
  def clientPushTakeTaskMaxWaitAttempts: Int = get(CLIENT_PUSH_TAKE_TASK_MAX_WAIT_ATTEMPTS)
  def clientPushSendBufferPoolExpireTimeout: Long = get(CLIENT_PUSH_SENDBUFFERPOOL_EXPIRETIMEOUT)
  def clientPushSendBufferPoolExpireCheckInterval: Long =
    get(CLIENT_PUSH_SENDBUFFERPOOL_CHECKEXPIREINTERVAL)
  def clientAdaptiveOptimizeSkewedPartitionReadEnabled: Boolean =
    get(CLIENT_ADAPTIVE_OPTIMIZE_SKEWED_PARTITION_READ_ENABLED)

  // //////////////////////////////////////////////////////
  //                   Client Shuffle                    //
  // //////////////////////////////////////////////////////
  def shuffleWriterMode: ShuffleMode = ShuffleMode.valueOf(get(SPARK_SHUFFLE_WRITER_MODE))
  def dynamicWriteModeEnabled =
    get(CLIENT_PUSH_DYNAMIC_WRITE_MODE_ENABLED)
  def dynamicWriteModePartitionNumThreshold =
    get(CLIENT_PUSH_DYNAMIC_WRITE_MODE_PARTITION_NUM_THRESHOLD)
  def shufflePartitionType: PartitionType = PartitionType.valueOf(get(SHUFFLE_PARTITION_TYPE))
  def shuffleRangeReadFilterEnabled: Boolean = get(SHUFFLE_RANGE_READ_FILTER_ENABLED)
  def shuffleForceFallbackEnabled: Boolean = get(SPARK_SHUFFLE_FORCE_FALLBACK_ENABLED)
  def checkWorkerEnabled: Boolean = get(CHECK_WORKER_ENABLED)
  def sparkShuffleFallbackPolicy: FallbackPolicy = {
    val fallbackPolicyGiven = FallbackPolicy.valueOf(get(SPARK_SHUFFLE_FALLBACK_POLICY))
    if (shuffleForceFallbackEnabled && FallbackPolicy.AUTO.equals(fallbackPolicyGiven)) {
      FallbackPolicy.ALWAYS
    } else {
      fallbackPolicyGiven
    }
  }
  def flinkShuffleFallbackPolicy: FallbackPolicy =
    FallbackPolicy.valueOf(get(FLINK_SHUFFLE_FALLBACK_POLICY))

  def shuffleFallbackPartitionThreshold: Long = get(SPARK_SHUFFLE_FALLBACK_PARTITION_THRESHOLD)
  def shuffleExpiredCheckIntervalMs: Long = get(SHUFFLE_EXPIRED_CHECK_INTERVAL)
  def shuffleManagerPort: Int = get(CLIENT_SHUFFLE_MANAGER_PORT)
  def shuffleChunkSize: Long = get(SHUFFLE_CHUNK_SIZE)
  def dfsReadChunkSize: Long = get(CLIENT_FETCH_DFS_READ_CHUNK_SIZE)
  def shufflePartitionSplitMode: PartitionSplitMode =
    PartitionSplitMode.valueOf(get(SHUFFLE_PARTITION_SPLIT_MODE))
  def shufflePartitionSplitThreshold: Long = get(SHUFFLE_PARTITION_SPLIT_THRESHOLD)
  def batchHandleChangePartitionEnabled: Boolean = get(CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED)
  def batchHandleChangePartitionBuckets: Int =
    get(CLIENT_BATCH_HANDLE_CHANGE_PARTITION_BUCKETS)
  def batchHandleChangePartitionNumThreads: Int = get(CLIENT_BATCH_HANDLE_CHANGE_PARTITION_THREADS)
  def batchHandleChangePartitionRequestInterval: Long =
    get(CLIENT_BATCH_HANDLE_CHANGE_PARTITION_INTERVAL)
  def batchHandleRemoveExpiredShufflesEnabled: Boolean =
    get(CLIENT_BATCH_REMOVE_EXPIRED_SHUFFLE_ENABLED)
  def batchHandleCommitPartitionEnabled: Boolean = get(CLIENT_BATCH_HANDLE_COMMIT_PARTITION_ENABLED)
  def batchHandleCommitPartitionNumThreads: Int = get(CLIENT_BATCH_HANDLE_COMMIT_PARTITION_THREADS)
  def batchHandleCommitPartitionRequestInterval: Long =
    get(CLIENT_BATCH_HANDLED_COMMIT_PARTITION_INTERVAL)
  def batchHandleReleasePartitionEnabled: Boolean =
    get(CLIENT_BATCH_HANDLE_RELEASE_PARTITION_ENABLED)
  def batchHandleReleasePartitionNumThreads: Int =
    get(CLIENT_BATCH_HANDLE_RELEASE_PARTITION_THREADS)
  def batchHandleReleasePartitionRequestInterval: Long =
    get(CLIENT_BATCH_HANDLED_RELEASE_PARTITION_INTERVAL)
  def enableReadLocalShuffleFile: Boolean = get(READ_LOCAL_SHUFFLE_FILE)
  def readLocalShuffleThreads: Int = get(READ_LOCAL_SHUFFLE_THREADS)
  def readStreamCreatorPoolThreads: Int = get(READ_STREAM_CREATOR_POOL_THREADS)

  def registerShuffleFilterExcludedWorkerEnabled: Boolean =
    get(REGISTER_SHUFFLE_FILTER_EXCLUDED_WORKER_ENABLED)
  def reviseLostShufflesEnabled: Boolean = get(REVISE_LOST_SHUFFLES_ENABLED)

  // //////////////////////////////////////////////////////
  //                       Worker                        //
  // //////////////////////////////////////////////////////

  /**
   * @return workingDir, usable space, flusher thread count, disk type
   *         check more details at CONFIGURATION_GUIDE.md
   */
  def workerBaseDirs: Seq[(String, Long, Int, Type)] = {
    // I assume there is no disk is bigger than 1 PB in recent days.
    val defaultMaxCapacity = Utils.byteStringAsBytes("1PB")
    get(WORKER_STORAGE_DIRS).map { storageDirs: Seq[String] =>
      storageDirs.map { str =>
        var maxCapacity = defaultMaxCapacity
        var diskType = HDD
        var flushThread = get(WORKER_FLUSHER_THREADS)
        val (dir, attributes) = str.split(":").toList match {
          case _dir :: tail => (_dir, tail)
          case nil => throw new IllegalArgumentException(s"Illegal storage dir: $nil")
        }
        var flushThreadsDefined = false
        attributes.foreach {
          case capacityStr if capacityStr.toLowerCase.startsWith("capacity=") =>
            maxCapacity = Utils.byteStringAsBytes(capacityStr.split("=")(1))
          case diskTypeStr if diskTypeStr.toLowerCase.startsWith("disktype=") =>
            diskType = Type.valueOf(diskTypeStr.split("=")(1))
            if (diskType == Type.MEMORY) {
              throw new IOException(s"Invalid diskType: $diskType")
            }
            if (!flushThreadsDefined) {
              flushThread = diskType match {
                case HDD => workerHddFlusherThreads
                case SSD => workerSsdFlusherThreads
                case _ => flushThread
              }
            }
          case threadCountStr if threadCountStr.toLowerCase.startsWith("flushthread=") =>
            flushThread = threadCountStr.split("=")(1).toInt
            flushThreadsDefined = true
          case illegal =>
            throw new IllegalArgumentException(s"Illegal attribute: $illegal")
        }
        (dir, maxCapacity, flushThread, diskType)
      }
    }.getOrElse {
      if (!hasHDFSStorage && !hasS3Storage) {
        val prefix = workerStorageBaseDirPrefix
        val number = workerStorageBaseDirNumber
        (1 to number).map { i =>
          (s"$prefix$i", defaultMaxCapacity, workerHddFlusherThreads, HDD)
        }
      } else {
        Seq.empty
      }
    }
  }

  def partitionSplitMinimumSize: Long = get(WORKER_PARTITION_SPLIT_MIN_SIZE)
  def partitionSplitMaximumSize: Long = get(WORKER_PARTITION_SPLIT_MAX_SIZE)

  def s3AccessKey: String = get(S3_ACCESS_KEY).getOrElse("")

  def s3SecretKey: String = get(S3_SECRET_KEY).getOrElse("")

  def s3EndpointRegion: String = get(S3_ENDPOINT_REGION).getOrElse("")

  def s3MultiplePartUploadMaxRetries: Int = get(S3_MPU_MAX_RETRIES)

  def s3Dir: String = {
    get(S3_DIR).map {
      s3Dir =>
        if (!Utils.isS3Path(s3Dir)) {
          log.error(s"${S3_DIR.key} configuration is wrong $s3Dir. Disable S3 support.")
          ""
        } else {
          s3Dir
        }
    }.getOrElse("")
  }

  def hdfsDir: String = {
    get(HDFS_DIR).map {
      hdfsDir =>
        if (!Utils.isHdfsPath(hdfsDir)) {
          log.error(s"${HDFS_DIR.key} configuration is wrong $hdfsDir. Disable HDFS support.")
          ""
        } else {
          hdfsDir
        }
    }.getOrElse("")
  }

  def workerStorageBaseDirPrefix: String = get(WORKER_STORAGE_BASE_DIR_PREFIX)
  def workerStorageBaseDirNumber: Int = get(WORKER_STORAGE_BASE_DIR_COUNT)
  def workerStorageExpireDirTimeout: Long = get(WORKER_STORAGE_EXPIRE_DIR_TIMEOUT)
  def creditStreamThreadsPerMountpoint: Int = get(WORKER_BUFFERSTREAM_THREADS_PER_MOUNTPOINT)
  def workerDirectMemoryRatioForReadBuffer: Double = get(WORKER_DIRECT_MEMORY_RATIO_FOR_READ_BUFFER)
  def partitionReadBuffersMin: Int = get(WORKER_PARTITION_READ_BUFFERS_MIN)
  def partitionReadBuffersMax: Int = get(WORKER_PARTITION_READ_BUFFERS_MAX)
  def readBufferAllocationWait: Long = get(WORKER_READBUFFER_ALLOCATIONWAIT)
  def readBufferTargetRatio: Double = get(WORKER_READBUFFER_TARGET_RATIO)
  def readBufferTargetUpdateInterval: Long = get(WORKER_READBUFFER_TARGET_UPDATE_INTERVAL)
  def readBufferTargetNotifyThreshold: Long = get(WORKER_READBUFFER_TARGET_NOTIFY_THRESHOLD)
  def readBuffersToTriggerReadMin: Int = get(WORKER_READBUFFERS_TOTRIGGERREAD_MIN)
  def readBufferDispatcherCheckThreadInterval: Long = get(WORKER_READBUFFER_CHECK_THREAD_INTERVAL)
  def workerStoragePolicyCreateFilePolicy: Option[List[String]] =
    get(WORKER_STORAGE_CREATE_FILE_POLICY).map {
      policy => policy.split(",").map(_.trim).toList
    }.orElse(Some(List("MEMORY", "HDD", "SSD", "HDFS", "OSS")))

  def workerStoragePolicyEvictFilePolicy: Option[Map[String, List[String]]] =
    get(WORKER_STORAGE_EVICT_POLICY).map {
      policy =>
        policy.split("\\|").map(group => {
          val groupArr = group.split(",")
          Map(groupArr.head -> groupArr.slice(1, groupArr.length).toList)
        }).reduce(_ ++ _)
    }.orElse(Some(Map("MEMORY" -> List("SSD", "HDD", "HDFS", "OSS"))))

  // //////////////////////////////////////////////////////
  //                   Decommission                      //
  // //////////////////////////////////////////////////////
  def workerDecommissionCheckInterval: Long = get(WORKER_DECOMMISSION_CHECK_INTERVAL)
  def workerDecommissionForceExitTimeout: Long = get(WORKER_DECOMMISSION_FORCE_EXIT_TIMEOUT)

  // //////////////////////////////////////////////////////
  //            Graceful Shutdown & Recover              //
  // //////////////////////////////////////////////////////
  def workerGracefulShutdown: Boolean = get(WORKER_GRACEFUL_SHUTDOWN_ENABLED)
  def workerGracefulShutdownTimeoutMs: Long = get(WORKER_GRACEFUL_SHUTDOWN_TIMEOUT)
  def workerGracefulShutdownCheckSlotsFinishedInterval: Long =
    get(WORKER_CHECK_SLOTS_FINISHED_INTERVAL)
  def workerGracefulShutdownCheckSlotsFinishedTimeoutMs: Long =
    get(WORKER_CHECK_SLOTS_FINISHED_TIMEOUT)
  def workerGracefulShutdownRecoverPath: String = get(WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH)
  def workerGracefulShutdownRecoverDbBackend: String =
    get(WORKER_GRACEFUL_SHUTDOWN_RECOVER_DB_BACKEND)
  def workerGracefulShutdownPartitionSorterCloseAwaitTimeMs: Long =
    get(WORKER_PARTITION_SORTER_SHUTDOWN_TIMEOUT)
  def workerGracefulShutdownFlusherShutdownTimeoutMs: Long = get(WORKER_FLUSHER_SHUTDOWN_TIMEOUT)
  def workerGracefulShutdownSaveCommittedFileInfoInterval: Long =
    get(WORKER_GRACEFUL_SHUTDOWN_SAVE_COMMITTED_FILEINFO_INTERVAL)
  def workerGracefulShutdownSaveCommittedFileInfoSync: Boolean =
    get(WORKER_GRACEFUL_SHUTDOWN_SAVE_COMMITTED_FILEINFO_SYNC)

  // //////////////////////////////////////////////////////
  //                      Flusher                        //
  // //////////////////////////////////////////////////////
  def workerFlusherBufferSize: Long = get(WORKER_FLUSHER_BUFFER_SIZE)
  def workerHdfsFlusherBufferSize: Long = get(WORKER_HDFS_FLUSHER_BUFFER_SIZE)
  def workerS3FlusherBufferSize: Long = get(WORKER_S3_FLUSHER_BUFFER_SIZE)
  def workerWriterCloseTimeoutMs: Long = get(WORKER_WRITER_CLOSE_TIMEOUT)
  def workerHddFlusherThreads: Int = get(WORKER_FLUSHER_HDD_THREADS)
  def workerSsdFlusherThreads: Int = get(WORKER_FLUSHER_SSD_THREADS)
  def workerHdfsFlusherThreads: Int = get(WORKER_FLUSHER_HDFS_THREADS)
  def workerS3FlusherThreads: Int = get(WORKER_FLUSHER_S3_THREADS)
  def workerCreateWriterMaxAttempts: Int = get(WORKER_WRITER_CREATE_MAX_ATTEMPTS)

  // //////////////////////////////////////////////////////
  //                    Disk Monitor                     //
  // //////////////////////////////////////////////////////
  def workerDiskTimeSlidingWindowSize: Int = get(WORKER_DISKTIME_SLIDINGWINDOW_SIZE)
  def workerDiskTimeSlidingWindowMinFlushCount: Int =
    get(WORKER_DISKTIME_SLIDINGWINDOW_MINFLUSHCOUNT)
  def workerDiskTimeSlidingWindowMinFetchCount: Int =
    get(WORKER_DISKTIME_SLIDINGWINDOW_MINFETCHCOUNT)
  def workerDiskReserveSize: Long = get(WORKER_DISK_RESERVE_SIZE)
  def workerDiskReserveRatio: Option[Double] = get(WORKER_DISK_RESERVE_RATIO)
  def workerDiskCleanThreads: Int = get(WORKER_DISK_CLEAN_THREADS)
  def workerDiskMonitorEnabled: Boolean = get(WORKER_DISK_MONITOR_ENABLED)
  def workerDiskMonitorCheckList: Seq[String] = get(WORKER_DISK_MONITOR_CHECKLIST)
  def workerDiskMonitorCheckInterval: Long = get(WORKER_DISK_MONITOR_CHECK_INTERVAL)
  def workerDiskMonitorSysBlockDir: String = get(WORKER_DISK_MONITOR_SYS_BLOCK_DIR)
  def workerDiskMonitorNotifyErrorThreshold: Int = get(WORKER_DISK_MONITOR_NOTIFY_ERROR_THRESHOLD)
  def workerDiskMonitorNotifyErrorExpireTimeout: Long =
    get(WORKER_DISK_MONITOR_NOTIFY_ERROR_EXPIRE_TIMEOUT)
  def workerDiskMonitorStatusCheckTimeout: Long = get(WORKER_DEVICE_STATUS_CHECK_TIMEOUT)

  // //////////////////////////////////////////////////////
  //                  Memory Manager                     //
  // //////////////////////////////////////////////////////
  def workerDirectMemoryRatioToPauseReceive: Double = get(WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE)
  def workerDirectMemoryRatioToPauseReplicate: Double =
    get(WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE)
  def workerDirectMemoryRatioToResume: Double = get(WORKER_DIRECT_MEMORY_RATIO_RESUME)
  def workerPinnedMemoryRatioToResume: Double = get(WORKER_PINNED_MEMORY_RATIO_RESUME)
  def workerPartitionSorterDirectMemoryRatioThreshold: Double =
    get(WORKER_PARTITION_SORTER_DIRECT_MEMORY_RATIO_THRESHOLD)
  def workerDirectMemoryPressureCheckIntervalMs: Long = get(WORKER_DIRECT_MEMORY_CHECK_INTERVAL)
  def workerPinnedMemoryCheckEnabled: Boolean = get(WORKER_PINNED_MEMORY_CHECK_ENABLED)
  def workerPinnedMemoryCheckIntervalMs: Long = get(WORKER_PINNED_MEMORY_CHECK_INTERVAL)
  def workerDirectMemoryReportIntervalSecond: Long = get(WORKER_DIRECT_MEMORY_REPORT_INTERVAL)
  def workerDirectMemoryTrimChannelWaitInterval: Long =
    get(WORKER_DIRECT_MEMORY_TRIM_CHANNEL_WAIT_INTERVAL)
  def workerDirectMemoryTrimFlushWaitInterval: Long =
    get(WORKER_DIRECT_MEMORY_TRIM_FLUSH_WAIT_INTERVAL)
  def workerDirectMemoryRatioForMemoryFilesStorage: Double =
    get(WORKER_DIRECT_MEMORY_RATIO_FOR_MEMORY_FILE_STORAGE)
  def workerMemoryFileStorageMaxFileSize: Long =
    get(WORKER_MEMORY_FILE_STORAGE_MAX_FILE_SIZE)
  def workerMemoryFileStorageEictAggressiveModeEnabled: Boolean =
    get(WORKER_MEMORY_FILE_STORAGE_EVICT_AGGRESSIVE_MODE_ENABLED)
  def workerMemoryFileStorageEvictRatio: Double =
    get(WORKER_MEMORY_FILE_STORAGE_EVICT_RATIO)

  // //////////////////////////////////////////////////////
  //                  Rate Limit controller              //
  // //////////////////////////////////////////////////////
  def workerCongestionControlEnabled: Boolean = get(WORKER_CONGESTION_CONTROL_ENABLED)
  def workerCongestionControlSampleTimeWindowSeconds: Long =
    get(WORKER_CONGESTION_CONTROL_SAMPLE_TIME_WINDOW)
  // TODO related to `WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE`,
  // `WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE`and `WORKER_DIRECT_MEMORY_RATIO_RESUME`,
  // we'd better refine the logic among them
  def workerCongestionControlDiskBufferLowWatermark: Long =
    get(WORKER_CONGESTION_CONTROL_DISK_BUFFER_LOW_WATERMARK)
  def workerCongestionControlDiskBufferHighWatermark: Long =
    get(WORKER_CONGESTION_CONTROL_DISK_BUFFER_HIGH_WATERMARK)
  def workerCongestionControlUserProduceSpeedLowWatermark: Long =
    get(WORKER_CONGESTION_CONTROL_USER_PRODUCE_SPEED_LOW_WATERMARK)
  def workerCongestionControlUserProduceSpeedHighWatermark: Long =
    get(WORKER_CONGESTION_CONTROL_USER_PRODUCE_SPEED_HIGH_WATERMARK)
  def workerCongestionControlWorkerProduceSpeedLowWatermark: Long =
    get(WORKER_CONGESTION_CONTROL_WORKER_PRODUCE_SPEED_LOW_WATERMARK)
  def workerCongestionControlWorkerProduceSpeedHighWatermark: Long =
    get(WORKER_CONGESTION_CONTROL_WORKER_PRODUCE_SPEED_HIGH_WATERMARK)

  def workerCongestionControlUserInactiveIntervalMs: Long =
    get(WORKER_CONGESTION_CONTROL_USER_INACTIVE_INTERVAL)
  def workerCongestionControlCheckIntervalMs: Long = get(WORKER_CONGESTION_CONTROL_CHECK_INTERVAL)

  // //////////////////////////////////////////////////////
  //                 Columnar Shuffle                    //
  // //////////////////////////////////////////////////////
  def columnarShuffleEnabled: Boolean = get(COLUMNAR_SHUFFLE_ENABLED)
  def columnarShuffleBatchSize: Int = get(COLUMNAR_SHUFFLE_BATCH_SIZE)
  def columnarShuffleOffHeapEnabled: Boolean = get(COLUMNAR_SHUFFLE_OFF_HEAP_ENABLED)
  def columnarShuffleDictionaryEnabled: Boolean = get(COLUMNAR_SHUFFLE_DICTIONARY_ENCODING_ENABLED)
  def columnarShuffleDictionaryMaxFactor: Double =
    get(COLUMNAR_SHUFFLE_DICTIONARY_ENCODING_MAX_FACTOR)

  def columnarShuffleCodeGenEnabled: Boolean = get(COLUMNAR_SHUFFLE_CODEGEN_ENABLED)

  // //////////////////////////////////////////////////////
  //                      test                           //
  // //////////////////////////////////////////////////////
  def testFetchFailure: Boolean = get(TEST_CLIENT_FETCH_FAILURE)
  def testMockDestroySlotsFailure: Boolean = get(TEST_CLIENT_MOCK_DESTROY_SLOTS_FAILURE)
  def testMockCommitFilesFailure: Boolean = get(TEST_MOCK_COMMIT_FILES_FAILURE)
  def testMockShuffleLost: Boolean = get(TEST_CLIENT_MOCK_SHUFFLE_LOST)
  def testMockShuffleLostShuffle: Int = get(TEST_CLIENT_MOCK_SHUFFLE_LOST_SHUFFLE)
  def testPushPrimaryDataTimeout: Boolean = get(TEST_CLIENT_PUSH_PRIMARY_DATA_TIMEOUT)
  def testPushReplicaDataTimeout: Boolean = get(TEST_WORKER_PUSH_REPLICA_DATA_TIMEOUT)
  def testRetryRevive: Boolean = get(TEST_CLIENT_RETRY_REVIVE)
  def testAlternative: String = get(TEST_ALTERNATIVE.key, "celeborn")
  def clientFlinkMemoryPerResultPartition: Long = get(CLIENT_MEMORY_PER_RESULT_PARTITION)
  def clientFlinkMemoryPerInputGate: Long = get(CLIENT_MEMORY_PER_INPUT_GATE)
  def clientFlinkNumConcurrentReading: Int = get(CLIENT_NUM_CONCURRENT_READINGS)
  def clientFlinkInputGateSupportFloatingBuffer: Boolean =
    get(CLIENT_INPUT_GATE_SUPPORT_FLOATING_BUFFER)
  def clientFlinkResultPartitionSupportFloatingBuffer: Boolean =
    get(CLIENT_RESULT_PARTITION_SUPPORT_FLOATING_BUFFER)
  def clientFlinkDataCompressionEnabled: Boolean = get(CLIENT_DATA_COMPRESSION_ENABLED)
  def clientShuffleMapPartitionSplitEnabled = get(CLIENT_SHUFFLE_MAPPARTITION_SPLIT_ENABLED)
  def clientChunkPrefetchEnabled = get(CLIENT_CHUNK_PREFETCH_ENABLED)
  def clientInputStreamCreationWindow = get(CLIENT_INPUTSTREAM_CREATION_WINDOW)

  def tagsEnabled: Boolean = get(TAGS_ENABLED)
  def tagsExpr: String = get(TAGS_EXPR)
  def preferClientTagsExpr: Boolean = get(PREFER_CLIENT_TAGS_EXPR)

  // //////////////////////////////////////////////////////
  //                    kerberos                         //
  // //////////////////////////////////////////////////////
  def hdfsStorageKerberosPrincipal = get(HDFS_STORAGE_KERBEROS_PRINCIPAL)
  def hdfsStorageKerberosKeytab = get(HDFS_STORAGE_KERBEROS_KEYTAB)

  // //////////////////////////////////////////////////////
  //                     TLS                             //
  // //////////////////////////////////////////////////////
  private def getSslConfig[V](config: ConfigEntry[V], module: String): V = {
    val valueOpt = getTransportConfImpl(module, config, config.valueConverter, allowDefault = false)

    if (valueOpt.isDefined) {
      return valueOpt.get
    }

    // Try without <module>, and if missing, use default

    // replace the module wildcard and check for global value
    val globalKey = config.key.replace(".<module>.", ".")
    val defaultValue = if (config.defaultValue.isDefined) config.defaultValueString else null
    config.valueConverter(get(globalKey, defaultValue))
  }

  private def asFileOrNull(fileName: Option[String]): File = {
    fileName.map(new File(_)).orNull
  }

  /**
   * Whether Secure (SSL/TLS) wire communication is enabled.
   */
  def sslEnabled(module: String): Boolean = {
    getSslConfig(SSL_ENABLED, module)
  }

  /**
   * SSL protocol (remember that SSLv3 was compromised) supported by Java
   */
  def sslProtocol(module: String): String = {
    getSslConfig(SSL_PROTOCOL, module)
  }

  /**
   * A comma separated list of ciphers
   */
  def sslRequestedCiphers(module: String): Array[String] = {
    getSslConfig(SSL_ENABLED_CIPHERS, module).map(_.split(",")).orNull
  }

  /**
   * The key-store file; can be relative to the current directory
   */
  def sslKeyStore(module: String): File = {
    val keyStore = getSslConfig(SSL_KEY_STORE, module)
    asFileOrNull(keyStore)
  }

  /**
   * The password to the key-store file
   */
  def sslKeyStorePassword(module: String): String = {
    getSslConfig(SSL_KEY_STORE_PASSWORD, module).orNull
  }

  /**
   * The trust-store file; can be relative to the current directory
   */
  def sslTrustStore(module: String): File = {
    asFileOrNull(getSslConfig(SSL_TRUST_STORE, module))
  }

  /**
   * The password to the trust-store file
   */
  def sslTrustStorePassword(module: String): String = {
    getSslConfig(SSL_TRUST_STORE_PASSWORD, module).orNull
  }

  /**
   * If using a trust-store that that reloads its configuration is enabled. If true,
   * when the trust-store file on disk changes, it will be reloaded
   */
  def sslTrustStoreReloadingEnabled(module: String): Boolean = {
    getSslConfig(SSL_TRUST_STORE_RELOADING_ENABLED, module)
  }

  /**
   * The interval, in milliseconds, the trust-store will reload its configuration
   */
  def sslTrustStoreReloadIntervalMs(module: String): Int = {
    getSslConfig(SSL_TRUST_STORE_RELOAD_INTERVAL_MS, module).toInt
  }

  /**
   * Internal config: the max size when chunking the stream with SSL
   */
  def maxSslEncryptedBlockSize(module: String): Int = {
    getSslConfig(MAX_SSL_ENCRYPTED_BLOCK_SIZE, module).toInt
  }

  def isAutoSslEnabled(module: String): Boolean = {
    getSslConfig(AUTO_SSL_ENABLED, module)
  }

  // //////////////////////////////////////////////////////
  //               Authentication                        //
  // //////////////////////////////////////////////////////
  def authEnabled: Boolean = {
    val authEnabled = get(AUTH_ENABLED)
    val internalPortEnabled = get(INTERNAL_PORT_ENABLED)
    if (authEnabled && !internalPortEnabled) {
      throw new IllegalArgumentException(
        s"${AUTH_ENABLED.key} is true, but ${INTERNAL_PORT_ENABLED.key} is false")
    }
    authEnabled && internalPortEnabled
  }

  def masterSendApplicationMetaThreads: Int = get(MASTER_SEND_APPLICATION_META_THREADS)

  def authEnabledOnClient: Boolean = {
    get(AUTH_ENABLED)
  }

  def workerApplicationRegistryCacheSize: Int = get(WORKER_APPLICATION_REGISTRY_CACHE_SIZE)

  // //////////////////////////////////////////////////////
  //                     Internal Port                   //
  // //////////////////////////////////////////////////////
  def internalPortEnabled: Boolean = get(INTERNAL_PORT_ENABLED)

  def masterInternalEndpoints: Array[String] = get(MASTER_INTERNAL_ENDPOINTS).toArray

  def haMasterNodeInternalPort(nodeId: String): Int = {
    val key = HA_MASTER_NODE_INTERNAL_PORT.key.replace("<id>", nodeId)
    getInt(key, HA_MASTER_NODE_INTERNAL_PORT.defaultValue.get)
  }

  def masterInternalPort: Int = get(MASTER_INTERNAL_PORT)

  def workerInternalPort: Int = get(WORKER_INTERNAL_PORT)

  // //////////////////////////////////////////////////////
  //                     Rack Resolver                   //
  // //////////////////////////////////////////////////////
  def rackResolverRefreshInterval = get(RACKRESOLVER_REFRESH_INTERVAL)

  def logCelebornConfEnabled = get(LOG_CELEBORN_CONF_ENABLED)

  def secretRedactionPattern = get(SECRET_REDACTION_PATTERN)

  def containerInfoProviderClass = get(CONTAINER_INFO_PROVIDER)
}

object CelebornConf extends Logging {

  val TRANSPORT_MODULE_FALLBACKS: Map[String, String] = Map(
    TransportModuleConstants.RPC_SERVICE_MODULE -> TransportModuleConstants.RPC_MODULE,
    TransportModuleConstants.RPC_APP_MODULE -> TransportModuleConstants.RPC_MODULE,

    // Internally, RPC_APP_MODULE is split into RPC_LIFECYCLEMANAGER_MODULE and
    // RPC_APP_CLIENT_MODULE, though this is not exposed to users.
    TransportModuleConstants.RPC_LIFECYCLEMANAGER_MODULE -> TransportModuleConstants.RPC_APP_MODULE,
    TransportModuleConstants.RPC_APP_CLIENT_MODULE -> TransportModuleConstants.RPC_APP_MODULE,

    // only for testing
    "test_child_module" -> "test_parent_module")

  // The keys are modules are internal to Celeborn, and users are not expected to directly
  // configure them. The values give the user exposed module.
  val INTERNAL_TRANSPORT_MODULES: Map[String, String] = Map(
    TransportModuleConstants.RPC_LIFECYCLEMANAGER_MODULE -> TransportModuleConstants.RPC_APP_MODULE,
    TransportModuleConstants.RPC_APP_CLIENT_MODULE -> TransportModuleConstants.RPC_APP_MODULE)

  /**
   * Holds information about keys that have been deprecated and do not have a replacement.
   *
   * @param key                The deprecated key.
   * @param version            The version in which the key was deprecated.
   * @param deprecationMessage Message to include in the deprecation warning.
   */
  private case class DeprecatedConfig(
      key: String,
      version: String,
      deprecationMessage: String)

  /**
   * Information about an alternate configuration key that has been deprecated.
   *
   * @param key         The deprecated config key.
   * @param version     The version in which the key was deprecated.
   * @param translation A translation function for converting old config values into new ones.
   */
  private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)

  /**
   * Holds information about keys that have been removed.
   *
   * @param key          The removed config key.
   * @param version      The version in which key was removed.
   * @param defaultValue The default config value. It can be used to notice
   *                     users that they set non-default value to an already removed config.
   * @param comment      Additional info regarding to the removed config.
   */
  case class RemovedConfig(key: String, version: String, defaultValue: String, comment: String)

  /**
   * Maps deprecated config keys to information about the deprecation.
   *
   * The extra information is logged as a warning when the config is present in the user's
   * configuration.
   */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig(
        "celeborn.worker.storage.baseDir.prefix",
        "0.4.0",
        "Please use celeborn.worker.storage.dirs"),
      DeprecatedConfig(
        "celeborn.worker.storage.baseDir.number",
        "0.4.0",
        "Please use celeborn.worker.storage.dirs"),
      DeprecatedConfig(
        "celeborn.client.spark.shuffle.forceFallback.enabled",
        "0.5.0",
        "Please use celeborn.client.spark.shuffle.fallback.policy"),
      DeprecatedConfig(
        "celeborn.shuffle.forceFallback.enabled",
        "0.5.0",
        "Please use celeborn.client.spark.shuffle.fallback.policy"),
      DeprecatedConfig(
        "celeborn.worker.congestionControl.low.watermark",
        "0.6.0",
        "Please use celeborn.worker.congestionControl.diskBuffer.low.watermark"),
      DeprecatedConfig(
        "celeborn.worker.congestionControl.high.watermark",
        "0.6.0",
        "Please use celeborn.worker.congestionControl.diskBuffer.high.watermark"),
      DeprecatedConfig(
        "celeborn.quota.identity.provider",
        "0.6.0",
        "Please use celeborn.identity.provider"),
      DeprecatedConfig(
        "celeborn.quota.identity.user-specific.tenant",
        "0.6.0",
        "Please use celeborn.identity.user-specific.tenant"),
      DeprecatedConfig(
        "celeborn.quota.identity.user-specific.userName",
        "0.6.0",
        "Please use celeborn.identity.user-specific.userName"))

    Map(configs.map { cfg => (cfg.key -> cfg) }: _*)
  }

  /**
   * The map contains info about removed SQL configs. Keys are SQL config names,
   * map values contain extra information like the version in which the config was removed,
   * config's default value and a comment.
   *
   * Please, add a removed configuration property here only when it affects behaviours.
   * By this, it makes migrations to new versions painless.
   */
  val removedConfigs: Map[String, RemovedConfig] = {
    val masterEndpointsTips = "The behavior is controlled by `celeborn.master.endpoints` now, " +
      "please check the documentation for details."
    val configs = Seq(
      RemovedConfig("rss.ha.master.hosts", "0.2.0", null, masterEndpointsTips),
      RemovedConfig("rss.ha.service.id", "0.2.0", "rss", "configuration key removed."),
      RemovedConfig("rss.ha.nodes.rss", "0.2.0", "1,2,3,", "configuration key removed."))
    Map(configs.map { cfg => cfg.key -> cfg }: _*)
  }

  /**
   * Maps a current config key to alternate keys that were used in previous version.
   *
   * The alternates are used in the order defined in this map. If deprecated configs are
   * present in the user's configuration, a warning is logged.
   */
  private val configsWithAlternatives = mutable.Map[String, Seq[AlternateConfig]](
    "none" -> Seq(
      AlternateConfig("none", "1.0")))

  /**
   * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
   * config keys.
   *
   * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
   */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  private def addDeprecatedConfig(entry: ConfigEntry[_], alt: (String, String => String)): Unit = {
    configsWithAlternatives.put(
      entry.key,
      configsWithAlternatives.getOrElse(entry.key, Seq.empty) :+ AlternateConfig(
        alt._1,
        entry.version,
        alt._2))
  }

  /**
   * Looks for available deprecated keys for the given config option, and return the first
   * value available.
   */
  def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst {
        case alt if conf.containsKey(alt.key) =>
          val value = conf.get(alt.key)
          if (alt.translation != null) alt.translation(value) else value
      }
    }
  }

  private def requireDefaultValueOfRemovedConf(key: String, value: String): Unit = {
    removedConfigs.get(key).foreach {
      case RemovedConfig(configName, version, defaultValue, comment) =>
        if (value != defaultValue) {
          throw new IllegalArgumentException(
            s"The config '$configName' was removed in v$version. $comment")
        }
    }
  }

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  private def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        s"The configuration key '$key' has been deprecated in v${cfg.version} and " +
          s"may be removed in the future. ${cfg.deprecationMessage}")
      return
    }

    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        s"The configuration key '$key' has been deprecated in v${cfg.version} and " +
          s"may be removed in the future. Please use the new key '$newKey' instead.")
      return
    }
  }

  private[this] val confEntriesUpdateLock = new Object

  @volatile
  private[celeborn] var confEntries: JMap[String, ConfigEntry[_]] = Collections.emptyMap()

  private def register(entry: ConfigEntry[_]): Unit = confEntriesUpdateLock.synchronized {
    require(
      !confEntries.containsKey(entry.key),
      s"Duplicate CelebornConfigEntry. ${entry.key} has been registered")
    val updatedMap = new JHashMap[String, ConfigEntry[_]](confEntries)
    updatedMap.put(entry.key, entry)
    confEntries = updatedMap

    entry.alternatives.foreach(addDeprecatedConfig(entry, _))
  }

  private[celeborn] def unregister(entry: ConfigEntry[_]): Unit =
    confEntriesUpdateLock.synchronized {
      val updatedMap = new JHashMap[String, ConfigEntry[_]](confEntries)
      updatedMap.remove(entry.key)
      confEntries = updatedMap

      configsWithAlternatives.remove(entry.key)
    }

  private[celeborn] def getConfigEntry(key: String): ConfigEntry[_] = {
    confEntries.get(key)
  }

  private[celeborn] def getConfigEntries: JCollection[ConfigEntry[_]] = {
    confEntries.values()
  }

  private[celeborn] def containsConfigEntry(entry: ConfigEntry[_]): Boolean = {
    getConfigEntry(entry.key) == entry
  }

  private[celeborn] def containsConfigKey(key: String): Boolean = {
    confEntries.containsKey(key)
  }

  private def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  val NETWORK_BIND_PREFER_IP: ConfigEntry[Boolean] =
    buildConf("celeborn.network.bind.preferIpAddress")
      .categories("network")
      .version("0.3.0")
      .doc("When `true`, prefer to use IP address, otherwise FQDN. This configuration only " +
        "takes effects when the bind hostname is not set explicitly, in such case, Celeborn " +
        "will find the first non-loopback address to bind.")
      .booleanConf
      .createWithDefault(true)

  val NETWORK_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.network.timeout")
      .categories("network")
      .version("0.2.0")
      .doc("Default timeout for network operations.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("240s")

  val NETWORK_CONNECT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.network.connect.timeout")
      .categories("network")
      .doc("Default socket connect timeout.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10s")

  val NETWORK_MEMORY_ALLOCATOR_ALLOW_CACHE: ConfigEntry[Boolean] =
    buildConf("celeborn.network.memory.allocator.allowCache")
      .categories("network")
      .internal
      .version("0.3.1")
      .doc("When false, globally disable thread-local cache in the shared PooledByteBufAllocator.")
      .booleanConf
      .createWithDefault(false)

  val NETWORK_MEMORY_ALLOCATOR_POOLED: ConfigEntry[Boolean] =
    buildConf("celeborn.network.memory.allocator.pooled")
      .categories("network")
      .internal
      .version("0.6.0")
      .doc("If disabled, always use UnpooledByteBufAllocator for aggressive memory reclamation, " +
        "this is helpful for cases that worker has high memory usage even after triming. " +
        "Disabling would cause performace degression and higher CPU usage.")
      .booleanConf
      .createWithDefault(true)

  val NETWORK_MEMORY_ALLOCATOR_SHARE: ConfigEntry[Boolean] =
    buildConf("celeborn.network.memory.allocator.share")
      .categories("network")
      .internal
      .version("0.3.0")
      .doc("Whether to share memory allocator.")
      .booleanConf
      .createWithDefault(true)

  val NETWORK_MEMORY_ALLOCATOR_ARENAS: OptionalConfigEntry[Int] =
    buildConf("celeborn.network.memory.allocator.numArenas")
      .categories("network")
      .version("0.3.0")
      .doc("Number of arenas for pooled memory allocator. Default value is Runtime.getRuntime.availableProcessors, min value is 2.")
      .intConf
      .createOptional

  val NETWORK_WILDCARD_ADDRESS_BIND: ConfigEntry[Boolean] =
    buildConf("celeborn.network.bind.wildcardAddress")
      .categories("network")
      .version("0.6.0")
      .doc("When `true`, the bind address will be set to a wildcard address, while the advertise address will " +
        "remain as whatever is set by `celeborn.network.advertise.preferIpAddress`. The wildcard address is a special " +
        "local IP address, and usually refers to 'any' and can only be used for bind operations. In the case of IPv4, " +
        "this is 0.0.0.0 and in the case of IPv6 this is ::0. This is helpful in dual-stack environments, where the " +
        "service must listen to both IPv4 and IPv6 clients.")
      .booleanConf
      .createWithDefault(false)

  val NETWORK_ADVERTISE_PREFER_IP: ConfigEntry[Boolean] =
    buildConf("celeborn.network.advertise.preferIpAddress")
      .categories("network")
      .version("0.6.0")
      .doc("When `true`, prefer to use IP address, otherwise FQDN for advertise address.")
      .fallbackConf(NETWORK_BIND_PREFER_IP)

  val NETWORK_MEMORY_ALLOCATOR_VERBOSE_METRIC: ConfigEntry[Boolean] =
    buildConf("celeborn.network.memory.allocator.verbose.metric")
      .categories("network")
      .version("0.3.0")
      .doc("Whether to enable verbose metric for pooled allocator.")
      .booleanConf
      .createWithDefault(false)

  val PORT_MAX_RETRY: ConfigEntry[Int] =
    buildConf("celeborn.port.maxRetries")
      .categories("network")
      .doc("When port is occupied, we will retry for max retry times.")
      .version("0.2.0")
      .intConf
      .createWithDefault(1)

  val RPC_IO_THREAD: OptionalConfigEntry[Int] =
    buildConf("celeborn.rpc.io.threads")
      .categories("network")
      .doc("Netty IO thread number of NettyRpcEnv to handle RPC request. " +
        "The default threads number is the number of runtime available processors.")
      .version("0.2.0")
      .intConf
      .createOptional

  val RPC_CONNECT_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.rpc.connect.threads")
      .categories("network")
      .version("0.2.0")
      .intConf
      .createWithDefault(64)

  val RPC_LOOKUP_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.rpc.lookupTimeout")
      .categories("network")
      .version("0.2.0")
      .doc("Timeout for RPC lookup operations.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val RPC_ASK_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.rpc.askTimeout")
      .categories("network")
      .version("0.2.0")
      .doc("Timeout for RPC ask operations. " +
        "It's recommended to set at least `240s` when `HDFS` is enabled in `celeborn.storage.availableTypes`")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  val RPC_DISPATCHER_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.rpc.dispatcher.threads")
      .withAlternative("celeborn.rpc.dispatcher.numThreads")
      .categories("network")
      .doc("Threads number of message dispatcher event loop. Default to 0, which is availableCore.")
      .version("0.3.0")
      .intConf
      .createWithDefault(0)

  val RPC_INBOX_CAPACITY: ConfigEntry[Int] =
    buildConf("celeborn.rpc.inbox.capacity")
      .categories("network")
      .doc("Specifies size of the in memory bounded capacity.")
      .version("0.5.0")
      .intConf
      .checkValue(
        v => v >= 0,
        "the capacity of inbox must be no less than 0, 0 means no limitation")
      .createWithDefault(0)

  val RPC_ROLE_DISPATHER_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.<role>.rpc.dispatcher.threads")
      .categories("network")
      .doc("Threads number of message dispatcher event loop for roles")
      .fallbackConf(RPC_DISPATCHER_THREADS)

  val RPC_SLOW_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.rpc.slow.threshold")
      .categories("network")
      .doc("threshold for RPC framework to log slow RPC")
      .version("0.6.0")
      .timeConf(TimeUnit.NANOSECONDS)
      .createWithDefaultString("1s")

  val RPC_SLOW_INTERVAL: OptionalConfigEntry[Long] =
    buildConf("celeborn.rpc.slow.interval")
      .categories("network")
      .doc("min interval (ms) for RPC framework to log slow RPC")
      .version("0.6.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  val RPC_SUMMARY_DUMP_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.rpc.dump.interval")
      .categories("network")
      .doc("min interval (ms) for RPC framework to dump performance summary")
      .version("0.6.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  val NETWORK_IO_MODE: OptionalConfigEntry[String] =
    buildConf("celeborn.<module>.io.mode")
      .categories("network")
      .doc("Netty EventLoopGroup backend, available options: NIO, EPOLL. If epoll mode is available, the default IO mode is EPOLL; otherwise, the default is NIO.")
      .stringConf
      .transform(_.toUpperCase)
      .checkValues(Set(IOMode.NIO.name(), IOMode.EPOLL.name()))
      .createOptional

  val NETWORK_IO_PREFER_DIRECT_BUFS: ConfigEntry[Boolean] =
    buildConf("celeborn.<module>.io.preferDirectBufs")
      .categories("network")
      .doc("If true, we will prefer allocating off-heap byte buffers within Netty. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
        s"works for shuffle client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
        s"works for master or worker. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push and fetch data. " +
        s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
        s"it works for worker receiving push data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate server or client of worker replicating data to peer worker. " +
        s"If setting <module> to `${TransportModuleConstants.FETCH_MODULE}`, " +
        s"it works for worker fetch server.")
      .booleanConf
      .createWithDefault(true)

  val NETWORK_IO_CONNECT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.<module>.io.connectTimeout")
      .categories("network")
      .doc("Socket connect timeout. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
        s"works for shuffle client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
        s"works for master or worker. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push and fetch data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for the replicate client of worker replicating data to peer worker.")
      .fallbackConf(NETWORK_CONNECT_TIMEOUT)

  val NETWORK_IO_CONNECTION_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.<module>.io.connectionTimeout")
      .categories("network")
      .doc("Connection active timeout. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
        s"works for shuffle client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
        s"works for master or worker. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push and fetch data. " +
        s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
        s"it works for worker receiving push data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate server or client of worker replicating data to peer worker. " +
        s"If setting <module> to `${TransportModuleConstants.FETCH_MODULE}`, " +
        s"it works for worker fetch server.")
      .fallbackConf(NETWORK_TIMEOUT)

  val NETWORK_IO_NUM_CONNECTIONS_PER_PEER: ConfigEntry[Int] =
    buildConf("celeborn.<module>.io.numConnectionsPerPeer")
      .categories("network")
      .doc("Number of concurrent connections between two nodes. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
        s"works for shuffle client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
        s"works for master or worker. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push and fetch data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate client of worker replicating data to peer worker.")
      .intConf
      .createWithDefault(1)

  val NETWORK_IO_BACKLOG: ConfigEntry[Int] =
    buildConf("celeborn.<module>.io.backLog")
      .categories("network")
      .doc(
        "Requested maximum length of the queue of incoming connections. Default 0 for no backlog. " +
          s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
          s"works for shuffle client. " +
          s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
          s"works for master or worker. " +
          s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
          s"it works for worker receiving push data. " +
          s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
          s"it works for replicate server of worker replicating data to peer worker. " +
          s"If setting <module> to `${TransportModuleConstants.FETCH_MODULE}`, " +
          s"it works for worker fetch server.")
      .intConf
      .createWithDefault(0)

  val NETWORK_IO_SERVER_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.<module>.io.serverThreads")
      .categories("network")
      .doc("Number of threads used in the server thread pool. Default to 0, which is 2x#cores. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
        s"works for shuffle client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
        s"works for master or worker. " +
        s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
        s"it works for worker receiving push data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate server of worker replicating data to peer worker. " +
        s"If setting <module> to `${TransportModuleConstants.FETCH_MODULE}`, " +
        s"it works for worker fetch server.")
      .intConf
      .createWithDefault(0)

  val NETWORK_IO_CLIENT_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.<module>.io.clientThreads")
      .categories("network")
      .doc("Number of threads used in the client thread pool. Default to 0, which is 2x#cores. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
        s"works for shuffle client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
        s"works for master or worker. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push and fetch data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate client of worker replicating data to peer worker.")
      .intConf
      .createWithDefault(0)

  val NETWORK_IO_RECEIVE_BUFFER: ConfigEntry[Long] =
    buildConf("celeborn.<module>.io.receiveBuffer")
      .categories("network")
      .doc("Receive buffer size (SO_RCVBUF). Note: the optimal size for receive buffer and send buffer " +
        "should be latency * network_bandwidth. Assuming latency = 1ms, network_bandwidth = 10Gbps " +
        "buffer size should be ~ 1.25MB. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
        s"works for shuffle client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
        s"works for master or worker. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push and fetch data. " +
        s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
        s"it works for worker receiving push data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate server or client of worker replicating data to peer worker. " +
        s"If setting <module> to `${TransportModuleConstants.FETCH_MODULE}`, " +
        s"it works for worker fetch server.")
      .version("0.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(0)

  val NETWORK_IO_SEND_BUFFER: ConfigEntry[Long] =
    buildConf("celeborn.<module>.io.sendBuffer")
      .categories("network")
      .doc("Send buffer size (SO_SNDBUF). " +
        s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
        s"works for shuffle client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
        s"works for master or worker. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push and fetch data. " +
        s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
        s"it works for worker receiving push data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate server or client of worker replicating data to peer worker. " +
        s"If setting <module> to `${TransportModuleConstants.FETCH_MODULE}`, " +
        s"it works for worker fetch server.")
      .version("0.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(0)

  val NETWORK_IO_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("celeborn.<module>.io.maxRetries")
      .categories("network")
      .doc(
        "Max number of times we will try IO exceptions (such as connection timeouts) per request. " +
          "If set to 0, we will not do any retries. " +
          s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
          s"it works for shuffle client push and fetch data. " +
          s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
          s"it works for replicate client of worker replicating data to peer worker. " +
          s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
          s"it works for Flink shuffle client push data.")
      .intConf
      .createWithDefault(3)

  val NETWORK_IO_RETRY_WAIT: ConfigEntry[Long] =
    buildConf("celeborn.<module>.io.retryWait")
      .categories("network")
      .doc("Time that we will wait in order to perform a retry after an IOException. " +
        "Only relevant if maxIORetries > 0. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push and fetch data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate client of worker replicating data to peer worker. " +
        s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
        s"it works for Flink shuffle client push data.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5s")

  val NETWORK_IO_LAZY_FD: ConfigEntry[Boolean] =
    buildConf("celeborn.<module>.io.lazyFD")
      .categories("network")
      .doc("Whether to initialize FileDescriptor lazily or not. If true, file descriptors are created only " +
        "when data is going to be transferred. This can reduce the number of open files. " +
        s"If setting <module> to `${TransportModuleConstants.FETCH_MODULE}`, " +
        s"it works for worker fetch server.")
      .booleanConf
      .createWithDefault(true)

  val NETWORK_VERBOSE_METRICS: ConfigEntry[Boolean] =
    buildConf("celeborn.<module>.io.enableVerboseMetrics")
      .categories("network")
      .doc("Whether to track Netty memory detailed metrics. If true, the detailed metrics of Netty " +
        "PoolByteBufAllocator will be gotten, otherwise only general memory usage will be tracked.")
      .booleanConf
      .createWithDefault(false)

  val NETWORK_IO_STORAGE_MEMORY_MAP_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.<module>.storage.memoryMapThreshold")
      .withAlternative("celeborn.storage.memoryMapThreshold")
      .categories("network")
      .internal
      .doc("Minimum size of a block that we should start using memory map rather than reading in through " +
        "normal IO operations. This prevents Celeborn from memory mapping very small blocks. In general, " +
        "memory mapping has high overhead for blocks close to or below the page size of the OS. " +
        s"If setting <module> to `${TransportModuleConstants.FETCH_MODULE}`, " +
        s"it works for worker fetch server.")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("2m")

  val MAX_CHUNKS_BEING_TRANSFERRED: OptionalConfigEntry[Long] =
    buildConf("celeborn.shuffle.io.maxChunksBeingTransferred")
      .categories("network")
      .doc("The max number of chunks allowed to be transferred at the same time on shuffle service. Note " +
        "that new incoming connections will be closed when the max number is hit. The client will retry " +
        "according to the shuffle retry configs (see `celeborn.<module>.io.maxRetries` and " +
        "`celeborn.<module>.io.retryWait`), if those limits are reached the task will fail with fetch failure.")
      .version("0.2.0")
      .longConf
      .createOptional

  val PUSH_TIMEOUT_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.<module>.push.timeoutCheck.interval")
      .categories("network")
      .doc("Interval for checking push data timeout. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push data. " +
        s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
        s"it works for Flink shuffle client push data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate client of worker replicating data to peer worker.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5s")

  val PUSH_TIMEOUT_CHECK_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.<module>.push.timeoutCheck.threads")
      .categories("network")
      .doc("Threads num for checking push data timeout. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push data. " +
        s"If setting <module> to `${TransportModuleConstants.PUSH_MODULE}`, " +
        s"it works for Flink shuffle client push data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate client of worker replicating data to peer worker.")
      .version("0.3.0")
      .intConf
      .createWithDefault(4)

  val FETCH_TIMEOUT_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.<module>.fetch.timeoutCheck.interval")
      .categories("network")
      .doc("Interval for checking fetch data timeout. " +
        s"It only support setting <module> to `${TransportModuleConstants.DATA_MODULE}` " +
        s"since it works for shuffle client fetch data.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5s")

  val FETCH_TIMEOUT_CHECK_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.<module>.fetch.timeoutCheck.threads")
      .categories("network")
      .doc("Threads num for checking fetch data timeout. " +
        s"It only support setting <module> to `${TransportModuleConstants.DATA_MODULE}` " +
        s"since it works for shuffle client fetch data.")
      .version("0.3.0")
      .intConf
      .createWithDefault(4)

  val CHANNEL_HEARTBEAT_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.<module>.heartbeat.interval")
      .withAlternative("celeborn.client.heartbeat.interval")
      .categories("network")
      .version("0.3.0")
      .doc("The heartbeat interval between worker and client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_APP_MODULE}`, " +
        s"works for shuffle client. " +
        s"If setting <module> to `${TransportModuleConstants.RPC_SERVICE_MODULE}`, " +
        s"works for master or worker. " +
        s"If setting <module> to `${TransportModuleConstants.DATA_MODULE}`, " +
        s"it works for shuffle client push and fetch data. " +
        s"If setting <module> to `${TransportModuleConstants.REPLICATE_MODULE}`, " +
        s"it works for replicate client of worker replicating data to peer worker. " +
        "If you are using the \"celeborn.client.heartbeat.interval\", " +
        "please use the new configs for each module according to your needs or " +
        "replace it with \"celeborn.rpc.heartbeat.interval\", " +
        "\"celeborn.data.heartbeat.interval\" and " +
        "\"celeborn.replicate.heartbeat.interval\". ")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  val MASTER_ENDPOINTS_RESOLVER: ConfigEntry[String] =
    buildConf("celeborn.master.endpoints.resolver")
      .categories("client", "worker")
      .doc("Resolver class that can be used for discovering and updating the master endpoints. This allows " +
        "users to provide a custom master endpoint resolver implementation. This is useful in environments " +
        "where the master nodes might change due to scaling operations or infrastructure updates. Clients " +
        "need to ensure that provided resolver class should be present in the classpath.")
      .version("0.6.0")
      .stringConf
      .checkValue(
        resolver => Utils.classIsLoadable(resolver),
        "Resolver class was not found in the classpath. Please check the class name " +
          "and ensure that it is present in classpath")
      .createWithDefault("org.apache.celeborn.common.client.StaticMasterEndpointResolver")

  val MASTER_ENDPOINTS: ConfigEntry[Seq[String]] =
    buildConf("celeborn.master.endpoints")
      .categories("client", "worker")
      .doc("Endpoints of master nodes for celeborn clients to connect. Client uses resolver provided by " +
        s"${MASTER_ENDPOINTS_RESOLVER.key} to resolve the master endpoints. By default Celeborn uses " +
        "`org.apache.celeborn.common.client.StaticMasterEndpointResolver` which take static master endpoints " +
        "as input. Allowed pattern: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. " +
        "If the port is omitted, 9097 will be used. If the master endpoints are not static then users can pass " +
        s"custom resolver implementation to discover master endpoints actively using ${MASTER_ENDPOINTS_RESOLVER.key}.")
      .version("0.2.0")
      .stringConf
      .toSequence
      .createWithDefaultString(s"<localhost>:9097")

  val MASTER_CLIENT_RPC_ASK_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.masterClient.rpc.askTimeout")
      .withAlternative("celeborn.rpc.haClient.askTimeout")
      .internal
      .categories("client", "worker")
      .version("0.3.0")
      .doc("Timeout for HA client RPC ask operations.")
      .fallbackConf(RPC_ASK_TIMEOUT)

  val MASTER_CLIENT_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("celeborn.masterClient.maxRetries")
      .withAlternative("celeborn.client.maxRetries")
      .internal
      .categories("client", "worker")
      .doc("Max retry times for client to connect master endpoint. The number of retries will be at least equal to the number of master endpoints.")
      .version("0.3.0")
      .intConf
      .createWithDefault(15)

  val APPLICATION_HEARTBEAT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.heartbeat.application.timeout")
      .withAlternative("celeborn.application.heartbeat.timeout")
      .categories("master")
      .version("0.3.0")
      .doc("Application heartbeat timeout.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("300s")

  val HDFS_EXPIRE_DIRS_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.hdfs.expireDirs.timeout")
      .categories("master")
      .version("0.3.0")
      .doc("The timeout for a expire dirs to be deleted on HDFS.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1h")

  val DFS_EXPIRE_DIRS_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.dfs.expireDirs.timeout")
      .categories("master")
      .version("0.6.0")
      .doc("The timeout for a expire dirs to be deleted on S3 or HDFS.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1h")

  val WORKER_HEARTBEAT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.heartbeat.worker.timeout")
      .withAlternative("celeborn.worker.heartbeat.timeout")
      .categories("master")
      .version("0.3.0")
      .doc("Worker heartbeat timeout.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  val ALLOW_WORKER_HOST_PATTERN: OptionalConfigEntry[Regex] =
    buildConf("celeborn.master.allowWorkerHostPattern")
      .categories("master")
      .version("0.6.0")
      .doc("Pattern of worker host that allowed to register with the master." +
        " If not set, all workers are allowed to register.")
      .regexConf
      .createOptional

  val DENY_WORKER_HOST_PATTERN: OptionalConfigEntry[Regex] =
    buildConf("celeborn.master.denyWorkerHostPattern")
      .categories("master")
      .version("0.6.0")
      .doc("Pattern of worker host that denied to register with the master." +
        " If not set, no workers are denied to register.")
      .regexConf
      .createOptional

  val WORKER_UNAVAILABLE_INFO_EXPIRE_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.workerUnavailableInfo.expireTimeout")
      .categories("master")
      .version("0.3.1")
      .doc("Worker unavailable info would be cleared when the retention period is expired." +
        " Set -1 to disable the expiration.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1800s")

  val MASTER_HOST: ConfigEntry[String] =
    buildConf("celeborn.master.host")
      .categories("master")
      .version("0.2.0")
      .doc("Hostname for master to bind.")
      .stringConf
      .createWithDefaultString("<localhost>")

  val MASTER_HTTP_HOST: ConfigEntry[String] =
    buildConf("celeborn.master.http.host")
      .withAlternative("celeborn.metrics.master.prometheus.host")
      .withAlternative("celeborn.master.metrics.prometheus.host")
      .categories("master")
      .version("0.4.0")
      .doc("Master's http host.")
      .stringConf
      .createWithDefaultString("<localhost>")

  val MASTER_PORT: ConfigEntry[Int] =
    buildConf("celeborn.master.port")
      .categories("master")
      .version("0.2.0")
      .doc("Port for master to bind.")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "Invalid port")
      .createWithDefault(9097)

  val MASTER_HTTP_PORT: ConfigEntry[Int] =
    buildConf("celeborn.master.http.port")
      .withAlternative("celeborn.metrics.master.prometheus.port")
      .withAlternative("celeborn.master.metrics.prometheus.port")
      .categories("master")
      .version("0.4.0")
      .doc("Master's http port.")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "Invalid port")
      .createWithDefault(9098)

  val MASTER_HTTP_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.master.http.maxWorkerThreads")
      .categories("master")
      .version("0.5.0")
      .doc("Maximum number of threads in the master http worker thread pool.")
      .intConf
      .checkValue(_ > 0, "Must be positive.")
      .createWithDefault(200)

  val MASTER_HTTP_STOP_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.http.stopTimeout")
      .categories("master")
      .version("0.5.0")
      .doc("Master http server stop timeout.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5s")

  val MASTER_HTTP_IDLE_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.http.idleTimeout")
      .categories("master")
      .version("0.5.0")
      .doc("Master http server idle timeout.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val MASTER_HTTP_AUTH_SUPPORTED_SCHEMES: ConfigEntry[Seq[String]] =
    buildConf("celeborn.master.http.auth.supportedSchemes")
      .categories("master")
      .version("0.6.0")
      .doc("A comma-separated list of master http auth supported schemes." +
        "<ul>" +
        " <li>SPNEGO: Kerberos/GSSAPI authentication.</li>" +
        " <li>BASIC: User-defined password authentication, the concreted implementation is" +
        " configurable via `celeborn.master.http.auth.basic.provider`.</li>" +
        " <li>BEARER: User-defined bearer token authentication, the concreted implementation is" +
        " configurable via `celeborn.master.http.auth.bearer.provider`.</li>" +
        "</ul>")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val MASTER_HTTP_SPNEGO_KEYTAB: OptionalConfigEntry[String] =
    buildConf("celeborn.master.http.spnego.keytab")
      .categories("master")
      .version("0.6.0")
      .doc("The keytab file for SPNego authentication.")
      .stringConf
      .createOptional

  val MASTER_HTTP_SPNEGO_PRINCIPAL: OptionalConfigEntry[String] =
    buildConf("celeborn.master.http.spnego.principal")
      .categories("master")
      .version("0.6.0")
      .doc("SPNego service principal, typical value would look like HTTP/_HOST@EXAMPLE.COM." +
        " SPNego service principal would be used when celeborn http authentication is enabled." +
        " This needs to be set only if SPNEGO is to be used in authentication.")
      .stringConf
      .createOptional

  val MASTER_HTTP_PROXY_CLIENT_IP_HEADER: ConfigEntry[String] =
    buildConf("celeborn.master.http.proxy.client.ip.header")
      .categories("master")
      .doc("The HTTP header to record the real client IP address. If your server is behind a load" +
        " balancer or other proxy, the server will see this load balancer or proxy IP address as" +
        " the client IP address, to get around this common issue, most load balancers or proxies" +
        " offer the ability to record the real remote IP address in an HTTP header that will be" +
        " added to the request for other devices to use. Note that, because the header value can" +
        " be specified to any IP address, so it will not be used for authentication.")
      .version("0.6.0")
      .stringConf
      .createWithDefault("X-Real-IP")

  val MASTER_HTTP_AUTH_BASIC_PROVIDER: ConfigEntry[String] =
    buildConf("celeborn.master.http.auth.basic.provider")
      .categories("master")
      .version("0.6.0")
      .doc("User-defined password authentication implementation of " +
        "org.apache.celeborn.spi.authentication.PasswdAuthenticationProvider")
      .stringConf
      .createWithDefault(classOf[AnonymousAuthenticationProviderImpl].getName)

  val MASTER_HTTP_AUTH_BEARER_PROVIDER: ConfigEntry[String] =
    buildConf("celeborn.master.http.auth.bearer.provider")
      .categories("master")
      .version("0.6.0")
      .doc("User-defined token authentication implementation of " +
        "org.apache.celeborn.spi.authentication.TokenAuthenticationProvider")
      .stringConf
      .createWithDefault(classOf[AnonymousAuthenticationProviderImpl].getName)

  val MASTER_HTTP_AUTH_ADMINISTERS: ConfigEntry[Seq[String]] =
    buildConf("celeborn.master.http.auth.administers")
      .categories("master")
      .version("0.6.0")
      .doc("A comma-separated list of users who have admin privileges," +
        s" Note, when ${MASTER_HTTP_AUTH_SUPPORTED_SCHEMES.key} is not set," +
        s" everyone is treated as administrator.")
      .stringConf
      .toSequence
      .createWithDefault(Seq.empty)

  val MASTER_HTTP_SSL_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.master.http.ssl.enabled")
      .categories("master")
      .version("0.6.0")
      .doc("Set this to true for using SSL encryption in http server.")
      .booleanConf
      .createWithDefault(false)

  val MASTER_HTTP_SSL_KEYSTORE_PATH: OptionalConfigEntry[String] =
    buildConf("celeborn.master.http.ssl.keystore.path")
      .categories("master")
      .version("0.6.0")
      .doc("SSL certificate keystore location.")
      .stringConf
      .createOptional

  val MASTER_HTTP_SSL_KEYSTORE_PASSWORD: OptionalConfigEntry[String] =
    buildConf("celeborn.master.http.ssl.keystore.password")
      .categories("master")
      .version("0.6.0")
      .doc("SSL certificate keystore password.")
      .stringConf
      .createOptional

  val MASTER_HTTP_SSL_KEYSTORE_TYPE: OptionalConfigEntry[String] =
    buildConf("celeborn.master.http.ssl.keystore.type")
      .categories("master")
      .version("0.6.0")
      .doc("SSL certificate keystore type.")
      .stringConf
      .createOptional

  val MASTER_HTTP_SSL_KEYSTORE_ALGORITHM: OptionalConfigEntry[String] =
    buildConf("celeborn.master.http.ssl.keystore.algorithm")
      .categories("master")
      .version("0.6.0")
      .doc("SSL certificate keystore algorithm.")
      .stringConf
      .createOptional

  val MASTER_HTTP_SSL_DISALLOWED_PROTOCOLS: ConfigEntry[Seq[String]] =
    buildConf("celeborn.master.http.ssl.disallowed.protocols")
      .categories("master")
      .version("0.6.0")
      .doc("SSL versions to disallow.")
      .stringConf
      .toSequence
      .createWithDefault(Seq("SSLv2", "SSLv3"))

  val MASTER_HTTP_SSL_INCLUDE_CIPHER_SUITES: ConfigEntry[Seq[String]] =
    buildConf("celeborn.master.http.ssl.include.ciphersuites")
      .categories("master")
      .version("0.6.0")
      .doc("A comma-separated list of include SSL cipher suite names.")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val HA_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.master.ha.enabled")
      .withAlternative("celeborn.ha.enabled")
      .categories("ha")
      .version("0.3.0")
      .doc("When true, master nodes run as Raft cluster mode.")
      .booleanConf
      .createWithDefault(false)

  val HA_MASTER_NODE_ID: OptionalConfigEntry[String] =
    buildConf("celeborn.master.ha.node.id")
      .withAlternative("celeborn.ha.master.node.id")
      .doc("Node id for master raft cluster in HA mode, if not define, " +
        "will be inferred by hostname.")
      .version("0.3.0")
      .stringConf
      .createOptional

  val HA_MASTER_NODE_HOST: ConfigEntry[String] =
    buildConf("celeborn.master.ha.node.<id>.host")
      .withAlternative("celeborn.ha.master.node.<id>.host")
      .categories("ha")
      .doc("Host to bind of master node <id> in HA mode.")
      .version("0.3.0")
      .stringConf
      .createWithDefaultString("<required>")

  val HA_MASTER_NODE_PORT: ConfigEntry[Int] =
    buildConf("celeborn.master.ha.node.<id>.port")
      .withAlternative("celeborn.ha.master.node.<id>.port")
      .categories("ha")
      .doc("Port to bind of master node <id> in HA mode.")
      .version("0.3.0")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "Invalid port")
      .createWithDefault(9097)

  val HA_MASTER_NODE_RATIS_HOST: OptionalConfigEntry[String] =
    buildConf("celeborn.master.ha.node.<id>.ratis.host")
      .withAlternative("celeborn.ha.master.node.<id>.ratis.host")
      .internal
      .categories("ha")
      .doc("Ratis host to bind of master node <id> in HA mode. If not provided, " +
        s"fallback to ${HA_MASTER_NODE_HOST.key}.")
      .version("0.3.0")
      .stringConf
      .createOptional

  val HA_MASTER_NODE_RATIS_PORT: ConfigEntry[Int] =
    buildConf("celeborn.master.ha.node.<id>.ratis.port")
      .withAlternative("celeborn.ha.master.node.<id>.ratis.port")
      .categories("ha")
      .doc("Ratis port to bind of master node <id> in HA mode.")
      .version("0.3.0")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "Invalid port")
      .createWithDefault(9872)

  val HA_MASTER_RATIS_RPC_TYPE: ConfigEntry[String] =
    buildConf("celeborn.master.ha.ratis.raft.rpc.type")
      .withAlternative("celeborn.ha.master.ratis.raft.rpc.type")
      .categories("ha")
      .doc("RPC type for Ratis, available options: netty, grpc.")
      .version("0.3.0")
      .stringConf
      .transform(_.toLowerCase)
      .checkValues(Set("netty", "grpc"))
      .createWithDefault("netty")

  val HA_MASTER_RATIS_STORAGE_DIR: ConfigEntry[String] =
    buildConf("celeborn.master.ha.ratis.raft.server.storage.dir")
      .withAlternative("celeborn.ha.master.ratis.raft.server.storage.dir")
      .categories("ha")
      .doc("Root storage directory to hold RaftServer data.")
      .version("0.3.0")
      .stringConf
      .createWithDefault("/tmp/ratis")

  val HA_MASTER_RATIS_STORAGE_STARTUP_OPTION: ConfigEntry[String] =
    buildConf("celeborn.master.ha.ratis.raft.server.storage.startup.option")
      .categories("ha")
      .doc("Startup option of RaftServer storage. Available options: RECOVER, FORMAT.")
      .version("0.5.0")
      .stringConf
      .checkValues(Set("RECOVER", "FORMAT"))
      .createWithDefault("RECOVER")

  val HA_MASTER_RATIS_LOG_SEGMENT_SIZE_MAX: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.log.segment.size.max")
      .withAlternative("celeborn.ha.master.ratis.raft.server.log.segment.size.max")
      .internal
      .categories("ha")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("4MB")

  val HA_MASTER_RATIS_LOG_PREALLOCATED_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.log.preallocated.size")
      .withAlternative("celeborn.ha.master.ratis.raft.server.log.preallocated.size")
      .internal
      .categories("ha")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("4MB")

  val HA_MASTER_RATIS_LOG_WRITE_BUFFER_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.log.write.buffer.size")
      .internal
      .categories("ha")
      .version("0.5.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("36MB")

  val HA_MASTER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS: ConfigEntry[Int] =
    buildConf("celeborn.master.ha.ratis.raft.server.log.appender.buffer.element-limit")
      .withAlternative("celeborn.ha.master.ratis.raft.server.log.appender.buffer.element-limit")
      .internal
      .categories("ha")
      .version("0.3.0")
      .intConf
      .createWithDefault(1024)

  val HA_MASTER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.log.appender.buffer.byte-limit")
      .withAlternative("celeborn.ha.master.ratis.raft.server.log.appender.buffer.byte-limit")
      .internal
      .categories("ha")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32MB")

  val HA_MASTER_RATIS_LOG_INSTALL_SNAPSHOT_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.master.ha.ratis.raft.server.log.appender.install.snapshot.enabled")
      .withAlternative("celeborn.ha.master.ratis.raft.server.log.appender.install.snapshot.enabled")
      .internal
      .categories("ha")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)

  val HA_MASTER_RATIS_LOG_PURGE_GAP: ConfigEntry[Int] =
    buildConf("celeborn.master.ha.ratis.raft.server.log.purge.gap")
      .withAlternative("celeborn.ha.master.ratis.raft.server.log.purge.gap")
      .internal
      .categories("ha")
      .version("0.3.0")
      .intConf
      .createWithDefault(1000000)

  val HA_MASTER_RATIS_RPC_REQUEST_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.rpc.request.timeout")
      .withAlternative("celeborn.ha.master.ratis.raft.server.rpc.request.timeout")
      .internal
      .categories("ha")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("3s")

  val HA_MASTER_RATIS_SERVER_RETRY_CACHE_EXPIRY_TIME: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.retrycache.expirytime")
      .withAlternative("celeborn.ha.master.ratis.raft.server.retrycache.expirytime")
      .internal
      .categories("ha")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("600s")

  val HA_MASTER_RATIS_RPC_TIMEOUT_MIN: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.rpc.timeout.min")
      .withAlternative("celeborn.ha.master.ratis.raft.server.rpc.timeout.min")
      .internal
      .categories("ha")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("3s")

  val HA_MASTER_RATIS_RPC_TIMEOUT_MAX: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.rpc.timeout.max")
      .withAlternative("celeborn.ha.master.ratis.raft.server.rpc.timeout.max")
      .internal
      .categories("ha")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("5s")

  val HA_MASTER_RATIS_CLIENT_RPC_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.client.rpc.timeout")
      .internal
      .categories("ha")
      .version("0.3.2")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("10s")

  val HA_MASTER_RATIS_CLIENT_RPC_WATCH_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.client.rpc.watch.timeout")
      .internal
      .categories("ha")
      .version("0.3.2")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("20s")

  val HA_MASTER_RATIS_FIRSTELECTION_TIMEOUT_MIN: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.first.election.timeout.min")
      .withAlternative("celeborn.ha.master.ratis.first.election.timeout.min")
      .internal
      .categories("ha")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("3s")

  val HA_MASTER_RATIS_FIRSTELECTION_TIMEOUT_MAX: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.first.election.timeout.max")
      .withAlternative("celeborn.ha.master.ratis.first.election.timeout.max")
      .internal
      .categories("ha")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("5s")

  val HA_MASTER_RATIS_LEADER_ELECTION_MEMBER_MAJORITY_ADD: ConfigEntry[Boolean] =
    buildConf("celeborn.master.ha.ratis.leader.election.member.majority.add")
      .internal
      .categories("ha")
      .version("0.6.0")
      .booleanConf
      .createWithDefault(true)

  val HA_MASTER_RATIS_NOTIFICATION_NO_LEADER_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.notification.no-leader.timeout")
      .withAlternative("celeborn.ha.master.ratis.raft.server.notification.no-leader.timeout")
      .internal
      .categories("ha")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("30s")

  val HA_MASTER_RATIS_RPC_SLOWNESS_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.rpc.slowness.timeout")
      .withAlternative("celeborn.ha.master.ratis.raft.server.rpc.slowness.timeout")
      .internal
      .categories("ha")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  val HA_MASTER_RATIS_ROLE_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.role.check.interval")
      .withAlternative("celeborn.ha.master.ratis.raft.server.role.check.interval")
      .internal
      .categories("ha")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")

  val HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.master.ha.ratis.raft.server.snapshot.auto.trigger.enabled")
      .withAlternative("celeborn.ha.master.ratis.raft.server.snapshot.auto.trigger.enabled")
      .internal
      .categories("ha")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)

  val HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.master.ha.ratis.raft.server.snapshot.auto.trigger.threshold")
      .withAlternative("celeborn.ha.master.ratis.raft.server.snapshot.auto.trigger.threshold")
      .internal
      .categories("ha")
      .version("0.3.0")
      .longConf
      .createWithDefault(200000L)

  val HA_MASTER_RATIS_SNAPSHOT_RETENTION_FILE_NUM: ConfigEntry[Int] =
    buildConf("celeborn.master.ha.ratis.raft.server.snapshot.retention.file.num")
      .withAlternative("celeborn.ha.master.ratis.raft.server.snapshot.retention.file.num")
      .internal
      .categories("ha")
      .version("0.3.0")
      .intConf
      .createWithDefault(3)

  val MASTER_PERSIST_WORKER_NETWORK_LOCATION: ConfigEntry[Boolean] =
    buildConf("celeborn.master.persist.workerNetworkLocation")
      .categories("master")
      .version("0.6.0")
      .booleanConf
      .createWithDefault(false)

  val MASTER_SLOT_ASSIGN_POLICY: ConfigEntry[String] =
    buildConf("celeborn.master.slot.assign.policy")
      .withAlternative("celeborn.slots.assign.policy")
      .categories("master")
      .version("0.3.0")
      .doc("Policy for master to assign slots, Celeborn supports two types of policy: roundrobin and loadaware. " +
        "Loadaware policy will be ignored when `HDFS` is enabled in `celeborn.storage.availableTypes`")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set(
        SlotsAssignPolicy.ROUNDROBIN.name,
        SlotsAssignPolicy.LOADAWARE.name))
      .createWithDefault(SlotsAssignPolicy.ROUNDROBIN.name)

  val MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_NUM: ConfigEntry[Int] =
    buildConf("celeborn.master.slot.assign.loadAware.numDiskGroups")
      .withAlternative("celeborn.slots.assign.loadAware.numDiskGroups")
      .categories("master")
      .doc("This configuration is a guidance for load-aware slot allocation algorithm. " +
        "This value is control how many disk groups will be created.")
      .version("0.3.0")
      .intConf
      .createWithDefault(5)

  val MASTER_SLOT_ASSIGN_LOADAWARE_DISKGROUP_GRADIENT: ConfigEntry[Double] =
    buildConf("celeborn.master.slot.assign.loadAware.diskGroupGradient")
      .withAlternative("celeborn.slots.assign.loadAware.diskGroupGradient")
      .categories("master")
      .doc("This value means how many more workload will be placed into a faster disk group " +
        "than a slower group.")
      .version("0.3.0")
      .doubleConf
      .createWithDefault(0.1)

  val MASTER_SLOT_ASSIGN_LOADAWARE_FLUSHTIME_WEIGHT: ConfigEntry[Double] =
    buildConf("celeborn.master.slot.assign.loadAware.flushTimeWeight")
      .withAlternative("celeborn.slots.assign.loadAware.flushTimeWeight")
      .categories("master")
      .doc(
        "Weight of average flush time when calculating ordering in load-aware assignment strategy")
      .version("0.3.0")
      .doubleConf
      .createWithDefault(0)

  val MASTER_SLOT_ASSIGN_LOADAWARE_FETCHTIME_WEIGHT: ConfigEntry[Double] =
    buildConf("celeborn.master.slot.assign.loadAware.fetchTimeWeight")
      .withAlternative("celeborn.slots.assign.loadAware.fetchTimeWeight")
      .categories("master")
      .doc(
        "Weight of average fetch time when calculating ordering in load-aware assignment strategy")
      .version("0.3.0")
      .doubleConf
      .createWithDefault(1)

  val MASTER_SLOT_ASSIGN_EXTRA_SLOTS: ConfigEntry[Int] =
    buildConf("celeborn.master.slot.assign.extraSlots")
      .withAlternative("celeborn.slots.assign.extraSlots")
      .categories("master")
      .version("0.3.0")
      .doc("Extra slots number when master assign slots.")
      .intConf
      .createWithDefault(100)

  val MASTER_SLOT_ASSIGN_MAX_WORKERS: ConfigEntry[Int] =
    buildConf("celeborn.master.slot.assign.maxWorkers")
      .categories("master")
      .version("0.3.1")
      .doc("Max workers that slots of one shuffle can be allocated on. Will choose the smaller positive one " +
        s"from Master side and Client side, see `celeborn.client.slot.assign.maxWorkers`.")
      .intConf
      .createWithDefault(10000)

  val ESTIMATED_PARTITION_SIZE_INITIAL_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.master.estimatedPartitionSize.initialSize")
      .withAlternative("celeborn.shuffle.initialEstimatedPartitionSize")
      .categories("master")
      .doc("Initial partition size for estimation, it will change according to runtime stats.")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64mb")

  val ESTIMATED_PARTITION_SIZE_MAX_SIZE: OptionalConfigEntry[Long] =
    buildConf("celeborn.master.estimatedPartitionSize.maxSize")
      .categories("master")
      .doc("Max partition size for estimation. Default value should be celeborn.worker.shuffle.partitionSplit.max * 2.")
      .version("0.4.1")
      .bytesConf(ByteUnit.BYTE)
      .createOptional

  val ESTIMATED_PARTITION_SIZE_MIN_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.master.estimatedPartitionSize.minSize")
      .withAlternative("celeborn.shuffle.minPartitionSizeToEstimate")
      .categories("master", "worker")
      .doc(
        "Ignore partition size smaller than this configuration of partition size for estimation.")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8mb")

  val ESTIMATED_PARTITION_SIZE_UPDATE_INITIAL_DELAY: ConfigEntry[Long] =
    buildConf("celeborn.master.estimatedPartitionSize.update.initialDelay")
      .withAlternative("celeborn.shuffle.estimatedPartitionSize.update.initialDelay")
      .categories("master")
      .doc("Initial delay time before start updating partition size for estimation.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5min")

  val ESTIMATED_PARTITION_SIZE_UPDATE_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.master.estimatedPartitionSize.update.interval")
      .withAlternative("celeborn.shuffle.estimatedPartitionSize.update.interval")
      .categories("master")
      .doc("Interval of updating partition size for estimation.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10min")

  val MASTER_RESOURCE_CONSUMPTION_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.master.userResourceConsumption.update.interval")
      .categories("master")
      .doc("Time length for a window about compute user resource consumption.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val CLUSTER_NAME: ConfigEntry[String] =
    buildConf("celeborn.cluster.name")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("Celeborn cluster name.")
      .stringConf
      .createWithDefaultString("default")

  val SHUFFLE_CHUNK_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.shuffle.chunk.size")
      .categories("worker")
      .version("0.2.0")
      .doc("Max chunk size of reducer's merged shuffle data. For example, if a reducer's " +
        "shuffle data is 128M and the data will need 16 fetch chunk requests to fetch.")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v < Integer.MAX_VALUE, "Chunk size can not be larger than 2GB")
      .createWithDefaultString("8m")

  val CLIENT_FETCH_DFS_READ_CHUNK_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.client.fetch.dfsReadChunkSize")
      .categories("client")
      .version("0.3.1")
      .doc("Max chunk size for DfsPartitionReader.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8m")

  val WORKER_PARTITION_SPLIT_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.shuffle.partitionSplit.enabled")
      .withAlternative("celeborn.worker.partition.split.enabled")
      .categories("worker")
      .version("0.3.0")
      .doc("enable the partition split on worker side")
      .booleanConf
      .createWithDefault(true)

  val WORKER_PARTITION_SPLIT_MIN_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.worker.shuffle.partitionSplit.min")
      .withAlternative("celeborn.shuffle.partitionSplit.min")
      .categories("worker")
      .doc("Min size for a partition to split")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1m")

  val WORKER_PARTITION_SPLIT_MAX_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.worker.shuffle.partitionSplit.max")
      .categories("worker")
      .doc("Specify the maximum partition size for splitting, and ensure that individual partition files are always smaller than this limit.")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("2g")

  val WORKER_STORAGE_DIRS: OptionalConfigEntry[Seq[String]] =
    buildConf("celeborn.worker.storage.dirs")
      .categories("worker")
      .version("0.2.0")
      .doc("Directory list to store shuffle data. It's recommended to configure one directory " +
        "on each disk. Storage size limit can be set for each directory. For the sake of " +
        "performance, there should be no more than 2 flush threads " +
        "on the same disk partition if you are using HDD, and should be 8 or more flush threads " +
        "on the same disk partition if you are using SSD. For example: " +
        "`dir1[:capacity=][:disktype=][:flushthread=],dir2[:capacity=][:disktype=][:flushthread=]`")
      .stringConf
      .toSequence
      .createOptional

  val WORKER_WORKING_DIR: ConfigEntry[String] =
    buildConf("celeborn.worker.storage.workingDir")
      .withAlternative("celeborn.worker.workingDir")
      .categories("worker")
      .doc("Worker's working dir path name.")
      .version("0.3.0")
      .stringConf
      .createWithDefault("celeborn-worker/shuffle_data")

  val WORKER_STORAGE_BASE_DIR_PREFIX: ConfigEntry[String] =
    buildConf("celeborn.worker.storage.baseDir.prefix")
      .internal
      .categories("worker")
      .version("0.2.0")
      .doc("Base directory for Celeborn worker to write if " +
        s"`${WORKER_STORAGE_DIRS.key}` is not set.")
      .stringConf
      .createWithDefault("/mnt/disk")

  val WORKER_STORAGE_BASE_DIR_COUNT: ConfigEntry[Int] =
    buildConf("celeborn.worker.storage.baseDir.number")
      .internal
      .categories("worker")
      .version("0.2.0")
      .doc(s"How many directories will be used if `${WORKER_STORAGE_DIRS.key}` is not set. " +
        s"The directory name is a combination of `${WORKER_STORAGE_BASE_DIR_PREFIX.key}` " +
        "and from one(inclusive) to `celeborn.worker.storage.baseDir.number`(inclusive) " +
        "step by one.")
      .intConf
      .createWithDefault(16)

  val WORKER_STORAGE_EXPIRE_DIR_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.storage.expireDirs.timeout")
      .categories("worker")
      .version("0.3.2")
      .doc(s"The timeout for a expire dirs to be deleted on disk.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1h")

  val HDFS_DIR: OptionalConfigEntry[String] =
    buildConf("celeborn.storage.hdfs.dir")
      .categories("worker", "master", "client")
      .version("0.2.0")
      .doc("HDFS base directory for Celeborn to store shuffle data.")
      .stringConf
      .createOptional

  val S3_DIR: OptionalConfigEntry[String] =
    buildConf("celeborn.storage.s3.dir")
      .categories("worker", "master", "client")
      .version("0.6.0")
      .doc("S3 base directory for Celeborn to store shuffle data.")
      .stringConf
      .createOptional

  val S3_SECRET_KEY: OptionalConfigEntry[String] =
    buildConf("celeborn.storage.s3.secret.key")
      .categories("worker", "master", "client")
      .version("0.6.0")
      .doc("S3 secret key for Celeborn to store shuffle data.")
      .stringConf
      .createOptional

  val S3_ACCESS_KEY: OptionalConfigEntry[String] =
    buildConf("celeborn.storage.s3.access.key")
      .categories("worker", "master", "client")
      .version("0.6.0")
      .doc("S3 access key for Celeborn to store shuffle data.")
      .stringConf
      .createOptional

  val S3_ENDPOINT_REGION: OptionalConfigEntry[String] =
    buildConf("celeborn.storage.s3.endpoint.region")
      .categories("worker", "master", "client")
      .version("0.6.0")
      .doc("S3 endpoint for Celeborn to store shuffle data.")
      .stringConf
      .createOptional

  val S3_MPU_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("celeborn.storage.s3.mpu.maxRetries")
      .categories("worker")
      .version("0.6.0")
      .doc("S3 MPU upload max retries.")
      .intConf
      .createWithDefault(5)

  val WORKER_DISK_RESERVE_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.worker.storage.disk.reserve.size")
      .withAlternative("celeborn.worker.disk.reserve.size")
      .categories("worker")
      .doc("Celeborn worker reserved space for each disk.")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("5G")

  val WORKER_DISK_RESERVE_RATIO: OptionalConfigEntry[Double] =
    buildConf("celeborn.worker.storage.disk.reserve.ratio")
      .categories("worker")
      .doc("Celeborn worker reserved ratio for each disk. The minimum usable size for each disk is the max space " +
        "between the reserved space and the space calculate via reserved ratio.")
      .version("0.3.2")
      .doubleConf
      .checkValue(v => v > 0.0 && v < 1.0, "Should be in (0.0, 1.0).")
      .createOptional

  val WORKER_DISK_CLEAN_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.disk.clean.threads")
      .categories("worker")
      .version("0.3.2")
      .doc("Thread number of worker to clean up directories of expired shuffle keys on disk.")
      .intConf
      .createWithDefault(4)

  val WORKER_CHECK_FILE_CLEAN_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("celeborn.worker.storage.checkDirsEmpty.maxRetries")
      .withAlternative("celeborn.worker.disk.checkFileClean.maxRetries")
      .categories("worker")
      .doc("The number of retries for a worker to check if the working directory is cleaned up before registering with the master.")
      .version("0.3.0")
      .intConf
      .createWithDefault(3)

  val WORKER_CHECK_FILE_CLEAN_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.storage.checkDirsEmpty.timeout")
      .withAlternative("celeborn.worker.disk.checkFileClean.timeout")
      .categories("worker")
      .doc("The wait time per retry for a worker to check if the working directory is cleaned up before registering with the master.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1000ms")

  val WORKER_STORAGE_CREATE_FILE_POLICY: OptionalConfigEntry[String] =
    buildConf("celeborn.worker.storage.storagePolicy.createFilePolicy")
      .categories("worker")
      .doc("This defined the order for creating files across available storages." +
        " Available storages options are: MEMORY,SSD,HDD,HDFS,OSS")
      .version("0.5.1")
      .stringConf
      .checkValue(
        _.split(",").map(str => StorageInfo.typeNames.contains(str.trim.toUpperCase)).forall(p =>
          p),
        "Will use default create file order. Default order: MEMORY,SSD,HDD,HDFS,OSS")
      .createOptional

  val WORKER_STORAGE_EVICT_POLICY: OptionalConfigEntry[String] =
    buildConf("celeborn.worker.storage.storagePolicy.evictPolicy")
      .categories("worker")
      .doc("This define the order of evict files if the storages are available." +
        " Available storages: MEMORY,SSD,HDD,HDFS. " +
        "Definition: StorageTypes|StorageTypes|StorageTypes. " +
        "Example: MEMORY,SSD|SSD,HDFS." +
        " The example means that a MEMORY shuffle file can be evicted to SSD " +
        "and a SSD shuffle file can be evicted to HDFS.")
      .version("0.5.1")
      .stringConf
      .checkValue(
        _.replace("|", ",").split(",").map(str =>
          StorageInfo.typeNames.contains(str.trim.toUpperCase)).forall(p => p),
        "Will use default evict order. Default order: MEMORY,SSD,HDD,HDFS,OSS")
      .createOptional

  val WORKER_HTTP_HOST: ConfigEntry[String] =
    buildConf("celeborn.worker.http.host")
      .withAlternative("celeborn.metrics.worker.prometheus.host")
      .withAlternative("celeborn.worker.metrics.prometheus.host")
      .categories("worker")
      .doc("Worker's http host.")
      .version("0.4.0")
      .stringConf
      .createWithDefault("<localhost>")

  val WORKER_HTTP_PORT: ConfigEntry[Int] =
    buildConf("celeborn.worker.http.port")
      .withAlternative("celeborn.metrics.worker.prometheus.port")
      .withAlternative("celeborn.worker.metrics.prometheus.port")
      .categories("worker")
      .doc("Worker's http port.")
      .version("0.4.0")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "Invalid port")
      .createWithDefault(9096)

  val WORKER_HTTP_MAX_WORKER_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.http.maxWorkerThreads")
      .categories("worker")
      .version("0.5.0")
      .doc("Maximum number of threads in the worker http worker thread pool.")
      .intConf
      .checkValue(_ > 0, "Must be positive.")
      .createWithDefault(200)

  val WORKER_HTTP_STOP_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.http.stopTimeout")
      .categories("worker")
      .version("0.5.0")
      .doc("Worker http server stop timeout.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5s")

  val WORKER_HTTP_IDLE_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.http.idleTimeout")
      .categories("worker")
      .version("0.5.0")
      .doc("Worker http server idle timeout.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val WORKER_HTTP_AUTH_SUPPORTED_SCHEMES: ConfigEntry[Seq[String]] =
    buildConf("celeborn.worker.http.auth.supportedSchemes")
      .categories("worker")
      .version("0.6.0")
      .doc("A comma-separated list of worker http auth supported schemes." +
        "<ul>" +
        " <li>SPNEGO: Kerberos/GSSAPI authentication.</li>" +
        " <li>BASIC: User-defined password authentication, the concreted implementation is" +
        " configurable via `celeborn.worker.http.auth.basic.provider`.</li>" +
        " <li>BEARER: User-defined bearer token authentication, the concreted implementation is" +
        " configurable via `celeborn.worker.http.auth.bearer.provider`.</li>" +
        "</ul>")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val WORKER_HTTP_SPNEGO_KEYTAB: OptionalConfigEntry[String] =
    buildConf("celeborn.worker.http.spnego.keytab")
      .categories("worker")
      .version("0.6.0")
      .doc("The keytab file for SPNego authentication.")
      .stringConf
      .createOptional

  val WORKER_HTTP_SPNEGO_PRINCIPAL: OptionalConfigEntry[String] =
    buildConf("celeborn.worker.http.spnego.principal")
      .categories("worker")
      .version("0.6.0")
      .doc("SPNego service principal, typical value would look like HTTP/_HOST@EXAMPLE.COM." +
        " SPNego service principal would be used when celeborn http authentication is enabled." +
        " This needs to be set only if SPNEGO is to be used in authentication.")
      .stringConf
      .createOptional

  val WORKER_HTTP_PROXY_CLIENT_IP_HEADER: ConfigEntry[String] =
    buildConf("celeborn.worker.http.proxy.client.ip.header")
      .categories("worker")
      .doc("The HTTP header to record the real client IP address. If your server is behind a load" +
        " balancer or other proxy, the server will see this load balancer or proxy IP address as" +
        " the client IP address, to get around this common issue, most load balancers or proxies" +
        " offer the ability to record the real remote IP address in an HTTP header that will be" +
        " added to the request for other devices to use. Note that, because the header value can" +
        " be specified to any IP address, so it will not be used for authentication.")
      .version("0.6.0")
      .stringConf
      .createWithDefault("X-Real-IP")

  val WORKER_HTTP_AUTH_BASIC_PROVIDER: ConfigEntry[String] =
    buildConf("celeborn.worker.http.auth.basic.provider")
      .categories("worker")
      .version("0.6.0")
      .doc("User-defined password authentication implementation of " +
        "org.apache.celeborn.common.authentication.PasswdAuthenticationProvider")
      .stringConf
      .createWithDefault(classOf[AnonymousAuthenticationProviderImpl].getName)

  val WORKER_HTTP_AUTH_BEARER_PROVIDER: ConfigEntry[String] =
    buildConf("celeborn.worker.http.auth.bearer.provider")
      .categories("worker")
      .version("0.6.0")
      .doc("User-defined token authentication implementation of " +
        "org.apache.celeborn.common.authentication.TokenAuthenticationProvider")
      .stringConf
      .createWithDefault(classOf[AnonymousAuthenticationProviderImpl].getName)

  val WORKER_HTTP_AUTH_ADMINISTERS: ConfigEntry[Seq[String]] =
    buildConf("celeborn.worker.http.auth.administers")
      .categories("worker")
      .version("0.6.0")
      .doc("A comma-separated list of users who have admin privileges," +
        s" Note, when ${WORKER_HTTP_AUTH_SUPPORTED_SCHEMES.key} is not set," +
        s" everyone is treated as administrator.")
      .stringConf
      .toSequence
      .createWithDefault(Seq.empty)

  val WORKER_HTTP_SSL_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.http.ssl.enabled")
      .categories("worker")
      .version("0.6.0")
      .doc("Set this to true for using SSL encryption in http server.")
      .booleanConf
      .createWithDefault(false)

  val WORKER_HTTP_SSL_KEYSTORE_PATH: OptionalConfigEntry[String] =
    buildConf("celeborn.worker.http.ssl.keystore.path")
      .categories("worker")
      .version("0.6.0")
      .doc("SSL certificate keystore location.")
      .stringConf
      .createOptional

  val WORKER_HTTP_SSL_KEYSTORE_PASSWORD: OptionalConfigEntry[String] =
    buildConf("celeborn.worker.http.ssl.keystore.password")
      .categories("worker")
      .version("0.6.0")
      .doc("SSL certificate keystore password.")
      .stringConf
      .createOptional

  val WORKER_HTTP_SSL_KEYSTORE_TYPE: OptionalConfigEntry[String] =
    buildConf("celeborn.worker.http.ssl.keystore.type")
      .categories("worker")
      .version("0.6.0")
      .doc("SSL certificate keystore type.")
      .stringConf
      .createOptional

  val WORKER_HTTP_SSL_KEYSTORE_ALGORITHM: OptionalConfigEntry[String] =
    buildConf("celeborn.worker.http.ssl.keystore.algorithm")
      .categories("worker")
      .version("0.6.0")
      .doc("SSL certificate keystore algorithm.")
      .stringConf
      .createOptional

  val WORKER_HTTP_SSL_DISALLOWED_PROTOCOLS: ConfigEntry[Seq[String]] =
    buildConf("celeborn.worker.http.ssl.disallowed.protocols")
      .categories("worker")
      .version("0.6.0")
      .doc("SSL versions to disallow.")
      .stringConf
      .toSequence
      .createWithDefault(Seq("SSLv2", "SSLv3"))

  val WORKER_HTTP_SSL_INCLUDE_CIPHER_SUITES: ConfigEntry[Seq[String]] =
    buildConf("celeborn.worker.http.ssl.include.ciphersuites")
      .categories("worker")
      .version("0.6.0")
      .doc("A comma-separated list of include SSL cipher suite names.")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val WORKER_RPC_PORT: ConfigEntry[Int] =
    buildConf("celeborn.worker.rpc.port")
      .categories("worker")
      .doc("Server port for Worker to receive RPC request.")
      .version("0.2.0")
      .intConf
      .createWithDefault(0)

  val WORKER_PUSH_PORT: ConfigEntry[Int] =
    buildConf("celeborn.worker.push.port")
      .categories("worker")
      .doc("Server port for Worker to receive push data request from ShuffleClient.")
      .version("0.2.0")
      .intConf
      .createWithDefault(0)

  val WORKER_FETCH_PORT: ConfigEntry[Int] =
    buildConf("celeborn.worker.fetch.port")
      .categories("worker")
      .doc("Server port for Worker to receive fetch data request from ShuffleClient.")
      .version("0.2.0")
      .intConf
      .createWithDefault(0)

  val WORKER_REPLICATE_PORT: ConfigEntry[Int] =
    buildConf("celeborn.worker.replicate.port")
      .categories("worker")
      .doc("Server port for Worker to receive replicate data request from other Workers.")
      .version("0.2.0")
      .intConf
      .createWithDefault(0)

  val WORKER_PUSH_IO_THREADS: OptionalConfigEntry[Int] =
    buildConf("celeborn.worker.push.io.threads")
      .categories("worker")
      .doc("Netty IO thread number of worker to handle client push data. " +
        s"The default threads number is the number of flush thread.")
      .version("0.2.0")
      .intConf
      .createOptional

  val WORKER_FETCH_IO_THREADS: OptionalConfigEntry[Int] =
    buildConf("celeborn.worker.fetch.io.threads")
      .categories("worker")
      .doc("Netty IO thread number of worker to handle client fetch data. " +
        s"The default threads number is the number of flush thread.")
      .version("0.2.0")
      .intConf
      .createOptional

  val WORKER_REPLICATE_IO_THREADS: OptionalConfigEntry[Int] =
    buildConf("celeborn.worker.replicate.io.threads")
      .categories("worker")
      .doc("Netty IO thread number of worker to replicate shuffle data. " +
        s"The default threads number is the number of flush thread.")
      .version("0.2.0")
      .intConf
      .createOptional

  val WORKER_REGISTER_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.register.timeout")
      .categories("worker")
      .doc("Worker register timeout.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("180s")

  val WORKER_CLOSE_IDLE_CONNECTIONS: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.closeIdleConnections")
      .categories("worker")
      .doc("Whether worker will close idle connections.")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(false)

  val WORKER_REPLICATE_FAST_FAIL_DURATION: ConfigEntry[Long] =
    buildConf("celeborn.worker.replicate.fastFail.duration")
      .categories("worker")
      .doc("If a replicate request not replied during the duration, worker will mark the replicate data request as failed. " +
        "It's recommended to set at least `240s` when `HDFS` is enabled in `celeborn.storage.availableTypes`.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  val WORKER_REPLICATE_RANDOM_CONNECTION_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.replicate.randomConnection.enabled")
      .categories("worker")
      .doc("Whether worker will create random connection to peer when replicate data. When false, worker tend to " +
        "reuse the same cached TransportClient to a specific replicate worker; when true, worker tend to use " +
        "different cached TransportClient. Netty will use the same thread to serve the same connection, so " +
        "with more connections replicate server can leverage more netty threads")
      .version("0.2.1")
      .booleanConf
      .createWithDefault(true)

  val WORKER_REPLICATE_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.replicate.threads")
      .categories("worker")
      .version("0.2.0")
      .doc("Thread number of worker to replicate shuffle data.")
      .intConf
      .createWithDefault(64)

  val WORKER_COMMIT_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.commitFiles.threads")
      .withAlternative("celeborn.worker.commit.threads")
      .categories("worker")
      .version("0.3.0")
      .doc("Thread number of worker to commit shuffle data files asynchronously. " +
        "It's recommended to set at least `128` when `HDFS` is enabled in `celeborn.storage.availableTypes`.")
      .intConf
      .createWithDefault(32)

  val WORKER_COMMIT_FILES_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.commitFiles.check.interval")
      .categories("worker")
      .version("0.6.0")
      .doc("Time length for a window about checking whether commit shuffle data files finished.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("100")

  val WORKER_CLEAN_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.clean.threads")
      .categories("worker")
      .version("0.3.2")
      .doc("Thread number of worker to clean up expired shuffle keys.")
      .intConf
      .createWithDefault(64)

  val WORKER_SHUFFLE_COMMIT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.commitFiles.timeout")
      .withAlternative("celeborn.worker.shuffle.commit.timeout")
      .categories("worker")
      .doc("Timeout for a Celeborn worker to commit files of a shuffle. " +
        "It's recommended to set at least `240s` when `HDFS` is enabled in `celeborn.storage.availableTypes`.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  val WORKER_PARTITION_SORTER_SORT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.sortPartition.timeout")
      .withAlternative("celeborn.worker.partitionSorter.sort.timeout")
      .categories("worker")
      .doc("Timeout for a shuffle file to sort.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("220s")

  val WORKER_PARTITION_SORTER_THREADS: OptionalConfigEntry[Int] =
    buildConf("celeborn.worker.sortPartition.threads")
      .withAlternative("celeborn.worker.partitionSorter.threads")
      .categories("worker")
      .doc("PartitionSorter's thread counts. " +
        "It's recommended to set at least `64` when `HDFS` is enabled in `celeborn.storage.availableTypes`.")
      .version("0.3.0")
      .intConf
      .createOptional

  val WORKER_PARTITION_SORTER_INDEX_CACHE_MAX_WEIGHT: ConfigEntry[Long] =
    buildConf("celeborn.worker.sortPartition.indexCache.maxWeight")
      .categories("worker")
      .doc("PartitionSorter's cache max weight for index buffer.")
      .version("0.4.0")
      .longConf
      .createWithDefault(100000)

  val WORKER_PARTITION_SORTER_INDEX_CACHE_EXPIRE: ConfigEntry[Long] =
    buildConf("celeborn.worker.sortPartition.indexCache.expire")
      .categories("worker")
      .doc("PartitionSorter's cache item expire time.")
      .version("0.4.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("180s")

  val WORKER_PARTITION_SORTER_RESERVED_MEMORY_PER_PARTITION: ConfigEntry[Long] =
    buildConf("celeborn.worker.sortPartition.reservedMemoryPerPartition")
      .withAlternative("celeborn.worker.partitionSorter.reservedMemoryPerPartition")
      .categories("worker")
      .doc("Reserved memory when sorting a shuffle file off-heap.")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v < Int.MaxValue, "Reserved memory per partition must be less than 2GB.")
      .createWithDefaultString("1mb")

  val WORKER_PARTITION_SORTER_PREFETCH_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.sortPartition.prefetch.enabled")
      .categories("worker")
      .doc("When true, partition sorter will prefetch the original partition files to page cache " +
        s"and reserve memory configured by `${WORKER_PARTITION_SORTER_RESERVED_MEMORY_PER_PARTITION.key}` " +
        "to allocate a block of memory for prefetching while sorting a shuffle file off-heap with page cache for non-hdfs files. " +
        "Otherwise, partition sorter seeks to position of each block and does not prefetch for non-hdfs files.")
      .version("0.5.0")
      .booleanConf
      .createWithDefault(true)

  val WORKER_SHUFFLE_BLOCK_COMPACTION_FACTOR: ConfigEntry[Double] =
    buildConf("celeborn.shuffle.sortPartition.block.compactionFactor")
      .categories("worker")
      .version("0.4.2")
      .doc("Combine sorted shuffle blocks such that size of compacted shuffle block does not " +
        s"exceed compactionFactor * ${SHUFFLE_CHUNK_SIZE.key}")
      .doubleConf
      .checkValue(v => v >= 0.0 && v <= 1.0, "Should be in [0.0, 1.0].")
      .createWithDefault(0.25)

  val WORKER_FLUSHER_BUFFER_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.worker.flusher.buffer.size")
      .categories("worker")
      .version("0.2.0")
      .doc("Size of buffer used by a single flusher.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("256k")

  val WORKER_HDFS_FLUSHER_BUFFER_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.worker.flusher.hdfs.buffer.size")
      .categories("worker")
      .version("0.3.0")
      .doc("Size of buffer used by a HDFS flusher.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("4m")

  val WORKER_S3_FLUSHER_BUFFER_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.worker.flusher.s3.buffer.size")
      .categories("worker")
      .version("0.6.0")
      .doc("Size of buffer used by a S3 flusher.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("6m")

  val WORKER_WRITER_CLOSE_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.writer.close.timeout")
      .categories("worker")
      .doc("Timeout for a file writer to close")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  val WORKER_FLUSHER_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.threads")
      .categories("worker")
      .doc("Flusher's thread count per disk for unknown-type disks.")
      .version("0.2.0")
      .intConf
      .createWithDefault(16)

  val WORKER_FLUSHER_HDD_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.hdd.threads")
      .categories("worker")
      .doc("Flusher's thread count per disk used for write data to HDD disks.")
      .version("0.2.0")
      .intConf
      .createWithDefault(1)

  val WORKER_FLUSHER_SSD_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.ssd.threads")
      .categories("worker")
      .doc("Flusher's thread count per disk used for write data to SSD disks.")
      .version("0.2.0")
      .intConf
      .createWithDefault(16)

  val WORKER_FLUSHER_HDFS_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.hdfs.threads")
      .categories("worker")
      .doc("Flusher's thread count used for write data to HDFS.")
      .version("0.2.0")
      .intConf
      .createWithDefault(8)

  val WORKER_FLUSHER_S3_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.s3.threads")
      .categories("worker")
      .doc("Flusher's thread count used for write data to S3.")
      .version("0.6.0")
      .intConf
      .createWithDefault(8)

  val WORKER_FLUSHER_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.flusher.shutdownTimeout")
      .categories("worker")
      .doc("Timeout for a flusher to shutdown.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")

  val WORKER_GRACEFUL_SHUTDOWN_SAVE_COMMITTED_FILEINFO_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.graceful.shutdown.saveCommittedFileInfo.interval")
      .categories("worker")
      .doc("Interval for a Celeborn worker to flush committed file infos into Level DB.")
      .version("0.3.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5s")

  val WORKER_GRACEFUL_SHUTDOWN_SAVE_COMMITTED_FILEINFO_SYNC: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.graceful.shutdown.saveCommittedFileInfo.sync")
      .categories("worker")
      .doc(
        "Whether to call sync method to save committed file infos into Level DB to handle OS crash.")
      .version("0.3.1")
      .booleanConf
      .createWithDefault(false)

  val WORKER_DISKTIME_SLIDINGWINDOW_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.diskTime.slidingWindow.size")
      .withAlternative("celeborn.worker.flusher.avgFlushTime.slidingWindow.size")
      .categories("worker")
      .doc("The size of sliding windows used to calculate statistics about flushed time and count.")
      .version("0.3.0")
      .intConf
      .createWithDefault(20)

  val WORKER_DISKTIME_SLIDINGWINDOW_MINFLUSHCOUNT: ConfigEntry[Int] =
    buildConf("celeborn.worker.diskTime.slidingWindow.minFlushCount")
      .withAlternative("celeborn.worker.flusher.avgFlushTime.slidingWindow.minCount")
      .categories("worker")
      .doc("The minimum flush count to enter a sliding window" +
        " to calculate statistics about flushed time and count.")
      .version("0.3.0")
      .internal
      .intConf
      .createWithDefault(500)

  val WORKER_DISKTIME_SLIDINGWINDOW_MINFETCHCOUNT: ConfigEntry[Int] =
    buildConf("celeborn.worker.diskTime.slidingWindow.minFetchCount")
      .categories("worker")
      .doc("The minimum fetch count to enter a sliding window" +
        " to calculate statistics about fetched time and count.")
      .version("0.2.1")
      .internal
      .intConf
      .createWithDefault(100)

  val WORKER_DIRECT_MEMORY_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.monitor.memory.check.interval")
      .withAlternative("celeborn.worker.memory.checkInterval")
      .categories("worker")
      .doc("Interval of worker direct memory checking.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10ms")

  val WORKER_PINNED_MEMORY_CHECK_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.monitor.pinnedMemory.check.enabled")
      .categories("worker")
      .doc("If true, MemoryManager will check worker should resume by pinned memory used.")
      .version("0.6.0")
      .booleanConf
      .createWithDefaultString("true")

  val WORKER_PINNED_MEMORY_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.monitor.pinnedMemory.check.interval")
      .categories("worker")
      .doc("Interval of worker direct pinned memory checking, " +
        "only takes effect when celeborn.network.memory.allocator.pooled and " +
        "celeborn.worker.monitor.pinnedMemory.check.enabled are enabled.")
      .version("0.6.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10s")

  val WORKER_DIRECT_MEMORY_REPORT_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.monitor.memory.report.interval")
      .withAlternative("celeborn.worker.memory.reportInterval")
      .categories("worker")
      .doc("Interval of worker direct memory tracker reporting to log.")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("10s")

  val WORKER_DIRECT_MEMORY_TRIM_CHANNEL_WAIT_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.monitor.memory.trimChannelWaitInterval")
      .categories("worker")
      .doc("Wait time after worker trigger channel to trim cache.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")

  val WORKER_DIRECT_MEMORY_TRIM_FLUSH_WAIT_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.monitor.memory.trimFlushWaitInterval")
      .categories("worker")
      .doc("Wait time after worker trigger StorageManger to flush data.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")

  val WORKER_DISK_MONITOR_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.monitor.disk.enabled")
      .categories("worker")
      .version("0.3.0")
      .doc("When true, worker will monitor device and report to master.")
      .booleanConf
      .createWithDefault(true)

  val WORKER_DEVICE_STATUS_CHECK_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.monitor.disk.check.timeout")
      .withAlternative("celeborn.worker.disk.check.timeout")
      .categories("worker")
      .doc("Timeout time for worker check device status.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val WORKER_DISK_MONITOR_CHECKLIST: ConfigEntry[Seq[String]] =
    buildConf("celeborn.worker.monitor.disk.checklist")
      .categories("worker")
      .version("0.2.0")
      .doc("Monitor type for disk, available items are: " +
        "iohang, readwrite and diskusage.")
      .stringConf
      .transform(_.toLowerCase)
      .toSequence
      .createWithDefaultString("readwrite,diskusage")

  val WORKER_DISK_MONITOR_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.monitor.disk.check.interval")
      .withAlternative("celeborn.worker.monitor.disk.checkInterval")
      .categories("worker")
      .version("0.3.0")
      .doc("Intervals between device monitor to check disk.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val WORKER_DISK_MONITOR_SYS_BLOCK_DIR: ConfigEntry[String] =
    buildConf("celeborn.worker.monitor.disk.sys.block.dir")
      .categories("worker")
      .version("0.2.0")
      .doc("The directory where linux file block information is stored.")
      .stringConf
      .createWithDefault("/sys/block")

  val WORKER_DISK_MONITOR_NOTIFY_ERROR_THRESHOLD: ConfigEntry[Int] =
    buildConf("celeborn.worker.monitor.disk.notifyError.threshold")
      .categories("worker")
      .version("0.3.0")
      .doc("Device monitor will only notify critical error once the accumulated valid non-critical error number " +
        "exceeding this threshold.")
      .intConf
      .createWithDefault(64)

  val WORKER_DISK_MONITOR_NOTIFY_ERROR_EXPIRE_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.monitor.disk.notifyError.expireTimeout")
      .categories("worker")
      .version("0.3.0")
      .doc("The expire timeout of non-critical device error. Only notify critical error when the number of non-critical " +
        "errors for a period of time exceeds threshold.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10m")

  val WORKER_WRITER_CREATE_MAX_ATTEMPTS: ConfigEntry[Int] =
    buildConf("celeborn.worker.writer.create.maxAttempts")
      .categories("worker")
      .version("0.2.0")
      .doc("Retry count for a file writer to create if its creation was failed.")
      .intConf
      .createWithDefault(3)

  val WORKER_PARTITION_SORTER_DIRECT_MEMORY_RATIO_THRESHOLD: ConfigEntry[Double] =
    buildConf("celeborn.worker.partitionSorter.directMemoryRatioThreshold")
      .categories("worker")
      .doc("Max ratio of partition sorter's memory for sorting, when reserved memory is higher than max partition " +
        "sorter memory, partition sorter will stop sorting." +
        " If this value is set to 0, partition files sorter will skip memory check and ServingState check.")
      .version("0.2.0")
      .doubleConf
      .checkValue(v => v >= 0.0 && v <= 1.0, "Should be in [0.0, 1.0].")
      .createWithDefault(0.1)

  val WORKER_DIRECT_MEMORY_RATIO_FOR_READ_BUFFER: ConfigEntry[Double] =
    buildConf("celeborn.worker.directMemoryRatioForReadBuffer")
      .categories("worker")
      .doc("Max ratio of direct memory for read buffer")
      .version("0.2.0")
      .doubleConf
      .createWithDefault(0.1)

  val WORKER_DIRECT_MEMORY_RATIO_FOR_MEMORY_FILE_STORAGE: ConfigEntry[Double] =
    buildConf("celeborn.worker.directMemoryRatioForMemoryFileStorage")
      .categories("worker")
      .doc("Max ratio of direct memory to store shuffle data. " +
        "This feature is experimental and disabled by default.")
      .version("0.5.0")
      .doubleConf
      .createWithDefault(0)

  val WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE: ConfigEntry[Double] =
    buildConf("celeborn.worker.directMemoryRatioToPauseReceive")
      .categories("worker")
      .doc("If direct memory usage reaches this limit, the worker will stop to receive data from Celeborn shuffle clients.")
      .version("0.2.0")
      .doubleConf
      .createWithDefault(0.85)

  val WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE: ConfigEntry[Double] =
    buildConf("celeborn.worker.directMemoryRatioToPauseReplicate")
      .categories("worker")
      .doc("If direct memory usage reaches this limit, the worker will stop to receive replication data from other workers. " +
        s"This value should be higher than ${WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE.key}.")
      .version("0.2.0")
      .doubleConf
      .createWithDefault(0.95)

  val WORKER_DIRECT_MEMORY_RATIO_RESUME: ConfigEntry[Double] =
    buildConf("celeborn.worker.directMemoryRatioToResume")
      .categories("worker")
      .doc("If direct memory usage is less than this limit, worker will resume.")
      .version("0.2.0")
      .doubleConf
      .createWithDefault(0.7)

  val WORKER_PINNED_MEMORY_RATIO_RESUME: ConfigEntry[Double] =
    buildConf("celeborn.worker.pinnedMemoryRatioToResume")
      .categories("worker")
      .doc("If pinned memory usage is less than this limit, worker will resume, " +
        "only takes effect when celeborn.network.memory.allocator.pooled and " +
        "celeborn.worker.monitor.pinnedMemory.check.enabled are enabled")
      .version("0.6.0")
      .doubleConf
      .createWithDefault(0.3)

  val WORKER_MEMORY_FILE_STORAGE_MAX_FILE_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.worker.memoryFileStorage.maxFileSize")
      .categories("worker")
      .doc("Max size for a memory storage file. It must be less than 2GB.")
      .version("0.5.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v < Int.MaxValue, "A single memory storage file can not be larger than 2GB")
      .createWithDefaultString("8MB")

  val WORKER_MEMORY_FILE_STORAGE_EVICT_AGGRESSIVE_MODE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.memoryFileStorage.evict.aggressiveMode.enabled")
      .categories("worker")
      .doc(
        "If this set to true, memory shuffle files will be evicted when worker is in PAUSED state." +
          " If the worker's offheap memory is not ample, set this to true " +
          "and decrease `celeborn.worker.directMemoryRatioForMemoryFileStorage` will be helpful.")
      .version("0.5.1")
      .booleanConf
      .createWithDefault(false)

  val WORKER_MEMORY_FILE_STORAGE_EVICT_RATIO: ConfigEntry[Double] =
    buildConf("celeborn.worker.memoryFileStorage.evict.ratio")
      .categories("worker")
      .doc("If memory shuffle storage usage rate is above this config, the memory storage shuffle files will evict to free memory.")
      .version("0.5.1")
      .doubleConf
      .createWithDefault(0.5)

  val WORKER_CONGESTION_CONTROL_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.congestionControl.enabled")
      .categories("worker")
      .doc("Whether to enable congestion control or not.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(false)

  val WORKER_CONGESTION_CONTROL_SAMPLE_TIME_WINDOW: ConfigEntry[Long] =
    buildConf("celeborn.worker.congestionControl.sample.time.window")
      .categories("worker")
      .doc("The worker holds a time sliding list to calculate users' produce/consume rate")
      .version("0.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("10s")

  val WORKER_CONGESTION_CONTROL_DISK_BUFFER_LOW_WATERMARK: ConfigEntry[Long] =
    buildConf("celeborn.worker.congestionControl.diskBuffer.low.watermark")
      .withAlternative("celeborn.worker.congestionControl.low.watermark")
      .categories("worker")
      .doc("Will stop congest users if the total pending bytes of disk buffer is lower than " +
        "this configuration")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(Long.MaxValue)

  val WORKER_CONGESTION_CONTROL_DISK_BUFFER_HIGH_WATERMARK: ConfigEntry[Long] =
    buildConf("celeborn.worker.congestionControl.diskBuffer.high.watermark")
      .withAlternative("celeborn.worker.congestionControl.high.watermark")
      .categories("worker")
      .doc("If the total bytes in disk buffer exceeds this configure, will start to congest " +
        "users whose produce rate is higher than the potential average consume rate. " +
        "The congestion will stop if the produce rate is lower or equal to the " +
        "average consume rate, or the total pending bytes lower than " +
        s"${WORKER_CONGESTION_CONTROL_DISK_BUFFER_LOW_WATERMARK.key}")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(Long.MaxValue)

  val WORKER_CONGESTION_CONTROL_USER_PRODUCE_SPEED_LOW_WATERMARK: ConfigEntry[Long] =
    buildConf("celeborn.worker.congestionControl.userProduceSpeed.low.watermark")
      .categories("worker")
      .doc("For those users that produce byte speeds less than this configuration, " +
        "stop congestion for these users")
      .version("0.6.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(Long.MaxValue)

  val WORKER_CONGESTION_CONTROL_USER_PRODUCE_SPEED_HIGH_WATERMARK: ConfigEntry[Long] =
    buildConf("celeborn.worker.congestionControl.userProduceSpeed.high.watermark")
      .categories("worker")
      .doc("For those users that produce byte speeds greater than this configuration, " +
        "start congestion for these users")
      .version("0.6.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(Long.MaxValue)

  val WORKER_CONGESTION_CONTROL_WORKER_PRODUCE_SPEED_LOW_WATERMARK: ConfigEntry[Long] =
    buildConf("celeborn.worker.congestionControl.workerProduceSpeed.low.watermark")
      .categories("worker")
      .doc("Stop congestion If worker total produce speed less than this configuration")
      .version("0.6.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(Long.MaxValue)

  val WORKER_CONGESTION_CONTROL_WORKER_PRODUCE_SPEED_HIGH_WATERMARK: ConfigEntry[Long] =
    buildConf("celeborn.worker.congestionControl.workerProduceSpeed.high.watermark")
      .categories("worker")
      .doc("Start congestion If worker total produce speed greater than this configuration")
      .version("0.6.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(Long.MaxValue)

  val WORKER_CONGESTION_CONTROL_USER_INACTIVE_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.congestionControl.user.inactive.interval")
      .categories("worker")
      .doc("How long will consider this user is inactive if it doesn't send data")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10min")

  val WORKER_CONGESTION_CONTROL_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.congestionControl.check.interval")
      .categories("worker")
      .doc(
        s"Interval of worker checks congestion if ${WORKER_CONGESTION_CONTROL_ENABLED.key} is true.")
      .version("0.3.2")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10ms")

  val WORKER_DECOMMISSION_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.decommission.checkInterval")
      .categories("worker")
      .doc(
        "The wait interval of checking whether all the shuffle expired during worker decommission")
      .version("0.4.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val WORKER_DECOMMISSION_FORCE_EXIT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.decommission.forceExitTimeout")
      .categories("worker")
      .doc("The wait time of waiting for all the shuffle expire during worker decommission.")
      .version("0.4.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("6h")

  val WORKER_GRACEFUL_SHUTDOWN_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.graceful.shutdown.enabled")
      .categories("worker")
      .doc("When true, during worker shutdown, the worker will wait for all released slots " +
        s"to be committed or destroyed.")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(false)

  val WORKER_GRACEFUL_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.graceful.shutdown.timeout")
      .categories("worker")
      .doc("The worker's graceful shutdown timeout time.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("600s")

  val WORKER_CHECK_SLOTS_FINISHED_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.graceful.shutdown.checkSlotsFinished.interval")
      .categories("worker")
      .doc("The wait interval of checking whether all released slots " +
        "to be committed or destroyed during worker graceful shutdown")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")

  val WORKER_CHECK_SLOTS_FINISHED_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.graceful.shutdown.checkSlotsFinished.timeout")
      .categories("worker")
      .doc("The wait time of waiting for the released slots" +
        " to be committed or destroyed during worker graceful shutdown.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("480s")

  val WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH: ConfigEntry[String] =
    buildConf("celeborn.worker.graceful.shutdown.recoverPath")
      .categories("worker")
      .doc("The path to store DB.")
      .version("0.2.0")
      .stringConf
      .transform(_.replace("<tmp>", System.getProperty("java.io.tmpdir")))
      .createWithDefault(s"<tmp>/recover")

  val WORKER_GRACEFUL_SHUTDOWN_RECOVER_DB_BACKEND: ConfigEntry[String] =
    buildConf("celeborn.worker.graceful.shutdown.recoverDbBackend")
      .categories("worker")
      .doc("Specifies a disk-based store used in local db. ROCKSDB or LEVELDB (deprecated).")
      .version("0.4.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set("LEVELDB", "ROCKSDB"))
      .createWithDefault("ROCKSDB")

  val WORKER_PARTITION_SORTER_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.graceful.shutdown.partitionSorter.shutdownTimeout")
      .categories("worker")
      .doc("The wait time of waiting for sorting partition files" +
        " during worker graceful shutdown.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  val WORKER_PARTITION_READ_BUFFERS_MIN: ConfigEntry[Int] =
    buildConf("celeborn.worker.partition.initial.readBuffersMin")
      .categories("worker")
      .version("0.3.0")
      .doc("Min number of initial read buffers")
      .intConf
      .createWithDefault(1)

  val WORKER_PARTITION_READ_BUFFERS_MAX: ConfigEntry[Int] =
    buildConf("celeborn.worker.partition.initial.readBuffersMax")
      .categories("worker")
      .version("0.3.0")
      .doc("Max number of initial read buffers")
      .intConf
      .createWithDefault(1024)

  val WORKER_BUFFERSTREAM_THREADS_PER_MOUNTPOINT: ConfigEntry[Int] =
    buildConf("celeborn.worker.bufferStream.threadsPerMountpoint")
      .categories("worker")
      .version("0.3.0")
      .doc("Threads count for read buffer per mount point.")
      .intConf
      .createWithDefault(8)

  val WORKER_READBUFFER_ALLOCATIONWAIT: ConfigEntry[Long] =
    buildConf("celeborn.worker.readBuffer.allocationWait")
      .categories("worker")
      .version("0.3.0")
      .doc("The time to wait when buffer dispatcher can not allocate a buffer.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("50ms")

  val WORKER_READBUFFER_TARGET_RATIO: ConfigEntry[Double] =
    buildConf("celeborn.worker.readBuffer.target.ratio")
      .categories("worker")
      .version("0.3.0")
      .doc("The target ratio for read ahead buffer's memory usage.")
      .doubleConf
      .createWithDefault(0.9)

  val WORKER_READBUFFER_TARGET_UPDATE_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.readBuffer.target.updateInterval")
      .categories("worker")
      .version("0.3.0")
      .doc("The interval for memory manager to calculate new read buffer's target memory.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("100ms")

  val WORKER_READBUFFER_TARGET_NOTIFY_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.worker.readBuffer.target.changeThreshold")
      .categories("worker")
      .version("0.3.0")
      .doc("The target ratio for pre read memory usage.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1mb")

  val WORKER_READBUFFERS_TOTRIGGERREAD_MIN: ConfigEntry[Int] =
    buildConf("celeborn.worker.readBuffer.toTriggerReadMin")
      .categories("worker")
      .version("0.3.0")
      .doc("Min buffers count for map data partition to trigger read.")
      .intConf
      .createWithDefault(32)

  val WORKER_READBUFFER_CHECK_THREAD_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.readBufferDispatcherThreadWatchdog.checkInterval")
      .categories("worker")
      .version("0.5.2")
      .internal
      .doc("The interval for worker to check read buffer dispatcher thread. 0 means disable.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(0)

  val WORKER_PUSH_HEARTBEAT_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.push.heartbeat.enabled")
      .categories("worker")
      .version("0.3.0")
      .doc("enable the heartbeat from worker to client when pushing data")
      .booleanConf
      .createWithDefault(false)

  val WORKER_PUSH_COMPOSITEBUFFER_MAXCOMPONENTS: ConfigEntry[Int] =
    buildConf("celeborn.worker.push.compositeBuffer.maxComponents")
      .internal
      .categories("worker")
      .version("0.3.0")
      .doc("Max components of Netty `CompositeByteBuf` in `FileWriter`'s `flushBuffer`. " +
        "When this value is too big, i.e. 256, there will be many memory fragments in Netty's memory pool, " +
        "and total direct memory can be significantly larger than the disk buffer. " +
        "When set to 1, Netty's direct memory is close to disk buffer, but performance " +
        "might decrease due to frequent memory copy during compaction.")
      .intConf
      .createWithDefault(128)

  val WORKER_FETCH_HEARTBEAT_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.fetch.heartbeat.enabled")
      .categories("worker")
      .version("0.3.0")
      .doc("enable the heartbeat from worker to client when fetching data")
      .booleanConf
      .createWithDefault(false)

  val WORKER_ACTIVE_CONNECTION_MAX: OptionalConfigEntry[Long] =
    buildConf("celeborn.worker.activeConnection.max")
      .categories("worker")
      .doc("If the number of active connections on a worker exceeds this configuration value, " +
        "the worker will be marked as high-load in the heartbeat report, " +
        "and the master will not include that node in the response of RequestSlots.")
      .version("0.3.1")
      .longConf
      .createOptional

  val WORKER_JVM_PROFILER_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.jvmProfiler.enabled")
      .categories("worker")
      .version("0.5.0")
      .doc("Turn on code profiling via async_profiler in workers.")
      .booleanConf
      .createWithDefault(false)

  val WORKER_JVM_PROFILER_OPTIONS: ConfigEntry[String] =
    buildConf("celeborn.worker.jvmProfiler.options")
      .categories("worker")
      .version("0.5.0")
      .doc("Options to pass on to the async profiler.")
      .stringConf
      .createWithDefault("event=wall,interval=10ms,alloc=2m,lock=10ms,chunktime=300s")

  val WORKER_JVM_PROFILER_LOCAL_DIR: ConfigEntry[String] =
    buildConf("celeborn.worker.jvmProfiler.localDir")
      .categories("worker")
      .version("0.5.0")
      .doc("Local file system path on worker where profiler output is saved. "
        + "Defaults to the working directory of the worker process.")
      .stringConf
      .createWithDefault(".")

  val WORKER_JVM_QUAKE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.jvmQuake.enabled")
      .categories("worker")
      .version("0.4.0")
      .doc("When true, Celeborn worker will start the jvm quake to monitor of gc behavior, " +
        "which enables early detection of memory management issues and facilitates fast failure.")
      .booleanConf
      .createWithDefault(false)

  val WORKER_JVM_QUAKE_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.jvmQuake.check.interval")
      .categories("worker")
      .version("0.4.0")
      .doc("Interval of gc behavior checking for worker jvm quake.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")

  val WORKER_JVM_QUAKE_RUNTIME_WEIGHT: ConfigEntry[Double] =
    buildConf("celeborn.worker.jvmQuake.runtimeWeight")
      .categories("worker")
      .version("0.4.0")
      .doc(
        "The factor by which to multiply running JVM time, when weighing it against GCing time. " +
          "'Deficit' is accumulated as `gc_time - runtime * runtime_weight`, and is compared against threshold " +
          "to determine whether to take action.")
      .doubleConf
      .createWithDefault(5)

  val WORKER_JVM_QUAKE_DUMP_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.worker.jvmQuake.dump.threshold")
      .categories("worker")
      .version("0.4.0")
      .doc("The threshold of heap dump for the maximum GC 'deficit' which can be accumulated before jvmquake takes action. " +
        "Meanwhile, there is no heap dump generated when dump threshold is greater than kill threshold.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val WORKER_JVM_QUAKE_KILL_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.worker.jvmQuake.kill.threshold")
      .categories("worker")
      .version("0.4.0")
      .doc("The threshold of system kill for the maximum GC 'deficit' which can be accumulated before jvmquake takes action.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  val WORKER_JVM_QUAKE_DUMP_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.jvmQuake.dump.enabled")
      .categories("worker")
      .version("0.4.0")
      .doc("Whether to heap dump for the maximum GC 'deficit' during worker jvm quake.")
      .booleanConf
      .createWithDefault(true)

  val WORKER_JVM_QUAKE_DUMP_PATH: ConfigEntry[String] =
    buildConf("celeborn.worker.jvmQuake.dump.path")
      .categories("worker")
      .version("0.4.0")
      .doc("The path of heap dump for the maximum GC 'deficit' during worker jvm quake.")
      .stringConf
      .transform(_.replace("<tmp>", System.getProperty("java.io.tmpdir"))
        .replace("<pid>", Utils.getProcessId))
      .createWithDefault(s"<tmp>/jvm-quake/dump/<pid>")

  val WORKER_JVM_QUAKE_EXIT_CODE: ConfigEntry[Int] =
    buildConf("celeborn.worker.jvmQuake.exitCode")
      .categories("worker")
      .version("0.4.0")
      .doc("The exit code of system kill for the maximum GC 'deficit' during worker jvm quake.")
      .intConf
      .createWithDefault(502)

  val APPLICATION_HEARTBEAT_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.client.application.heartbeatInterval")
      .withAlternative("celeborn.application.heartbeatInterval")
      .categories("client")
      .version("0.3.0")
      .doc("Interval for client to send heartbeat message to master.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10s")

  val APPLICATION_UNREGISTER_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.application.unregister.enabled")
      .categories("client")
      .version("0.3.2")
      .doc("When true, Celeborn client will inform celeborn master the application is already shutdown during client " +
        "exit, this allows the cluster to release resources immediately, resulting in resource savings.")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_EXCLUDE_PEER_WORKER_ON_FAILURE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.excludePeerWorkerOnFailure.enabled")
      .categories("client")
      .version("0.3.0")
      .doc("When true, Celeborn will exclude partition's peer worker on failure " +
        "when push data to replica failed.")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_EXCLUDED_WORKER_EXPIRE_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.excludedWorker.expireTimeout")
      .withAlternative("celeborn.worker.excluded.expireTimeout")
      .categories("client")
      .version("0.3.0")
      .doc("Timeout time for LifecycleManager to clear reserved excluded worker. Default to be 1.5 * `celeborn.master.heartbeat.worker.timeout` " +
        "to cover worker heartbeat timeout check period")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("180s")

  val CLIENT_CHECKED_USE_ALLOCATED_WORKERS: ConfigEntry[Boolean] =
    buildConf("celeborn.client.checked.useAllocatedWorkers")
      .internal
      .categories("client")
      .version("0.3.0")
      .doc("When true, Celeborn will use local allocated workers as candidate being checked workers(check the workers " +
        "whether unKnown in master), this may be more useful for map partition to regenerate the lost data), " +
        "otherwise use local black list as candidate being checked workers.")
      .booleanConf
      .createWithDefault(false)

  val TEST_CLIENT_MOCK_DESTROY_SLOTS_FAILURE: ConfigEntry[Boolean] =
    buildConf("celeborn.test.client.mockDestroySlotsFailure")
      .internal
      .categories("test", "client")
      .doc("Fail destroy slots request for test")
      .version("0.3.2")
      .booleanConf
      .createWithDefault(false)

  val TEST_MOCK_COMMIT_FILES_FAILURE: ConfigEntry[Boolean] =
    buildConf("celeborn.test.mockCommitFilesFailure")
      .internal
      .categories("test", "client", "worker")
      .doc("Fail commit files request for test")
      .version("0.3.2")
      .booleanConf
      .createWithDefault(false)

  val TEST_CLIENT_MOCK_SHUFFLE_LOST: ConfigEntry[Boolean] =
    buildConf("celeborn.test.client.mockShuffleLost")
      .internal
      .categories("test", "client")
      .doc("Mock shuffle lost.")
      .version("0.5.2")
      .internal
      .booleanConf
      .createWithDefault(false)

  val TEST_CLIENT_MOCK_SHUFFLE_LOST_SHUFFLE: ConfigEntry[Int] =
    buildConf("celeborn.test.client.mockShuffleLostShuffle")
      .internal
      .categories("test", "client")
      .doc("Mock shuffle lost for shuffle")
      .version("0.5.2")
      .internal
      .intConf
      .createWithDefault(0)

  val CLIENT_PUSH_REPLICATE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.push.replicate.enabled")
      .withAlternative("celeborn.push.replicate.enabled")
      .categories("client")
      .doc("When true, Celeborn worker will replicate shuffle data to another Celeborn worker " +
        "asynchronously to ensure the pushed shuffle data won't be lost after the node failure. " +
        "It's recommended to set `false` when `HDFS` is enabled in `celeborn.storage.availableTypes`.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_PUSH_BUFFER_INITIAL_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.client.push.buffer.initial.size")
      .withAlternative("celeborn.push.buffer.initial.size")
      .categories("client")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8k")

  val CLIENT_PUSH_BUFFER_MAX_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.client.push.buffer.max.size")
      .withAlternative("celeborn.push.buffer.max.size")
      .categories("client")
      .version("0.3.0")
      .doc("Max size of reducer partition buffer memory for shuffle hash writer. The pushed " +
        "data will be buffered in memory before sending to Celeborn worker. For performance " +
        "consideration keep this buffer size higher than 32K. Example: If reducer amount is " +
        "2000, buffer size is 64K, then each task will consume up to `64KiB * 2000 = 125MiB` " +
        "heap memory.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64k")

  val CLIENT_PUSH_QUEUE_CAPACITY: ConfigEntry[Int] =
    buildConf("celeborn.client.push.queue.capacity")
      .withAlternative("celeborn.push.queue.capacity")
      .categories("client")
      .version("0.3.0")
      .doc("Push buffer queue size for a task. The maximum memory is " +
        "`celeborn.client.push.buffer.max.size` * `celeborn.client.push.queue.capacity`, " +
        "default: 64KiB * 512 = 32MiB")
      .intConf
      .createWithDefault(512)

  val CLIENT_PUSH_MAX_REQS_IN_FLIGHT_TOTAL: ConfigEntry[Int] =
    buildConf("celeborn.client.push.maxReqsInFlight.total")
      .withAlternative("celeborn.push.maxReqsInFlight")
      .categories("client")
      .version("0.3.0")
      .doc("Amount of total Netty in-flight requests. The maximum memory is " +
        "`celeborn.client.push.maxReqsInFlight.total` * `celeborn.client.push.buffer.max.size` " +
        "* compression ratio(1 in worst case): 64KiB * 256 = 16MiB")
      .intConf
      .createWithDefault(256)

  val CLIENT_PUSH_MAX_REQS_IN_FLIGHT_PERWORKER: ConfigEntry[Int] =
    buildConf("celeborn.client.push.maxReqsInFlight.perWorker")
      .categories("client")
      .version("0.3.0")
      .doc(
        "Amount of Netty in-flight requests per worker. Default max memory of in flight requests " +
          " per worker is `celeborn.client.push.maxReqsInFlight.perWorker` * `celeborn.client.push.buffer.max.size` " +
          "* compression ratio(1 in worst case): 64KiB * 32 = 2MiB. The maximum memory will " +
          "not exceed `celeborn.client.push.maxReqsInFlight.total`.")
      .intConf
      .createWithDefault(32)

  val CLIENT_PUSH_MAX_REVIVE_TIMES: ConfigEntry[Int] =
    buildConf("celeborn.client.push.revive.maxRetries")
      .categories("client")
      .version("0.3.0")
      .doc("Max retry times for reviving when celeborn push data failed.")
      .intConf
      .createWithDefault(5)

  val CLIENT_PUSH_REVIVE_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.client.push.revive.interval")
      .categories("client")
      .version("0.3.0")
      .doc("Interval for client to trigger Revive to LifecycleManager. The number of partitions in one Revive " +
        "request is `celeborn.client.push.revive.batchSize`.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("100ms")

  val CLIENT_PUSH_REVIVE_BATCHSIZE: ConfigEntry[Int] =
    buildConf("celeborn.client.push.revive.batchSize")
      .categories("client")
      .version("0.3.0")
      .doc("Max number of partitions in one Revive request.")
      .intConf
      .createWithDefault(2048)

  val CLIENT_PUSH_EXCLUDE_WORKER_ON_FAILURE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.push.excludeWorkerOnFailure.enabled")
      .categories("client")
      .doc("Whether to enable shuffle client-side push exclude workers on failures.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_PUSH_LIMIT_STRATEGY: ConfigEntry[String] =
    buildConf("celeborn.client.push.limit.strategy")
      .categories("client")
      .doc("The strategy used to control the push speed. " +
        "Valid strategies are SIMPLE and SLOWSTART. The SLOWSTART strategy usually works with " +
        "congestion control mechanism on the worker side.")
      .version("0.3.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set("SIMPLE", "SLOWSTART"))
      .createWithDefaultString("SIMPLE")

  val CLIENT_PUSH_SLOW_START_INITIAL_SLEEP_TIME: ConfigEntry[Long] =
    buildConf("celeborn.client.push.slowStart.initialSleepTime")
      .categories("client")
      .version("0.3.0")
      .doc(s"The initial sleep time if the current max in flight requests is 0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("500ms")

  val CLIENT_PUSH_SLOW_START_MAX_SLEEP_TIME: ConfigEntry[Long] =
    buildConf("celeborn.client.push.slowStart.maxSleepTime")
      .categories("client")
      .version("0.3.0")
      .doc(s"If ${CLIENT_PUSH_LIMIT_STRATEGY.key} is set to SLOWSTART, push side will " +
        "take a sleep strategy for each batch of requests, this controls " +
        "the max sleep time if the max in flight requests limit is 1 for a long time")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("2s")

  val CLIENT_PUSH_DATA_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.push.timeout")
      .withAlternative("celeborn.push.data.timeout")
      .categories("client")
      .version("0.3.0")
      .doc(s"Timeout for a task to push data rpc message. This value should better be more than twice of `${PUSH_TIMEOUT_CHECK_INTERVAL.key}`")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(_ > 0, "Value must be positive!")
      .createWithDefaultString("120s")

  val TEST_CLIENT_PUSH_PRIMARY_DATA_TIMEOUT: ConfigEntry[Boolean] =
    buildConf("celeborn.test.worker.pushPrimaryDataTimeout")
      .withAlternative("celeborn.test.pushMasterDataTimeout")
      .internal
      .categories("test", "worker")
      .version("0.3.0")
      .doc("Whether to test push primary data timeout")
      .booleanConf
      .createWithDefault(false)

  val TEST_WORKER_PUSH_REPLICA_DATA_TIMEOUT: ConfigEntry[Boolean] =
    buildConf("celeborn.test.worker.pushReplicaDataTimeout")
      .internal
      .categories("test", "worker")
      .version("0.3.0")
      .doc("Whether to test push replica data timeout")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_PUSH_LIMIT_IN_FLIGHT_TIMEOUT: OptionalConfigEntry[Long] =
    buildConf("celeborn.client.push.limit.inFlight.timeout")
      .withAlternative("celeborn.push.limit.inFlight.timeout")
      .categories("client")
      .doc("Timeout for netty in-flight requests to be done. " +
        s"Default value should be `${CLIENT_PUSH_DATA_TIMEOUT.key} * 2`.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  val CLIENT_PUSH_LIMIT_IN_FLIGHT_SLEEP_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.client.push.limit.inFlight.sleepInterval")
      .withAlternative("celeborn.push.limit.inFlight.sleepInterval")
      .categories("client")
      .doc("Sleep interval when check netty in-flight requests to be done.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("50ms")

  val CLIENT_PUSH_SORT_RANDOMIZE_PARTITION_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.push.sort.randomizePartitionId.enabled")
      .withAlternative("celeborn.push.sort.randomizePartitionId.enabled")
      .categories("client")
      .doc(
        "Whether to randomize partitionId in push sorter. If true, partitionId will be randomized " +
          "when sort data to avoid skew when push to worker")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_PUSH_RETRY_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.client.push.retry.threads")
      .withAlternative("celeborn.push.retry.threads")
      .categories("client")
      .doc("Thread number to process shuffle re-send push data requests.")
      .version("0.3.0")
      .intConf
      .createWithDefault(8)

  val CLIENT_PUSH_TAKE_TASK_WAIT_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.client.push.takeTaskWaitInterval")
      .categories("client")
      .doc("Wait interval if no task available to push to worker.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("50ms")

  val CLIENT_PUSH_TAKE_TASK_MAX_WAIT_ATTEMPTS: ConfigEntry[Int] =
    buildConf("celeborn.client.push.takeTaskMaxWaitAttempts")
      .categories("client")
      .doc("Max wait times if no task available to push to worker.")
      .version("0.3.0")
      .intConf
      .createWithDefault(1)

  val CLIENT_PUSH_SENDBUFFERPOOL_EXPIRETIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.push.sendBufferPool.expireTimeout")
      .categories("client")
      .doc("Timeout before clean up SendBufferPool. If SendBufferPool is idle for more than this time, " +
        "the send buffers and push tasks will be cleaned up.")
      .version("0.3.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  val CLIENT_PUSH_SENDBUFFERPOOL_CHECKEXPIREINTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.client.push.sendBufferPool.checkExpireInterval")
      .categories("client")
      .doc("Interval to check expire for send buffer pool. If the pool has been idle " +
        s"for more than `${CLIENT_PUSH_SENDBUFFERPOOL_EXPIRETIMEOUT.key}`, the pooled send buffers and push tasks will be cleaned up.")
      .version("0.3.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val TEST_CLIENT_RETRY_REVIVE: ConfigEntry[Boolean] =
    buildConf("celeborn.test.client.retryRevive")
      .withAlternative("celeborn.test.retryRevive")
      .internal
      .categories("test", "client")
      .doc("Fail push data and request for test")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_FETCH_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.fetch.timeout")
      .withAlternative("celeborn.fetch.timeout")
      .categories("client")
      .version("0.3.0")
      .doc("Timeout for a task to open stream and fetch chunk.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("600s")

  val CLIENT_FETCH_BUFFER_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.client.fetch.buffer.size")
      .categories("client")
      .version("0.4.0")
      .doc("Size of reducer partition buffer memory for shuffle reader. The fetched data " +
        "will be buffered in memory before consuming. For performance consideration keep " +
        s"this buffer size not less than `${CLIENT_PUSH_BUFFER_MAX_SIZE.key}`.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64k")

  val CLIENT_FETCH_MAX_REQS_IN_FLIGHT: ConfigEntry[Int] =
    buildConf("celeborn.client.fetch.maxReqsInFlight")
      .withAlternative("celeborn.fetch.maxReqsInFlight")
      .categories("client")
      .version("0.3.0")
      .doc("Amount of in-flight chunk fetch request.")
      .intConf
      .createWithDefault(3)

  val CLIENT_FETCH_MAX_RETRIES_FOR_EACH_REPLICA: ConfigEntry[Int] =
    buildConf("celeborn.client.fetch.maxRetriesForEachReplica")
      .withAlternative("celeborn.fetch.maxRetriesForEachReplica")
      .withAlternative("celeborn.fetch.maxRetries")
      .categories("client")
      .version("0.3.0")
      .doc("Max retry times of fetch chunk on each replica")
      .intConf
      .createWithDefault(3)

  val CLIENT_STAGE_RERUN_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.spark.stageRerun.enabled")
      .withAlternative("celeborn.client.spark.fetch.throwsFetchFailure")
      .categories("client")
      .version("0.4.0")
      .doc("Whether to enable stage rerun. If true, client throws FetchFailedException instead of CelebornIOException.")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_FETCH_EXCLUDE_WORKER_ON_FAILURE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.fetch.excludeWorkerOnFailure.enabled")
      .categories("client")
      .doc("Whether to enable shuffle client-side fetch exclude workers on failure.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_FETCH_EXCLUDED_WORKER_EXPIRE_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.fetch.excludedWorker.expireTimeout")
      .categories("client")
      .doc(
        "ShuffleClient is a static object, it will be used in the whole lifecycle of Executor, " +
          "We give a expire time for excluded workers to avoid a transient worker issues.")
      .version("0.3.0")
      .fallbackConf(CLIENT_EXCLUDED_WORKER_EXPIRE_TIMEOUT)

  val TEST_CLIENT_FETCH_FAILURE: ConfigEntry[Boolean] =
    buildConf("celeborn.test.client.fetchFailure")
      .withAlternative("celeborn.test.fetchFailure")
      .internal
      .categories("test", "client")
      .version("0.3.0")
      .doc("Whether to test fetch chunk failure")
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_RANGE_READ_FILTER_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.rangeReadFilter.enabled")
      .withAlternative("celeborn.shuffle.rangeReadFilter.enabled")
      .categories("client")
      .version("0.2.0")
      .doc("If a spark application have skewed partition, this value can set to true to improve performance.")
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_PARTITION_TYPE: ConfigEntry[String] =
    buildConf("celeborn.client.shuffle.partition.type")
      .withAlternative("celeborn.shuffle.partition.type")
      .categories("client")
      .doc("Type of shuffle's partition.")
      .version("0.3.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set(
        PartitionType.REDUCE.name,
        PartitionType.MAP.name,
        PartitionType.MAPGROUP.name))
      .createWithDefault(PartitionType.REDUCE.name)

  val SHUFFLE_PARTITION_SPLIT_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.client.shuffle.partitionSplit.threshold")
      .withAlternative("celeborn.shuffle.partitionSplit.threshold")
      .categories("client")
      .doc("Shuffle file size threshold, if file size exceeds this, trigger split.")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1G")

  val SHUFFLE_PARTITION_SPLIT_MODE: ConfigEntry[String] =
    buildConf("celeborn.client.shuffle.partitionSplit.mode")
      .withAlternative("celeborn.shuffle.partitionSplit.mode")
      .categories("client")
      .doc("soft: the shuffle file size might be larger than split threshold. " +
        "hard: the shuffle file size will be limited to split threshold.")
      .version("0.3.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set(PartitionSplitMode.SOFT.name, PartitionSplitMode.HARD.name))
      .createWithDefault(PartitionSplitMode.SOFT.name)

  val SHUFFLE_COMPRESSION_CODEC: ConfigEntry[String] =
    buildConf("celeborn.client.shuffle.compression.codec")
      .withAlternative("celeborn.shuffle.compression.codec")
      .withAlternative("remote-shuffle.job.compression.codec")
      .categories("client")
      .doc("The codec used to compress shuffle data. By default, Celeborn provides three codecs: `lz4`, `zstd`, `none`. " +
        "`none` means that shuffle compression is disabled. " +
        "Since Flink version 1.16, zstd is supported for Flink shuffle client.")
      .version("0.3.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set(
        CompressionCodec.LZ4.name,
        CompressionCodec.ZSTD.name,
        CompressionCodec.NONE.name))
      .createWithDefault(CompressionCodec.LZ4.name)

  val SHUFFLE_DECOMPRESSION_LZ4_XXHASH_INSTANCE: OptionalConfigEntry[String] =
    buildConf("celeborn.client.shuffle.decompression.lz4.xxhash.instance")
      .categories("client")
      .doc("Decompression XXHash instance for Lz4. Available options: JNI, JAVASAFE, JAVAUNSAFE.")
      .version("0.3.2")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set("JNI", "JAVASAFE", "JAVAUNSAFE"))
      .createOptional

  val SHUFFLE_COMPRESSION_ZSTD_LEVEL: ConfigEntry[Int] =
    buildConf("celeborn.client.shuffle.compression.zstd.level")
      .withAlternative("celeborn.shuffle.compression.zstd.level")
      .categories("client")
      .doc("Compression level for Zstd compression codec, its value should be an integer " +
        "between -5 and 22. Increasing the compression level will result in better compression " +
        "at the expense of more CPU and memory.")
      .version("0.3.0")
      .intConf
      .checkValue(
        value => value >= -5 && value <= 22,
        s"Compression level for Zstd compression codec should be an integer between -5 and 22.")
      .createWithDefault(1)

  val SHUFFLE_EXPIRED_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.client.shuffle.expired.checkInterval")
      .withAlternative("celeborn.shuffle.expired.checkInterval")
      .categories("client")
      .version("0.3.0")
      .doc("Interval for client to check expired shuffles.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  val CLIENT_SHUFFLE_MANAGER_PORT: ConfigEntry[Int] =
    buildConf("celeborn.client.shuffle.manager.port")
      .withAlternative("celeborn.shuffle.manager.port")
      .categories("client")
      .version("0.3.0")
      .doc("Port used by the LifecycleManager on the Driver.")
      .intConf
      .checkValue(
        (port: Int) => {
          if (port != 0) {
            logWarning(
              "The user specifies the port used by the LifecycleManager on the Driver, and its" +
                s" values is $port, which may cause port conflicts and startup failure.")
          }
          true
        },
        "")
      .createWithDefault(0)

  val CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.batchHandleChangePartition.enabled")
      .withAlternative("celeborn.shuffle.batchHandleChangePartition.enabled")
      .categories("client")
      .internal
      .doc("When true, LifecycleManager will handle change partition request in batch. " +
        "Otherwise, LifecycleManager will process the requests one by one")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_BATCH_REMOVE_EXPIRED_SHUFFLE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.batchHandleRemoveExpiredShuffles.enabled")
      .categories("client")
      .version("0.6.0")
      .doc("Whether to batch remove expired shuffles. This is an optimization switch on removing expired shuffles.")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_BATCH_HANDLE_CHANGE_PARTITION_BUCKETS: ConfigEntry[Int] =
    buildConf("celeborn.client.shuffle.batchHandleChangePartition.partitionBuckets")
      .categories("client")
      .doc("Max number of change partition requests which can be concurrently processed.")
      .version("0.5.0")
      .intConf
      .createWithDefault(256)

  val CLIENT_BATCH_HANDLE_CHANGE_PARTITION_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.client.shuffle.batchHandleChangePartition.threads")
      .withAlternative("celeborn.shuffle.batchHandleChangePartition.threads")
      .categories("client")
      .doc("Threads number for LifecycleManager to handle change partition request in batch.")
      .version("0.3.0")
      .intConf
      .createWithDefault(8)

  val CLIENT_BATCH_HANDLE_CHANGE_PARTITION_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.client.shuffle.batchHandleChangePartition.interval")
      .withAlternative("celeborn.shuffle.batchHandleChangePartition.interval")
      .categories("client")
      .doc("Interval for LifecycleManager to schedule handling change partition requests in batch.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("100ms")

  val CLIENT_BATCH_HANDLE_COMMIT_PARTITION_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.batchHandleCommitPartition.enabled")
      .withAlternative("celeborn.shuffle.batchHandleCommitPartition.enabled")
      .categories("client")
      .internal
      .doc("When true, LifecycleManager will handle commit partition request in batch. " +
        "Otherwise, LifecycleManager won't commit partition before stage end")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_BATCH_HANDLE_COMMIT_PARTITION_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.client.shuffle.batchHandleCommitPartition.threads")
      .withAlternative("celeborn.shuffle.batchHandleCommitPartition.threads")
      .categories("client")
      .doc("Threads number for LifecycleManager to handle commit partition request in batch.")
      .version("0.3.0")
      .intConf
      .createWithDefault(8)

  val CLIENT_BATCH_HANDLED_COMMIT_PARTITION_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.client.shuffle.batchHandleCommitPartition.interval")
      .withAlternative("celeborn.shuffle.batchHandleCommitPartition.interval")
      .categories("client")
      .doc("Interval for LifecycleManager to schedule handling commit partition requests in batch.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5s")

  val CLIENT_BATCH_HANDLE_RELEASE_PARTITION_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.batchHandleReleasePartition.enabled")
      .categories("client")
      .internal
      .doc("When true, LifecycleManager will handle release partition request in batch. " +
        "Otherwise, LifecycleManager will process release partition request immediately")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_BATCH_HANDLE_RELEASE_PARTITION_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.client.shuffle.batchHandleReleasePartition.threads")
      .categories("client")
      .doc("Threads number for LifecycleManager to handle release partition request in batch.")
      .version("0.3.0")
      .intConf
      .createWithDefault(8)

  val CLIENT_BATCH_HANDLED_RELEASE_PARTITION_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.client.shuffle.batchHandleReleasePartition.interval")
      .categories("client")
      .doc(
        "Interval for LifecycleManager to schedule handling release partition requests in batch.")
      .version("0.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5s")

  val CLIENT_REGISTER_SHUFFLE_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("celeborn.client.registerShuffle.maxRetries")
      .withAlternative("celeborn.shuffle.register.maxRetries")
      .categories("client")
      .version("0.3.0")
      .doc("Max retry times for client to register shuffle.")
      .intConf
      .createWithDefault(3)

  val CLIENT_REGISTER_SHUFFLE_RETRY_WAIT: ConfigEntry[Long] =
    buildConf("celeborn.client.registerShuffle.retryWait")
      .withAlternative("celeborn.shuffle.register.retryWait")
      .categories("client")
      .version("0.3.0")
      .doc("Wait time before next retry if register shuffle failed.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")

  val CLIENT_RESERVE_SLOTS_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("celeborn.client.reserveSlots.maxRetries")
      .withAlternative("celeborn.slots.reserve.maxRetries")
      .categories("client")
      .version("0.3.0")
      .doc("Max retry times for client to reserve slots.")
      .intConf
      .createWithDefault(3)

  val CLIENT_RESERVE_SLOTS_RETRY_WAIT: ConfigEntry[Long] =
    buildConf("celeborn.client.reserveSlots.retryWait")
      .withAlternative("celeborn.slots.reserve.retryWait")
      .categories("client")
      .version("0.3.0")
      .doc("Wait time before next retry if reserve slots failed.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")

  val CLIENT_COMMIT_FILE_REQUEST_MAX_RETRY: ConfigEntry[Int] =
    buildConf("celeborn.client.requestCommitFiles.maxRetries")
      .categories("client")
      .doc("Max retry times for requestCommitFiles RPC.")
      .version("0.3.0")
      .intConf
      .checkValue(v => v > 0, "Value must be positive")
      .createWithDefault(4)

  val CLIENT_COMMIT_IGNORE_EXCLUDED_WORKERS: ConfigEntry[Boolean] =
    buildConf("celeborn.client.commitFiles.ignoreExcludedWorker")
      .categories("client")
      .version("0.3.0")
      .doc("When true, LifecycleManager will skip workers which are in the excluded list.")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.dynamicResourceEnabled")
      .categories("client")
      .version("0.6.0")
      .doc("When enabled, the ChangePartitionManager will obtain candidate workers from the availableWorkers pool " +
        "during heartbeats when worker resource change.")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_SHUFFLE_DYNAMIC_RESOURCE_FACTOR: ConfigEntry[Double] =
    buildConf("celeborn.client.shuffle.dynamicResourceFactor")
      .categories("client")
      .version("0.6.0")
      .doc("The ChangePartitionManager will check whether (unavailable workers / shuffle allocated workers) " +
        "is more than the factor before obtaining candidate workers from the requestSlots RPC response " +
        s"when `${CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED.key}` set true")
      .doubleConf
      .checkValue(v => v >= 0.0 && v <= 1.0, "Should be in [0.0, 1.0].")
      .createWithDefault(0.5)

  val CLIENT_PUSH_STAGE_END_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.push.stageEnd.timeout")
      .withAlternative("celeborn.push.stageEnd.timeout")
      .categories("client")
      .doc(s"Timeout for waiting StageEnd. " +
        s"During this process, there are `${CLIENT_COMMIT_FILE_REQUEST_MAX_RETRY.key}` times for retry opportunities for committing files " +
        s"and 1 times for releasing slots request. User can customize this value according to your setting. " +
        s"By default, the value is the max timeout value `${NETWORK_IO_CONNECTION_TIMEOUT.key}`.")
      .version("0.3.0")
      .fallbackConf(NETWORK_IO_CONNECTION_TIMEOUT)

  val CLIENT_RESERVE_SLOTS_RPC_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.rpc.reserveSlots.askTimeout")
      .categories("client")
      .version("0.3.0")
      .doc("Timeout for LifecycleManager request reserve slots.")
      .fallbackConf(RPC_ASK_TIMEOUT)

  val CLIENT_RPC_REGISTER_SHUFFLE_ASK_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.rpc.registerShuffle.askTimeout")
      .withAlternative("celeborn.rpc.registerShuffle.askTimeout")
      .categories("client")
      .version("0.3.0")
      .doc(s"Timeout for ask operations during register shuffle. " +
        s"During this process, there are two times for retry opportunities for requesting slots, " +
        s"one request for establishing a connection with Worker and " +
        s"`${CLIENT_RESERVE_SLOTS_MAX_RETRIES.key}` times for retry opportunities for reserving slots. " +
        s"User can customize this value according to your setting.")
      .fallbackConf(RPC_ASK_TIMEOUT)

  val CLIENT_RPC_REQUEST_PARTITION_LOCATION_ASK_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.rpc.requestPartition.askTimeout")
      .categories("client")
      .version("0.2.0")
      .doc(s"Timeout for ask operations during requesting change partition location, such as reviving or splitting partition. " +
        s"During this process, there are `${CLIENT_RESERVE_SLOTS_MAX_RETRIES.key}` times for retry opportunities for reserving slots. " +
        s"User can customize this value according to your setting.")
      .fallbackConf(RPC_ASK_TIMEOUT)

  val CLIENT_RPC_GET_REDUCER_FILE_GROUP_ASK_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.rpc.getReducerFileGroup.askTimeout")
      .categories("client")
      .version("0.2.0")
      .doc(s"Timeout for ask operations during getting reducer file group information. " +
        s"During this process, there are `${CLIENT_COMMIT_FILE_REQUEST_MAX_RETRY.key}` times for retry opportunities for committing files " +
        s"and 1 times for releasing slots request. User can customize this value according to your setting.")
      .fallbackConf(RPC_ASK_TIMEOUT)

  val CLIENT_RPC_COMMIT_FILES_ASK_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.client.rpc.commitFiles.askTimeout")
      .categories("client")
      .version("0.4.1")
      .doc("Timeout for CommitHandler commit files.")
      .fallbackConf(RPC_ASK_TIMEOUT)

  val CLIENT_RPC_CACHE_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.client.rpc.cache.size")
      .withAlternative("celeborn.rpc.cache.size")
      .categories("client")
      .version("0.3.0")
      .doc("The max cache items count for rpc cache.")
      .intConf
      .createWithDefault(256)

  val CLIENT_RPC_CACHE_CONCURRENCY_LEVEL: ConfigEntry[Int] =
    buildConf("celeborn.client.rpc.cache.concurrencyLevel")
      .withAlternative("celeborn.rpc.cache.concurrencyLevel")
      .categories("client")
      .version("0.3.0")
      .doc("The number of write locks to update rpc cache.")
      .intConf
      .createWithDefault(32)

  val CLIENT_RPC_CACHE_EXPIRE_TIME: ConfigEntry[Long] =
    buildConf("celeborn.client.rpc.cache.expireTime")
      .withAlternative("celeborn.rpc.cache.expireTime")
      .categories("client")
      .version("0.3.0")
      .doc("The time before a cache item is removed.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("15s")

  val CLIENT_RPC_SHARED_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.client.rpc.shared.threads")
      .categories("client")
      .version("0.3.2")
      .doc("Number of shared rpc threads in LifecycleManager.")
      .intConf
      .createWithDefault(16)

  val CLIENT_RPC_MAX_RETIRES: ConfigEntry[Int] =
    buildConf("celeborn.client.rpc.maxRetries")
      .categories("client")
      .version("0.3.2")
      .doc("Max RPC retry times in LifecycleManager.")
      .intConf
      .createWithDefault(3)

  val CLIENT_RESERVE_SLOTS_RACKAWARE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.reserveSlots.rackaware.enabled")
      .withAlternative("celeborn.client.reserveSlots.rackware.enabled")
      .categories("client")
      .version("0.3.1")
      .doc("Whether need to place different replicates on different racks when allocating slots.")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_SLOT_ASSIGN_MAX_WORKERS: ConfigEntry[Int] =
    buildConf("celeborn.client.slot.assign.maxWorkers")
      .categories("client")
      .version("0.3.1")
      .doc("Max workers that slots of one shuffle can be allocated on. Will choose the smaller positive one " +
        s"from Master side and Client side, see `${CelebornConf.MASTER_SLOT_ASSIGN_MAX_WORKERS.key}`.")
      .intConf
      .createWithDefault(10000)

  val CLIENT_CLOSE_IDLE_CONNECTIONS: ConfigEntry[Boolean] =
    buildConf("celeborn.client.closeIdleConnections")
      .categories("client")
      .doc("Whether client will close idle connections.")
      .version("0.3.0")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_PUSH_DYNAMIC_WRITE_MODE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.spark.push.dynamicWriteMode.enabled")
      .categories("client")
      .doc("Whether to dynamically switch push write mode based on conditions.If true, " +
        s"shuffle mode will be only determined by partition count")
      .version("0.5.0")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_PUSH_DYNAMIC_WRITE_MODE_PARTITION_NUM_THRESHOLD: ConfigEntry[Int] =
    buildConf("celeborn.client.spark.push.dynamicWriteMode.partitionNum.threshold")
      .categories("client")
      .doc(s"Threshold of shuffle partition number for dynamically switching push writer mode. " +
        s"When the shuffle partition number is greater than this value, " +
        s"use the sort-based shuffle writer for memory efficiency; " +
        s"otherwise use the hash-based shuffle writer for speed. " +
        s"This configuration only takes effect when ${CLIENT_PUSH_DYNAMIC_WRITE_MODE_ENABLED.key} is true.")
      .version("0.5.0")
      .intConf
      .createWithDefault(2000)

  val SPARK_SHUFFLE_WRITER_MODE: ConfigEntry[String] =
    buildConf("celeborn.client.spark.shuffle.writer")
      .withAlternative("celeborn.shuffle.writer")
      .categories("client")
      .doc(s"Celeborn supports the following kind of shuffle writers. 1. hash: hash-based shuffle writer " +
        s"works fine when shuffle partition count is normal; 2. sort: sort-based shuffle writer works fine " +
        s"when memory pressure is high or shuffle partition count is huge. " +
        s"This configuration only takes effect when ${CLIENT_PUSH_DYNAMIC_WRITE_MODE_ENABLED.key} is false.")
      .version("0.3.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set(ShuffleMode.HASH.name, ShuffleMode.SORT.name))
      .createWithDefault(ShuffleMode.HASH.name)

  val CLIENT_PUSH_UNSAFEROW_FASTWRITE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.spark.push.unsafeRow.fastWrite.enabled")
      .categories("client")
      .version("0.2.2")
      .doc("This is Celeborn's optimization on UnsafeRow for Spark and it's true by default. " +
        "If you have changed UnsafeRow's memory layout set this to false.")
      .booleanConf
      .createWithDefault(true)

  val CHECK_WORKER_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.checkWorker.enabled")
      .withAlternative("celeborn.client.spark.shuffle.checkWorker.enabled")
      .categories("client")
      .doc("When true, before registering shuffle, LifecycleManager should check " +
        "if current cluster have available workers, if cluster don't have available " +
        "workers, fallback to default shuffle.")
      .version("0.5.0")
      .booleanConf
      .createWithDefault(true)

  val SPARK_SHUFFLE_FALLBACK_POLICY: ConfigEntry[String] =
    buildConf("celeborn.client.spark.shuffle.fallback.policy")
      .categories("client")
      .version("0.5.0")
      .doc("Celeborn supports the following kind of fallback policies. " +
        s"1. ${FallbackPolicy.ALWAYS.name}: always use spark built-in shuffle implementation; " +
        s"2. ${FallbackPolicy.AUTO.name}: prefer to use celeborn shuffle implementation, and fallback to use spark " +
        "built-in shuffle implementation based on certain factors, e.g. availability of enough workers and quota, " +
        "shuffle partition number; " +
        s"3. ${FallbackPolicy.NEVER.name}: always use celeborn shuffle implementation, and fail fast when it it is " +
        "concluded that fallback is required based on factors above.")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set(
        FallbackPolicy.ALWAYS.name,
        FallbackPolicy.AUTO.name,
        FallbackPolicy.NEVER.name))
      .createWithDefault(FallbackPolicy.AUTO.name)

  val SPARK_SHUFFLE_FORCE_FALLBACK_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.spark.shuffle.forceFallback.enabled")
      .withAlternative("celeborn.shuffle.forceFallback.enabled")
      .categories("client")
      .version("0.3.0")
      .doc("Always use spark built-in shuffle implementation. This configuration is deprecated, " +
        s"consider configuring `${CelebornConf.SPARK_SHUFFLE_FALLBACK_POLICY.key}` instead.")
      .booleanConf
      .createWithDefault(false)

  val SPARK_SHUFFLE_FALLBACK_PARTITION_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.client.spark.shuffle.fallback.numPartitionsThreshold")
      .withAlternative("celeborn.shuffle.forceFallback.numPartitionsThreshold")
      .withAlternative("celeborn.client.spark.shuffle.forceFallback.numPartitionsThreshold")
      .categories("client")
      .version("0.5.0")
      .doc("Celeborn will only accept shuffle of partition number lower than this configuration value. " +
        s"This configuration only takes effect when `${CelebornConf.SPARK_SHUFFLE_FALLBACK_POLICY.key}` " +
        s"is `${FallbackPolicy.AUTO.name}`.")
      .longConf
      .createWithDefault(Int.MaxValue)

  val FLINK_SHUFFLE_FALLBACK_POLICY: ConfigEntry[String] =
    buildConf("celeborn.client.flink.shuffle.fallback.policy")
      .categories("client")
      .version("0.6.0")
      .doc("Celeborn supports the following kind of fallback policies. " +
        s"1. ${FallbackPolicy.ALWAYS.name}: always use flink built-in shuffle implementation; " +
        s"2. ${FallbackPolicy.AUTO.name}: prefer to use celeborn shuffle implementation, and fallback to use flink " +
        "built-in shuffle implementation based on certain factors, e.g. availability of enough workers and quota; " +
        s"3. ${FallbackPolicy.NEVER.name}: always use celeborn shuffle implementation, and fail fast when it it is " +
        "concluded that fallback is required based on factors above.")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValues(Set(
        FallbackPolicy.ALWAYS.name,
        FallbackPolicy.AUTO.name,
        FallbackPolicy.NEVER.name))
      .createWithDefault(FallbackPolicy.AUTO.name)

  val CLIENT_PUSH_SORT_MEMORY_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.client.spark.push.sort.memory.threshold")
      .withAlternative("celeborn.push.sortMemory.threshold")
      .categories("client")
      .doc("When SortBasedPusher use memory over the threshold, will trigger push data.")
      .version("0.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64m")

  val CLIENT_PUSH_SORT_USE_ADAPTIVE_MEMORY_THRESHOLD: ConfigEntry[Boolean] =
    buildConf("celeborn.client.spark.push.sort.memory.useAdaptiveThreshold")
      .categories("client")
      .doc("Adaptively adjust sort-based shuffle writer's memory threshold")
      .version("0.5.0")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_PUSH_SORT_SMALL_PUSH_TOLERATE_FACTOR: ConfigEntry[Double] =
    buildConf("celeborn.client.spark.push.sort.memory.smallPushTolerateFactor")
      .categories("client")
      .doc(s"Only be in effect when ${CLIENT_PUSH_SORT_USE_ADAPTIVE_MEMORY_THRESHOLD.key} is" +
        " turned on. The larger this value is, the more aggressive Celeborn will enlarge the " +
        " Sort-based Shuffle writer's memory threshold. Specifically, this config controls when to" +
        " enlarge the sort shuffle writer's memory threshold. With N bytes data in memory and V as" +
        " the value of this config, if the number of pushes, C," +
        " when using sort based shuffle writer C >= (1 + V) * C' where C' is the number of pushes" +
        " if we were using hash based writer, we will enlarge the memory threshold by 2X.")
      .version("0.5.0")
      .doubleConf
      .checkValue(v => v >= 0.0, "Value must be no less than 0")
      .createWithDefault(0.2)

  val CLIENT_PUSH_SORT_MAX_MEMORY_FACTOR: ConfigEntry[Double] =
    buildConf("celeborn.client.spark.push.sort.memory.maxMemoryFactor")
      .categories("client")
      .doc(
        "the max portion of executor memory which can be used for SortBasedWriter buffer (only" +
          s" valid when ${CLIENT_PUSH_SORT_USE_ADAPTIVE_MEMORY_THRESHOLD.key} is enabled")
      .version("0.5.0")
      .doubleConf
      .checkValue(v => v > 0.0 && v <= 1.0, "Value must be between 0 and 1 (inclusive)")
      .createWithDefault(0.4)

  val CLIENT_APPLICATION_UUID_SUFFIX_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.application.uuidSuffix.enabled")
      .categories("client")
      .version("0.6.0")
      .doc("Whether to add UUID suffix for application id for unique. When `true`, add UUID suffix for unique application id. Currently, this only applies to Spark and MR.")
      .booleanConf
      .createWithDefault(false)

  val TEST_ALTERNATIVE: OptionalConfigEntry[String] =
    buildConf("celeborn.test.alternative.key")
      .withAlternative("celeborn.test.alternative.deprecatedKey")
      .categories("test")
      .internal
      .version("0.3.0")
      .stringConf
      .createOptional

  val METRICS_CONF: OptionalConfigEntry[String] =
    buildConf("celeborn.metrics.conf")
      .categories("metrics")
      .doc("Custom metrics configuration file path. Default use `metrics.properties` in classpath.")
      .version("0.3.0")
      .stringConf
      .createOptional

  val METRICS_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.metrics.enabled")
      .categories("metrics")
      .doc("When true, enable metrics system.")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_SAMPLE_RATE: ConfigEntry[Double] =
    buildConf("celeborn.metrics.sample.rate")
      .categories("metrics")
      .doc("It controls if Celeborn collect timer metrics for some operations. Its value should be in [0.0, 1.0].")
      .version("0.2.0")
      .doubleConf
      .checkValue(v => v >= 0.0 && v <= 1.0, "Should be in [0.0, 1.0].")
      .createWithDefault(1.0)

  val METRICS_SLIDING_WINDOW_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.metrics.timer.slidingWindow.size")
      .categories("metrics")
      .doc("The sliding window size of timer metric.")
      .version("0.2.0")
      .intConf
      .createWithDefault(4096)

  val METRICS_COLLECT_CRITICAL_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.metrics.collectPerfCritical.enabled")
      .categories("metrics")
      .doc("It controls whether to collect metrics which may affect performance. When enable, Celeborn collects them.")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(false)

  val METRICS_CAPACITY: ConfigEntry[Int] =
    buildConf("celeborn.metrics.capacity")
      .categories("metrics")
      .doc("The maximum number of metrics which a source can use to generate output strings.")
      .version("0.2.0")
      .intConf
      .createWithDefault(4096)

  val METRICS_EXTRA_LABELS: ConfigEntry[Seq[String]] =
    buildConf("celeborn.metrics.extraLabels")
      .categories("metrics")
      .doc("If default metric labels are not enough, extra metric labels can be customized. " +
        "Labels' pattern is: `<label1_key>=<label1_value>[,<label2_key>=<label2_value>]*`; e.g. `env=prod,version=1`")
      .version("0.3.0")
      .stringConf
      .toSequence
      .checkValue(
        labels => labels.map(_ => Try(Utils.parseMetricLabels(_))).forall(_.isSuccess),
        "Allowed pattern is: `<label1_key>:<label1_value>[,<label2_key>:<label2_value>]*`")
      .createWithDefault(Seq.empty)

  val METRICS_WORKER_APP_TOP_RESOURCE_CONSUMPTION_COUNT: ConfigEntry[Int] =
    buildConf("celeborn.metrics.worker.app.topResourceConsumption.count")
      .categories("metrics")
      .doc("Size for top items about top resource consumption applications list of worker. " +
        "The top resource consumption is determined by sum of diskBytesWritten and hdfsBytesWritten. " +
        "The top resource consumption count prevents the total number of metrics from exceeding the metrics capacity.")
      .version("0.6.0")
      .intConf
      .createWithDefault(50)

  val METRICS_WORKER_PAUSE_SPENT_TIME_FORCE_APPEND_THRESHOLD: ConfigEntry[Int] =
    buildConf("celeborn.metrics.worker.pauseSpentTime.forceAppend.threshold")
      .categories("metrics")
      .doc("Force append worker pause spent time even if worker still in pause serving state. " +
        "Help user can find worker pause spent time increase, when worker always been pause state.")
      .intConf
      .createWithDefault(10)

  val METRICS_PROMETHEUS_PATH: ConfigEntry[String] =
    buildConf("celeborn.metrics.prometheus.path")
      .categories("metrics")
      .doc("URI context path of prometheus metrics HTTP server.")
      .version("0.4.0")
      .stringConf
      .checkValue(path => path.startsWith("/"), "Context path must start with '/'")
      .createWithDefault("/metrics/prometheus")

  val METRICS_JSON_PATH: ConfigEntry[String] =
    buildConf("celeborn.metrics.json.path")
      .categories("metrics")
      .doc("URI context path of json metrics HTTP server.")
      .version("0.4.0")
      .stringConf
      .checkValue(path => path.startsWith("/"), "Context path must start with '/'")
      .createWithDefault("/metrics/json")

  val METRICS_JSON_PRETTY_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.metrics.json.pretty.enabled")
      .categories("metrics")
      .doc("When true, view metrics in json pretty format")
      .version("0.4.0")
      .booleanConf
      .createWithDefault(true)

  val IDENTITY_PROVIDER: ConfigEntry[String] =
    buildConf("celeborn.identity.provider")
      .withAlternative("celeborn.quota.identity.provider")
      .categories("client")
      .doc(s"IdentityProvider class name. Default class is " +
        s"`${classOf[DefaultIdentityProvider].getName}`. " +
        s"Optional values: " +
        s"${classOf[HadoopBasedIdentityProvider].getName} user name will be obtained by UserGroupInformation.getUserName; " +
        s"${classOf[DefaultIdentityProvider].getName} user name and tenant id are default values or user-specific values.")
      .version("0.6.0")
      .stringConf
      .createWithDefault(classOf[DefaultIdentityProvider].getName)

  val USER_SPECIFIC_TENANT: ConfigEntry[String] =
    buildConf("celeborn.identity.user-specific.tenant")
      .withAlternative("celeborn.quota.identity.user-specific.tenant")
      .categories("client")
      .doc(s"Tenant id if ${IDENTITY_PROVIDER.key} is ${classOf[DefaultIdentityProvider].getName}.")
      .version("0.6.0")
      .stringConf
      .createWithDefault(IdentityProvider.DEFAULT_TENANT_ID)

  val USER_SPECIFIC_USERNAME: ConfigEntry[String] =
    buildConf("celeborn.identity.user-specific.userName")
      .withAlternative("celeborn.quota.identity.user-specific.userName")
      .categories("client")
      .doc(s"User name if ${IDENTITY_PROVIDER.key} is ${classOf[DefaultIdentityProvider].getName}.")
      .version("0.6.0")
      .stringConf
      .createWithDefault(IdentityProvider.DEFAULT_USERNAME)

  val QUOTA_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.quota.enabled")
      .categories("quota", "master", "client")
      .doc(
        "When Master side sets to true, the master will enable to check the quota via QuotaManager. " +
          "When Client side sets to true, LifecycleManager will request Master side to check " +
          "whether the current user has enough quota before registration of shuffle. " +
          "Fallback to the default shuffle service when Master side checks that " +
          "there is no enough quota for current user.")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(true)

  val CONTAINER_INFO_PROVIDER: ConfigEntry[String] =
    buildConf("celeborn.container.info.provider")
      .categories("master", "worker")
      .doc(s"ContainerInfoProvider class name. Default class is " +
        s"`org.apache.celeborn.server.common.container.DefaultContainerInfoProvider`. ")
      .version("0.6.0")
      .stringConf
      .createWithDefault("org.apache.celeborn.server.common.container.DefaultContainerInfoProvider")

  val QUOTA_DISK_BYTES_WRITTEN: ConfigEntry[Long] =
    buildConf("celeborn.quota.tenant.diskBytesWritten")
      .categories("quota")
      .dynamic
      .doc("Quota dynamic configuration for written disk bytes.")
      .version("0.5.0")
      .longConf
      .createWithDefault(Long.MaxValue)

  val QUOTA_DISK_FILE_COUNT: ConfigEntry[Long] =
    buildConf("celeborn.quota.tenant.diskFileCount")
      .categories("quota")
      .dynamic
      .doc("Quota dynamic configuration for written disk file count.")
      .version("0.5.0")
      .longConf
      .createWithDefault(Long.MaxValue)

  val QUOTA_HDFS_BYTES_WRITTEN: ConfigEntry[Long] =
    buildConf("celeborn.quota.tenant.hdfsBytesWritten")
      .categories("quota")
      .dynamic
      .doc("Quota dynamic configuration for written hdfs bytes.")
      .version("0.5.0")
      .longConf
      .createWithDefault(Long.MaxValue)

  val QUOTA_HDFS_FILE_COUNT: ConfigEntry[Long] =
    buildConf("celeborn.quota.tenant.hdfsFileCount")
      .categories("quota")
      .dynamic
      .doc("Quota dynamic configuration for written hdfs file count.")
      .version("0.5.0")
      .longConf
      .createWithDefault(Long.MaxValue)

  val QUOTA_INTERRUPT_SHUFFLE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.quota.interruptShuffle.enabled")
      .categories("quota", "client")
      .version("0.6.0")
      .doc("Whether to enable interrupt shuffle when quota exceeds.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.columnarShuffle.enabled")
      .withAlternative("celeborn.columnar.shuffle.enabled")
      .categories("columnar-shuffle")
      .version("0.2.0")
      .doc("Whether to enable columnar-based shuffle.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_BATCH_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.columnarShuffle.batch.size")
      .withAlternative("celeborn.columnar.shuffle.batch.size")
      .categories("columnar-shuffle")
      .version("0.3.0")
      .doc("Vector batch size for columnar shuffle.")
      .intConf
      .checkValue(v => v > 0, "Value must be positive")
      .createWithDefault(10000)

  val COLUMNAR_SHUFFLE_OFF_HEAP_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.columnarShuffle.offHeap.enabled")
      .withAlternative("celeborn.columnar.offHeap.enabled")
      .categories("columnar-shuffle")
      .version("0.3.0")
      .doc("Whether to use off heap columnar vector.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_DICTIONARY_ENCODING_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.columnarShuffle.encoding.dictionary.enabled")
      .withAlternative("celeborn.columnar.shuffle.encoding.dictionary.enabled")
      .categories("columnar-shuffle")
      .version("0.3.0")
      .doc("Whether to use dictionary encoding for columnar-based shuffle data.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_DICTIONARY_ENCODING_MAX_FACTOR: ConfigEntry[Double] =
    buildConf("celeborn.columnarShuffle.encoding.dictionary.maxFactor")
      .withAlternative("celeborn.columnar.shuffle.encoding.dictionary.maxFactor")
      .categories("columnar-shuffle")
      .version("0.3.0")
      .doc("Max factor for dictionary size. The max dictionary size is " +
        s"`min(${Utils.bytesToString(Short.MaxValue)}, ${COLUMNAR_SHUFFLE_BATCH_SIZE.key} * " +
        s"celeborn.columnar.shuffle.encoding.dictionary.maxFactor)`.")
      .doubleConf
      .createWithDefault(0.3)

  val COLUMNAR_SHUFFLE_CODEGEN_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.columnarShuffle.codegen.enabled")
      .withAlternative("celeborn.columnar.shuffle.codegen.enabled")
      .categories("columnar-shuffle")
      .version("0.3.0")
      .doc("Whether to use codegen for columnar-based shuffle.")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_NUM_CONCURRENT_READINGS: ConfigEntry[Int] =
    buildConf("celeborn.client.flink.inputGate.concurrentReadings")
      .withAlternative("remote-shuffle.job.concurrent-readings-per-gate")
      .categories("client")
      .version("0.3.0")
      .doc("Max concurrent reading channels for a input gate.")
      .intConf
      .createWithDefault(Int.MaxValue)

  val CLIENT_MEMORY_PER_RESULT_PARTITION: ConfigEntry[Long] =
    buildConf("celeborn.client.flink.resultPartition.memory")
      .withAlternative("remote-shuffle.job.memory-per-partition")
      .categories("client")
      .version("0.3.0")
      .doc("Memory reserved for a result partition.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64m")

  val CLIENT_MEMORY_PER_INPUT_GATE: ConfigEntry[Long] =
    buildConf("celeborn.client.flink.inputGate.memory")
      .withAlternative("remote-shuffle.job.memory-per-gate")
      .categories("client")
      .version("0.3.0")
      .doc("Memory reserved for a input gate.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32m")

  val CLIENT_INPUT_GATE_SUPPORT_FLOATING_BUFFER: ConfigEntry[Boolean] =
    buildConf("celeborn.client.flink.inputGate.supportFloatingBuffer")
      .withAlternative("remote-shuffle.job.support-floating-buffer-per-input-gate")
      .categories("client")
      .version("0.3.0")
      .doc("Whether to support floating buffer in Flink input gates.")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_DATA_COMPRESSION_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.flink.compression.enabled")
      .withAlternative("remote-shuffle.job.enable-data-compression")
      .categories("client")
      .version("0.3.0")
      .doc("Whether to compress data in Flink plugin.")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_RESULT_PARTITION_SUPPORT_FLOATING_BUFFER: ConfigEntry[Boolean] =
    buildConf("celeborn.client.flink.resultPartition.supportFloatingBuffer")
      .withAlternative("remote-shuffle.job.support-floating-buffer-per-output-gate")
      .categories("client")
      .version("0.3.0")
      .doc("Whether to support floating buffer for result partitions.")
      .booleanConf
      .createWithDefault(true)

  val CLIENT_MR_PUSH_DATA_MAX: ConfigEntry[Long] =
    buildConf("celeborn.client.mr.pushData.max")
      .categories("client")
      .version("0.4.0")
      .doc("Max size for a push data sent from mr client.")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(nVal => nVal < 2147483548, "Max size for a push data should be less than 2GB-20.")
      .createWithDefaultString("32m")

  val ACTIVE_STORAGE_TYPES: ConfigEntry[String] =
    buildConf("celeborn.storage.availableTypes")
      .withAlternative("celeborn.storage.activeTypes")
      .categories("master", "worker", "client")
      .version("0.3.0")
      .doc(
        "Enabled storages. Available options: MEMORY,HDD,SSD,HDFS,S3. Note: HDD and SSD would be treated as identical.")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .checkValue(p => p.split(",").map(StorageInfo.validate).reduce(_ && _), "")
      .createWithDefault("HDD")

  val READ_LOCAL_SHUFFLE_FILE: ConfigEntry[Boolean] =
    buildConf("celeborn.client.readLocalShuffleFile.enabled")
      .categories("client")
      .version("0.3.1")
      .doc("Enable read local shuffle file for clusters that co-deployed with yarn node manager.")
      .booleanConf
      .createWithDefault(false)

  val READ_LOCAL_SHUFFLE_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.client.readLocalShuffleFile.threads")
      .categories("client")
      .version("0.3.1")
      .doc("Threads count for read local shuffle file.")
      .intConf
      .createWithDefault(4)

  val READ_STREAM_CREATOR_POOL_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.client.eagerlyCreateInputStream.threads")
      .categories("client")
      .version("0.3.1")
      .doc("Threads count for streamCreatorPool in CelebornShuffleReader.")
      .intConf
      .createWithDefault(32)

  val CLIENT_SHUFFLE_MAPPARTITION_SPLIT_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.mapPartition.split.enabled")
      .categories("client")
      .doc(
        "whether to enable shuffle partition split. Currently, this only applies to MapPartition.")
      .version("0.3.1")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_CHUNK_PREFETCH_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.chunk.prefetch.enabled")
      .categories("client")
      .doc("Whether to enable chunk prefetch when creating CelebornInputStream.")
      .version("0.6.0")
      .booleanConf
      .createWithDefault(false)

  val CLIENT_INPUTSTREAM_CREATION_WINDOW: ConfigEntry[Int] =
    buildConf("celeborn.client.inputStream.creation.window")
      .categories("client")
      .doc("Window size that CelebornShuffleReader pre-creates CelebornInputStreams, for coalesced scenario " +
        "where multiple Partitions are read")
      .version("0.6.0")
      .intConf
      .createWithDefault(16)

  val MAX_DEFAULT_NETTY_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.io.maxDefaultNettyThreads")
      .categories("network")
      .doc("Max default netty threads")
      .version("0.3.2")
      .intConf
      .createWithDefault(64)

  val HDFS_STORAGE_KERBEROS_PRINCIPAL: OptionalConfigEntry[String] =
    buildConf("celeborn.storage.hdfs.kerberos.principal")
      .categories("master", "worker")
      .version("0.3.2")
      .doc("Kerberos principal for HDFS storage connection.")
      .stringConf
      .createOptional

  val HDFS_STORAGE_KERBEROS_KEYTAB: OptionalConfigEntry[String] =
    buildConf("celeborn.storage.hdfs.kerberos.keytab")
      .categories("master", "worker")
      .version("0.3.2")
      .doc("Kerberos keytab file path for HDFS storage connection.")
      .stringConf
      .createOptional

  val DYNAMIC_CONFIG_STORE_BACKEND: OptionalConfigEntry[String] =
    buildConf("celeborn.dynamicConfig.store.backend")
      .categories("master", "worker")
      .doc(
        "Store backend for dynamic config service. The store backend can be specified in two ways:" +
          " - Using the short name of the store backend defined in the implementation of `ConfigStore#getName` " +
          "whose return value can be mapped to the corresponding backend implementation. Available options: FS, DB." +
          " - Using the service class name of the store backend implementation. " +
          "If not provided, it means that dynamic configuration is disabled.")
      .version("0.4.0")
      .stringConf
      .createOptional

  val DYNAMIC_CONFIG_REFRESH_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.dynamicConfig.refresh.interval")
      .categories("master", "worker")
      .version("0.4.0")
      .doc("Interval for refreshing the corresponding dynamic config periodically.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  val DYNAMIC_CONFIG_STORE_FS_PATH: OptionalConfigEntry[String] =
    buildConf("celeborn.dynamicConfig.store.fs.path")
      .categories("master", "worker")
      .version("0.5.0")
      .doc(
        "The path of dynamic config file for fs store backend. The file format should be yaml. " +
          "The default path is `${CELEBORN_CONF_DIR}/dynamicConfig.yaml`.")
      .stringConf
      .createOptional

  val DYNAMIC_CONFIG_STORE_DB_FETCH_PAGE_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.dynamicConfig.store.db.fetch.pageSize")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("The page size for db store to query configurations.")
      .intConf
      .createWithDefaultString("1000")

  val DYNAMIC_CONFIG_STORE_DB_HIKARI_DRIVER_CLASS_NAME: ConfigEntry[String] =
    buildConf("celeborn.dynamicConfig.store.db.hikari.driverClassName")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("The jdbc driver class name of db store backend.")
      .stringConf
      .createWithDefaultString("")

  val DYNAMIC_CONFIG_STORE_DB_HIKARI_JDBC_URL: ConfigEntry[String] =
    buildConf("celeborn.dynamicConfig.store.db.hikari.jdbcUrl")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("The jdbc url of db store backend.")
      .stringConf
      .createWithDefaultString("")

  val DYNAMIC_CONFIG_STORE_DB_HIKARI_USERNAME: ConfigEntry[String] =
    buildConf("celeborn.dynamicConfig.store.db.hikari.username")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("The username of db store backend.")
      .stringConf
      .createWithDefaultString("")

  val DYNAMIC_CONFIG_STORE_DB_HIKARI_PASSWORD: ConfigEntry[String] =
    buildConf("celeborn.dynamicConfig.store.db.hikari.password")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("The password of db store backend.")
      .stringConf
      .createWithDefaultString("")

  val DYNAMIC_CONFIG_STORE_DB_HIKARI_CONNECTION_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.dynamicConfig.store.db.hikari.connectionTimeout")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("The connection timeout that a client will wait for a connection from the pool for db store backend.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val DYNAMIC_CONFIG_STORE_DB_HIKARI_IDLE_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.dynamicConfig.store.db.hikari.idleTimeout")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("The idle timeout that a connection is allowed to sit idle in the pool for db store backend.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("600s")

  val DYNAMIC_CONFIG_STORE_DB_HIKARI_MAX_LIFETIME: ConfigEntry[Long] =
    buildConf("celeborn.dynamicConfig.store.db.hikari.maxLifetime")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("The maximum lifetime of a connection in the pool for db store backend.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1800s")

  val DYNAMIC_CONFIG_STORE_DB_HIKARI_MAXIMUM_POOL_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.dynamicConfig.store.db.hikari.maximumPoolSize")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("The maximum pool size of db store backend.")
      .intConf
      .createWithDefaultString("2")

  val REGISTER_SHUFFLE_FILTER_EXCLUDED_WORKER_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.register.filterExcludedWorker.enabled")
      .categories("client")
      .version("0.4.0")
      .doc("Whether to filter excluded worker when register shuffle.")
      .booleanConf
      .createWithDefault(false)

  val REVISE_LOST_SHUFFLES_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.shuffle.reviseLostShuffles.enabled")
      .categories("client")
      .version("0.6.0")
      .doc("Whether to revise lost shuffles.")
      .booleanConf
      .createWithDefault(false)

  val NETWORK_IO_SASL_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.<module>.io.saslTimeout")
      .categories("network")
      .doc("Timeout for a single round trip of auth message exchange, in milliseconds.")
      .version("0.5.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val INTERNAL_PORT_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.internal.port.enabled")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("Whether to create a internal port on Masters/Workers for " +
        "inter-Masters/Workers communication. This is beneficial when SASL authentication " +
        "is enforced for all interactions between clients and Celeborn Services, but the services " +
        "can exchange messages without being subject to SASL authentication.")
      .booleanConf
      .createWithDefault(false)

  val AUTH_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.auth.enabled")
      .categories("auth")
      .version("0.5.0")
      .doc("Whether to enable authentication. Authentication will be enabled only when " +
        s"${INTERNAL_PORT_ENABLED.key} is enabled as well.")
      .booleanConf
      .createWithDefault(false)

  val MASTER_INTERNAL_PORT: ConfigEntry[Int] =
    buildConf("celeborn.master.internal.port")
      .categories("master")
      .version("0.5.0")
      .doc(
        "Internal port on the master where both workers and other master nodes connect.")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "Invalid port")
      .createWithDefault(8097)

  val HA_MASTER_NODE_INTERNAL_PORT: ConfigEntry[Int] =
    buildConf("celeborn.master.ha.node.<id>.internal.port")
      .categories("ha")
      .doc(
        "Internal port for the workers and other masters to bind to a master node <id> in HA mode.")
      .version("0.5.0")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "Invalid port")
      .createWithDefault(8097)

  val MASTER_INTERNAL_ENDPOINTS: ConfigEntry[Seq[String]] =
    buildConf("celeborn.master.internal.endpoints")
      .categories("worker")
      .doc("Endpoints of master nodes just for celeborn workers to connect, allowed pattern " +
        "is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:8097,clb2:8097,clb3:8097`. " +
        "If the port is omitted, 8097 will be used.")
      .version("0.5.0")
      .stringConf
      .toSequence
      .checkValue(
        endpoints => endpoints.map(_ => Try(Utils.parseHostPort(_))).forall(_.isSuccess),
        "Allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`")
      .createWithDefaultString(s"<localhost>:8097")

  val RACKRESOLVER_REFRESH_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.master.rackResolver.refresh.interval")
      .categories("master")
      .version("0.5.0")
      .doc("Interval for refreshing the node rack information periodically.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val WORKER_INTERNAL_PORT: ConfigEntry[Int] =
    buildConf("celeborn.worker.internal.port")
      .categories("worker")
      .doc("Internal server port on the Worker where the master nodes connect.")
      .version("0.5.0")
      .intConf
      .createWithDefault(0)

  val MASTER_SEND_APPLICATION_META_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.master.send.applicationMeta.threads")
      .categories("master")
      .doc("Number of threads used by the Master to send ApplicationMeta to Workers.")
      .version("0.5.0")
      .intConf
      .checkValue(_ > 0, "number of threads should be positive")
      .createWithDefault(8)

  val WORKER_APPLICATION_REGISTRY_CACHE_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.worker.applicationRegistry.cache.size")
      .categories("worker", "auth")
      .doc("Cache size of the application registry on Workers.")
      .version("0.5.0")
      .intConf
      .createWithDefault(10000)

  val CLIENT_ADAPTIVE_OPTIMIZE_SKEWED_PARTITION_READ_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.client.adaptive.optimizeSkewedPartitionRead.enabled")
      .categories("client")
      .version("0.5.0")
      .doc("If this is true, Celeborn will adaptively split skewed partitions instead of reading them by Spark map " +
        "range. Please note that this feature requires the `Celeborn-Optimize-Skew-Partitions-spark3_3.patch`. ")
      .booleanConf
      .createWithDefault(false)

  //  SSL Configs

  val SSL_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.ssl.<module>.enabled")
      .categories("network", "ssl")
      .version("0.5.0")
      .doc("Enables SSL for securing wire traffic.")
      .booleanConf
      .createWithDefault(false)

  val SSL_PROTOCOL: ConfigEntry[String] =
    buildConf("celeborn.ssl.<module>.protocol")
      .categories("network", "ssl")
      .version("0.5.0")
      .doc("TLS protocol to use.<br/> The protocol must be supported by JVM.<br/> The reference " +
        "list of protocols can be found in the \"Additional JSSE Standard Names\" section of " +
        "the Java security guide. For Java 11, for example, the list can be found " +
        "[here](https://docs.oracle.com/en/java/javase/11/docs/specs/" +
        "security/standard-names.html#additional-jsse-standard-names)")
      .stringConf
      // TLSv1.3 requires specific java version, defaulting to v1.2
      .createWithDefault("TLSv1.2")

  val SSL_ENABLED_CIPHERS: OptionalConfigEntry[String] =
    buildConf("celeborn.ssl.<module>.enabledAlgorithms")
      .categories("network", "ssl")
      .version("0.5.0")
      .doc("A comma-separated list of ciphers. The " +
        "specified ciphers must be supported by JVM.<br/>The reference list of protocols " +
        "can be found in the \"JSSE Cipher Suite Names\" section of the Java security guide. " +
        "The list for Java 11, for example, can be found at " +
        "[this page](https://docs.oracle.com/en/java/javase/11/docs/specs/security/" +
        "standard-names.html#jsse-cipher-suite-names)<br/>Note: If not set, the default " +
        "cipher suite for the JRE will be used")
      .stringConf
      .createOptional

  val SSL_KEY_STORE: OptionalConfigEntry[String] =
    buildConf("celeborn.ssl.<module>.keyStore")
      .categories("network", "ssl")
      .version("0.5.0")
      .doc("Path to the key store file.<br/> The path can be absolute or relative to the " +
        "directory in which the process is started.")
      .stringConf
      .createOptional

  val SSL_KEY_STORE_PASSWORD: OptionalConfigEntry[String] =
    buildConf("celeborn.ssl.<module>.keyStorePassword")
      .categories("network", "ssl")
      .version("0.5.0")
      .doc("Password to the key store.")
      .stringConf
      .createOptional

  val SSL_TRUST_STORE: OptionalConfigEntry[String] =
    buildConf("celeborn.ssl.<module>.trustStore")
      .categories("network", "ssl")
      .version("0.5.0")
      .doc("Path to the trust store file.<br/> The path can be absolute or relative to the " +
        "directory in which the process is started.")
      .stringConf
      .createOptional

  val SSL_TRUST_STORE_PASSWORD: OptionalConfigEntry[String] =
    buildConf("celeborn.ssl.<module>.trustStorePassword")
      .categories("network", "ssl")
      .version("0.5.0")
      .doc("Password for the trust store.")
      .stringConf
      .createOptional

  val SSL_TRUST_STORE_RELOADING_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.ssl.<module>.trustStoreReloadingEnabled")
      .categories("network", "ssl")
      .version("0.5.0")
      .doc("Whether the trust store should be reloaded periodically.<br/> This setting is " +
        "mostly only useful for Celeborn services (masters, workers), and not applications.")
      .booleanConf
      .createWithDefault(false)

  val SSL_TRUST_STORE_RELOAD_INTERVAL_MS: ConfigEntry[Long] =
    buildConf("celeborn.ssl.<module>.trustStoreReloadIntervalMs")
      .categories("network", "ssl")
      .version("0.5.0")
      .doc("The interval at which the trust store should be reloaded (in milliseconds), when " +
        "enabled. This setting is mostly only useful for server components, not applications.")
      .timeConf(TimeUnit.MILLISECONDS)
      // We treat this as an int, so validate
      .checkValue(
        p => p > 0 && p <= Int.MaxValue,
        s"Invalid trustStoreReloadIntervalMs, must be a position number upto ${Int.MaxValue}")
      .createWithDefaultString("10s")

  val MAX_SSL_ENCRYPTED_BLOCK_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.ssl.<module>.maxEncryptedBlockSize")
      .categories("network", "ssl")
      .version("0.5.0")
      .internal
      .doc("The max size when chunking the stream with SSL")
      .bytesConf(ByteUnit.BYTE)
      // We treat this as an int, so validate
      .checkValue(
        p => p > 0 && p <= Int.MaxValue,
        s"Invalid maxEncryptedBlockSize, must be a position number upto ${Int.MaxValue}")
      .createWithDefaultString("64k")

  val AUTO_SSL_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.ssl.<module>.autoSslEnabled")
      .categories("network", "ssl")
      .version("0.5.0")
      .internal
      .doc("Enable auto ssl for encrypted communication between lifecyclemanager and " +
        "executors.<br/><br/> This is applicable only for " +
        s"${TransportModuleConstants.RPC_APP_MODULE} module and ignored for " +
        "others. Additionally if truststore or keystore are present, this config is " +
        "ignored.<br/><br/>Lifecyclemanager generates a self-signed certificate, which is " +
        "used for SSL. Given use of self-signed certificate, auto ssl only provides over the wire encryption")
      .booleanConf
      .createWithDefault(false)

  val SECRET_REDACTION_PATTERN =
    buildConf("celeborn.redaction.regex")
      .categories("master", "worker")
      .doc("Regex to decide which Celeborn configuration properties and environment variables in " +
        "master and worker environments contain sensitive information. When this regex matches " +
        "a property key or value, the value is redacted from the logging.")
      .version("0.5.0")
      .regexConf
      .createWithDefault("(?i)secret|password|token|access[.]key".r)

  val LOG_CELEBORN_CONF_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.logConf.enabled")
      .categories("master", "worker")
      .version("0.5.0")
      .doc("When `true`, log the CelebornConf for debugging purposes.")
      .booleanConf
      .createWithDefault(false)

  val TAGS_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.tags.enabled")
      .categories("master")
      .doc("Whether to enable tags for workers.")
      .version("0.6.0")
      .booleanConf
      .createWithDefault(true)

  val TAGS_EXPR: ConfigEntry[String] =
    buildConf("celeborn.tags.tagsExpr")
      .categories("master", "client")
      .version("0.6.0")
      .doc("Expression to filter workers by tags. The expression is a comma-separated list of " +
        "tags. The expression is evaluated as a logical AND of all tags. For example, " +
        "`prod,high-io` filters workers that have both the `prod` and `high-io` tags.")
      .dynamic
      .stringConf
      .createWithDefault("")

  val PREFER_CLIENT_TAGS_EXPR: ConfigEntry[Boolean] =
    buildConf("celeborn.tags.preferClientTagsExpr")
      .categories("master")
      .doc("When `true`, prefer the tags expression provided by the client over the tags " +
        "expression provided by the master.")
      .dynamic
      .version("0.6.0")
      .booleanConf
      .createWithDefault(false)

  val MASTER_EXCLUDE_WORKER_UNHEALTHY_DISK_RATIO_THRESHOLD: ConfigEntry[Double] =
    buildConf("celeborn.master.excludeWorker.unhealthyDiskRatioThreshold")
      .categories("master")
      .version("0.6.0")
      .doc("Max ratio of unhealthy disks for excluding worker, when unhealthy disk is larger than max unhealthy count, master will exclude worker." +
        " If this value is set to 1, master will exclude worker of which disks are all unhealthy.")
      .doubleConf
      .checkValue(v => v > 0.0 && v <= 1.0, "Should be in (0.0, 1.0].")
      .createWithDefault(1)
}
