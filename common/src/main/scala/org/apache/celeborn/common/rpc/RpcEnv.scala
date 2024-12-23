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

package org.apache.celeborn.common.rpc

import java.io.File
import java.util.Random
import java.util.concurrent.TimeUnit

import scala.concurrent.Future

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.common.rpc.netty.NettyRpcEnvFactory
import org.apache.celeborn.common.util.Utils

/**
 * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
 * so that it can be created via Reflection.
 */
object RpcEnv {

  def create(
      name: String,
      transportModule: String,
      host: String,
      port: Int,
      conf: CelebornConf,
      securityContext: Option[RpcSecurityContext]): RpcEnv = {
    create(name, transportModule, host, host, port, conf, 0, securityContext)
  }

  def create(
      name: String,
      transportModule: String,
      host: String,
      port: Int,
      conf: CelebornConf,
      numUsableCores: Int,
      securityContext: Option[RpcSecurityContext],
      source: Option[AbstractSource]): RpcEnv = {
    val bindAddress =
      if (conf.bindWildcardAddress) TransportModuleConstants.WILDCARD_BIND_ADDRESS else host
    val advertiseAddress = Utils.localHostNameForAdvertiseAddress(conf, name)
    create(
      name,
      transportModule,
      bindAddress,
      advertiseAddress,
      port,
      conf,
      numUsableCores,
      securityContext,
      source)
  }

  def create(
      name: String,
      transportModule: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      conf: CelebornConf,
      numUsableCores: Int,
      securityContext: Option[RpcSecurityContext] = None,
      source: Option[AbstractSource] = None): RpcEnv = {
    val config =
      RpcEnvConfig(
        conf,
        name,
        transportModule,
        bindAddress,
        advertiseAddress,
        port,
        numUsableCores,
        securityContext,
        source)
    new NettyRpcEnvFactory().create(config)
  }
}

/**
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
 * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
 * sender, or logging them if no such sender or `NotSerializableException`.
 *
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 */
abstract class RpcEnv(config: RpcEnvConfig) {

  private[celeborn] val defaultLookupTimeout = config.conf.rpcLookupTimeout
  private[celeborn] val waitTimeBound = config.conf.rpcTimeoutRetryWaitMs.toInt

  /**
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
   */
  private[celeborn] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Return the address that [[RpcEnv]] is listening to.
   */
  def address: RpcAddress

  /**
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `addr` asynchronously.
   */
  def asyncSetupEndpointRefByAddr(addr: RpcEndpointAddress): Future[RpcEndpointRef]

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `addr`. This is a blocking action.
   */
  def setupEndpointRefByAddr(addr: RpcEndpointAddress): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByAddr(addr), addr.rpcAddress)
  }

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
   * This is a blocking action.
   */
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByAddr(RpcEndpointAddress(address, endpointName))
  }

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName` with timeout retry.
   * This is a blocking action.
   */
  def setupEndpointRef(
      address: RpcAddress,
      endpointName: String,
      retryCount: Int): RpcEndpointRef = {
    var numRetries = retryCount
    while (numRetries > 0) {
      numRetries -= 1
      try {
        return setupEndpointRefByAddr(RpcEndpointAddress(address, endpointName))
      } catch {
        case e: RpcTimeoutException =>
          if (numRetries > 0) {
            val random = new Random
            val retryWaitMs = random.nextInt(waitTimeBound)
            try {
              TimeUnit.MILLISECONDS.sleep(retryWaitMs)
            } catch {
              case _: InterruptedException =>
                throw e
            }
          } else {
            throw e
          }
        case e: RpcEndpointNotFoundException =>
          throw e
      }
    }
    // should never be here
    null
  }

  /**
   * Stop [[RpcEndpoint]] specified by `endpoint`.
   */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
   * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
   * call [[awaitTermination()]] straight after [[shutdown()]].
   */
  def shutdown(): Unit

  /**
   * Wait until [[RpcEnv]] exits.
   */
  def awaitTermination(): Unit

  /**
   * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
   * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
   */
  def deserialize[T](deserializationAction: () => T): T

  def rpcSource(): RpcSource
}

/**
 * A server used by the RpcEnv to server files to other processes owned by the application.
 *
 * The file server can return URIs handled by common libraries (such as "http" or "hdfs"), or
 * it can return "spark" URIs which will be handled by `RpcEnv#fetchFile`.
 */
private[celeborn] trait RpcEnvFileServer {

  /**
   * Adds a file to be served by this RpcEnv. This is used to serve files from the driver
   * to executors when they're stored on the driver's local file system.
   *
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addFile(file: File): String

  /**
   * Adds a jar to be served by this RpcEnv. Similar to `addFile` but for jars added using
   * `SparkContext.addJar`.
   *
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addJar(file: File): String

  /**
   * Adds a local directory to be served via this file server.
   *
   * @param baseUri Leading URI path (files can be retrieved by appending their relative
   *                path to this base URI). This cannot be "files" nor "jars".
   * @param path Path to the local directory.
   * @return URI for the root of the directory in the file server.
   */
  def addDirectory(baseUri: String, path: File): String

  /** Validates and normalizes the base URI for directories. */
  protected def validateDirectoryUri(baseUri: String): String = {
    val fixedBaseUri = "/" + baseUri.stripPrefix("/").stripSuffix("/")
    require(
      fixedBaseUri != "/files" && fixedBaseUri != "/jars",
      "Directory URI cannot be /files nor /jars.")
    fixedBaseUri
  }

}

private[celeborn] case class RpcEnvConfig(
    conf: CelebornConf,
    name: String,
    transportModule: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    numUsableCores: Int,
    securityContext: Option[RpcSecurityContext],
    source: Option[AbstractSource]) {
  assert(RpcEnvConfig.VALID_TRANSPORT_MODULES.contains(transportModule))
}

object RpcEnvConfig {
  private val VALID_TRANSPORT_MODULES = Set(
    TransportModuleConstants.RPC_APP_MODULE,
    TransportModuleConstants.RPC_MODULE,
    TransportModuleConstants.RPC_APP_CLIENT_MODULE,
    TransportModuleConstants.RPC_LIFECYCLEMANAGER_MODULE,
    TransportModuleConstants.RPC_SERVICE_MODULE)
}
