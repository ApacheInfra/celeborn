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

package org.apache.celeborn.service.deploy.master.clustermeta.ha

import java.io.IOException
import java.net._
import java.util

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.ratis.util.NetUtils

import org.apache.celeborn.common.RssConf
import org.apache.celeborn.common.RssConf._
import org.apache.celeborn.common.internal.Logging

case class MasterClusterInfo(
    localNode: NodeDetails,
    peerNodes: util.List[NodeDetails])

object MasterClusterInfo extends Logging {

  @throws[IllegalArgumentException]
  def loadHAConfig(conf: RssConf): MasterClusterInfo = {
    val localNodeIdOpt = masterNodeId(conf)
    val clusterNodeIds = masterNodeIds(conf)

    val masterNodes = clusterNodeIds.map { nodeId =>
      val ratisHost = RssConf.masterRatisHost(conf, nodeId)
      val ratisPort = RssConf.masterRatisPort(conf, nodeId)
      val addr: InetSocketAddress = Try(NetUtils.createSocketAddr(ratisHost, ratisPort)) match {
        case Success(socketAddress) => socketAddress
        case Failure(e) =>
          throw new IOException(
            s"Couldn't create socket address for node[$nodeId] $ratisHost:$ratisPort",
            e)
      }
      if (addr.isUnresolved)
        logError(s"Address of node[$nodeId] $ratisHost:$ratisPort couldn't be resolved. " +
          s"Proceeding with unresolved host to create Ratis ring.")
      masterNode(nodeId, addr, ratisPort)
    }

    val (localNodes, peerNodes) = localNodeIdOpt match {
      case Some(currentNodeId) =>
        masterNodes.partition { currentNodeId == _.getNodeId }
      case None =>
        masterNodes.partition { node =>
          !node.getRpcAddress.isUnresolved && isLocalAddress(node.getRpcAddress.getAddress)
        }
    }

    if (localNodes.isEmpty)
      throw new IllegalArgumentException("Can not found local node")

    if (localNodes.length > 1) {
      val nodesAddr = localNodes.map(_.getRpcAddressString).mkString(",")
      throw new IllegalArgumentException(
        s"Detecting multi instances[$nodesAddr] in single node, please specific ${HA_MASTER_NODE_ID.key}.")
    }

    MasterClusterInfo(localNodes.head, peerNodes.toList.asJava)
  }

  def masterNode(
      nodeId: String,
      rpcAddress: InetSocketAddress,
      ratisPort: Int): NodeDetails = {
    require(nodeId != null)
    new NodeDetails.Builder()
      .setNodeId(nodeId)
      .setRpcAddress(rpcAddress)
      .setRatisPort(ratisPort)
      .build
  }

  private def isLocalAddress(addr: InetAddress): Boolean = {
    if (addr.isAnyLocalAddress || addr.isLoopbackAddress)
      return true
    Try(NetworkInterface.getByInetAddress(addr)) match {
      case Success(value) => value != null
      case Failure(_) => false
    }
  }
}
