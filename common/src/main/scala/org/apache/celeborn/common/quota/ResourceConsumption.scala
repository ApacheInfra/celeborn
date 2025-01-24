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

package org.apache.celeborn.common.quota

import java.util

import scala.collection.JavaConverters._

import org.apache.celeborn.common.util.{CollectionUtils, Utils}

case class ResourceConsumption(
    diskBytesWritten: Long,
    diskFileCount: Long,
    hdfsBytesWritten: Long,
    hdfsFileCount: Long,
    var subResourceConsumptions: util.Map[String, ResourceConsumption] = null) {

  def withSubResourceConsumptions(
      resourceConsumptions: util.Map[String, ResourceConsumption]): ResourceConsumption = {
    subResourceConsumptions = resourceConsumptions
    this
  }

  def add(other: ResourceConsumption): ResourceConsumption = {
    ResourceConsumption(
      diskBytesWritten + other.diskBytesWritten,
      diskFileCount + other.diskFileCount,
      hdfsBytesWritten + other.hdfsBytesWritten,
      hdfsFileCount + other.hdfsFileCount)
  }

  def subtract(other: ResourceConsumption): ResourceConsumption = {
    ResourceConsumption(
      diskBytesWritten - other.diskBytesWritten,
      diskFileCount - other.diskFileCount,
      hdfsBytesWritten - other.hdfsBytesWritten,
      hdfsFileCount - other.hdfsFileCount)
  }

  def addSubResourceConsumptions(otherSubResourceConsumptions: Map[
    String,
    ResourceConsumption]): Map[String, ResourceConsumption] = {
    if (CollectionUtils.isNotEmpty(subResourceConsumptions)) {
      subResourceConsumptions.asScala.foldRight(otherSubResourceConsumptions)(
        (subResourceConsumption, resourceConsumptions) => {
          if (resourceConsumptions.contains(subResourceConsumption._1)) {
            resourceConsumptions + (subResourceConsumption._1 -> subResourceConsumption._2.add(
              resourceConsumptions(
                subResourceConsumption._1)))
          } else {
            resourceConsumptions + (subResourceConsumption._1 -> subResourceConsumption._2)
          }
        })
    } else {
      otherSubResourceConsumptions
    }
  }

  def addWithSubResourceConsumptions(other: (ResourceConsumption, Map[String, ResourceConsumption]))
      : (ResourceConsumption, Map[String, ResourceConsumption]) = {
    (add(other._1), addSubResourceConsumptions(other._2))
  }

  override def toString: String = {
    val subResourceConsumptionString =
      if (CollectionUtils.isEmpty(subResourceConsumptions)) {
        "empty"
      } else {
        subResourceConsumptions.asScala.map { case (identifier, resourceConsumption) =>
          s"$identifier -> $resourceConsumption"
        }.mkString("(", ",", ")")
      }
    s"ResourceConsumption(diskBytesWritten: ${Utils.bytesToString(diskBytesWritten)}," +
      s" diskFileCount: $diskFileCount," +
      s" hdfsBytesWritten: ${Utils.bytesToString(hdfsBytesWritten)}," +
      s" hdfsFileCount: $hdfsFileCount," +
      s" subResourceConsumptions: $subResourceConsumptionString)"
  }

  def simpleString: String = {
    s"ResourceConsumption(diskBytesWritten: ${Utils.bytesToString(diskBytesWritten)}," +
      s" diskFileCount: $diskFileCount," +
      s" hdfsBytesWritten: ${Utils.bytesToString(hdfsBytesWritten)}," +
      s" hdfsFileCount: $hdfsFileCount)"
  }
}
