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

import java.util.Properties

import org.apache.celeborn.common.exception.CelebornException

private[celeborn] object CelebornBuildInfo {
  val (
    celeborn_version: String,
    celeborn_branch: String,
    celeborn_revision: String,
    celeborn_build_user: String,
    celeborn_build_date: String) = {

    val resourceStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(
      "celeborn-version-info.properties")
    if (resourceStream == null) {
      throw new CelebornException("Could not find celeborn-version-info.properties")
    }

    try {
      val unknownProp = "<unknown>"
      val props = new Properties()
      props.load(resourceStream)
      (
        props.getProperty("version", unknownProp),
        props.getProperty("branch", unknownProp),
        props.getProperty("revision", unknownProp),
        props.getProperty("user", unknownProp),
        props.getProperty("date", unknownProp))
    } catch {
      case e: Exception =>
        throw new CelebornException(
          "Error loading properties from celeborn-version-info.properties",
          e)
    } finally {
      if (resourceStream != null) {
        try {
          resourceStream.close()
        } catch {
          case e: Exception =>
            throw new CelebornException("Error closing celeborn build info resource stream", e)
        }
      }
    }
  }
}
