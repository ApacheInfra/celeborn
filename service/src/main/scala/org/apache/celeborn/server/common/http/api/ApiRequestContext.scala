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

package org.apache.celeborn.server.common.http.api

import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Context

import org.eclipse.jetty.server.handler.ContextHandler

import org.apache.celeborn.server.common.HttpService

private[celeborn] trait ApiRequestContext {
  @Context
  protected var servletContext: ServletContext = _

  @Context
  protected var httpRequest: HttpServletRequest = _

  final protected def rs: HttpService = RestServiceContext.get(servletContext)
}

private[celeborn] object RestServiceContext {
  private val attribute = getClass.getCanonicalName

  def set(contextHandler: ContextHandler, rs: HttpService): Unit = {
    contextHandler.setAttribute(attribute, rs)
  }

  def get(context: ServletContext): HttpService = {
    context.getAttribute(attribute).asInstanceOf[HttpService]
  }
}
