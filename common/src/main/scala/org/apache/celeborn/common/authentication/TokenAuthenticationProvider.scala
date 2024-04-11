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

package org.apache.celeborn.common.authentication

import javax.security.sasl.AuthenticationException

trait TokenAuthenticationProvider {

  /**
   * The authenticate method is called by the celeborn authentication layer
   * to authenticate token for their requests.
   * If the token is to be granted, return nothing/throw nothing.
   * When the token is to be disallowed, throw an appropriate [[AuthenticationException]].
   *
   * @param token The token received over the connection request.
   * @return The identifier associated with the token
   *
   * @throws AuthenticationException When the token is found to be invalid by the implementation
   */
  @throws[AuthenticationException]
  def authenticate(token: String): String
}
