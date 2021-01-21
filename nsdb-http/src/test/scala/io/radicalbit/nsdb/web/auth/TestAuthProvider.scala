/*
 * Copyright 2018-2020 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.web.auth

import akka.http.scaladsl.model.HttpRequest
import io.radicalbit.nsdb.security.NSDbAuthProvider
import io.radicalbit.nsdb.security.NSDbAuthProvider.AuthResponse

class TestAuthProvider extends NSDbAuthProvider[String] {

  def headerName: String = "testHeader"

  override def extractHttpUserInfo(request: HttpRequest): String = {
    val javaOpt = request.getHeader("testHeader")
    if (javaOpt.isPresent) javaOpt.get().value() else ""
  }

  override def extractWsUserIngo(subProtocols: Seq[String]): String = subProtocols.mkString(" ")

  override def checkDbAuth(db: String, userInfo: String, writePermission: Boolean): AuthResponse =
    if (userInfo.isEmpty) AuthResponse(success = false, failReason = "header not provided")
    else if (db == "notAuthorizedDb")
      AuthResponse(success = false, failReason = s"forbidden access to db $db")
    else if (db == "readOnlyDb" && writePermission)
      AuthResponse(success = false, failReason = s"forbidden write access to namespace $db")
    else AuthResponse(success = true)

  override def checkNamespaceAuth(db: String,
                                  namespace: String,
                                  userInfo: String,
                                  writePermission: Boolean): AuthResponse =
    if (userInfo.isEmpty) AuthResponse(success = false, failReason = "header not provided")
    else if (namespace == "notAuthorizedNamespace")
      AuthResponse(success = false, failReason = s"forbidden access to namespace $namespace")
    else if (namespace == "readOnlyNamespace" && writePermission)
      AuthResponse(success = false, failReason = s"forbidden write access to namespace $namespace")
    else AuthResponse(success = true)

  override def checkMetricAuth(db: String,
                               namespace: String,
                               metric: String,
                               userInfo: String,
                               writePermission: Boolean): AuthResponse =
    if (userInfo.isEmpty) AuthResponse(success = false, failReason = "header not provided")
    else if (metric == "notAuthorizedMetric")
      AuthResponse(success = false, failReason = s"forbidden access to metric $metric")
    else if (metric == "readOnlyMetric" && writePermission)
      AuthResponse(success = false, failReason = s"forbidden write access to metric $metric")
    else AuthResponse(success = true)
}
