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

import io.radicalbit.nsdb.security.NSDbAuthorizationProvider
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider.AuthorizationResponse

import java.util
import scala.collection.JavaConverters._

class TestAuthProvider extends NSDbAuthorizationProvider {

  def headerName: String = "testHeader"

  override def extractHttpSecurityPayload(rawHeaders: util.Map[String, String]): String =
    Option(rawHeaders.get("testHeader")).getOrElse("")

  override def extractWsSecurityPayload(subProtocols: java.util.List[String]): String =
    subProtocols.asScala.mkString(" ")

  override def getGrpcSecurityHeader(): String = "dummyHeader"

  override def checkDbAuth(db: String, userInfo: String, writePermission: Boolean): AuthorizationResponse =
    if (userInfo.isEmpty) new AuthorizationResponse(false, "header not provided")
    else if (db == "notAuthorizedDb")
      new AuthorizationResponse(false, s"forbidden access to db $db")
    else if (db == "readOnlyDb" && writePermission)
      new AuthorizationResponse(false, s"forbidden write access to namespace $db")
    else new AuthorizationResponse(true)

  override def checkNamespaceAuth(db: String,
                                  namespace: String,
                                  userInfo: String,
                                  writePermission: Boolean): AuthorizationResponse =
    if (userInfo.isEmpty) new AuthorizationResponse(false, "header not provided")
    else if (namespace == "notAuthorizedNamespace")
      new AuthorizationResponse(false, s"forbidden access to namespace $namespace")
    else if (namespace == "readOnlyNamespace" && writePermission)
      new AuthorizationResponse(false, s"forbidden write access to namespace $namespace")
    else new AuthorizationResponse(true)

  override def checkMetricAuth(db: String,
                               namespace: String,
                               metric: String,
                               userInfo: String,
                               writePermission: Boolean): AuthorizationResponse =
    if (userInfo.isEmpty) new AuthorizationResponse(false, "header not provided")
    else if (metric == "notAuthorizedMetric")
      new AuthorizationResponse(false, s"forbidden access to metric $metric")
    else if (metric == "readOnlyMetric" && writePermission)
      new AuthorizationResponse(false, s"forbidden write access to metric $metric")
    else new AuthorizationResponse(true)
}
