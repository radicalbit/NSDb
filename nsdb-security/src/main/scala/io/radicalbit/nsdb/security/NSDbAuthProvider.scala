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

package io.radicalbit.nsdb.security

import akka.http.scaladsl.model.HttpRequest
import io.radicalbit.nsdb.security.NSDbAuthProvider.AuthResponse

trait NSDbAuthProvider[UserInfo] {

  def extractHttpUserInfo(request: HttpRequest): UserInfo

  def extractWsUserIngo(subProtocols: Seq[String]): UserInfo

  /**
    * Checks if a request against a Db is authorized.
    * @param db the Db to check.
//    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @return authorization response.
    */
  def checkDbAuth(db: String, userInfo: UserInfo, writePermission: Boolean): AuthResponse

  /**
    * Checks if a request against a Namespace is authorized.
    * @param db the Db to check.
    * @param namespace the Namespace to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @return authorization response.
    */
  def checkNamespaceAuth(db: String, namespace: String, userInfo: UserInfo, writePermission: Boolean): AuthResponse

  /**
    * Checks if a request against a Metric is authorized.
    * @param db the Db to check.
    * @param namespace the Namespace to check.
    * @param metric the Metric to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @return authorization response.
    */
  def checkMetricAuth(db: String,
                      namespace: String,
                      metric: String,
                      userInfo: UserInfo,
                      writePermission: Boolean): AuthResponse

}

object NSDbAuthProvider {

  /**
    * Java friendly class for handling authorization process.
    * @param success: if authorization succeeded or not.
    * @param failReason: the fail reason.
    */
  case class AuthResponse(success: Boolean, failReason: String = "")

  /**
    * Always allow authorization provider. No checks are performed at all.
    */
  private object EmptyAuthorizationProvider extends NSDbAuthProvider[String] {

    override def extractHttpUserInfo(request: HttpRequest): String = ""

    override def extractWsUserIngo(subProtocols: Seq[String]): String = ""

    override def checkDbAuth(db: String, userInfo: String, writePermission: Boolean): AuthResponse =
      AuthResponse(success = true)

    override def checkNamespaceAuth(db: String,
                                    namespace: String,
                                    userInfo: String,
                                    writePermission: Boolean): AuthResponse =
      AuthResponse(success = true)

    override def checkMetricAuth(db: String,
                                 namespace: String,
                                 metric: String,
                                 userInfo: String,
                                 writePermission: Boolean): AuthResponse =
      AuthResponse(success = true)
  }

  def empty: NSDbAuthProvider[String] = EmptyAuthorizationProvider
}
