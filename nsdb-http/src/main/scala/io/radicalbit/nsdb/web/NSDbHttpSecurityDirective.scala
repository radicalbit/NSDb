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

package io.radicalbit.nsdb.web

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive, Directive1}
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider

import scala.collection.JavaConverters._

/**
  * Manage authorization process in Http calls.
  */
object NSDbHttpSecurityDirective {

  /**
    * Extract http request headers in a raw format [[ Map[String, String] ]] that is suitable to be used by the authorization provider.
    */
  def extractRawHeaders: Directive1[Map[String, String]] = Directive[Tuple1[Map[String, String]]] { inner => ctx =>
    inner(Tuple1(ctx.request.headers.map(h => h.name() -> h.value()).toMap))(ctx)
  }

  /**
    * Forwards, if authorized, a request against a Db.
    * @param db the Db to check.
    * @param writePermission true if write permission is required.
    * @param authorizationProvider the [[NSDbAuthorizationProvider]] to use to perform authorization checks.
    * @return the destination route or a 403 if authorization check fails.
    */
  def withDbAuthorization(db: String,
                          writePermission: Boolean,
                          authorizationProvider: NSDbAuthorizationProvider): Directive[Unit] =
    extractRawHeaders.flatMap[Unit] { rawHeaders =>
      val authorizationCheck =
        authorizationProvider.checkDbAuth(db,
                                          authorizationProvider.extractHttpSecurityPayload(rawHeaders.asJava),
                                          writePermission)
      if (authorizationCheck.isSuccess) pass
      else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${authorizationCheck.getFailReason}"))
    }

  /**
    * Forwards, if authorized, a request against a Namespace.
    * @param db the Db to check.
    * @param namespace the Namespace to check.
    * @param writePermission true if write permission is required.
    * @param authorizationProvider the [[NSDbAuthorizationProvider]] to use to perform authorization checks.
    * @return the destination route or a 403 if authorization check fails.
    */
  def withNamespaceAuthorization(db: String,
                                 namespace: String,
                                 writePermission: Boolean,
                                 authorizationProvider: NSDbAuthorizationProvider): Directive[Unit] =
    extractRawHeaders.flatMap[Unit] { rawHeaders =>
      val authorizationCheck =
        authorizationProvider.checkNamespaceAuth(db,
                                                 namespace,
                                                 authorizationProvider.extractHttpSecurityPayload(rawHeaders.asJava),
                                                 writePermission)
      if (authorizationCheck.isSuccess) pass
      else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${authorizationCheck.getFailReason}"))
    }

  /**
    * Forwards, if authorized, a request against a Metric.
    * @param db the Db to check.
    * @param namespace the Namespace to check.
    * @param metric the Metric to check.
    * @param writePermission true if write permission is required.
    * @param authorizationProvider the [[NSDbAuthorizationProvider]] to use to perform authorization checks.
    * @return the destination route or a 403 if authorization check fails.
    */
  def withMetricAuthorization(db: String,
                              namespace: String,
                              metric: String,
                              writePermission: Boolean,
                              authorizationProvider: NSDbAuthorizationProvider): Directive[Unit] =
    extractRawHeaders.flatMap[Unit] { rawHeaders =>
      val authorizationCheck =
        authorizationProvider.checkMetricAuth(db,
                                              namespace,
                                              metric,
                                              authorizationProvider.extractHttpSecurityPayload(rawHeaders.asJava),
                                              writePermission)
      if (authorizationCheck.isSuccess) pass
      else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${authorizationCheck.getFailReason}"))
    }

}
