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
import akka.http.scaladsl.server.{Directive, Route}
import io.radicalbit.nsdb.security.NSDbAuthorizationProvider

import scala.collection.JavaConverters._

/**
  * Class to inherit in order to develop a custom authentication provider
  */
class NSDbHttpSecurityDirective(authProvider: NSDbAuthorizationProvider) {

  /**
    * Forwards, if authorized, a request against a Db.
    * @param db the Db to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @param route the route to forward the request to.
    * @return the destination route or a 403 if authorization check fails.
    */
  final def authorizeDb(db: String, writePermission: Boolean)(route: Route)(
      implicit rawHeaders: Map[String, String]): Route = {
    val check =
      authProvider.checkDbAuth(db, authProvider.extractHttpSecurityPayload(rawHeaders.asJava), writePermission)
    if (check.isSuccess) route
    else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${check.getFailReason}"))
  }

  /**
    * Forwards, if authorized, a request against a Namespace.
    * @param db the Db to check.
    * @param namespace the Namespace to check.
//    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @param route the route to forward the request to.
    * @return the destination route or a 403 if authorization check fails.
    */
  final def authorizeNamespace(db: String, namespace: String, writePermission: Boolean)(route: Route)(
      implicit rawHeaders: Map[String, String]): Route = {
    val check =
      authProvider.checkNamespaceAuth(db,
                                      namespace,
                                      authProvider.extractHttpSecurityPayload(rawHeaders.asJava),
                                      writePermission)
    if (check.isSuccess) route
    else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${check.getFailReason}"))
  }

  /**
    * Forwards, if authorized, a request against a Metric.
    * @param db the Db to check.
    * @param namespace the Namespace to check.
    * @param metric the Metric to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @param route the route to forward the request to.
    * @return the destination route or a 403 if authorization check fails.
    */
  final def authorizeMetric(db: String, namespace: String, metric: String, writePermission: Boolean)(route: Route)(
      implicit rawHeaders: Map[String, String]): Route = {
    val check =
      authProvider.checkMetricAuth(db,
                                   namespace,
                                   metric,
                                   authProvider.extractHttpSecurityPayload(rawHeaders.asJava),
                                   writePermission)
    if (check.isSuccess) route
    else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${check.getFailReason}"))
  }

}

object NSDbHttpSecurityDirective {

  def extractRawHeaders: Directive[Tuple1[Map[String, String]]] = Directive[Tuple1[Map[String, String]]] {
    inner => ctx =>
      inner(Tuple1(ctx.request.headers.map(h => h.name() -> h.value()).toMap))(ctx)
  }

}
