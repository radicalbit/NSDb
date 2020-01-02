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

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

/**
  * Allows Cross-Origin call to exposed Http Apis.
  */
object CORSSupport {

  val optionsSupport: Route = {
    options { complete("") }
  }

  val corsHeaders = List(
    RawHeader("Access-Control-Allow-Origin", "*"),
    RawHeader("Access-Control-Allow-Credentials", "true"),
    RawHeader("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS, DELETE"),
    RawHeader(
      "Access-Control-Allow-Headers",
      "Access-Control-Allow-Origin, Access-Control-Allow-Credentials, X-Requested-With, Content-Type, Authorization"
    )
  )

  def withCors(route: Route) = respondWithHeaders(corsHeaders) { route ~ optionsSupport }

}

/**
  * Adds a custom header containing NSDb version
  */
object VersionHeader {
  import io.radicalbit.nsdb.{BuildInfo => NSDbBuildInfo}

  final val NSDbVersionHeaderKey = "NSDb-Version"

  final val versionHeader = List(RawHeader(NSDbVersionHeaderKey, NSDbBuildInfo.version))

  def withNSDbVersion(route: Route) = respondWithHeaders(versionHeader) { route }

}
