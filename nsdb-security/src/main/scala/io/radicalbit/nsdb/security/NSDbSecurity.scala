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

import akka.event.LoggingAdapter
import com.typesafe.config.Config

import scala.util.Try

/**
  * Trait to mix-in in order to have access to the configured authorization provider.
  */
trait NSDbSecurity {

  def config: Config

  def logger: LoggingAdapter

  /**
    * configuration key to set if security is enabled or not.
    */
  lazy val security: Boolean = config.getBoolean("nsdb.security.enabled")

  /**
    * configuration key to set the authorization provider FQCN.
    */
  lazy val authProviderClassName: String = config.getString("nsdb.security.auth-provider-class")

  /**
    * Authorization provider instance, retrieved based on the configuration provided.
    * If there is any error during the dynamic instantiation process, a [[Left]] will be returned.
    */
  lazy val authProvider: Either[String, NSDbAuthProvider[_]] =
    if (!security) {
      logger.info("Security is not enabled")
      Right(NSDbAuthProvider.empty)
    } else if (authProviderClassName != "") {
      logger.debug(s"Trying to load class $authProviderClassName")
      Try(Class.forName(authProviderClassName).asSubclass(classOf[NSDbAuthProvider[_]]).newInstance).toEither.left
        .map(_.getMessage)
    } else {
      Left("a valid classname must be provided if security is enabled")
    }
}
