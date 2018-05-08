/*
 * Copyright 2018 Radicalbit S.r.l.
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

import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.radicalbit.nsdb.common.exception.NsdbSecurityException
import io.radicalbit.nsdb.security.http.{EmptyAuthorization, NSDBAuthProvider}

import scala.util.{Failure, Success, Try}

/**
  * Trait to mix-in in order to have access to the configured authorization provider.
  */
trait NsdbSecurity {

  protected def logger: Logger

  def config: Config

  /**
    * configuration key to set if security is enabled or not.
    */
  lazy val security = config.getBoolean("nsdb.security.enabled")

  /**
    * configuration key to set the authorization provider FQCN.
    */
  lazy val authProviderClassName = config.getString("nsdb.security.auth-provider-class")

  /**
    * Authorization provider instance, retrieved based on the configuration provided.
    * If there is any error during the dynamic instantiation process, a [[Failure]] will be returned.
    */
  lazy val authProvider: Try[NSDBAuthProvider] =
    if (!security) {
      logger.info("Security is not enabled")
      Success(new EmptyAuthorization)
    } else if (authProviderClassName != "") {
      logger.debug(s"Trying to load class $authProviderClassName")
      Try(Class.forName(authProviderClassName).asSubclass(classOf[NSDBAuthProvider]).newInstance)
    } else {
      Failure(new NsdbSecurityException("a valid classname must be provided if security is enabled"))
    }
}
