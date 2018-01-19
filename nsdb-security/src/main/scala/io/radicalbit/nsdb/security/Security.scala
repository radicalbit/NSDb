package io.radicalbit.nsdb.security

import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.radicalbit.nsdb.security.http.{EmptyAuthorization, NSDBAuthProvider}

import scala.util.{Failure, Success, Try}

trait NsdbSecurity {

  protected def logger: Logger

  def config: Config

  lazy val security              = config.getBoolean("nsdb.security.enabled")
  lazy val authProviderClassName = config.getString("nsdb.security.auth-provider-class")

  lazy val authProvider: NSDBAuthProvider =
    if (!security) {
      logger.info("Security is not enabled")
      new EmptyAuthorization
    } else if (authProviderClassName != "") {
      logger.debug(s"Trying to load class $authProviderClassName")
      Try(Class.forName(authProviderClassName).asSubclass(classOf[NSDBAuthProvider]).newInstance) match {
        case Success(instance) =>
          logger.debug(s"$authProviderClassName successfully loaded")
          instance
        case Failure(ex) =>
          logger.error("", ex)
          System.exit(0)
          null
      }
    } else {
      logger.error("a valid classname must be provided if security is enabled")
      System.exit(1)
      null
    }
}
