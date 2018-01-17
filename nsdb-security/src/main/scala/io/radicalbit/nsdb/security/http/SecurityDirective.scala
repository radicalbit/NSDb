package io.radicalbit.nsdb.security.http

import akka.http.scaladsl.server.directives.RouteDirectives.reject
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Route}
import akka.http.scaladsl.server.directives.{HeaderDirectives, SecurityDirectives}
import io.radicalbit.nsdb.security.model.{Db, Metric, Namespace}

trait NSDBAuthProvider extends HeaderDirectives with SecurityDirectives {

  def headerName: String

  def authorizeDb(ent: Db, header: Option[String])(route: Route): Route =
    if (checkDbAuth(ent, header)) route else reject(AuthorizationFailedRejection)

  def authorizeNamespace(ent: Namespace, header: Option[String])(route: Route): Route =
    if (checkNamespaceAuth(ent, header)) route else reject(AuthorizationFailedRejection)

  def authorizeMetric(ent: Metric, header: Option[String])(route: Route): Route =
    if (checkMetricAuth(ent, header)) route else reject(AuthorizationFailedRejection)

  def checkDbAuth(ent: Db, header: Option[String]): Boolean

  def checkNamespaceAuth(ent: Namespace, header: Option[String]): Boolean

  def checkMetricAuth(ent: Metric, header: Option[String]): Boolean

}

class EmptyAuthorization extends NSDBAuthProvider {

  def headerName: String = "notRelevant"

  override def checkDbAuth(ent: Db, header: Option[String]): Boolean = true

  override def checkNamespaceAuth(ent: Namespace, header: Option[String]): Boolean = true

  override def checkMetricAuth(ent: Metric, header: Option[String]): Boolean = true
}
