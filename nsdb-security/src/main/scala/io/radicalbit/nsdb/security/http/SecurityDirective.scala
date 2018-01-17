package io.radicalbit.nsdb.security.http

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.{HeaderDirectives, SecurityDirectives}
import io.radicalbit.nsdb.security.model.{Db, Metric, Namespace}

trait NSDBAuthProvider extends HeaderDirectives with SecurityDirectives {

  def headerName: String

  def authorizeDb(ent: Db, header: Option[String])(route: Route): Route

  def authorizeNamespace(ent: Namespace, header: Option[String])(route: Route): Route

  def authorizeMetric(ent: Metric, header: Option[String])(route: Route): Route

}

class EmpyAuthorization extends NSDBAuthProvider {

  def headerName: String = "pippo"

  override def authorizeDb(ent: Db, header: Option[String])(route: Route): Route = {
    authorize(true) {
      route
    }
  }

  def authorizeNamespace(ent: Namespace, header: Option[String])(route: Route): Route = {
    authorize(true) {
      route
    }
  }

  def authorizeMetric(ent: Metric, header: Option[String])(route: Route): Route = {
    authorize(true) {
      route
    }
  }
}
