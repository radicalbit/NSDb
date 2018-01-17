package io.radicalbit.nsdb.security.http

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.directives.RouteDirectives.reject
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Route}
import akka.http.scaladsl.server.directives.{HeaderDirectives, SecurityDirectives}
import io.radicalbit.nsdb.security.model.{Db, Metric, Namespace}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._

/**
  * Java friendly class for handling authorization process
  * @param success: if authorization succeeded or not
  * @param failReason: the fail reason
  */
case class AuthResponse(success: Boolean, failReason: String = "")

/**
  * interface to inherit in order to develop a custom authentication provider
  */
trait NSDBAuthProvider extends HeaderDirectives with SecurityDirectives {

  /**
    * name of the header to be retrieved from HTTP request
    * @return the header name
    */
  def headerName: String

  /**
    * method to check if a request against a Db is authorized
    * @param ent the entity to check
    * @param header the header to check
    * @return authorization response
    */
  def checkDbAuth(ent: Db, header: Option[String]): AuthResponse

  /**
    *method to check if a request against a Namespace is authorized
    * @param ent the entity to check
    * @param header the header to check
    * @return authorization response
    */
  def checkNamespaceAuth(ent: Namespace, header: Option[String]): AuthResponse

  /**
    * method to check if a request against a Metric is authorized
    * @param ent the entity to check
    * @param header the header to check
    * @return authorization response
    */
  def checkMetricAuth(ent: Metric, header: Option[String]): AuthResponse

  def authorizeDb(ent: Db, header: Option[String])(route: Route): Route = {
    val check = checkDbAuth(ent, header)
    if (check.success) route
    else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${check.failReason}"))
  }

  def authorizeNamespace(ent: Namespace, header: Option[String])(route: Route): Route = {
    val check = checkNamespaceAuth(ent, header)
    if (check.success) route
    else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${check.failReason}"))
  }

  def authorizeMetric(ent: Metric, header: Option[String])(route: Route): Route = {
    val check = checkMetricAuth(ent, header)
    if (check.success) route
    else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${check.failReason}"))
  }

}

class EmptyAuthorization extends NSDBAuthProvider {

  def headerName: String = "notRelevant"

  override def checkDbAuth(ent: Db, header: Option[String]): AuthResponse = AuthResponse(success = true)

  override def checkNamespaceAuth(ent: Namespace, header: Option[String]): AuthResponse = AuthResponse(success = true)

  override def checkMetricAuth(ent: Metric, header: Option[String]): AuthResponse = AuthResponse(success = true)
}
