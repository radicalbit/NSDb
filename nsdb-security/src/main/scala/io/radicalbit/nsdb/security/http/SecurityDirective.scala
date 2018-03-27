package io.radicalbit.nsdb.security.http

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import io.radicalbit.nsdb.security.model.{Db, Metric, Namespace}

/**
  * Java friendly class for handling authorization process.
  * @param success: if authorization succeeded or not.
  * @param failReason: the fail reason.
  */
case class AuthResponse(success: Boolean, failReason: String = "")

/**
  * abstract class to inherit in order to develop a custom authentication provider
  */
abstract class NSDBAuthProvider {

  /**
    * Header to be retrieved from HTTP request.
    * @return header name.
    */
  def headerName: String

  /**
    * Check if a request against a Db is authorized.
    * @param ent the entity to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @return authorization response.
    */
  def checkDbAuth(ent: Db, header: String, writePermission: Boolean): AuthResponse

  /**
    * Check if a request against a Namespace is authorized.
    * @param ent the entity to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @return authorization response.
    */
  def checkNamespaceAuth(ent: Namespace, header: String, writePermission: Boolean): AuthResponse

  /**
    * Check if a request against a Metric is authorized.
    * @param ent the entity to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @return authorization response.
    */
  def checkMetricAuth(ent: Metric, header: String, writePermission: Boolean): AuthResponse

  /**
    * Forward, if authorized, a request against a Db.
    * @param ent the entity to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @param route the route to forward the request to.
    * @return the destination route or a 403 if authorization check fails.
    */
  final def authorizeDb(ent: Db, header: Option[String], writePermission: Boolean)(route: Route): Route = {
    val check = checkDbAuth(ent, header getOrElse "", writePermission)
    if (check.success) route
    else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${check.failReason}"))
  }

  /**
    * Forward, if authorized, a request against a Namespace.
    * @param ent the entity to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @param route the route to forward the request to.
    * @return the destination route or a 403 if authorization check fails.
    */
  final def authorizeNamespace(ent: Namespace, header: Option[String], writePermission: Boolean)(route: Route): Route = {
    val check = checkNamespaceAuth(ent, header getOrElse "", writePermission)
    if (check.success) route
    else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${check.failReason}"))
  }

  /**
    * Forward, if authorized, a request against a Metric.
    * @param ent the entity to check.
    * @param header the header to check; empty string if not present.
    * @param writePermission true if write permission is required.
    * @param route the route to forward the request to.
    * @return the destination route or a 403 if authorization check fails.
    */
  final def authorizeMetric(ent: Metric, header: Option[String], writePermission: Boolean)(route: Route): Route = {
    val check = checkMetricAuth(ent, header getOrElse "", writePermission)
    if (check.success) route
    else complete(HttpResponse(StatusCodes.Forbidden, entity = s"not authorized ${check.failReason}"))
  }

}

/**
  * Always allaw authorization provider. No checks are performed at all.
  */
class EmptyAuthorization extends NSDBAuthProvider {

  def headerName: String = "notRelevant"

  override def checkDbAuth(ent: Db, header: String, writePermission: Boolean): AuthResponse =
    AuthResponse(success = true)

  override def checkNamespaceAuth(ent: Namespace, header: String, writePermission: Boolean): AuthResponse =
    AuthResponse(success = true)

  override def checkMetricAuth(ent: Metric, header: String, writePermission: Boolean): AuthResponse =
    AuthResponse(success = true)
}
