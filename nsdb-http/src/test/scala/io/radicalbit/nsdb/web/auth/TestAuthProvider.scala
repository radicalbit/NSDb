package io.radicalbit.nsdb.web.auth

import io.radicalbit.nsdb.security.http.{AuthResponse, NSDBAuthProvider}
import io.radicalbit.nsdb.security.model.{Db, Metric, Namespace}

class TestAuthProvider extends NSDBAuthProvider {

  def headerName: String = "testHeader"

  override def checkDbAuth(ent: Db, header: String, writePermission: Boolean): AuthResponse =
    if (header.isEmpty) AuthResponse(success = false, failReason = "header not provided")
    else if (ent.db == "notAuthorizedDb")
      AuthResponse(success = false, failReason = s"forbidden access to db ${ent.db}")
    else if (ent.db == "readOnlyDb" && writePermission)
      AuthResponse(success = false, failReason = s"forbidden write access to namespace ${ent.db}")
    else AuthResponse(success = true)

  override def checkNamespaceAuth(ent: Namespace, header: String, writePermission: Boolean): AuthResponse =
    if (header.isEmpty) AuthResponse(success = false, failReason = "header not provided")
    else if (ent.namespace == "notAuthorizedNamespace")
      AuthResponse(success = false, failReason = s"forbidden access to namespace ${ent.namespace}")
    else if (ent.namespace == "readOnlyNamespace" && writePermission)
      AuthResponse(success = false, failReason = s"forbidden write access to namespace ${ent.namespace}")
    else AuthResponse(success = true)

  override def checkMetricAuth(ent: Metric, header: String, writePermission: Boolean): AuthResponse =
    if (header.isEmpty) AuthResponse(success = false, failReason = "header not provided")
    else if (ent.metric == "notAuthorizedMetric")
      AuthResponse(success = false, failReason = s"forbidden access to metric ${ent.metric}")
    else if (ent.metric == "readOnlyMetric" && writePermission)
      AuthResponse(success = false, failReason = s"forbidden write access to metric ${ent.metric}")
    else AuthResponse(success = true)
}
