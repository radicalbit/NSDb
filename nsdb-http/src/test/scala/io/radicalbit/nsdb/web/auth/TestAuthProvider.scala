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
