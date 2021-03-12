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

package io.radicalbit.nsdb.rpc.test

import io.grpc.Context
import io.radicalbit.nsdb.rpc.server.GRPCService
import io.radicalbit.nsdb.rpc.test.securityTest.DummySecureServiceGrpc.DummySecureService
import io.radicalbit.nsdb.rpc.test.securityTest.{DummyResponse, DummySecureRequest}
import scalapb.descriptors.ServiceDescriptor

import scala.concurrent.{ExecutionContext, Future}

object DummyServiceGrpc {

  class DummySecureServiceGrpc(implicit executionContext: ExecutionContext)
      extends DummySecureService
      with GRPCService {
    override def dummySecure(request: DummySecureRequest): Future[DummyResponse] = {
      val p = getSecurityPaylaod(Context.current())
      Future(DummyResponse(p, s"${request.db}-${request.namespace}-${request.metric}"))
    }

    override def serviceDescriptor: ServiceDescriptor = this.serviceCompanion.scalaDescriptor
  }

}
