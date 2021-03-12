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

package io.radicalbit.nsdb.rpc.server

import io.grpc.Context
import io.radicalbit.nsdb.rpc.security.SecurityProto
import scalapb.descriptors.ServiceDescriptor

trait GRPCService { this: _root_.scalapb.grpc.AbstractService =>

  def serviceDescriptor: ServiceDescriptor

  def isAuthorized: Boolean = serviceDescriptor.getOptions.extension(SecurityProto.isAuthorized)

  def getSecurityPaylaod(context: Context): String =
    Option(GRPCService.securityPayloadKey.get(context)).getOrElse("")

}

object GRPCService {
  final val securityPayloadKey = Context.key[String]("SECURITY_PAYLOAD")
}
