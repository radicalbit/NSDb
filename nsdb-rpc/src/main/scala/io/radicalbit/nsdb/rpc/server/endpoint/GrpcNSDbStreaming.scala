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

package io.radicalbit.nsdb.rpc.server.endpoint

import akka.actor.{ActorRef, ActorSystem}
import io.grpc.stub.StreamObserver
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.server.actor.StreamActor
import io.radicalbit.nsdb.rpc.server.actor.StreamActor.RegisterQuery
import io.radicalbit.nsdb.rpc.streaming.NSDbStreamingGrpc.NSDbStreaming
import io.radicalbit.nsdb.rpc.streaming.SQLStreamingResponse

class GrpcNSDbStreaming(publisherActor: ActorRef)(implicit system: ActorSystem) extends NSDbStreaming {
  override def streamSQL(request: SQLRequestStatement, responseObserver: StreamObserver[SQLStreamingResponse]): Unit = {
    val actor = system.actorOf(StreamActor.props("", publisherActor, 500, responseObserver))
    actor ! RegisterQuery(request.db, request.namespace, request.metric, request.statement)
  }
}
