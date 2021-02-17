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

package io.radicalbit.nsdb.api.scala.streaming

import io.grpc.stub.StreamObserver
import io.radicalbit.nsdb.api.scala.{NSDB, SQLStatement}
import io.radicalbit.nsdb.rpc.requestSQL.SQLRequestStatement
import io.radicalbit.nsdb.rpc.streaming.SQLStreamingResponse

import scala.concurrent.ExecutionContext

object NSDbStreaming {

  implicit class toNSDbStreaming(nsdb: NSDB) {

    def subscribe(statement: SQLStatement)(
        callBack: SQLStreamingResponse => Unit,
        errorCallBack: Throwable => Unit = _ => ())(implicit ec: ExecutionContext) = {
      nsdb.client.subscribe(
        SQLRequestStatement(statement.db, statement.namespace, statement.metric, statement.sQLStatement),
        new StreamObserver[SQLStreamingResponse] {
          override def onNext(value: SQLStreamingResponse): Unit = callBack(value)
          override def onError(t: Throwable): Unit               = errorCallBack(t)
          override def onCompleted(): Unit                       = {}
        }
      )
    }

  }
}
