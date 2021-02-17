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

package io.radicalbit.nsdb.rpc.server.actor

import akka.actor.{ActorRef, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.grpc.stub.StreamObserver
import io.radicalbit.nsdb.actors.PublisherActor.Commands._
import io.radicalbit.nsdb.client.rpc.converter.GrpcBitConverters.BitConverter
import io.radicalbit.nsdb.common.protocol.NSDbSerializable
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.protocol.RealTimeProtocol.Events.{
  RecordsPublished,
  SubscribedByQueryString,
  SubscriptionByQueryStringFailed
}
import io.radicalbit.nsdb.rpc.server.actor.StreamActor.{RegisterQuery, Terminate}
import io.radicalbit.nsdb.rpc.streaming.SQLStreamingResponse.Payload
import io.radicalbit.nsdb.rpc.streaming.{
  SQLStreamingResponse,
  RecordsPublished => RecordsPublishedGrpc,
  SubscribedByQueryString => SubscribedByQueryStringGrpc,
  SubscriptionByQueryStringFailed => SubscriptionByQueryStringFailedGrpc
}
import io.radicalbit.nsdb.sql.parser.SQLStatementParser
import io.radicalbit.nsdb.sql.parser.StatementParserResult._
import io.radicalbit.nsdb.util.ActorPathLogging

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration._

/**
  * Bridge actor between [[io.radicalbit.nsdb.actors.PublisherActor]] and the Grpc Stream endpoint.
  *
  * @param publisher             global Publisher Actor.
  * @param publishInterval       publish to web socket interval.
  */
class StreamActor(publisher: ActorRef, publishInterval: Int, observer: StreamObserver[SQLStreamingResponse])
    extends ActorPathLogging {

  implicit val timeout: Timeout =
    Timeout(context.system.settings.config.getDuration("nsdb.stream.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)
  import context.dispatcher

  private val buffer: mutable.Queue[SQLStreamingResponse] = mutable.Queue.empty

  override def preStart(): Unit = {
    context.system.scheduler.schedule(0.seconds, publishInterval.millis) {
      while (buffer.nonEmpty) observer.onNext(buffer.dequeue())
    }
  }

  def receive: Receive = {
    case RegisterQuery(db, namespace, metric, inputQueryString) =>
      new SQLStatementParser().parse(db, namespace, inputQueryString) match {
        case SqlStatementParserSuccess(queryString, statement: SelectSQLStatement) =>
          publisher ! SubscribeBySqlStatement(self, db, namespace, metric, queryString, statement)
        case SqlStatementParserSuccess(_, _) =>
          observer.onNext(
            SQLStreamingResponse(db,
                                 namespace,
                                 metric,
                                 Payload.SubscriptionByQueryStringFailed(
                                   SubscriptionByQueryStringFailedGrpc(inputQueryString, "not a select statement"))))
        case SqlStatementParserFailure(_, error) =>
          observer.onNext(
            SQLStreamingResponse(
              db,
              namespace,
              metric,
              Payload.SubscriptionByQueryStringFailed(SubscriptionByQueryStringFailedGrpc(inputQueryString, error))))
      }
    case SubscribedByQueryString(db, namespace, metric, _, quid, records) =>
      observer.onNext(
        SQLStreamingResponse(
          db,
          namespace,
          metric,
          Payload.SubscribedByQueryString(SubscribedByQueryStringGrpc(quid, records.map(_.asGrpcBit)))))
    case SubscriptionByQueryStringFailed(db, namespace, metric, queryString, reason) =>
      observer.onNext(
        SQLStreamingResponse(
          db,
          namespace,
          metric,
          Payload.SubscriptionByQueryStringFailed(SubscriptionByQueryStringFailedGrpc(queryString, reason))))
    case RecordsPublished(quid, db, namespace, metric, records) =>
      buffer += SQLStreamingResponse(db,
                                     namespace,
                                     metric,
                                     Payload.RecordsPublished(RecordsPublishedGrpc(quid, records.map(_.asGrpcBit))))
    case Terminate =>
      (publisher ? Unsubscribe(self)).foreach { _ =>
        self ! PoisonPill
        observer.onCompleted()
      }
    case msg @ _ =>
      log.error(s"Unexpected message : $msg")
  }
}

object StreamActor {
  case class Connect(outgoing: ActorRef) extends NSDbSerializable

  case object Terminate
  case class RegisterQuery(db: String, namespace: String, metric: String, queryString: String) extends NSDbSerializable

  def props(publisherActor: ActorRef, refreshPeriod: Int, observer: StreamObserver[SQLStreamingResponse]) =
    Props(new StreamActor(publisherActor, refreshPeriod, observer))
}
