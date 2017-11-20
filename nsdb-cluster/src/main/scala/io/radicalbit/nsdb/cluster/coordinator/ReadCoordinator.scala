package io.radicalbit.nsdb.cluster.coordinator

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.concurrent.Future

class ReadCoordinator(namespaceSchemaActor: ActorRef) extends Actor with ActorLogging {

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.read-coordinatoor.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  import context.dispatcher

  override def receive: Receive = init

  def init: Receive = {
    case SubscribeNamespaceDataActor(actor: ActorRef, _) =>
      context.become(subscribed(actor))
      sender() ! NamespaceDataActorSubscribed(actor)
  }

  def subscribed(namespaceDataActor: ActorRef): Receive = {

    case msg: GetNamespaces =>
      namespaceDataActor forward msg
    case msg: GetMetrics =>
      namespaceDataActor forward msg
    case msg: GetSchema =>
      namespaceSchemaActor forward msg
      namespaceDataActor forward msg
    case ExecuteStatement(statement) =>
      log.debug(s"executing $statement")
      (namespaceSchemaActor ? GetSchema(statement.db, statement.namespace, statement.metric))
        .flatMap {
          case SchemaGot(_, _, _, Some(schema)) =>
            namespaceDataActor ? ExecuteSelectStatement(statement, schema)
          case _ => Future(SelectStatementFailed(s"Metric ${statement.metric} does not exist "))
        }
        .pipeTo(sender())
  }
}

object ReadCoordinator {

  def props(schemaActor: ActorRef): Props =
    Props(new ReadCoordinator(schemaActor))

}
