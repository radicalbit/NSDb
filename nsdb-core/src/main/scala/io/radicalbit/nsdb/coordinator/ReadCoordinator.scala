package io.radicalbit.nsdb.coordinator

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.index.Schema

import scala.concurrent.Future

class ReadCoordinator(schemaActor: ActorRef, namespaceActor: ActorRef) extends Actor with ActorLogging {
  import ReadCoordinator._

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.read-coordinatoor.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)

  import context.dispatcher

  override def receive: Receive = {

    case GetNamespaces =>
      namespaceActor forward GetNamespaces
    case msg @ GetMetrics(_) =>
      namespaceActor forward msg
    case msg @ GetSchema(_, _) =>
      schemaActor forward msg
      namespaceActor forward msg
    case ExecuteStatement(statement) =>
      log.debug(s"executing $statement")
      (schemaActor ? GetSchema(statement.namespace, statement.metric))
        .mapTo[SchemaGot]
        .flatMap {
          case SchemaGot(_, _, Some(schema)) =>
            namespaceActor ? ExecuteSelectStatement(statement, schema)
          case _ => Future(SelectStatementFailed(s"No schema found for metric ${statement.metric}"))
        }
        .pipeTo(sender())
  }
}

object ReadCoordinator {

  def props(schemaActor: ActorRef, indexerActor: ActorRef): Props =
    Props(new ReadCoordinator(schemaActor, indexerActor))

  case object GetNamespaces
  case class GetMetrics(namespace: String)
  case class GetSchema(namespace: String, metric: String)
  case class ExecuteStatement(selectStatement: SelectSQLStatement)
  case class ExecuteSelectStatement(selectStatement: SelectSQLStatement, schema: Schema)
  case class NamespacesGot(namespaces: Seq[String])
  case class SchemaGot(namespace: String, metric: String, schema: Option[Schema])
  case class MetricsGot(namespace: String, metrics: Seq[String])
  case class SelectStatementExecuted(namespace: String, metric: String, values: Seq[Bit])
  case class SelectStatementFailed(reason: String)
}
