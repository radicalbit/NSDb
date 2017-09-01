package io.radicalbit.nsdb.coordinator

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.statement.SelectSQLStatement
import io.radicalbit.nsdb.index.Schema

import scala.concurrent.Future
import scala.concurrent.duration._

class ReadCoordinator(schemaActor: ActorRef, namespaceActor: ActorRef) extends Actor with ActorLogging {
  import ReadCoordinator._

  implicit val timeout: Timeout = 1 second
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
  case class SelectStatementExecuted[T](values: Seq[T])
  case class SelectStatementFailed(reason: String)
}
