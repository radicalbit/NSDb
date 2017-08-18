package io.radicalbit.nsdb.coordinator

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.NamespaceSchemaActor.commands.GetSchema
import io.radicalbit.nsdb.actors.NamespaceSchemaActor.events.SchemaGot
import io.radicalbit.nsdb.index.Schema
import io.radicalbit.nsdb.statement.SelectSQLStatement

import scala.concurrent.Future
import scala.concurrent.duration._

class ReadCoordinator(schemaActor: ActorRef, indexerActor: ActorRef) extends Actor with ActorLogging {
  import ReadCoordinator._

  implicit val timeout: Timeout = 1 second
  import context.dispatcher

  override def receive: Receive = {

    case ExecuteStatement(statement) =>
      (schemaActor ? GetSchema(statement.namespace, statement.metric))
        .mapTo[SchemaGot]
        .flatMap {
          case SchemaGot(_, _, Some(schema)) =>
            indexerActor ? ExecuteSelectStatement(statement, schema)
          case _ => Future(SelectStatementFailed(s"No schema found for metric ${statement.metric}"))
        }
        .pipeTo(sender())
  }
}

object ReadCoordinator {

  def props(schemaActor: ActorRef, indexerActor: ActorRef): Props =
    Props(new ReadCoordinator(schemaActor, indexerActor))

  case class ExecuteStatement(selectStatement: SelectSQLStatement)
  case class ExecuteSelectStatement(selectStatement: SelectSQLStatement, schema: Schema)
  case class SelectStatementExecuted[T](values: Seq[T])
  case class SelectStatementFailed(reason: String)
}
