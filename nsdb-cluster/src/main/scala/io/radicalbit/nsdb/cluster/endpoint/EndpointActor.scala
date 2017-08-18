package io.radicalbit.nsdb.cluster.endpoint

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.client.ClusterClientReceptionist
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.endpoint.EndpointActor.{ExecuteSQLStatement, SQLStatementExecuted}
import io.radicalbit.nsdb.coordinator.ReadCoordinator
import io.radicalbit.nsdb.model.{Record, RecordOut}
import io.radicalbit.nsdb.statement.{InsertSQLStatement, SQLStatement, SelectSQLStatement}

import scala.concurrent.duration._
import akka.pattern.ask
import akka.pattern.pipe
import io.radicalbit.nsdb.coordinator.ReadCoordinator.{SelectStatementExecuted, SelectStatementFailed}
import io.radicalbit.nsdb.coordinator.WriteCoordinator.{InputMapped, MapInput}

import scala.concurrent.Future

object EndpointActor {

  def props(readCoordinator: ActorRef, writeCoordinator: ActorRef) =
    Props(new EndpointActor(readCoordinator = readCoordinator, writeCoordinator = writeCoordinator))

  sealed trait EndpointInputProtocol

  case class ExecuteSQLStatement(statement: SQLStatement) extends EndpointInputProtocol

  sealed trait EndpointOutputProtocol

  // TODO: the result must be well structured
  // TODO: it must contain the schema (for both select and insert) and the final retrieved data (only for the select)
  case class SQLStatementExecuted(res: Seq[RecordOut] = Seq.empty) extends EndpointOutputProtocol
}

class EndpointActor(readCoordinator: ActorRef, writeCoordinator: ActorRef) extends Actor with ActorLogging {

  implicit val timeout: Timeout = 1 second
  import context.dispatcher

  ClusterClientReceptionist(context.system).registerService(self)

  def receive = {

    case ExecuteSQLStatement(statement: SelectSQLStatement) =>
      (readCoordinator ? ReadCoordinator.ExecuteStatement(statement))
        .map {
          case SelectStatementExecuted(values: Seq[RecordOut]) =>
            SQLStatementExecuted(values)
          case SelectStatementFailed(reason) =>
            throw new RuntimeException(s"Cannot execute the given select statement. The reason is $reason.")
        }
        .pipeTo(sender())

    case ExecuteSQLStatement(statement: InsertSQLStatement) =>
      val result = InsertSQLStatement
        .unapply(statement)
        .map {
          case (namespace, metric, ts, dimensions, fields) =>
            val timestamp = ts getOrElse System.currentTimeMillis
            (writeCoordinator ? MapInput(timestamp, metric, Record(timestamp, dimensions.fields, fields.fields)))
              .mapTo[InputMapped]
        }
        .getOrElse(Future(throw new RuntimeException("The insert SQL statement is invalid.")))

      result.map(_ => SQLStatementExecuted(res = Seq.empty)).pipeTo(sender())
  }
}
