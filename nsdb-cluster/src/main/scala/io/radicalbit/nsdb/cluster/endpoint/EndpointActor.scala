package io.radicalbit.nsdb.cluster.endpoint

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.client.ClusterClientReceptionist
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.concurrent.Future

object EndpointActor {

  def props(readCoordinator: ActorRef, writeCoordinator: ActorRef) =
    Props(new EndpointActor(readCoordinator = readCoordinator, writeCoordinator = writeCoordinator))

}

class EndpointActor(readCoordinator: ActorRef, writeCoordinator: ActorRef) extends Actor with ActorLogging {

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.rpc-akka-endpoint.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  ClusterClientReceptionist(context.system).registerService(self)

  def receive = {

    case ExecuteSQLStatement(statement: SelectSQLStatement) =>
      (readCoordinator ? ExecuteStatement(statement))
        .map {
          case SelectStatementExecuted(db, namespace, metric, values: Seq[Bit]) =>
            SQLStatementExecuted(db = db, namespace = namespace, metric = metric, values)
          case SelectStatementFailed(reason) =>
            throw new RuntimeException(s"Cannot execute the given select statement. The reason is $reason.")
        }
        .pipeTo(sender())

    case ExecuteSQLStatement(statement: InsertSQLStatement) =>
      val result = InsertSQLStatement
        .unapply(statement)
        .map {
          case (db, namespace, metric, ts, dimensions, value) =>
            val timestamp = ts getOrElse System.currentTimeMillis
            writeCoordinator ? MapInput(
              timestamp,
              db,
              namespace,
              metric,
              Bit(timestamp = timestamp, value = value, dimensions = dimensions.map(_.fields).getOrElse(Map.empty)))
        }
        .getOrElse(Future(throw new RuntimeException("The insert SQL statement is invalid.")))
      result
        .map {
          case x: InputMapped =>
            SQLStatementExecuted(db = x.db, namespace = x.namespace, metric = x.metric, res = Seq.empty)
          case x: RecordRejected =>
            SQLStatementFailed(db = x.db, namespace = x.namespace, metric = x.metric, reason = x.reasons.mkString(","))
        }
        .pipeTo(sender())

    case ExecuteSQLStatement(statement: DeleteSQLStatement) =>
      (writeCoordinator ? ExecuteDeleteStatement(statement))
        .mapTo[DeleteStatementExecuted]
        .map(x => SQLStatementExecuted(db = x.db, namespace = x.namespace, metric = x.metric, res = Seq.empty))
        .pipeTo(sender())

    case ExecuteSQLStatement(statement: DropSQLStatement) =>
      (writeCoordinator ? DropMetric(statement.db, statement.namespace, statement.metric))
        .mapTo[MetricDropped]
        .map(x => SQLStatementExecuted(db = x.db, namespace = x.namespace, metric = x.metric, res = Seq.empty))
        .pipeTo(sender())

    case ExecuteCommandStatement(ShowMetrics(db, namespace)) =>
      (readCoordinator ? GetMetrics(db, namespace)).mapTo[MetricsGot].map {
        case MetricsGot(db, namespace, metrics) => NamespaceMetricsListRetrieved(db, namespace, metrics.toList)
      } pipeTo sender()

    case ExecuteCommandStatement(DescribeMetric(db, namespace, metric)) =>
      (readCoordinator ? GetSchema(db, namespace = namespace, metric = metric))
        .mapTo[SchemaGot]
        .map {
          case SchemaGot(db, namespace, metric, schema) =>
            val fields = schema
              .map(
                _.fields.map(field => MetricField(name = field.name, `type` = field.indexType.getClass.getSimpleName)))
              .getOrElse(List.empty[MetricField])
            MetricSchemaRetrieved(db, namespace, metric, fields.toList)
        }
        .pipeTo(sender())
  }
}
