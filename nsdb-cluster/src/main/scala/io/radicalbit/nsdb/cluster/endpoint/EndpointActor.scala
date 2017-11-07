package io.radicalbit.nsdb.cluster.endpoint

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.client.ClusterClientReceptionist
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.NamespaceDataActor.events.RecordRejected
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.coordinator.ReadCoordinator
import io.radicalbit.nsdb.coordinator.ReadCoordinator._
import io.radicalbit.nsdb.coordinator.WriteCoordinator._

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
      (readCoordinator ? ReadCoordinator.ExecuteStatement(statement))
        .map {
          case SelectStatementExecuted(namespace, metric, values: Seq[Bit]) =>
            SQLStatementExecuted(namespace = namespace, metric = metric, values)
          case SelectStatementFailed(reason) =>
            throw new RuntimeException(s"Cannot execute the given select statement. The reason is $reason.")
        }
        .pipeTo(sender())

    case ExecuteSQLStatement(statement: InsertSQLStatement) =>
      val result = InsertSQLStatement
        .unapply(statement)
        .map {
          case (namespace, metric, ts, dimensions, value) =>
            val timestamp = ts getOrElse System.currentTimeMillis
            writeCoordinator ? MapInput(
              timestamp,
              namespace,
              metric,
              Bit(timestamp = timestamp, value = value, dimensions = dimensions.map(_.fields).getOrElse(Map.empty)))
        }
        .getOrElse(Future(throw new RuntimeException("The insert SQL statement is invalid.")))
      result
        .map {
          case x: InputMapped => SQLStatementExecuted(namespace = x.namespace, metric = x.metric, res = Seq.empty)
          case x: RecordRejected =>
            SQLStatementFailed(namespace = x.namespace, metric = x.metric, reason = x.reasons.mkString(","))
        }
        .pipeTo(sender())

    case ExecuteSQLStatement(statement: DeleteSQLStatement) =>
      (writeCoordinator ? ExecuteDeleteStatement(statement))
        .mapTo[DeleteStatementExecuted]
        .map(x => SQLStatementExecuted(namespace = x.namespace, metric = x.metric, res = Seq.empty))
        .pipeTo(sender())

    case ExecuteSQLStatement(statement: DropSQLStatement) =>
      (writeCoordinator ? DropMetric(statement.namespace, statement.metric))
        .mapTo[MetricDropped]
        .map(x => SQLStatementExecuted(namespace = x.namespace, metric = x.metric, res = Seq.empty))
        .pipeTo(sender())

    case ExecuteCommandStatement(ShowMetrics(namespace)) =>
      (readCoordinator ? GetMetrics(namespace)).mapTo[MetricsGot].map {
        case MetricsGot(namespace, metrics) => NamespaceMetricsListRetrieved(namespace, metrics.toList)
      } pipeTo (sender())

    case ExecuteCommandStatement(DescribeMetric(namespace, metric)) =>
      (readCoordinator ? GetSchema(namespace = namespace, metric = metric))
        .mapTo[SchemaGot]
        .map {
          case SchemaGot(namespace, metric, schema) =>
            val fields = schema
              .map(
                _.fields.map(field => MetricField(name = field.name, `type` = field.indexType.getClass.getSimpleName)))
              .getOrElse(List.empty[MetricField])
            MetricSchemaRetrieved(namespace, metric, fields.toList)
        }
        .pipeTo(sender())
  }
}
