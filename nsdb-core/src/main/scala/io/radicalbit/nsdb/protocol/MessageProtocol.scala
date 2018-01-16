package io.radicalbit.nsdb.protocol

import akka.actor.ActorRef
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DeleteSQLStatement, SelectSQLStatement}
import io.radicalbit.nsdb.index.Schema

object MessageProtocol {

  object Commands {
    case class GetNamespaces(db: String)
    case class GetMetrics(db: String, namespace: String)
    case class GetSchema(db: String, namespace: String, metric: String)
    case class ExecuteStatement(selectStatement: SelectSQLStatement)
    case class ExecuteSelectStatement(selectStatement: SelectSQLStatement, schema: Schema)

    case class FlatInput(ts: Long, db: String, namespace: String, metric: String, data: Array[Byte])
    case class MapInput(ts: Long, db: String, namespace: String, metric: String, record: Bit)
    case class PublishRecord(db: String, namespace: String, metric: String, record: Bit, schema: Schema)
    case class ExecuteDeleteStatement(statement: DeleteSQLStatement)
    case class ExecuteDeleteStatementInternal(statement: DeleteSQLStatement, schema: Schema)
    case class DropMetric(db: String, namespace: String, metric: String)
    case class DeleteNamespace(db: String, namespace: String)

    case class UpdateSchema(db: String, namespace: String, metric: String, newSchema: Schema)
    case class UpdateSchemaFromRecord(db: String, namespace: String, metric: String, record: Bit)
    case class DeleteSchema(db: String, namespace: String, metric: String)
    case class DeleteAllSchemas(db: String, namespace: String)

    case class GetCount(db: String, namespace: String, metric: String)
    case class AddRecord(db: String, namespace: String, metric: String, bit: Bit)
    case class AddRecords(db: String, namespace: String, metric: String, bits: Seq[Bit])
    case class DeleteRecord(db: String, namespace: String, metric: String, bit: Bit)
    case class DeleteAllMetrics(db: String, namespace: String)

    case object GetReadCoordinator
    case object GetWriteCoordinator
    case object GetPublisher

    case class SubscribeNamespaceDataActor(actor: ActorRef, nodeName: Option[String] = None)
  }

  object Events {
    case class NamespacesGot(db: String, namespaces: Set[String])
    case class SchemaGot(db: String, namespace: String, metric: String, schema: Option[Schema])
    case class MetricsGot(db: String, namespace: String, metrics: Set[String])
    case class SelectStatementExecuted(db: String, namespace: String, metric: String, values: Seq[Bit])
    case class SelectStatementFailed(reason: String)

    case class InputMapped(db: String, namespace: String, metric: String, record: Bit)
    case class DeleteStatementExecuted(db: String, namespace: String, metric: String)
    case class DeleteStatementFailed(db: String, namespace: String, metric: String, reason: String)
    case class MetricDropped(db: String, namespace: String, metric: String)
    case class NamespaceDeleted(db: String, namespace: String)

    case class SchemaUpdated(db: String, namespace: String, metric: String, schema: Schema)
    case class UpdateSchemaFailed(db: String, namespace: String, metric: String, errors: List[String])
    case class SchemaDeleted(db: String, namespace: String, metric: String)
    case class AllSchemasDeleted(db: String, namespace: String)

    case class CountGot(db: String, namespace: String, metric: String, count: Int)
    case class RecordAdded(db: String, namespace: String, metric: String, record: Bit)
    case class RecordsAdded(db: String, namespace: String, metric: String, record: Seq[Bit])
    case class RecordRejected(db: String, namespace: String, metric: String, record: Bit, reasons: List[String])
    case class RecordDeleted(db: String, namespace: String, metric: String, record: Bit)
    case class AllMetricsDeleted(db: String, namespace: String)

    case class NamespaceDataActorSubscribed(actor: ActorRef, host: Option[String] = None)
    case class NamespaceDataActorSubscriptionFailed(actor: ActorRef, host: Option[String] = None, reason: String)
  }

}
