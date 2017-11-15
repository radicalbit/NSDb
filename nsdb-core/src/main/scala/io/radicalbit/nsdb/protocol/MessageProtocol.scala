package io.radicalbit.nsdb.protocol

import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{DeleteSQLStatement, SelectSQLStatement}
import io.radicalbit.nsdb.index.Schema

object MessageProtocol {

  object Commands {
    case object GetNamespaces
    case class GetMetrics(namespace: String)
    case class GetSchema(namespace: String, metric: String)
    case class ExecuteStatement(selectStatement: SelectSQLStatement)
    case class ExecuteSelectStatement(selectStatement: SelectSQLStatement, schema: Schema)

    case class FlatInput(ts: Long, namespace: String, metric: String, data: Array[Byte])
    case class MapInput(ts: Long, namespace: String, metric: String, record: Bit)
    case class InputMapped(namespace: String, metric: String, record: Bit)
    case class ExecuteDeleteStatement(statement: DeleteSQLStatement)
    case class DropMetric(namespace: String, metric: String)
    case class DeleteNamespace(namespace: String)

    case class UpdateSchema(namespace: String, metric: String, newSchema: Schema)
    case class UpdateSchemaFromRecord(namespace: String, metric: String, record: Bit)
    case class DeleteSchema(namespace: String, metric: String)
    case class DeleteAllSchemas(namespace: String)

    case class AddRecord(namespace: String, metric: String, bit: Bit)
    case class AddRecords(namespace: String, metric: String, bits: Seq[Bit])
    case class DeleteRecord(namespace: String, metric: String, bit: Bit)
    case class DeleteMetric(namespace: String, metric: String)
    case class DeleteAllMetrics(namespace: String)

    case object GetReadCoordinator
    case object GetWriteCoordinator
    case object GetPublisher
  }

  object Events {
    case class NamespacesGot(namespaces: Seq[String])
    case class SchemaGot(namespace: String, metric: String, schema: Option[Schema])
    case class MetricsGot(namespace: String, metrics: Seq[String])
    case class SelectStatementExecuted(namespace: String, metric: String, values: Seq[Bit])
    case class SelectStatementFailed(reason: String)

    case class DeleteStatementExecuted(namespace: String, metric: String)
    case class DeleteStatementFailed(namespace: String, metric: String, reason: String)
    case class MetricDropped(namespace: String, metric: String)
    case class NamespaceDeleted(namespace: String)

    case class SchemaUpdated(namespace: String, metric: String)
    case class UpdateSchemaFailed(namespace: String, metric: String, errors: List[String])
    case class SchemaDeleted(namespace: String, metric: String)
    case class AllSchemasDeleted(namespace: String)

    case class GetCount(namespace: String, metric: String)
    case class CountGot(namespace: String, metric: String, count: Int)
    case class RecordAdded(namespace: String, metric: String, record: Bit)
    case class RecordsAdded(namespace: String, metric: String, record: Seq[Bit])
    case class RecordRejected(namespace: String, metric: String, record: Bit, reasons: List[String])
    case class RecordDeleted(namespace: String, metric: String, record: Bit)
    case class MetricDeleted(namespace: String, metric: String)
    case class AllMetricsDeleted(namespace: String)
  }

}