package io.radicalbit.nsdb.common.protocol

import io.radicalbit.nsdb.common.statement.{CommandStatement, SQLStatement}

sealed trait EndpointInputProtocol
case class ExecuteCommandStatement(statement: CommandStatement) extends EndpointInputProtocol
case class ExecuteSQLStatement(statement: SQLStatement)         extends EndpointInputProtocol

sealed trait EndpointOutputProtocol
sealed trait SQLStatementResult {
  def db: String
  def namespace: String
  def metric: String
}
// TODO: the result must be well structured
// TODO: it must contain the schema (for both select and insert) and the final retrieved data (only for the select)
case class SQLStatementExecuted(db: String, namespace: String, metric: String, res: Seq[Bit] = Seq.empty)
    extends SQLStatementResult
    with EndpointOutputProtocol
case class SQLStatementFailed(db: String, namespace: String, metric: String, reason: String)
    extends SQLStatementResult
    with EndpointOutputProtocol

sealed trait CommandStatementExecuted extends EndpointOutputProtocol

case class MetricField(name: String, `type`: String)

case class NamespaceMetricsListRetrieved(db: String, namespace: String, metrics: List[String])
    extends CommandStatementExecuted
case class MetricSchemaRetrieved(db: String, namespace: String, metric: String, fields: List[MetricField])
    extends CommandStatementExecuted
case class CommandStatementExecutedWithFailure(reason: String) extends CommandStatementExecuted
