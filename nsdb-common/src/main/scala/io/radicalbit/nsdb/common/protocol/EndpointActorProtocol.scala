package io.radicalbit.nsdb.common.protocol

import io.radicalbit.nsdb.common.statement.{CommandStatement, SQLStatement}

sealed trait EndpointInputProtocol
case class ExecuteCommandStatement(statement: CommandStatement) extends EndpointInputProtocol
case class ExecuteSQLStatement(statement: SQLStatement)         extends EndpointInputProtocol

sealed trait EndpointOutputProtocol
// TODO: the result must be well structured
// TODO: it must contain the schema (for both select and insert) and the final retrieved data (only for the select)
case class SQLStatementExecuted(namespace: String, metric: String, res: Seq[Bit] = Seq.empty)
    extends EndpointOutputProtocol

trait CommandStatementExecuted                                                     extends EndpointOutputProtocol
case class NamespaceMetricsListRetrieved(namespace: String, metrics: List[String]) extends CommandStatementExecuted
case class MetricField(name: String, `type`: String)                               extends CommandStatementExecuted
case class MetricSchemaRetrieved(namespace: String, metric: String, fields: List[MetricField])
    extends CommandStatementExecuted
