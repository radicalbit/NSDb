package io.radicalbit.nsdb.common.protocol

import io.radicalbit.nsdb.common.statement.{CommandStatement, SQLStatement}

/**
  * Trait for all input messages accepted by Grpc endpoint.
  * subclasses are [[ExecuteCommandStatement]] and [[ExecuteSQLStatement]].
  */
sealed trait EndpointInputProtocol
case class ExecuteCommandStatement(statement: CommandStatement) extends EndpointInputProtocol
case class ExecuteSQLStatement(statement: SQLStatement)         extends EndpointInputProtocol

/**
  * Trait for all output messages returned by Grpc endpoint.
  * subclasses are [[SQLStatementResult]] and [[CommandStatementExecuted]]
  */
sealed trait EndpointOutputProtocol

/**
  * Results of the execution of a sql statement through Grpc channel.
  * [[SQLStatementExecuted]] if no errors occur.
  * [[SQLStatementFailed]] if an error occurs.
  */
sealed trait SQLStatementResult extends EndpointOutputProtocol {
  def db: String
  def namespace: String
  def metric: String
}

/**
  * Case class returned by Grpc if a sql statement has been executed successfully.
  * @param db the statement db.
  * @param namespace the statement namespace.
  * @param metric the statement metric.
  * @param res sequence of bit retrieved by the sql query if query is a select query and there are results to be returned,
  *            empty for insert or delete statements.
  */
case class SQLStatementExecuted(db: String, namespace: String, metric: String, res: Seq[Bit] = Seq.empty)
    extends SQLStatementResult

/**
  * Case class returned if an error occurs during execution of a sql statement through Grpc channel.
  * @param db the statement db.
  * @param namespace the statement namespace.
  * @param metric the statement metric.
  * @param reason the error.
  */
case class SQLStatementFailed(db: String, namespace: String, metric: String, reason: String) extends SQLStatementResult

/**
  * Describes a metric field. See [[MetricSchemaRetrieved]].
  * @param name field name.
  * @param `type` field type (e.g. VARCHAR, INT, BIGINT, ecc.).
  */
case class MetricField(name: String, `type`: String)

/**
  * Trait for every command statement executed through Grpc channel.
  * subclasses are
  *
  * - [[NamespaceMetricsListRetrieved]] for show metrics command.
  *
  * - [[MetricSchemaRetrieved]] for describe metric command.
  *
  * - [[NamespacesListRetrieved]] for show namespaces command.
  */
sealed trait CommandStatementExecuted extends EndpointOutputProtocol
case class NamespaceMetricsListRetrieved(db: String, namespace: String, metrics: List[String])
    extends CommandStatementExecuted
case class MetricSchemaRetrieved(db: String, namespace: String, metric: String, fields: List[MetricField])
    extends CommandStatementExecuted
case class NamespacesListRetrieved(db: String, namespaces: Seq[String]) extends CommandStatementExecuted

/**
  * Case class returned if any of the above commands fail.
  * @param reason the error.
  */
case class CommandStatementExecutedWithFailure(reason: String) extends CommandStatementExecuted
