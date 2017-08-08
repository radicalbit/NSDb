package io.radicalbit.nsdb.coordinator

import akka.actor.Actor
import io.radicalbit.nsdb.statement.SelectSQLStatement

class ReadCoordinator extends Actor {
  import ReadCoordinator._

  override def receive: Receive = {
    case ExecuteSelectStatement => sender() ! SelectStatementExecuted(Values(0))
  }
}

object ReadCoordinator {
  case class ExecuteSelectStatement(selectStatement: SelectSQLStatement)
  case class SelectStatementExecuted(values: Values)
  case class Values(n: Int)
}
