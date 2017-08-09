package io.radicalbit.nsdb.coordinator

import akka.actor.{Actor, ActorRef, Props}
import io.radicalbit.nsdb.statement.SelectSQLStatement

class ReadCoordinator(indexerActor: ActorRef) extends Actor {
  import ReadCoordinator._

  override def receive: Receive = {
    case msg @ ExecuteSelectStatement(_) =>
      indexerActor.forward(msg)
  }
}

object ReadCoordinator {

  def props(indexerActor: ActorRef): Props =
    Props(new ReadCoordinator(indexerActor))

  case class ExecuteSelectStatement(selectStatement: SelectSQLStatement)
  case class SelectStatementExecuted[T](values: Seq[T])
  case class SelectStatementFailed(reason: String)
}
