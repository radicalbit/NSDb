package io.radicalbit.nsdb

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.coordinator.ReadCoordinator
import io.radicalbit.nsdb.coordinator.ReadCoordinator.SelectStatementExecuted
import io.radicalbit.nsdb.core.Core
import io.radicalbit.nsdb.statement.SelectSQLStatement

object Client extends App with Core {

  import scala.concurrent.duration._

  override implicit lazy val system = ActorSystem("nsdb-client")

  implicit val timeout    = Timeout(10 second)
  implicit val dispatcher = system.dispatcher

  var counter: Int = 0

  lazy val writeCoordinator =
    system.actorSelection("akka.tcp://NsdbSystem@127.0.0.1:2552/user/guardian/write-coordinator")
  lazy val readCoordinator =
    system.actorSelection("akka.tcp://NsdbSystem@127.0.0.1:2552/user/guardian/read-coordinator")

  def executeSqlSelectStatement(statement: SelectSQLStatement) =
    (readCoordinator ? ReadCoordinator.ExecuteSelectStatement(statement)).mapTo[SelectStatementExecuted].map(_.values)

}
