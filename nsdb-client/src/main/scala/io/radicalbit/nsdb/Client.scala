package io.radicalbit.nsdb

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.coordinator.ReadCoordinator
import io.radicalbit.nsdb.coordinator.ReadCoordinator.{SelectStatementExecuted, SelectStatementFailed}
import io.radicalbit.nsdb.coordinator.WriteCoordinator.{InputMapped, MapInput}
import io.radicalbit.nsdb.core.Core
import io.radicalbit.nsdb.model.{Record, RecordOut}
import io.radicalbit.nsdb.statement.{InsertSQLStatement, SelectSQLStatement}

import scala.concurrent.Future

object Client extends App with Core {

  import scala.concurrent.duration._

  override implicit lazy val system = ActorSystem("nsdb-client", ConfigFactory.load("client"))

  implicit val timeout    = Timeout(10 second)
  implicit val dispatcher = system.dispatcher

  var counter: Int = 0

  lazy val writeCoordinator =
    system.actorSelection("akka.tcp://NsdbSystem@127.0.0.1:2552/user/guardian/write-coordinator")
  lazy val readCoordinator =
    system.actorSelection("akka.tcp://NsdbSystem@127.0.0.1:2552/user/guardian/read-coordinator")

  def executeSqlSelectStatement(statement: SelectSQLStatement) =
    (readCoordinator ? ReadCoordinator.ExecuteStatement(statement))
      .mapTo[SelectStatementExecuted[RecordOut]]
      .map(_.values)

}

class ClientDelegate(implicit system: ActorSystem) {
  import scala.concurrent.duration._

  implicit val timeout = Timeout(10 second)

  lazy val readCoordinator =
    system.actorSelection("akka.tcp://NsdbSystem@127.0.0.1:2552/user/guardian/read-coordinator")
  lazy val writeCoordinator =
    system.actorSelection("akka.tcp://NsdbSystem@127.0.0.1:2552/user/guardian/write-coordinator")

  def executeSqlSelectStatement(statement: SelectSQLStatement): Future[Seq[RecordOut]] = {

    implicit val dispatcher = system.dispatcher

    (readCoordinator ? ReadCoordinator.ExecuteStatement(statement))
      .flatMap {
        case e: SelectStatementExecuted[RecordOut] => Future.successful(e.values)
        case e: SelectStatementFailed              => Future.successful(Seq.empty)
      }
  }

  def executeSqlInsertStatement(statement: InsertSQLStatement): Future[InputMapped] = {
    val (metric, ts, dimensions, fields) = InsertSQLStatement.unapply(statement).get

    val timestamp = ts getOrElse System.currentTimeMillis
    (writeCoordinator ? MapInput(timestamp, metric, Record(timestamp, dimensions.fields, fields.fields)))
      .mapTo[InputMapped]
  }

}
