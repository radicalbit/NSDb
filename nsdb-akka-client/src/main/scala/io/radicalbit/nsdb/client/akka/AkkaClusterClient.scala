package io.radicalbit.nsdb.client.akka

import akka.actor.{ActorPath, ActorSystem}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.common.statement.{CommandStatement, SQLStatement}

import scala.concurrent.Future
import scala.reflect.ClassTag

class AkkaClusterClient(host: String, port: Int)(implicit system: ActorSystem) {
  import scala.concurrent.duration._

  private val EndpointActorPath = "/user/endpoint-actor"

  implicit val timeout = Timeout(10 second)

  implicit val dispatcher = system.dispatcher

  val initialContacts = Set(ActorPath.fromString(s"akka.tcp://nsdb@$host:$port/system/receptionist"))

  val settings = ClusterClientSettings(system).withInitialContacts(initialContacts)

  val clusterClient = system.actorOf(ClusterClient.props(settings))

  private def executeCommand[IN, OUT](command: IN)(implicit tag: ClassTag[OUT]): Future[OUT] =
    (clusterClient ? ClusterClient.Send(EndpointActorPath, command, true)).mapTo[OUT]

  def executeSqlStatement(statement: SQLStatement): Future[SQLStatementExecuted] =
    executeCommand[ExecuteSQLStatement, SQLStatementExecuted](ExecuteSQLStatement(statement))

  def executeCommandStatement(statement: CommandStatement): Future[CommandStatementExecuted] =
    executeCommand[ExecuteCommandStatement, CommandStatementExecuted](ExecuteCommandStatement(statement))
}
