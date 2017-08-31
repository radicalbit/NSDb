package io.radicalbit.nsdb.client.akka

import akka.actor.{ActorPath, ActorSystem}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.{ExecuteSQLStatement, SQLStatementExecuted}
import io.radicalbit.nsdb.common.statement.SQLStatement

import scala.concurrent.Future

class AkkaClusterClient(host: String = "127.0.0.1", port: Int = 2552)(implicit system: ActorSystem) {
  import scala.concurrent.duration._

  private val EndpointActorPath = "/user/endpoint-actor"

  implicit val timeout = Timeout(10 second)

  implicit val dispatcher = system.dispatcher

  val initialContacts = Set(ActorPath.fromString(s"akka.tcp://nsdb@$host:$port/system/receptionist"))

  val settings = ClusterClientSettings(system).withInitialContacts(initialContacts)

  val clusterClient = system.actorOf(ClusterClient.props(settings))

  def executeSqlStatement(statement: SQLStatement): Future[SQLStatementExecuted] =
    (clusterClient ? ClusterClient.Send(EndpointActorPath, ExecuteSQLStatement(statement), true))
      .mapTo[SQLStatementExecuted]
}
