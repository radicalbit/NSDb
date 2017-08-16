package io.radicalbit.nsdb.api

import akka.actor.{ActorPath, ActorSystem}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}

object MainClient extends App {

  val system = ActorSystem("client-test")

  val initialContacts = Set(ActorPath.fromString("akka.tcp://nsdb@127.0.0.1:2552/system/receptionist"))
  val settings        = ClusterClientSettings(system).withInitialContacts(initialContacts)

  val clusterClient = system.actorOf(ClusterClient.props(settings))
  clusterClient ! ClusterClient.Send("/user/endpoint-actor", "hello", true)

}
