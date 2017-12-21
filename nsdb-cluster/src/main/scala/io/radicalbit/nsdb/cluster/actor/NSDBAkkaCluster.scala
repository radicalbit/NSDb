package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetReadCoordinator, GetWriteCoordinator}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

trait NSDBAkkaCluster {
  implicit val system: ActorSystem = ActorSystem("nsdb", ConfigFactory.load("cluster"))
}

trait NSDBAActors { this: NSDBAkkaCluster =>

  implicit val timeout =
    Timeout(system.settings.config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val executionContext = system.dispatcher

  val metadataCache = system.actorOf(Props[ReplicatedMetadataCache], "metadata-cache")

  val metadataCoordinator = system.actorOf(MetadataCoordinator.props(metadataCache), name = "metadata-coordinator")

  lazy val guardian = system.actorOf(DatabaseActorsGuardian.props, "guardian")

  system.actorOf(Props[ClusterListener], name = "clusterListener")

  for {
    readCoordinator  <- (guardian ? GetReadCoordinator).mapTo[ActorRef]
    writeCoordinator <- (guardian ? GetWriteCoordinator).mapTo[ActorRef]
    _ = new GrpcEndpoint(readCoordinator = readCoordinator, writeCoordinator = writeCoordinator)
  } ()
}

trait ProductionCluster extends NSDBAkkaCluster with NSDBAActors
