package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import io.radicalbit.nsdb.actors.DatabaseActorsGuardian
import io.radicalbit.nsdb.cluster.endpoint.{EndpointActor, GrpcEndpoint}
import io.radicalbit.nsdb.core.{Core, CoreActors}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

trait NSDBAkkaCluster extends Core {

  override val system: ActorSystem = ActorSystem("nsdb", ConfigFactory.load("cluster"))
}

trait NSDBAActors extends CoreActors { this: Core =>

  implicit val timeout =
    Timeout(system.settings.config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val executionContext = system.dispatcher

  val metadataCache = system.actorOf(Props[ReplicatedMetadataCache], "metadata-cache")

  system.actorOf(MetadataCoordinator.props(metadataCache), name = "metadata-coordinator")

  system.actorOf(Props[ClusterListener], name = "clusterListener")

  for {
    readCoordinator  <- (guardian ? DatabaseActorsGuardian.GetReadCoordinator).mapTo[ActorRef]
    writeCoordinator <- (guardian ? DatabaseActorsGuardian.GetWriteCoordinator).mapTo[ActorRef]
    _ = system.actorOf(EndpointActor.props(readCoordinator = readCoordinator, writeCoordinator = writeCoordinator),
                       "endpoint-actor")
    _ = new GrpcEndpoint(readCoordinator = readCoordinator, writeCoordinator = writeCoordinator)
  } ()
}

trait ProductionCluster extends NSDBAkkaCluster with NSDBAActors
