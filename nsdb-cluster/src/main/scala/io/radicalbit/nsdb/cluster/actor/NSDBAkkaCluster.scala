package io.radicalbit.nsdb.cluster.actor

import akka.actor.{ActorRef, ActorSystem}
import io.radicalbit.nsdb.actors.DatabaseActorsGuardian
import io.radicalbit.nsdb.cluster.endpoint.EndpointActor
import io.radicalbit.nsdb.core.{Core, CoreActors}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

trait NSDBAkkaCluster extends Core {

  override val system: ActorSystem = ActorSystem("nsdb", ConfigFactory.load("cluster"))
}

trait NSDBAActors extends CoreActors { this: Core =>

  implicit val executionContext = system.dispatcher
  implicit val timeout: Timeout = 1 second

  (for {
    readCoordinator  <- (guardian ? DatabaseActorsGuardian.GetReadCoordinator).mapTo[ActorRef]
    writeCoordinator <- (guardian ? DatabaseActorsGuardian.GetWriteCoordinator).mapTo[ActorRef]
  } yield
    system.actorOf(EndpointActor.props(readCoordinator = readCoordinator, writeCoordinator = writeCoordinator),
                   "endpoint-actor")).recover {
    case t =>
      system.log.error("Cannot start the cluster successfully", t)
      System.exit(0)
  }
}

trait ProductionCluster extends NSDBAkkaCluster with NSDBAActors
