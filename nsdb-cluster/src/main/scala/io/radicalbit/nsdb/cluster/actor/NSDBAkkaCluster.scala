package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetReadCoordinator, GetWriteCoordinator}

trait NSDBAkkaCluster {

  def config: Config

  implicit lazy val system: ActorSystem = ActorSystem("nsdb", config)
}

trait NSDBAActors { this: NSDBAkkaCluster =>

  implicit val timeout =
    Timeout(config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val executionContext = system.dispatcher

  lazy val guardian = system.actorOf(DatabaseActorsGuardian.props, "guardian")

  for {
    readCoordinator  <- (guardian ? GetReadCoordinator).mapTo[ActorRef]
    writeCoordinator <- (guardian ? GetWriteCoordinator).mapTo[ActorRef]
    _ = new GrpcEndpoint(readCoordinator = readCoordinator, writeCoordinator = writeCoordinator)
  } ()
}

trait ProductionCluster extends NSDBAkkaCluster with NSDBAActors
