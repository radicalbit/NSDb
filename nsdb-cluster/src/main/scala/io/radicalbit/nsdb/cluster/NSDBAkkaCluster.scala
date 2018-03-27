package io.radicalbit.nsdb.cluster

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import io.radicalbit.nsdb.cluster.actor.DatabaseActorsGuardian
import io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetReadCoordinator, GetWriteCoordinator}

/**
  * Creates the [[ActorSystem]] based on a configuration provided by the concrete implementation
  */
trait NSDBAkkaCluster {

  def config: Config

  implicit lazy val system: ActorSystem = ActorSystem("nsdb", config)
}

/**
  * Creates the top level actor [[DatabaseActorsGuardian]] and grpc endpoint [[GrpcEndpoint]] based on coordinators
  */
trait NSDBAActors { this: NSDBAkkaCluster =>

  implicit val timeout =
    Timeout(config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val executionContext = system.dispatcher

  lazy val guardian = system.actorOf(Props[DatabaseActorsGuardian], "guardian")

  for {
    readCoordinator  <- (guardian ? GetReadCoordinator).mapTo[ActorRef]
    writeCoordinator <- (guardian ? GetWriteCoordinator).mapTo[ActorRef]
    _ = new GrpcEndpoint(readCoordinator = readCoordinator, writeCoordinator = writeCoordinator)
  } ()
}

/**
  * Simply mix in [[NSDBAkkaCluster]] with [[NSDBAActors]]
  */
trait ProductionCluster extends NSDBAkkaCluster with NSDBAActors
