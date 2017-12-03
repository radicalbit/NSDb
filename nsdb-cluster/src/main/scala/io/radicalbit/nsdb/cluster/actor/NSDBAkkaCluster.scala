package io.radicalbit.nsdb.cluster.actor

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.cluster.endpoint.GrpcEndpoint
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{GetReadCoordinator, GetWriteCoordinator}

trait NSDBAkkaCluster {
  private val config = ConfigFactory
    .parseFile(Paths.get(System.getProperty("confDir"), "cluster.conf").toFile)
    .resolve()
    .withFallback(ConfigFactory.load("cluster"))

  implicit val system: ActorSystem = ActorSystem("nsdb", config)
}

trait NSDBAActors { this: NSDBAkkaCluster =>

  implicit val timeout =
    Timeout(system.settings.config.getDuration("nsdb.global.timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  implicit val executionContext = system.dispatcher

  val guardian = system.actorOf(DatabaseActorsGuardian.props, "guardian")

  for {
    readCoordinator  <- (guardian ? GetReadCoordinator).mapTo[ActorRef]
    writeCoordinator <- (guardian ? GetWriteCoordinator).mapTo[ActorRef]
    _ = new GrpcEndpoint(readCoordinator = readCoordinator, writeCoordinator = writeCoordinator)
  } ()
}

trait ProductionCluster extends NSDBAkkaCluster with NSDBAActors
