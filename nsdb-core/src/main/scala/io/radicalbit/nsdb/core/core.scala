package io.radicalbit.nsdb.core

import akka.actor._
import akka.util.Timeout
import io.radicalbit.nsdb.actors.DatabaseActorsGuardian

trait Core {
  protected implicit def system: ActorSystem
}

trait CoreActors { this: Core =>
  // define actors here
  lazy val guardian = system.actorOf(DatabaseActorsGuardian.props, "guardian")
}
