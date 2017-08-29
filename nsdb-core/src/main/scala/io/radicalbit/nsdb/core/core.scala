package io.radicalbit.nsdb.core

import akka.actor._
import akka.util.Timeout
import io.radicalbit.nsdb.actors.DatabaseActorsGuardian
import scala.concurrent.duration._

trait Core {
  protected implicit def system: ActorSystem
  implicit val timeout: Timeout = Timeout(1 second)

}

trait CoreActors { this: Core =>
  // define actors here
  lazy val guardian = system.actorOf(DatabaseActorsGuardian.props, "guardian")
}
