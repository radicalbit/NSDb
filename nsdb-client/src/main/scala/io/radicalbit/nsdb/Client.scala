package io.radicalbit.nsdb

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.radicalbit.actors.DatabaseActorsGuardian
import io.radicalbit.coordinator.WriteCoordinator
import io.radicalbit.nsdb.core.{Core, CoreActors}
import io.radicalbit.nsdb.model.Record

// this class currently is used only for test purposes
object Client extends App with Core {

  import scala.concurrent.duration._

  override implicit lazy val system = ActorSystem("nsdb-client")

  implicit val timeout    = Timeout(10 second)
  implicit val dispatcher = system.dispatcher

  var counter: Int = 0

//  val clientActor = system.actorOf(Props[ClientActor], "clientActor")

//  clientActor ! "GetWriteCoordinator"

//
  val guardian = system.actorSelection("akka.tcp://NsdbSystem@127.0.0.1:2552/user/guardian")

  (guardian ? DatabaseActorsGuardian.GetWriteCoordinator).mapTo[ActorRef].map { writeCoordinator =>
    println(s"Write Coordinator $writeCoordinator")
//    while (true) {
//      val res = x ? WriteCoordinator.MapInput(
//        ts = System.currentTimeMillis,
//        metric = "test",
//        record = Record(System.currentTimeMillis, Map("dim" + counter -> ("val" + counter)), Map.empty))
//      counter += 1
//      Thread.sleep(500)
//    }
  } recover {
    case t => sys.error(t.getStackTrace.mkString)
  }

}
