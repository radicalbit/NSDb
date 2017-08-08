package io.radicalbit.nsdb

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import io.radicalbit.actors.DatabaseActorsGuardian
import io.radicalbit.coordinator.WriteCoordinator
import io.radicalbit.nsdb.core.{Core, CoreActors}
import io.radicalbit.nsdb.model.Record

// this class currently is used only for test purposes
object Cluster extends App with Core with CoreActors {

  override implicit lazy val system = ActorSystem("NsdbSystem")

  import akka.pattern.ask

  import scala.concurrent.duration._

  implicit val timeout    = Timeout(10 second)
  implicit val dispatcher = system.dispatcher

  var counter: Int = 0

  (guardian ? DatabaseActorsGuardian.GetWriteCoordinator).mapTo[ActorRef].map { x =>
//    println("WRITE COORDINATOR ")
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
