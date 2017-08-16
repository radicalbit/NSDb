package io.radicalbit

import akka.actor.ActorRef
import akka.util.Timeout
import io.radicalbit.actors.DatabaseActorsGuardian
import io.radicalbit.coordinator.WriteCoordinator
import io.radicalbit.nsdb.core.{BootedCore, CoreActors}
import io.radicalbit.nsdb.model.Record

// this class currently is used only for test purposes
object Main extends App with BootedCore with CoreActors {

  import akka.pattern.ask
  import scala.concurrent.duration._

  implicit val timeout    = Timeout(10 second)
  implicit val dispatcher = system.dispatcher

  var counter: Int = 0

  (guardian ? DatabaseActorsGuardian.GetWriteCoordinator).mapTo[ActorRef].map { x =>
    while (true) {
      val res = x ? WriteCoordinator.MapInput(
        ts = System.currentTimeMillis,
        metric = "test",
        record = Record(System.currentTimeMillis, Map("dim" + counter -> ("val" + counter)), Map.empty))
      counter += 1
      Thread.sleep(500)
    }
  } recover {
    case t => sys.error(t.getStackTrace.mkString)
  }

}
