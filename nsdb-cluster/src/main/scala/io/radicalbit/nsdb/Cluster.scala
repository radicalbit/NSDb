package io.radicalbit.nsdb

import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.core.{Core, CoreActors}

object Cluster extends App with Core with CoreActors {

  override implicit lazy val system = ActorSystem("NsdbSystem", ConfigFactory.load("cluster"))

  import scala.concurrent.duration._

  implicit val timeout    = Timeout(10 second)
  implicit val dispatcher = system.dispatcher

  var counter: Int = 0

  guardian

}
