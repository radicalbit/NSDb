package io.radicalbit.nsdb.minicluster

import akka.actor.ActorSystem
import io.radicalbit.nsdb.cluster.NSDbActors
import io.radicalbit.nsdb.common.configuration.NsdbConfigProvider

import scala.concurrent.duration._
import scala.concurrent.Await

trait NSDBAkkaMiniCluster { this: NsdbConfigProvider with NSDbActors =>

  implicit var system: ActorSystem = _

  def start(): Unit = {
    system = ActorSystem("NSDb", config)
    initTopLevelActors()
  }

  def stop(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    Await.result(system.whenTerminated, 10.seconds)
  }
}
