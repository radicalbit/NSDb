package io.radicalbit.nsdb.minicluster

import akka.actor.ActorSystem
import io.radicalbit.nsdb.cluster.NSDBActors
import io.radicalbit.nsdb.common.NsdbConfig

import scala.concurrent.duration._
import scala.concurrent.Await

trait NSDBAkkaMiniCluster { this: NsdbConfig with NSDBActors =>

  implicit var system: ActorSystem = _

  def start(): Unit = {
    system = ActorSystem("nsdb", config)
    initTopLevelActors()
  }

  def stop(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    Await.result(system.whenTerminated, 10.seconds)
  }
}
