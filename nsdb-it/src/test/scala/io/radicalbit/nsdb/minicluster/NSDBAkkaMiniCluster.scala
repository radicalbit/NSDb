package io.radicalbit.nsdb.minicluster

import akka.actor.ActorSystem
import io.radicalbit.nsdb.cluster.NSDBAActors
import io.radicalbit.nsdb.common.NsdbConfig

import scala.concurrent.duration._
import scala.concurrent.Await

trait NSDBAkkaMiniCluster { this: NsdbConfig with NSDBAActors =>

  implicit var system: ActorSystem = ActorSystem("nsdb", config)

  def start(): Unit = initTopLevelActors()

  def stop(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    Await.result(system.whenTerminated, 10.seconds)
  }
}
