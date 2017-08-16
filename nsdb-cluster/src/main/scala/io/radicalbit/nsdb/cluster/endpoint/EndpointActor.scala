package io.radicalbit.nsdb.cluster.endpoint

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.client.ClusterClientReceptionist

object EndpointActor {

  def props = Props(new EndpointActor)

}

class EndpointActor extends Actor with ActorLogging {

  ClusterClientReceptionist(context.system).registerService(self)
  println(s"################# ===========>>>>>>>>>> RECEPTIONIST REGISTERED")

  def receive = {
    case msg => println(s"################# ===========>>>>>>>>>> $msg")
  }

}
