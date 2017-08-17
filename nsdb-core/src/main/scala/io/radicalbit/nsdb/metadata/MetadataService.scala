package io.radicalbit.nsdb.metadata

import akka.actor.{Actor, Props}

object MetadataService {

  def props = Props(new MetadataService)

}

class MetadataService extends Actor {

  def receive = {
    case _ =>
  }

}
