package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorLogging}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Send}
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands.{AddLocation, AddLocations, UpdateLocation}
import io.radicalbit.nsdb.cluster.index.Location

import scala.collection.mutable

class MetadataCoordinator extends Actor with ActorLogging {

  private val metadatas: mutable.Map[String, Seq[Location]] = mutable.Map.empty

  val mediator = DistributedPubSub(context.system).mediator

  override def receive: Receive = {
    case msg @ AddLocation(namespace, location) =>
      mediator ! Publish("metadata", msg)
    case msg @ AddLocations(namespace, locations) =>
      mediator ! Publish("metadata", msg)
    case msg @ UpdateLocation(_, _, _) =>
      mediator ! Publish("metadata", msg)
  }
}

object MetadataCoordinator {

  object commands {

    case class GetLocations(namespace: String, metric: String)
    case class GetLocation(namespace: String, metric: String, timestamp: Long)
    case class UpdateLocation(namespace: String, oldLocation: Location, newLocation: Location)
    case class AddLocation(namespace: String, location: Location)
    case class AddLocations(namespace: String, locations: Seq[Location])
    case class DeleteLocation(namespace: String, location: Location)
    case class DeleteNamespace(namespace: String)

  }

  object events {

    case class LocationsGot(namespace: String, metric: String, locations: Seq[Location])
    case class LocationGot(namespace: String, metric: String, timestamp: Long, location: Option[Location])
    case class LocationUpdated(namespace: String, oldLocation: Location, newLocation: Location)
    case class LocationAdded(namespace: String, location: Location)
    case class LocationsAdded(namespace: String, locations: Seq[Location])
    case class LocationDeleted(namespace: String, location: Location)
    case class NamespaceDeleted(namespace: String)

  }
}
