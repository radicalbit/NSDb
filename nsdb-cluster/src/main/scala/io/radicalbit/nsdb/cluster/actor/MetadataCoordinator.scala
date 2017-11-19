package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands.{AddLocation, GetLastLocation}
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.events.{AddLocationFailed, LocationAdded, LocationGot}
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.index.Location

class MetadataCoordinator(cache: ActorRef) extends Actor with ActorLogging {

  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  override def receive: Receive = {
    case GetLastLocation(namespace, metric, _) =>
      (cache ? GetLocationsFromCache(MetricKey(namespace, metric))).map {
        case CachedLocations(_, values) if values.nonEmpty => LocationGot(namespace, metric, Some(values.last))
        case CachedLocations(_, _)                         => LocationGot(namespace, metric, None)
      }

    case msg @ AddLocation(namespace, location, occurredOn) =>
      (cache ? PutInCache(LocationKey(namespace, location.metric, location.from, location.to), location))
        .map {
          case Cached(_, Some(_)) =>
            mediator ! Publish("metadata", msg)
            LocationAdded(namespace, location, occurredOn)
          case _ => AddLocationFailed(namespace, location, occurredOn)
        }
        .pipeTo(sender)

  }
}

object MetadataCoordinator {

  object commands {

    case class GetLocations(namespace: String, metric: String, occurredOn: Long = System.currentTimeMillis)
    case class GetLastLocation(namespace: String, metric: String, occurredOn: Long = System.currentTimeMillis)
    case class AddLocation(namespace: String, location: Location, occurredOn: Long = System.currentTimeMillis)
    case class AddLocations(namespace: String, locations: Seq[Location], occurredOn: Long = System.currentTimeMillis)
    case class DeleteLocation(namespace: String, location: Location, occurredOn: Long = System.currentTimeMillis)
    case class DeleteNamespace(namespace: String, occurredOn: Long = System.currentTimeMillis)

  }

  object events {

    case class LocationsGot(namespace: String, metric: String, locations: Seq[Location], occurredOn: Long)
    case class LocationGot(namespace: String, metric: String, location: Option[Location])
    case class LocationUpdated(namespace: String, oldLocation: Location, newOccupation: Long, occurredOn: Long)
    case class UpdateLocationFailed(namespace: String, oldLocation: Location, newOccupation: Long, occurredOn: Long)
    case class LocationAdded(namespace: String, location: Location, occurredOn: Long)
    case class AddLocationFailed(namespace: String, location: Location, occurredOn: Long)
    case class LocationsAdded(namespace: String, locations: Seq[Location], occurredOn: Long)
    case class LocationDeleted(namespace: String, location: Location, occurredOn: Long)
    case class NamespaceDeleted(namespace: String, occurredOn: Long)

  }

  def props(cache: ActorRef): Props = Props(new MetadataCoordinator(cache))
}
