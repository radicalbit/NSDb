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
    case GetLastLocation(db, namespace, metric, _) =>
      (cache ? GetLocationsFromCache(MetricKey(db, namespace, metric))).map {
        case CachedLocations(_, values) if values.nonEmpty => LocationGot(db, namespace, metric, Some(values.last))
        case CachedLocations(_, _)                         => LocationGot(db, namespace, metric, None)
      }

    case msg @ AddLocation(db, namespace, location, occurredOn) =>
      (cache ? PutInCache(LocationKey(db, namespace, location.metric, location.from, location.to), location))
        .map {
          case Cached(_, Some(_)) =>
            mediator ! Publish("metadata", msg)
            LocationAdded(db, namespace, location, occurredOn)
          case _ => AddLocationFailed(db, namespace, location, occurredOn)
        }
        .pipeTo(sender)
  }
}

object MetadataCoordinator {

  object commands {

    case class GetLocations(db: String, namespace: String, metric: String, occurredOn: Long = System.currentTimeMillis)
    case class GetLastLocation(db: String,
                               namespace: String,
                               metric: String,
                               occurredOn: Long = System.currentTimeMillis)
    case class UpdateLocation(db: String,
                              namespace: String,
                              oldLocation: Location,
                              newOccupation: Long,
                              occurredOn: Long = System.currentTimeMillis)
    case class AddLocation(db: String,
                           namespace: String,
                           location: Location,
                           occurredOn: Long = System.currentTimeMillis)
    case class AddLocations(db: String,
                            namespace: String,
                            locations: Seq[Location],
                            occurredOn: Long = System.currentTimeMillis)
    case class DeleteLocation(db: String,
                              namespace: String,
                              location: Location,
                              occurredOn: Long = System.currentTimeMillis)
    case class DeleteNamespace(db: String, namespace: String, occurredOn: Long = System.currentTimeMillis)
  }

  object events {

    case class LocationsGot(db: String, namespace: String, metric: String, locations: Seq[Location], occurredOn: Long)
    case class LocationGot(db: String,
                           namespace: String,
                           metric: String,
//                           timestamp: Long,
                           location: Option[Location])
//                           occurredOn: Long)
    case class LocationUpdated(db: String,
                               namespace: String,
                               oldLocation: Location,
                               newOccupation: Long,
                               occurredOn: Long)
    case class UpdateLocationFailed(db: String,
                                    namespace: String,
                                    oldLocation: Location,
                                    newOccupation: Long,
                                    occurredOn: Long)
    case class LocationAdded(db: String, namespace: String, location: Location)
    case class AddLocationFailed(db: String, namespace: String, location: Location)
    case class LocationsAdded(db: String, namespace: String, locations: Seq[Location])
    case class LocationDeleted(db: String, namespace: String, location: Location)
    case class NamespaceDeleted(db: String, namespace: String, occurredOn: Long)
  }

  def props(cache: ActorRef): Props = Props(new MetadataCoordinator(cache))
}
