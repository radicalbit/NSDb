package io.radicalbit.nsdb.cluster.coordinator

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.pattern._
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.index.Location

import scala.concurrent.Future
import scala.util.Random

/**
  * Actor that handles metadata (i.e. write location for metrics)
  * @param cache cluster aware metric's location cache
  */
class MetadataCoordinator(cache: ActorRef) extends Actor with ActorLogging {

  /**
    * mediator for [[DistributedPubSub]] system
    */
  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  val cluster = Cluster(context.system)

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.metadata-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  lazy val sharding: Boolean          = context.system.settings.config.getBoolean("nsdb.sharding.enabled")
  lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

  override def receive: Receive = if (sharding) shardBehaviour else noShardBehaviour

  /**
    * behaviour in case shard is false
    */
  def noShardBehaviour: Receive = {
    case GetLocations(db, namespace, metric) =>
      val nodeName =
        s"${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}"
      sender ! LocationsGot(db, namespace, metric, Seq(Location(metric, nodeName, 0, 0)))
    case GetWriteLocation(db, namespace, metric, timestamp) =>
      val nodeName =
        s"${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}"
      sender ! LocationGot(db, namespace, metric, Some(Location(metric, nodeName, 0, 0)))
  }

  /**
    * behaviour in case shard is true
    */
  def shardBehaviour: Receive = {
    case GetLocations(db, namespace, metric) =>
      val f = (cache ? GetLocationsFromCache(MetricKey(db, namespace, metric)))
        .mapTo[CachedLocations]
        .map(l => LocationsGot(db, namespace, metric, l.value))
      f.pipeTo(sender())
    case GetWriteLocation(db, namespace, metric, timestamp) =>
      (cache ? GetLocationsFromCache(MetricKey(db, namespace, metric)))
        .flatMap {
          case CachedLocations(_, values) if values.nonEmpty =>
            values.find(v => v.from <= timestamp && v.to >= timestamp) match {
              case Some(loc) => Future(LocationGot(db, namespace, metric, Some(loc)))
              case None =>
                val nodeName =
                  s"${cluster.selfAddress.host.getOrElse("noHost")}_${cluster.selfAddress.port.getOrElse(2552)}"
                (self ? AddLocation(db,
                                    namespace,
                                    Location(metric, nodeName, timestamp, timestamp + shardingInterval.toMillis)))
                  .map {
                    case LocationAdded(_, _, location) => LocationGot(db, namespace, metric, Some(location))
                    case AddLocationFailed(_, _, _)    => LocationGot(db, namespace, metric, None)
                  }
            }

          case CachedLocations(_, _) =>
            val randomNode = Random.shuffle(cluster.state.members).head
            val nodeName =
              s"${randomNode.address.host.getOrElse("noHost")}_${randomNode.address.port.getOrElse(2552)}"
            (self ? AddLocation(db, namespace, Location(metric, nodeName, 0, shardingInterval.toMillis))).map {
              case LocationAdded(_, _, location) => LocationGot(db, namespace, metric, Some(location))
              case AddLocationFailed(_, _, _)    => LocationGot(db, namespace, metric, None)
            }
        }
        .pipeTo(sender())

    case msg @ AddLocation(db, namespace, location) =>
      (cache ? PutInCache(LocationKey(db, namespace, location.metric, location.from, location.to), location))
        .map {
          case Cached(_, Some(_)) =>
            mediator ! Publish("metadata", msg)
            LocationAdded(db, namespace, location)
          case _ => AddLocationFailed(db, namespace, location)
        }
        .pipeTo(sender)
  }
}

object MetadataCoordinator {

  object commands {

    case class GetLocations(db: String, namespace: String, metric: String)
    case class GetWriteLocation(db: String, namespace: String, metric: String, timestamp: Long)
    case class AddLocation(db: String, namespace: String, location: Location)
    case class AddLocations(db: String, namespace: String, locations: Seq[Location])
    case class DeleteLocation(db: String, namespace: String, location: Location)
    case class DeleteNamespace(db: String, namespace: String, occurredOn: Long = System.currentTimeMillis)
  }

  object events {

    case class LocationsGot(db: String, namespace: String, metric: String, locations: Seq[Location])
    case class LocationGot(db: String, namespace: String, metric: String, location: Option[Location])
    case class UpdateLocationFailed(db: String, namespace: String, oldLocation: Location, newOccupation: Long)
    case class LocationAdded(db: String, namespace: String, location: Location)
    case class AddLocationFailed(db: String, namespace: String, location: Location)
    case class LocationsAdded(db: String, namespace: String, locations: Seq[Location])
    case class LocationDeleted(db: String, namespace: String, location: Location)
    case class NamespaceDeleted(db: String, namespace: String, occurredOn: Long)
  }

  def props(cache: ActorRef): Props = Props(new MetadataCoordinator(cache))
}
