package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ddata.{DistributedData, LWWMap, LWWMapKey}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.index.Location

import scala.concurrent.duration._

case class LocationKey(namespace: String, metric: String, from: Long, to: Long)
case class MetricKey(namespace: String, metric: String)

object ReplicatedMetadataCache {
  private final case class Request(key: LocationKey, replyTo: ActorRef)

  final case class PutInCache(key: LocationKey, value: Location)
  final case class GetFromCache(key: LocationKey)
  final case class Cached(key: LocationKey, value: Option[Location])
  final case class Evict(key: LocationKey)
}

class ReplicatedMetadataCache extends Actor with ActorLogging {

  import ReplicatedMetadataCache._
  import akka.cluster.ddata.Replicator._

  implicit val cluster: Cluster = Cluster(context.system)

  val replicator: ActorRef = DistributedData(context.system).replicator

  def locationDataKey(entryKey: LocationKey): LWWMapKey[LocationKey, Location] =
    LWWMapKey("location-cache-" + math.abs(entryKey.hashCode) % 100)

  def metricDataKey(entryKey: LocationKey): LWWMapKey[LocationKey, Location] =
    LWWMapKey("metric-cache-" + math.abs(entryKey.hashCode) % 100)

  val writeDuration = 5 seconds

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  def receive: Receive = {
    case PutInCache(key, value) =>
      (replicator ? Update(locationDataKey(key), LWWMap(), WriteMajority(writeDuration))(_ + (key -> value)))
        .map {
          case UpdateSuccess(_, _) => Cached(key, Some(value))
          case _                   => Cached(key, None)
        }
        .pipeTo(sender())
    case Evict(key) =>
      (replicator ? Update(locationDataKey(key), LWWMap(), WriteMajority(writeDuration))(_ - key))
        .map(_ => Cached(key, None))
        .pipeTo(sender())
    case GetFromCache(key) =>
      log.debug("searching for key {} in cache", key)
      replicator ! Get(locationDataKey(key), ReadMajority(writeDuration), Some(Request(key, sender())))
    case g @ GetSuccess(LWWMapKey(_), Some(Request(key, replyTo))) =>
      g.dataValue match {
        case data: LWWMap[_, _] =>
          data.asInstanceOf[LWWMap[LocationKey, Location]].get(key) match {
            case Some(value) => replyTo ! Cached(key, Some(value))
            case None        => replyTo ! Cached(key, None)
          }
      }
    case NotFound(_, Some(Request(key, replyTo))) =>
      replyTo ! Cached(key, None)
    case msg: UpdateResponse[_] =>
      log.debug("received not handled update message {}", msg)
  }
}
