package io.radicalbit.nsdb.cluster.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ddata._
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.index.Location

import scala.concurrent.duration._

case class LocationKey(db: String, namespace: String, metric: String, from: Long, to: Long)
case class MetricKey(db: String, namespace: String, metric: String)

object ReplicatedMetadataCache {
  private final case class Request(key: LocationKey, replyTo: ActorRef)
  private final case class MetricRequest(key: MetricKey, replyTo: ActorRef)

  final case class PutInCache(key: LocationKey, value: Location)
  final case class GetFromCache(key: LocationKey)
  final case class GetLocationsFromCache(key: MetricKey)
  final case class Cached(key: LocationKey, value: Option[Location])
  final case class CachedLocations(key: MetricKey, value: Seq[Location])
  final case class Evict(key: LocationKey)
}

class ReplicatedMetadataCache extends Actor with ActorLogging {

  import ReplicatedMetadataCache._
  import akka.cluster.ddata.Replicator._

  implicit val cluster: Cluster = Cluster(context.system)

  val replicator: ActorRef = DistributedData(context.system).replicator

  def locationDataKey(entryKey: LocationKey): LWWMapKey[LocationKey, Location] =
    LWWMapKey("location-cache-" + math.abs(entryKey.hashCode) % 100)

  def metricDataKey(entryKey: MetricKey): LWWMapKey[MetricKey, Seq[Location]] =
    LWWMapKey("metric-cache-" + math.abs(entryKey.hashCode) % 100)

  val writeDuration = 5 seconds

  implicit val timeout: Timeout = Timeout(
    context.system.settings.config.getDuration("nsdb.write-coordinator.timeout", TimeUnit.SECONDS),
    TimeUnit.SECONDS)
  import context.dispatcher

  def receive: Receive = {
    case PutInCache(key, value) =>
      val f = for {
        loc <- (replicator ? Update(locationDataKey(key), LWWMap(), WriteMajority(writeDuration))(_ + (key -> value)))
          .map {
            case UpdateSuccess(_, _) => {
              Cached(key, Some(value))
            }
            case _ => Cached(key, None)
          }
        metricKey = MetricKey(key.db, key.namespace, key.metric)
        getMetric <- (replicator ? Get(metricDataKey(metricKey), ReadMajority(writeDuration))).map {
          case g @ GetSuccess(LWWMapKey(_), _) =>
            CachedLocations(
              metricKey,
              g.dataValue
                .asInstanceOf[LWWMap[MetricKey, Seq[Location]]]
                .get(metricKey)
                .getOrElse(Seq.empty)
            )
          case g => {
            CachedLocations(metricKey, Seq.empty)
          }
        }
        metr <- {
          val locMap  = getMetric.value.map(e => (e.from, e.to) -> e).toMap
          val newLocs = (locMap + ((value.from, value.to) -> value)).values.map(identity).toSeq
          (replicator ? Update(metricDataKey(metricKey), LWWMap(), WriteMajority(writeDuration))(
            _ + (metricKey -> newLocs)))
            .map {
              case UpdateSuccess(_, _) => {
                CachedLocations(metricKey, newLocs)
              }
              case _ => Cached(key, None)
            }
        }
      } yield loc
      f.pipeTo(sender())
    case Evict(key) =>
      val f = for {
        loc <- (replicator ? Update(locationDataKey(key), LWWMap(), WriteMajority(writeDuration))(_ - key))
          .map(_ => Cached(key, None))
        metricKey = MetricKey(key.db, key.namespace, key.metric)
        getMetric <- (replicator ? Get(metricDataKey(metricKey), ReadMajority(writeDuration))).map {
          case g @ GetSuccess(LWWMapKey(_), _) =>
            CachedLocations(
              metricKey,
              g.dataValue
                .asInstanceOf[LWWMap[MetricKey, Seq[Location]]]
                .get(metricKey)
                .getOrElse(Seq.empty)
            )
          case g => {
            CachedLocations(metricKey, Seq.empty)
          }
        }
        metr <- {
          val locMap  = getMetric.value.map(e => (e.from, e.to) -> e).toMap
          val newLocs = (locMap - ((key.from, key.to))).values.map(identity).toSeq
          (replicator ? Update(metricDataKey(metricKey), LWWMap(), WriteMajority(writeDuration))(
            _ + (metricKey -> newLocs)))
            .map {
              case UpdateSuccess(_, _) => {
                CachedLocations(metricKey, newLocs)
              }
              case _ => Cached(key, None)
            }
        }
      } yield loc
      f.pipeTo(sender)
    case GetFromCache(key) =>
      log.debug("searching for key {} in cache", key)
      replicator ! Get(locationDataKey(key), ReadMajority(writeDuration), Some(Request(key, sender())))
    case GetLocationsFromCache(key) =>
      log.debug("searching for key {} in cache", key)
      replicator ! Get(metricDataKey(key), ReadMajority(writeDuration), Some(MetricRequest(key, sender())))
    case g @ GetSuccess(LWWMapKey(_), Some(Request(key, replyTo))) =>
      g.dataValue.asInstanceOf[LWWMap[LocationKey, Location]].get(key) match {
        case Some(value) => replyTo ! Cached(key, Some(value))
        case None        => replyTo ! Cached(key, None)
      }
    case g @ GetSuccess(LWWMapKey(_), Some(MetricRequest(key, replyTo))) =>
      g.dataValue
        .asInstanceOf[LWWMap[MetricKey, Seq[Location]]]
        .get(MetricKey(key.db, key.namespace, key.metric)) match {
        case Some(value) => replyTo ! CachedLocations(MetricKey(key.db, key.namespace, key.metric), value)
        case None        => replyTo ! CachedLocations(MetricKey(key.db, key.namespace, key.metric), Seq.empty)
      }
    case NotFound(_, Some(Request(key, replyTo))) =>
      replyTo ! Cached(key, None)
    case msg: UpdateResponse[_] =>
      log.debug("received not handled update message {}", msg)
  }
}
