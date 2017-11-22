package io.radicalbit.nsdb.cluster.coordinator

import java.time.Duration
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.util.Timeout
import io.radicalbit.commit_log.CommitLogService.{Delete, Insert}
import io.radicalbit.nsdb.actors.PublisherActor.Events.RecordsPublished
import io.radicalbit.nsdb.cluster.actor.{LocationKey, MetricKey}
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands.{AddLocation, GetWriteLocation, UpdateLocation}
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.events.{AddLocationFailed, LocationAdded, LocationGot}
import io.radicalbit.nsdb.cluster.actor.ReplicatedMetadataCache.{
  Cached,
  CachedLocations,
  GetLocationsFromCache,
  PutInCache
}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementExecuted

import scala.concurrent.Future

object Facilities {

  class TestCommitLogService extends Actor {
    def receive = {
      case Insert(ts, metric, record) =>
        sender() ! WroteToCommitLogAck(ts, metric, record)
      case Delete(_, _) => sys.error("Not Implemented")
    }
  }

  class TestSubscriber extends Actor {
    var receivedMessages = 0
    def receive = {
      case RecordsPublished(_, _, _) =>
        receivedMessages += 1
    }
  }

  class FakeReadCoordinatorActor extends Actor {
    def receive: Receive = {
      case ExecuteStatement(_) =>
        sender() ! SelectStatementExecuted(db = "db",
                                           namespace = "testNamespace",
                                           metric = "testMetric",
                                           values = Seq.empty)
    }
  }

  class FakeMetadataCoordinator extends Actor with ActorLogging {

    lazy val shardingInterval: Duration = context.system.settings.config.getDuration("nsdb.sharding.interval")

    override def receive: Receive = {
      case GetWriteLocation(db, namespace, metric, timestamp) =>
        val location = Location(metric, "node1", timestamp, timestamp + shardingInterval.toMillis, timestamp)
        sender() ! LocationGot(db, namespace, metric, Some(location))

      case msg @ AddLocation(db, namespace, location, occurredOn) =>
//        (cache ? PutInCache(LocationKey(db, namespace, location.metric, location.from, location.to), location))
//          .map {
//            case Cached(_, Some(_)) =>
//              mediator ! Publish("metadata", msg)
//              LocationAdded(db, namespace, location, occurredOn)
//            case _ => AddLocationFailed(db, namespace, location, occurredOn)
//          }
//          .pipeTo(sender)

      case msg @ UpdateLocation(db, namespace, oldLocation, newOccupation, occurredOn) =>
//        val newLocation = oldLocation.copy(occupied = newOccupation)
//        (cache ? PutInCache(LocationKey(db, namespace, oldLocation.metric, oldLocation.from, oldLocation.to),
//          newLocation))
//          .map {
//            case Cached(_, Some(_)) =>
//              mediator ! Publish("metadata", msg)
//              LocationAdded(db, namespace, newLocation, occurredOn)
//            case _ => AddLocationFailed(db, namespace, newLocation, occurredOn)
//          }
//          .pipeTo(sender)
    }
  }

}
