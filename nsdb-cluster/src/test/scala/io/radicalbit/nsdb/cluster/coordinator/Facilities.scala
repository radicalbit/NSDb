package io.radicalbit.nsdb.cluster.coordinator

import java.time.Duration

import akka.actor.{Actor, ActorLogging}
import io.radicalbit.commit_log.CommitLogService.{Delete, Insert}
import io.radicalbit.nsdb.actors.PublisherActor.Events.RecordsPublished
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands.GetWriteLocation
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.events.LocationGot
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor.WroteToCommitLogAck
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.SelectStatementExecuted

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
        val location = Location(metric, "node1", timestamp, timestamp + shardingInterval.toMillis)
        sender() ! LocationGot(db, namespace, metric, Some(location))
    }
  }

}
