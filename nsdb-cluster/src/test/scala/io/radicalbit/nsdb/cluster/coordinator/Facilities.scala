package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.Actor
import io.radicalbit.commit_log.CommitLogService.{Delete, Insert}
import io.radicalbit.nsdb.actors.PublisherActor.Events.RecordsPublished
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

}
