package io.radicalbit.nsdb.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import io.radicalbit.nsdb.actors.PublisherActor.Command.SubscribeBySqlStatement
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{BIGINT, VARCHAR}
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.PublishRecord
import org.scalatest.{FlatSpecLike, Matchers, OneInstancePerTest}

class TestPressuredActor extends Actor {

  //we need to make the actor sleep to enqueue messages
  override def preStart(): Unit = {
    Thread.sleep(1000)
  }

  override def receive: Receive = {
    case m => sender() ! m
  }
}

class PublisherPriorityMailboxSpec
    extends TestKit(ActorSystem("PublisherPriorityMailboxSpec"))
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest {

  val probe = TestProbe()

  val testRecord = Bit(100, 25, Map("name" -> "john"))

  val schema = Schema("people", Set(SchemaField("timestamp", BIGINT()), SchemaField("name", VARCHAR())))

  val testSqlStatement = SelectSQLStatement(
    db = "db",
    namespace = "registry",
    metric = "people",
    distinct = false,
    fields = AllFields,
    condition = Some(
      Condition(ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
    limit = Some(LimitOperator(4))
  )

  "PublisherPriorityMailbox" should "guarantee that subscribe messages comes before of publish ones" in {

    val actor = system.actorOf(Props[TestPressuredActor].withDispatcher("akka.actor.publisher-dispatcher"))

    (1 to 10).foreach { _ =>
      probe.send(actor, PublishRecord("db", "namespace", "people", testRecord, schema))
    }

    probe.send(actor, SubscribeBySqlStatement(probe.testActor, "queryString", testSqlStatement))

    probe.expectMsgType[SubscribeBySqlStatement]

    (1 to 10).foreach { _ =>
      probe.expectMsgType[PublishRecord]
    }
  }
}
