package io.radicalbit.nsdb.actors

import java.nio.file.Paths

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.apache.lucene.store.MMapDirectory
import org.scalatest._

import scala.concurrent.duration._

class FakeReadCoordinatorActor extends Actor {
  def receive: Receive = {
    case ExecuteStatement(_) =>
      sender() ! SelectStatementExecuted(db = "db", namespace = "registry", metric = "people", values = Seq.empty)
  }
}

class FakeNamespaceSchemaActor extends Actor {
  def receive: Receive = {
    case GetSchema(_, _, _) =>
      sender() ! SchemaGot(
        db = "db",
        namespace = "registry",
        metric = "people",
        schema = Some(Schema("people", Seq(SchemaField("timestamp", BIGINT()), SchemaField("surname", VARCHAR())))))
  }
}

class PublisherActorSpec
    extends TestKit(ActorSystem("PublisherActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val basePath   = "target/test_index/PublisherActorSpec"
  val probe      = TestProbe()
  val probeActor = probe.testActor
  val publisherActor =
    TestActorRef[PublisherActor](
      PublisherActor.props(basePath,
                           system.actorOf(Props[FakeReadCoordinatorActor]),
                           system.actorOf(Props[FakeNamespaceSchemaActor])))

  val testSqlStatement = SelectSQLStatement(
    db = "db",
    namespace = "registry",
    metric = "people",
    fields = AllFields,
    condition = Some(
      Condition(ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
    limit = Some(LimitOperator(4))
  )

  val testRecordNotSatisfy = Bit(0, 23, Map("name"   -> "john"))
  val testRecordSatisfy    = Bit(100, 25, Map("name" -> "john"))

  val schema = Schema("people", Seq(SchemaField("timestamp", BIGINT()), SchemaField("name", VARCHAR())))

  before {
    val queryIndex: QueryIndex = new QueryIndex(new MMapDirectory(Paths.get(basePath, "queries")))
    implicit val writer        = queryIndex.getWriter
    queryIndex.deleteAll()
    writer.close()
  }

  "PublisherActor" should "make other actors subscribe and unsubscribe" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement))
    probe.expectMsgType[SubscribedByQueryString]

    publisherActor.underlyingActor.queries.keys.size shouldBe 1
    publisherActor.underlyingActor.queries.values.head.query shouldBe testSqlStatement

    publisherActor.underlyingActor.subscribedActors.keys.size shouldBe 1
    publisherActor.underlyingActor.subscribedActors.values.head shouldBe Set(probeActor)

    probe.send(publisherActor, Unsubscribe(probeActor))
    probe.expectMsgType[Unsubscribed]

    publisherActor.underlyingActor.subscribedActors.keys.size shouldBe 0
  }

  "PublisherActor" should "subscribe more than once" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement))
    val firstId = probe.expectMsgType[SubscribedByQueryString].quid

    publisherActor.underlyingActor.queries.keys.size shouldBe 1
    publisherActor.underlyingActor.queries.values.head.query shouldBe testSqlStatement

    publisherActor.underlyingActor.subscribedActors.values.size shouldBe 1
    publisherActor.underlyingActor.subscribedActors.values.head shouldBe Set(probeActor)

    probe.send(publisherActor,
               SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement.copy(metric = "anotherOne")))
    val secondId = probe.expectMsgType[SubscribedByQueryString].quid

    publisherActor.underlyingActor.queries.keys.size shouldBe 2
    publisherActor.underlyingActor.subscribedActors.keys.size shouldBe 2
    publisherActor.underlyingActor.subscribedActors.keys.toSeq.contains(firstId) shouldBe true
    publisherActor.underlyingActor.subscribedActors.keys.toSeq.contains(secondId) shouldBe true
    publisherActor.underlyingActor.subscribedActors.values.head shouldBe Set(probeActor)
    publisherActor.underlyingActor.subscribedActors.values.last shouldBe Set(probeActor)
  }

  "PublisherActor" should "do nothing if an event that does not satisfy a query comes" in {
    publisherActor.underlyingActor.queries.clear()
    publisherActor.underlyingActor.subscribedActors.clear()
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement))
    probe.expectMsgType[SubscribedByQueryString]

    publisherActor.underlyingActor.queries.keys.size shouldBe 1
    publisherActor.underlyingActor.subscribedActors.keys.size shouldBe 1

    probe.send(publisherActor, PublishRecord("db", "namespace", "rooms", testRecordNotSatisfy, schema))
    probe.expectNoMessage(3 seconds)

    probe.send(publisherActor, PublishRecord("db", "namespace", "people", testRecordNotSatisfy, schema))
    probe.expectNoMessage(3 seconds)
  }

  "PublisherActor" should "send a messge to all its subscribers when a matching event comes" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement))
    probe.expectMsgType[SubscribedByQueryString]

    probe.send(publisherActor, PublishRecord("db", "registry", "people", testRecordSatisfy, schema))
    val recordPublished = probe.expectMsgType[RecordsPublished]
    recordPublished.metric shouldBe "people"
    recordPublished.records shouldBe Seq(testRecordSatisfy)
  }
}
