package io.radicalbit.nsdb.actors

import java.nio.file.Paths

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.PublisherActor.Command.SubscribeBySqlStatement
import io.radicalbit.nsdb.actors.PublisherActor.Events.{RecordPublished, Subscribed}
import io.radicalbit.nsdb.common.protocol.Record
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.QueryIndex
import org.apache.lucene.store.FSDirectory
import org.scalatest._

class PublisherActorSpec
    extends TestKit(ActorSystem("PublisherActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val basePath       = "target/test_index_publisher_actor"
  val probe          = TestProbe()
  val probeActor     = probe.testActor
  val publisherActor = TestActorRef[PublisherActor](PublisherActor.props(basePath))

  val testSqlStatement = SelectSQLStatement(
    namespace = "registry",
    metric = "people",
    fields = AllFields,
    condition = Some(
      Condition(ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
    limit = Some(LimitOperator(4))
  )
  val testRecordNotSatisfy = Record(0, Map("name"   -> "john"), Map.empty)
  val testRecordSatisfy    = Record(100, Map("name" -> "john"), Map.empty)

  before {
    val queryIndex: QueryIndex = new QueryIndex(FSDirectory.open(Paths.get(basePath, "queries")))
    implicit val writer        = queryIndex.getWriter
    queryIndex.deleteAll()
    writer.close()
  }

  "PublisherActor" should "make other actors subscribe" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, testSqlStatement))
    probe.expectMsgType[Subscribed]

    publisherActor.underlyingActor.queries.keys.size shouldBe 1
    publisherActor.underlyingActor.queries.values.head.query shouldBe testSqlStatement

    publisherActor.underlyingActor.subscribedActors.keys.size shouldBe 1
    publisherActor.underlyingActor.subscribedActors.values.head shouldBe probeActor

  }

  "PublisherActor" should "prevent one actor to subscribe more than once" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, testSqlStatement))
    probe.expectMsgType[Subscribed]

    publisherActor.underlyingActor.queries.keys.size shouldBe 1
    publisherActor.underlyingActor.queries.values.head.query shouldBe testSqlStatement

    publisherActor.underlyingActor.subscribedActors.values.size shouldBe 1
    publisherActor.underlyingActor.subscribedActors.values.head shouldBe probeActor

    val id = publisherActor.underlyingActor.subscribedActors.keys.head

    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, testSqlStatement))
    probe.expectMsgType[Subscribed]

    publisherActor.underlyingActor.queries.keys.size shouldBe 1
    publisherActor.underlyingActor.subscribedActors.keys.size shouldBe 1
    publisherActor.underlyingActor.subscribedActors.keys.head shouldBe id
    publisherActor.underlyingActor.subscribedActors.values.head shouldBe probeActor
  }

  "PublisherActor" should "do nothing if an event that does not satisfy a query comes" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, testSqlStatement))
    probe.expectMsgType[Subscribed]

    probe.send(publisherActor, RecordPublished("rooms", testRecordNotSatisfy))
    probe.expectNoMsg()

    probe.send(publisherActor, RecordPublished("people", testRecordNotSatisfy))
    probe.expectNoMsg()
  }

  "PublisherActor" should "send a messge to all its subscribers when a matching event comes" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, testSqlStatement))
    probe.expectMsgType[Subscribed]

    probe.send(publisherActor, RecordPublished("people", testRecordSatisfy))
    val recordPublished = probe.expectMsgType[RecordPublished]
    recordPublished.metric shouldBe "people"
    recordPublished.record shouldBe testRecordSatisfy
  }

  "PublisherActor" should "recover its queries when it is restarted" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, testSqlStatement))
    val subscribed = probe.expectMsgType[Subscribed]

    probe.send(publisherActor, PoisonPill)

    val newPublisherActor = system.actorOf(PublisherActor.props("target/test_index_publisher_actor"))

    probe.send(newPublisherActor, SubscribeBySqlStatement(probeActor, testSqlStatement))
    val newSubscribed = probe.expectMsgType[Subscribed]

    newSubscribed.qid shouldBe subscribed.qid
  }
}
