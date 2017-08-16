package io.radicalbit.nsdb.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.PublisherActor.{RecordPublished, SubscribeBySqlStatement, Subscribed}
import io.radicalbit.nsdb.model.Record
import io.radicalbit.nsdb.statement._
import org.scalatest._

class PublisherActorSpec
    extends TestKit(ActorSystem("PublisherActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest {

  val probe          = TestProbe()
  val probeActor     = probe.testActor
  val publisherActor = TestActorRef[PublisherActor](PublisherActor.props("target/test_index_publisher_actor"))

  val testSqlStatement = SelectSQLStatement(
    metric = "people",
    fields = AllFields,
    condition = Some(
      Condition(ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
    limit = Some(LimitOperator(4))
  )
  val testRecordNotSatisfy = Record(0, Map("name"   -> "john"), Map.empty)
  val testRecordSatisfy    = Record(100, Map("name" -> "john"), Map.empty)

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
}
