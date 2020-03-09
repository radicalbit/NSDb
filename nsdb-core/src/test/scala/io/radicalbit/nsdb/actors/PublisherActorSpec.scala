/*
 * Copyright 2018-2020 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.actors

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.PublisherActor.Command.{SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events._
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType, ValueFieldType}
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{BIGINT, VARCHAR}
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest._

import scala.concurrent.duration._

class FakeReadCoordinatorActor extends Actor {
  def receive: Receive = {
    case ExecuteStatement(statement) =>
      sender() ! SelectStatementExecuted(statement, values = Seq.empty)
  }
}

class PublisherActorSpec
    extends TestKit(ActorSystem("PublisherActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val probe      = TestProbe()
  val probeActor = probe.testActor
  val publisherActor =
    TestActorRef[PublisherActor](
      PublisherActor.props(
        system.actorOf(Props[FakeReadCoordinatorActor].withDispatcher("akka.actor.control-aware-dispatcher"))))

  val testSqlStatement = SelectSQLStatement(
    db = "db",
    namespace = "registry",
    metric = "people",
    distinct = false,
    fields = AllFields(),
    condition = Some(
      Condition(
        ComparisonExpression(dimension = "timestamp",
                             comparison = GreaterOrEqualToOperator,
                             value = AbsoluteComparisonValue(10L)))),
    limit = Some(LimitOperator(4))
  )

  val testRecordNotSatisfy = Bit(0, 23, Map("name"   -> "john"), Map.empty)
  val testRecordSatisfy    = Bit(100, 25, Map("name" -> "john"), Map.empty)

  val schema = Schema(
    "people",
    Map(
      "timestamp" -> SchemaField("timestamp", DimensionFieldType, BIGINT()),
      "name"      -> SchemaField("name", DimensionFieldType, VARCHAR()),
      "value"     -> SchemaField("value", ValueFieldType, BIGINT())
    )
  )

  "PublisherActor" should "make other actors subscribe and unsubscribe" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement))
    probe.expectMsgType[SubscribedByQueryString]

    publisherActor.underlyingActor.queries.keys.size shouldBe 1
    publisherActor.underlyingActor.queries.values.head.query shouldBe testSqlStatement

    publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 1
    publisherActor.underlyingActor.subscribedActorsByQueryId.values.head shouldBe Set(probeActor)

    probe.send(publisherActor, Unsubscribe(probeActor))
    probe.expectMsgType[Unsubscribed]

    publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 0
  }

  "PublisherActor" should "subscribe more than once" in {
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement))
    val firstId = probe.expectMsgType[SubscribedByQueryString].quid

    publisherActor.underlyingActor.queries.keys.size shouldBe 1
    publisherActor.underlyingActor.queries.values.head.query shouldBe testSqlStatement

    publisherActor.underlyingActor.subscribedActorsByQueryId.values.size shouldBe 1
    publisherActor.underlyingActor.subscribedActorsByQueryId.values.head shouldBe Set(probeActor)

    probe.send(publisherActor,
               SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement.copy(metric = "anotherOne")))
    val secondId = probe.expectMsgType[SubscribedByQueryString].quid

    publisherActor.underlyingActor.queries.keys.size shouldBe 2
    publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 2
    publisherActor.underlyingActor.subscribedActorsByQueryId.keys.toSeq.contains(firstId) shouldBe true
    publisherActor.underlyingActor.subscribedActorsByQueryId.keys.toSeq.contains(secondId) shouldBe true
    publisherActor.underlyingActor.subscribedActorsByQueryId.values.head shouldBe Set(probeActor)
    publisherActor.underlyingActor.subscribedActorsByQueryId.values.last shouldBe Set(probeActor)
  }

  "PublisherActor" should "do nothing if an event that does not satisfy a query comes" in {
    publisherActor.underlyingActor.queries.clear()
    publisherActor.underlyingActor.subscribedActorsByQueryId.clear()
    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement))
    probe.expectMsgType[SubscribedByQueryString]

    publisherActor.underlyingActor.queries.keys.size shouldBe 1
    publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 1

    probe.send(publisherActor, PublishRecord("db", "namespace", "rooms", testRecordNotSatisfy, schema))
    probe.expectNoMessage(3 seconds)

    probe.send(publisherActor, PublishRecord("db", "namespace", "people", testRecordNotSatisfy, schema))
    probe.expectNoMessage(3 seconds)
  }

  "PublisherActor" should "send a message to all its subscribers when a matching event comes" in {

    val secondProbe = TestProbe()

    probe.send(publisherActor, SubscribeBySqlStatement(probeActor, "queryString", testSqlStatement))
    probe.expectMsgType[SubscribedByQueryString]

    secondProbe.send(publisherActor, SubscribeBySqlStatement(secondProbe.ref, "queryString", testSqlStatement))
    secondProbe.expectMsgType[SubscribedByQueryString]

    probe.send(publisherActor, PublishRecord("db", "registry", "people", testRecordSatisfy, schema))
    val recordPublished = probe.expectMsgType[RecordsPublished]
    recordPublished.metric shouldBe "people"
    recordPublished.records shouldBe Seq(testRecordSatisfy)

    secondProbe.expectMsgType[RecordsPublished]

    probe.expectNoMessage(5 seconds)
    secondProbe.expectNoMessage(5 seconds)
  }
}
