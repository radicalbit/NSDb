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

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.actors.PublisherActor.Commands.{SubscribeBySqlStatement, Unsubscribe}
import io.radicalbit.nsdb.actors.PublisherActor.Events.Unsubscribed
import io.radicalbit.nsdb.actors.RealTimeProtocol.Events.{RecordsPublished, SubscribedByQueryString}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.model.{Schema, TimeContext}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import org.scalatest._

import scala.concurrent.duration._

class PublisherActorSpec
    extends TestKit(ActorSystem("PublisherActorSpec"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val testTimeContext: TimeContext = TimeContext(currentTime = 0)

  val testRecordNotSatisfy = Bit(0, 23L, Map.empty, Map("name"   -> "john"))
  val testRecordSatisfy    = Bit(100, 25L, Map.empty, Map("name" -> "john"))

  val schema = Schema("people", testRecordSatisfy)

  val probe      = TestProbe()
  val probeActor = probe.testActor
  val publisherActor =
    TestActorRef[PublisherActor](
      PublisherActor.props(
        system.actorOf(EmptyReadCoordinator.props(schema).withDispatcher("akka.actor.control-aware-dispatcher"))))

  val testPlainSqlStatement = SelectSQLStatement(
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

  val testStandardAggregatedSqlStatement = SelectSQLStatement(
    db = "db",
    namespace = "registry",
    metric = "people",
    distinct = false,
    fields = ListFields(List(Field("*", Some(SumAggregation("*"))))),
    groupBy = Some(SimpleGroupByAggregation("name"))
  )

  def testTemporalAggregatedSqlStatement(aggregation: Aggregation) = SelectSQLStatement(
    db = "db",
    namespace = "registry",
    metric = "people",
    distinct = false,
    fields = ListFields(List(Field("value", Some(aggregation)))),
    condition = Some(
      Condition(
        ComparisonExpression(dimension = "timestamp",
                             comparison = GreaterOrEqualToOperator,
                             value = AbsoluteComparisonValue(10L)))),
    groupBy = Some(TemporalGroupByAggregation(3000, 3, "S"))
  )

  "PublisherActor" should {
    "make other actors subscribe and unsubscribe to plain queries" in {
      probe.send(publisherActor,
                 SubscribeBySqlStatement(probeActor,
                                         "db",
                                         "namespace",
                                         "metric",
                                         "queryString",
                                         testPlainSqlStatement,
                                         Some(testTimeContext)))
      probe.expectMsgType[SubscribedByQueryString]

      publisherActor.underlyingActor.plainQueries.keys.size shouldBe 1
      publisherActor.underlyingActor.plainQueries.values.head.query shouldBe testPlainSqlStatement

      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 1
      publisherActor.underlyingActor.subscribedActorsByQueryId.values.head shouldBe Set(probeActor)

      probe.send(publisherActor, Unsubscribe(probeActor))
      probe.expectMsgType[Unsubscribed]

      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 0
    }

    "make other actors subscribe and unsubscribe to standard aggregated queries" in {
      probe.send(publisherActor,
                 SubscribeBySqlStatement(probeActor,
                                         "db",
                                         "namespace",
                                         "metric",
                                         "queryString",
                                         testStandardAggregatedSqlStatement))
      probe.expectMsgType[SubscribedByQueryString]

      publisherActor.underlyingActor.aggregatedQueries.keys.size shouldBe 1
      publisherActor.underlyingActor.aggregatedQueries.values.head.query shouldBe testStandardAggregatedSqlStatement

      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 1
      publisherActor.underlyingActor.subscribedActorsByQueryId.values.head shouldBe Set(probeActor)

      probe.send(publisherActor, Unsubscribe(probeActor))
      probe.expectMsgType[Unsubscribed]

      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 0
    }

    "make other actors subscribe and unsubscribe to temporal aggregated queries" in {
      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(CountAggregation("value")))
      )
      probe.expectMsgType[SubscribedByQueryString]

      publisherActor.underlyingActor.temporalAggregatedQueries.keys.size shouldBe 1
      publisherActor.underlyingActor.temporalAggregatedQueries.values.head.query shouldBe testTemporalAggregatedSqlStatement(
        CountAggregation("value"))

      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 1
      publisherActor.underlyingActor.subscribedActorsByQueryId.values.head shouldBe Set(probeActor)

      probe.send(publisherActor, Unsubscribe(probeActor))
      probe.expectMsgType[Unsubscribed]

      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 0
    }

    "subscribe more than once" in {
      probe.send(publisherActor,
                 SubscribeBySqlStatement(probeActor, "db", "namespace", "metric", "queryString", testPlainSqlStatement))
      val firstId = probe.expectMsgType[SubscribedByQueryString].quid

      publisherActor.underlyingActor.plainQueries.keys.size shouldBe 1
      publisherActor.underlyingActor.plainQueries.values.head.query shouldBe testPlainSqlStatement

      publisherActor.underlyingActor.subscribedActorsByQueryId.values.size shouldBe 1
      publisherActor.underlyingActor.subscribedActorsByQueryId.values.head shouldBe Set(probeActor)

      probe.send(publisherActor,
                 SubscribeBySqlStatement(probeActor,
                                         "db",
                                         "namespace",
                                         "anotherOne",
                                         "queryString",
                                         testPlainSqlStatement.copy(metric = "anotherOne")))
      val secondId = probe.expectMsgType[SubscribedByQueryString].quid

      publisherActor.underlyingActor.plainQueries.keys.size shouldBe 2
      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 2
      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.toSeq.contains(firstId) shouldBe true
      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.toSeq.contains(secondId) shouldBe true
      publisherActor.underlyingActor.subscribedActorsByQueryId.values.head shouldBe Set(probeActor)
      publisherActor.underlyingActor.subscribedActorsByQueryId.values.last shouldBe Set(probeActor)
    }

    "do nothing if an event that does not satisfy a query comes" in {
      publisherActor.underlyingActor.plainQueries.clear()
      publisherActor.underlyingActor.subscribedActorsByQueryId.clear()
      probe.send(publisherActor,
                 SubscribeBySqlStatement(probeActor, "db", "namespace", "metric", "queryString", testPlainSqlStatement))
      probe.expectMsgType[SubscribedByQueryString]

      publisherActor.underlyingActor.plainQueries.keys.size shouldBe 1
      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 1

      probe.send(publisherActor, PublishRecord("db", "namespace", "rooms", testRecordNotSatisfy, schema))
      probe.expectNoMessage(3 seconds)

      probe.send(publisherActor, PublishRecord("db", "namespace", "people", testRecordNotSatisfy, schema))
      probe.expectNoMessage(3 seconds)
    }

    "do nothing if an event that does not satisfy a temporal query comes" in {
      publisherActor.underlyingActor.plainQueries.clear()
      publisherActor.underlyingActor.subscribedActorsByQueryId.clear()
      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(CountAggregation("value")))
      )
      probe.expectMsgType[SubscribedByQueryString]

      publisherActor.underlyingActor.temporalAggregatedQueries.keys.size shouldBe 1
      publisherActor.underlyingActor.subscribedActorsByQueryId.keys.size shouldBe 1

      probe.send(publisherActor, PublishRecord("db", "namespace", "rooms", testRecordNotSatisfy, schema))
      probe.expectNoMessage(3 seconds)

      probe.send(publisherActor, PublishRecord("db", "namespace", "people", testRecordNotSatisfy, schema))
      probe.expectNoMessage(3 seconds)
    }

    "send a message to all its subscribers when a matching event comes for a plain query" in {

      val secondProbe = TestProbe()

      probe.send(publisherActor,
                 SubscribeBySqlStatement(probeActor, "db", "namespace", "metric", "queryString", testPlainSqlStatement))
      probe.expectMsgType[SubscribedByQueryString]

      secondProbe.send(
        publisherActor,
        SubscribeBySqlStatement(secondProbe.ref, "db", "namespace", "metric", "queryString", testPlainSqlStatement))
      secondProbe.expectMsgType[SubscribedByQueryString]

      probe.send(publisherActor, PublishRecord("db", "registry", "people", testRecordSatisfy, schema))
      val recordPublished = probe.expectMsgType[RecordsPublished]
      recordPublished.metric shouldBe "people"
      recordPublished.records shouldBe Seq(testRecordSatisfy)

      secondProbe.expectMsgType[RecordsPublished]

      probe.expectNoMessage(5 seconds)
      secondProbe.expectNoMessage(5 seconds)
    }

    "send a message to all its subscribers when only one matching event comes for a temporal aggregated query" in {

      val secondProbe = TestProbe()

      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(CountAggregation("value")),
                                Some(testTimeContext))
      )
      probe.expectMsgType[SubscribedByQueryString]

      secondProbe.send(
        publisherActor,
        SubscribeBySqlStatement(secondProbe.ref,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(CountAggregation("value")),
                                Some(testTimeContext))
      )
      secondProbe.expectMsgType[SubscribedByQueryString]

      probe.send(publisherActor, PublishRecord("db", "registry", "people", testRecordSatisfy, schema))
      val recordPublished = probe.expectMsgType[RecordsPublished]
      recordPublished.metric shouldBe "people"
      recordPublished.records shouldBe Seq(
        Bit(100, 1L, Map("upperBound" -> 100L, "lowerBound" -> 100L), Map("count(*)" -> 1L))
      )

      secondProbe.expectMsgType[RecordsPublished]

      probe.expectNoMessage(5 seconds)
      secondProbe.expectNoMessage(5 seconds)

      publisherActor.underlyingActor.temporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0
    }

    "send a message to all its subscribers when multiple matching event comes for a temporal count query" in {

      val secondProbe = TestProbe()

      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(CountAggregation("value")),
                                Some(testTimeContext))
      )
      probe.expectMsgType[SubscribedByQueryString]

      secondProbe.send(
        publisherActor,
        SubscribeBySqlStatement(secondProbe.ref,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(CountAggregation("value")),
                                Some(testTimeContext))
      )
      secondProbe.expectMsgType[SubscribedByQueryString]

      (1 to 10).foreach { i =>
        probe.send(
          publisherActor,
          PublishRecord("db", "registry", "people", Bit(100 + i, 25L, Map.empty, Map("name" -> "john")), schema))
      }

      val recordPublished = probe.expectMsgType[RecordsPublished]
      recordPublished.metric shouldBe "people"
      recordPublished.records shouldBe Seq(
        Bit(110, 10L, Map("upperBound" -> 110L, "lowerBound" -> 101L), Map("count(*)" -> 10L))
      )

      secondProbe.expectMsgType[RecordsPublished]

      probe.expectNoMessage(5 seconds)
      secondProbe.expectNoMessage(5 seconds)

      publisherActor.underlyingActor.temporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0
    }

    "send a message to all its subscribers when multiple matching event comes for a multiple count query with different aggregations" in {

      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(CountAggregation("value")),
                                Some(testTimeContext))
      )
      probe.expectMsgType[SubscribedByQueryString]
      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(SumAggregation("value")),
                                Some(testTimeContext))
      )
      probe.expectMsgType[SubscribedByQueryString]
      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(AvgAggregation("value")),
                                Some(testTimeContext))
      )
      probe.expectMsgType[SubscribedByQueryString]
      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(MinAggregation("value")),
                                Some(testTimeContext))
      )
      probe.expectMsgType[SubscribedByQueryString]
      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(MaxAggregation("value")),
                                Some(testTimeContext))
      )
      probe.expectMsgType[SubscribedByQueryString]

      (1 to 10).foreach { i =>
        probe.send(
          publisherActor,
          PublishRecord("db", "registry", "people", Bit(100 + i, 25L, Map.empty, Map("name" -> "john")), schema))
      }

      val countBucketPublished = probe.expectMsgType[RecordsPublished]
      countBucketPublished.metric shouldBe "people"
      countBucketPublished.records shouldBe Seq(
        Bit(110, 10L, Map("upperBound" -> 110L, "lowerBound" -> 101L), Map("count(*)" -> 10L))
      )

      val sumBucketPublished = probe.expectMsgType[RecordsPublished]
      sumBucketPublished.metric shouldBe "people"
      sumBucketPublished.records shouldBe Seq(
        Bit(110, 250L, Map("upperBound" -> 110L, "lowerBound" -> 101L), Map("sum(*)" -> 250L))
      )

      val avgBucketPublished = probe.expectMsgType[RecordsPublished]
      avgBucketPublished.metric shouldBe "people"
      avgBucketPublished.records shouldBe Seq(
        Bit(110, 25.0, Map("upperBound" -> 110L, "lowerBound" -> 101L), Map("avg(*)" -> 25.0))
      )

      val minBucketPublished = probe.expectMsgType[RecordsPublished]
      minBucketPublished.metric shouldBe "people"
      minBucketPublished.records shouldBe Seq(
        Bit(110, 25L, Map("upperBound" -> 110L, "lowerBound" -> 101L), Map("min(*)" -> 25L))
      )

      val maxBucketPublished = probe.expectMsgType[RecordsPublished]
      maxBucketPublished.metric shouldBe "people"
      maxBucketPublished.records shouldBe Seq(
        Bit(110, 25L, Map("upperBound" -> 110L, "lowerBound" -> 101L), Map("max(*)" -> 25L))
      )

      probe.expectNoMessage(5 seconds)

      publisherActor.underlyingActor.temporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0
    }

    "do nothing if a late event comes for a temporal query without a grace period set" in {

      val lateTimeContext = TimeContext(10000L)

      probe.send(
        publisherActor,
        SubscribeBySqlStatement(probeActor,
                                "db",
                                "namespace",
                                "metric",
                                "queryString",
                                testTemporalAggregatedSqlStatement(CountAggregation("value")),
                                Some(lateTimeContext))
      )
      probe.expectMsgType[SubscribedByQueryString]

      probe.send(publisherActor, PublishRecord("db", "registry", "people", testRecordSatisfy, schema))

      publisherActor.underlyingActor.temporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0
    }

    "do nothing if a late event comes beyond the grace period" in {

      val lateTimeContext = TimeContext(System.currentTimeMillis)
      val eventStartTime  = lateTimeContext.currentTime + 1000L

      probe.send(
        publisherActor,
        SubscribeBySqlStatement(
          probeActor,
          "db",
          "namespace",
          "metric",
          "queryString",
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "metric",
            distinct = false,
            fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
            condition = Some(
              Condition(
                ComparisonExpression(dimension = "timestamp",
                                     comparison = GreaterOrEqualToOperator,
                                     value = AbsoluteComparisonValue(10L)))),
            groupBy = Some(TemporalGroupByAggregation(5000, 3, "S")),
            gracePeriod = Some(GracePeriod("S", 20))
          ),
          Some(lateTimeContext)
        )
      )
      probe.expectMsgType[SubscribedByQueryString]

      (1 to 10).foreach { i =>
        probe.send(publisherActor,
                   PublishRecord("db",
                                 "namespace",
                                 "metric",
                                 Bit(eventStartTime + i, 25L, Map.empty, Map("name" -> "john")),
                                 schema))
      }

      val recordPublished = probe.expectMsgType[RecordsPublished]
      recordPublished.metric shouldBe "people"
      recordPublished.records shouldBe Seq(
        Bit(eventStartTime + 10,
            10L,
            Map("upperBound" -> (eventStartTime + 10L), "lowerBound" -> (eventStartTime + 1L)),
            Map("count(*)"   -> 10L))
      )

      probe.expectNoMessage(1 seconds)

      publisherActor.underlyingActor.temporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 1
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0

      //sending an event beyond the grace period
      probe.send(publisherActor,
                 PublishRecord("db",
                               "namespace",
                               "metric",
                               Bit(eventStartTime - 20000, 25L, Map.empty, Map("name" -> "john")),
                               schema))

      probe.expectNoMessage(1 seconds)
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 1
      probe.expectNoMessage(10 seconds)
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 0
    }

    "send an update for a late event within a grace period for a temporal query" in {
      val lateTimeContext = TimeContext(System.currentTimeMillis)
      val eventStartTime  = lateTimeContext.currentTime + 1000L

      probe.send(
        publisherActor,
        SubscribeBySqlStatement(
          probeActor,
          "db",
          "namespace",
          "metric",
          "queryString",
          SelectSQLStatement(
            db = "db",
            namespace = "namespace",
            metric = "metric",
            distinct = false,
            fields = ListFields(List(Field("value", Some(CountAggregation("value"))))),
            condition = Some(
              Condition(
                ComparisonExpression(dimension = "timestamp",
                                     comparison = GreaterOrEqualToOperator,
                                     value = AbsoluteComparisonValue(10L)))),
            groupBy = Some(TemporalGroupByAggregation(5000, 3, "S")),
            gracePeriod = Some(GracePeriod("S", 20))
          ),
          Some(lateTimeContext)
        )
      )
      probe.expectMsgType[SubscribedByQueryString]

      (1 to 10).foreach { i =>
        probe.send(publisherActor,
                   PublishRecord("db",
                                 "namespace",
                                 "metric",
                                 Bit(eventStartTime + i, 25L, Map.empty, Map("name" -> "john")),
                                 schema))
      }

      val recordPublished = probe.expectMsgType[RecordsPublished]
      recordPublished.metric shouldBe "people"
      recordPublished.records shouldBe Seq(
        Bit(eventStartTime + 10,
            10L,
            Map("upperBound" -> (eventStartTime + 10L), "lowerBound" -> (eventStartTime + 1L)),
            Map("count(*)"   -> 10L))
      )

      probe.expectNoMessage(1 seconds)

      publisherActor.underlyingActor.temporalBuckets.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 1
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0

      //time has passed... now this is a late event
      probe.send(
        publisherActor,
        PublishRecord("db", "namespace", "metric", Bit(eventStartTime, 25L, Map.empty, Map("name" -> "john")), schema))

      probe.expectNoMessage(1 seconds)
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 1
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 1
      publisherActor.underlyingActor.temporalAggregatedQueries.size shouldBe 1
      publisherActor.underlyingActor.lateEventsToCheck.head shouldBe
        (publisherActor.underlyingActor.temporalAggregatedQueries.head._1,
        Seq(Bit(eventStartTime, 25L, Map.empty, Map("name" -> "john"))))

      val lateRecordPublished = probe.expectMsgType[RecordsPublished]
      lateRecordPublished.metric shouldBe "people"
      lateRecordPublished.records shouldBe Seq(
        Bit(eventStartTime + 10,
            11L,
            Map("upperBound" -> (eventStartTime + 10L), "lowerBound" -> eventStartTime),
            Map("count(*)"   -> 11L))
      )

      probe.expectNoMessage(1 seconds)
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 1

      //sending multiple late events
      (1 to 10).foreach { i =>
        probe.send(publisherActor,
                   PublishRecord("db",
                                 "namespace",
                                 "metric",
                                 Bit(eventStartTime + i, 25L, Map.empty, Map("name" -> "john")),
                                 schema))
      }

      val multipleLateRecordPublished = probe.expectMsgType[RecordsPublished]
      multipleLateRecordPublished.metric shouldBe "people"
      multipleLateRecordPublished.records shouldBe Seq(
        Bit(eventStartTime + 10,
            21L,
            Map("upperBound" -> (eventStartTime + 10L), "lowerBound" -> eventStartTime),
            Map("count(*)"   -> 21L))
      )

      probe.expectNoMessage(1 seconds)
      publisherActor.underlyingActor.lateEventsToCheck.size shouldBe 0
      publisherActor.underlyingActor.lateTemporalBuckets.size shouldBe 0

    }
  }
}
