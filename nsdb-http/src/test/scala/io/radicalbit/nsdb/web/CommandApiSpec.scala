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

package io.radicalbit.nsdb.web

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.actor.FakeReadCoordinator
import io.radicalbit.nsdb.common.model.MetricInfo
import io.radicalbit.nsdb.model.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import io.radicalbit.nsdb.web.routes.CommandApi
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object CommandApiSpec {

  class FakeWriteCoordinator extends Actor {
    override def receive: Receive = {
      case DeleteNamespace(db, namespace)    => sender() ! NamespaceDeleted(db, namespace)
      case DropMetric(db, namespace, metric) => sender() ! MetricDropped(db, namespace, metric)
    }
  }

  class FakeMetadataCoordinator extends Actor {
    override def receive: Receive = {
      case GetTopology =>
        sender() ! TopologyGot(Set(NSDbNode("address", "id")))
      case GetLocations(db, namespace, metric) =>
        sender() ! LocationsGot(db, namespace, metric, Seq(Location.empty))
      case GetMetricInfo(db, namespace, "metricWithoutInfo") =>
        sender() ! MetricInfoGot(db, namespace, "metricWithoutInfo", None)
      case GetMetricInfo(db, namespace, "nonExistingMetric") =>
        sender() ! MetricInfoGot(db, namespace, "nonExistingMetric", None)
      case GetMetricInfo(db, namespace, metric) =>
        sender() ! MetricInfoGot(db, namespace, metric, Some(MetricInfo(db, namespace, metric, 100, 100)))
    }
  }
}

class CommandApiSpec extends FlatSpec with Matchers with ScalatestRouteTest with CommandApi {

  import FakeReadCoordinator.Data._
  import io.radicalbit.nsdb.web.CommandApiSpec._

  override def readCoordinator: ActorRef = system.actorOf(Props[FakeReadCoordinator])

  override def writeCoordinator: ActorRef = system.actorOf(Props[FakeWriteCoordinator])

  override def metadataCoordinator: ActorRef = system.actorOf(Props[FakeMetadataCoordinator])

  override def authenticationProvider: NSDBAuthProvider = new TestAuthProvider

  override val ec: ExecutionContext = ExecutionContext.Implicits.global

  override implicit val formats: DefaultFormats = DefaultFormats
  override implicit val timeout: Timeout        = 5 seconds

  val testSecuredRoutes = Route.seal(
    commandsApi
  )

  "CommandApi" should "get cluster topology" in {
    Get("/commands/topology").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(TopologyGot(Set(NSDbNode("address", "id"))))
    }
  }

  "CommandApi" should "get locations for a metric" in {
    Get("/commands/locations/db/namespace/metric").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(LocationsGot("db", "namespace", "metric", Seq(Location.empty)))
    }
  }

  "CommandsApi" should "show dbs" in {
    Get("/commands/dbs").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(ShowDbsResponse(dbs = dbs))
    }
  }

  "CommandsApi" should "show namespaces" in {
    Get("/commands/db1/namespaces").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(ShowNamespacesResponse(namespaces = namespaces))
    }
  }

  "CommandsApi" should "show metrics" in {
    Get("/commands/db1/namespace1/metrics").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(ShowMetricsResponse(metrics = metrics))
    }
  }

  "CommandsApi" should "describe existing metric" in {
    Get("/commands/db1/namespace1/metricWithoutInfo").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(
        DescribeMetricResponse(
          schemas("metricWithoutInfo").fieldsMap.map {
            case (_, field) => Field(name = field.name, `type` = field.indexType.getClass.getSimpleName)
          }.toSet,
          None
        ))
    }

    Get("/commands/db1/namespace1/metric1").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(
        DescribeMetricResponse(
          schemas("metric1").fieldsMap.map {
            case (_, field) => Field(name = field.name, `type` = field.indexType.getClass.getSimpleName)
          }.toSet,
          Some(MetricInfo("db1", "namespace1", "metric1", 100, 100))
        ))
    }
  }

  "CommandsApi" should "describe initialized metric but without schema" in {
    Get("/commands/db1/namespace1/metricWithoutSchema").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(
        DescribeMetricResponse(
          Set.empty,
          Some(MetricInfo("db1", "namespace1", "metricWithoutSchema", 100, 100))
        ))
    }
  }

  "CommandsApi" should "return NotFound for a non existing metric" in {
    Get("/commands/db1/namespace1/nonExistingMetric").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe NotFound
    }
  }

  "CommandsApi" should "drop a namespace" in {
    Delete("/commands/db1/namespace1").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
    }
  }

  "CommandsApi" should "drop metric" in {
    Delete("/commands/db1/namespace1/metric1").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
    }
  }

}
