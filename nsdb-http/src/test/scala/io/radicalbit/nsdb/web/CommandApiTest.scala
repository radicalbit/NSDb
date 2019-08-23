/*
 * Copyright 2018 Radicalbit S.r.l.
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
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import akka.http.scaladsl.model.StatusCodes._
import io.radicalbit.nsdb.common.protocol.DimensionFieldType
import io.radicalbit.nsdb.security.http.NSDBAuthProvider
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import io.radicalbit.nsdb.web.routes.CommandApi
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

object CommandApiTest {

  object Data {
    val dbs        = Set("db1", "db2")
    val namespaces = Set("namespace1", "namespace2")
    val metrics    = Set("metric1", "metric2")

    val schemas = Map(
      "metric1" -> Schema(
        "metric1",
        Map(
          "dim1" -> SchemaField("dim1", DimensionFieldType, VARCHAR()),
          "dim2" -> SchemaField("dim2", DimensionFieldType, INT()),
          "dim3" -> SchemaField("dim3", DimensionFieldType, BIGINT())
        )
      ),
      "metric2" -> Schema(
        "metric2",
        Map(
          "dim1" -> SchemaField("dim1", DimensionFieldType, VARCHAR()),
          "dim2" -> SchemaField("dim2", DimensionFieldType, INT()),
          "dim3" -> SchemaField("dim3", DimensionFieldType, BIGINT())
        )
      )
    )
  }

  class FakeReadCoordinator extends Actor {

    import Data._

    override def receive: Receive = {
      case GetDbs =>
        sender() ! DbsGot(dbs)
      case GetNamespaces(db) =>
        sender() ! NamespacesGot(db, namespaces)
      case GetMetrics(db, namespace) =>
        sender ! MetricsGot(db, namespace, metrics)
      case GetSchema(db, namespace, metric) =>
        sender() ! SchemaGot(db, namespace, metric, schemas.get(metric))
    }
  }

  class FakeWriteCoordinator extends Actor {
    override def receive: Receive = {
      case DeleteNamespace(db, namespace)    => sender() ! NamespaceDeleted(db, namespace)
      case DropMetric(db, namespace, metric) => sender() ! MetricDropped(db, namespace, metric)
    }
  }
}

class CommandApiTest extends FlatSpec with Matchers with ScalatestRouteTest with CommandApi {

  import io.radicalbit.nsdb.web.CommandApiTest._
  import io.radicalbit.nsdb.web.CommandApiTest.Data._

  override def readCoordinator: ActorRef = system.actorOf(Props[FakeReadCoordinator])

  override def writeCoordinator: ActorRef = system.actorOf(Props[FakeWriteCoordinator])

  override def authenticationProvider: NSDBAuthProvider = new TestAuthProvider

  override implicit val formats: DefaultFormats = DefaultFormats
  override implicit val timeout: Timeout        = 5 seconds

  val testSecuredRoutes = Route.seal(
    commandsApi
  )

  "CommandsApi show dbs" should "return dbs" in {
    Get("/commands/dbs").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(ShowDbsResponse(dbs = dbs))
    }
  }

  "CommandsApi show namespaces" should "return namespaces" in {
    Get("/commands/db1/namespaces").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(ShowNamespacesResponse(namespaces = namespaces))
    }
  }

  "CommandsApi show metrics" should "return namespaces" in {
    Get("/commands/db1/namespace1/metrics").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(ShowMetricsResponse(metrics = metrics))
    }
  }

  "CommandsApi describe existing metric " should "return description" in {
    Get("/commands/db1/namespace1/metric1").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
      val entity = entityAs[String]
      entity shouldBe write(
        DescribeMetricResponse(
          schemas("metric1").fieldsMap.map {
            case (_, field) => Field(name = field.name, `type` = field.indexType.getClass.getSimpleName)
          }.toSet
        ))
    }
  }

  "CommandsApi describe not existing metric " should "return NotFound" in {
    Get("/commands/db1/namespace1/metric10").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe NotFound
    }
  }

  "CommandsApi drop namespace" should " drop" in {
    Delete("/commands/db1/namespace1").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
    }
  }

  "CommandsApi drop metric" should " drop" in {
    Delete("/commands/db1/namespace1/metric1").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
      status shouldBe OK
    }
  }

}
