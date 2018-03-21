package io.radicalbit.nsdb.web

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import akka.http.scaladsl.model.StatusCodes._
import io.radicalbit.nsdb.web.auth.TestAuthProvider
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

object Data {
    val namespaces = Set("namespace1", "namespace2")
    val metrics = Set("metric1", "metric2")

    val schemas = Map(
        "metric1" -> Schema("metric1", Set(SchemaField("dim1", VARCHAR()), SchemaField("dim2", INT()), SchemaField("dim3", BIGINT()))),
        "metric2" -> Schema("metric2", Set(SchemaField("dim1", VARCHAR()), SchemaField("dim2", INT()), SchemaField("dim3", BIGINT()))),
    )
}


class FakeReaderCoordinator extends Actor {
    import Data._

    override def receive: Receive = {
        case GetNamespaces(db) =>
            sender() ! NamespacesGot(db, namespaces)
        case GetMetrics(db, namespace) =>
            sender ! MetricsGot(db, namespace, metrics)
        case GetSchema(db, namespace, metric) =>
            sender() ! SchemaGot(db, namespace, metric, schemas.get(metric))
    }
}

class FakeWriterCoordinator extends Actor {
    override def receive: Receive = {
        case DeleteNamespace(db, namespace) => sender() ! NamespaceDeleted(db, namespace)
        case DropMetric(db, namespace, metric) => sender() ! MetricDropped(db, namespace, metric)
    }
}

class CommandsApiTest extends FlatSpec with Matchers with ScalatestRouteTest with ApiResources {

    import Data._

    override implicit val formats: DefaultFormats = DefaultFormats
    override implicit val timeout: Timeout = 5 seconds

    val testSecuredRoutes = Route.seal(
        apiResources(null, system.actorOf(Props[FakeReaderCoordinator]), system.actorOf(Props[FakeWriterCoordinator]), new TestAuthProvider)
    )

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
            entity shouldBe write(DescribeMetricResponse(
                schemas("metric1")
                  .fields
                  .map(field =>
                      Field(name = field.name, `type` = field.indexType.getClass.getSimpleName))
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
