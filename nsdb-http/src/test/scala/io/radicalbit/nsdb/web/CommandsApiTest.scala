package io.radicalbit.nsdb.web

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.HttpHeader.ParsingResult.Ok
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.security.http.EmptyAuthorization
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


class FakeReadCoordinator extends Actor {
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

class FakeWriteCoordinator extends Actor {
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
        apiResources(null, system.actorOf(Props[FakeReadCoordinator]), system.actorOf(Props[FakeWriteCoordinator]), new TestAuthProvider)
    )

    "CommandsApi" should "show namespaces" in {
        Get("/commands/db1/namespaces").withHeaders(RawHeader("testHeader", "testHeader")) ~> testSecuredRoutes ~> check {
            status shouldBe OK
            val entity = entityAs[String]
            entity shouldBe write(ShowNamespacesResponse(namespaces = namespaces))
        }
    }

}
