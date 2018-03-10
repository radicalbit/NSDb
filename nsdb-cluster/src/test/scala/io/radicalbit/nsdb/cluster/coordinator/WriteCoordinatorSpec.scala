package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.{Actor, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.commands.{GetLocations, GetWriteLocation}
import io.radicalbit.nsdb.cluster.coordinator.MetadataCoordinator.events.{LocationGot, LocationsGot}
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import org.scalatest._

import scala.concurrent.Await

class DummyMetadataCoordinator extends Actor {
  override def receive: Receive = {
    case GetWriteLocation(db, namespace, metric, _) =>
      sender ! LocationGot(db, namespace, metric, Some(Location(metric, "testNode", 0, 0)))
    case GetLocations(db, namespace, metric) =>
      sender ! LocationsGot(db, namespace, metric, Seq(Location(metric, "testNode", 0, 0)))
  }
}

class WriteCoordinatorSpec
    extends TestKit(ActorSystem("nsdb-test"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfter
    with WriteCoordinatorBehaviour {

  val basePath = "target/test_index/WriteCoordinatorSpec"

  val db        = "writeCoordinatorSpecDB"
  val namespace = "testNamespace"

  import akka.pattern.ask

  import scala.concurrent.duration._
  implicit val timeout = Timeout(3 seconds)

  override def beforeAll {
    Await.result(writeCoordinatorActor ? SubscribeNamespaceDataActor(namespaceDataActor, "node1"), 3 seconds)
    Await.result(writeCoordinatorActor ? DeleteNamespace(db, namespace), 3 seconds)
    Await.result(namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace, "testMetric", record1), 3 seconds)
  }

  "WriteCoordinator" should behave.like(defaultBehaviour)
}
