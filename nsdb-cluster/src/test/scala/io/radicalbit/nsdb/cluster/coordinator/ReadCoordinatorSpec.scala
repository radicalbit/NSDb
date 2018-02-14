package io.radicalbit.nsdb.cluster.coordinator

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.SchemaActor
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import org.scalatest._

import scala.concurrent.Await

class ReadCoordinatorSpec
    extends TestKit(ActorSystem("nsdb-test"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with WriteInterval
    with ReadCoordinatorBehaviour {

  override val probe       = TestProbe()
  val probeActor           = probe.ref
  override val basePath    = "target/test_index/ReadCoordinatorSpec"
  override val db          = "db"
  override val namespace   = "registry"
  val schemaActor          = system.actorOf(SchemaActor.props(basePath, db, namespace))
  val namespaceDataActor   = system.actorOf(NamespaceDataActor.props(basePath))
  val readCoordinatorActor = system actorOf ReadCoordinator.props(null, schemaActor)

  override def beforeAll(): Unit = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(5 second)

    Await.result(readCoordinatorActor ? SubscribeNamespaceDataActor(namespaceDataActor), 3 seconds)
    Await.result(namespaceDataActor ? DropMetric(db, namespace, "people"), 3 seconds)
    Await.result(schemaActor ? UpdateSchemaFromRecord(db, namespace, "people", testRecords.head), 3 seconds)

    expectNoMessage(interval)

    Await.result(schemaActor ? UpdateSchemaFromRecord(db, namespace, "people", testRecords.head), 3 seconds)

    testRecords.foreach { record =>
      Await.result(namespaceDataActor ? AddRecord(db, namespace, "people", record), 3 seconds)
    }

    expectNoMessage(interval)
  }

  "ReadCoordinator" should behave.like(defaultBehaviour)
}
