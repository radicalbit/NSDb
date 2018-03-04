package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.SchemaActor
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor.AddRecordToLocation
import io.radicalbit.nsdb.cluster.index.Location
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

    val location = Location(_: String, "testNode", 0, 0)

    Await.result(readCoordinatorActor ? SubscribeNamespaceDataActor(namespaceDataActor, "testNode"), 3 seconds)

    //long metric
    Await.result(namespaceDataActor ? DropMetric(db, namespace, LongMetric.name), 3 seconds)
    Await.result(schemaActor ? UpdateSchemaFromRecord(db, namespace, LongMetric.name, LongMetric.testRecords.head),
                 3 seconds)

    LongMetric.testRecords.foreach { record =>
      Await.result(namespaceDataActor ? AddRecordToLocation(db, namespace, record, location(LongMetric.name)),
                   3 seconds)
    }

    //double metric
    Await.result(namespaceDataActor ? DropMetric(db, namespace, DoubleMetric.name), 3 seconds)
    Await.result(schemaActor ? UpdateSchemaFromRecord(db, namespace, DoubleMetric.name, DoubleMetric.testRecords.head),
                 3 seconds)

    DoubleMetric.testRecords.foreach { record =>
      Await.result(namespaceDataActor ? AddRecordToLocation(db, namespace, record, location(DoubleMetric.name)),
                   3 seconds)
    }

    //aggregation metric
    Await.result(namespaceDataActor ? DropMetric(db, namespace, AggregationMetric.name), 3 seconds)
    Await.result(
      schemaActor ? UpdateSchemaFromRecord(db, namespace, AggregationMetric.name, AggregationMetric.testRecords.head),
      3 seconds)

    AggregationMetric.testRecords.foreach { record =>
      Await.result(namespaceDataActor ? AddRecord(db, namespace, AggregationMetric.name, record), 3 seconds)
    }

    expectNoMessage(interval)
  }

  "ReadCoordinator in shard mode" should behave.like(defaultBehaviour)
}
