package io.radicalbit.nsdb.cluster.actor

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.commands._
import io.radicalbit.nsdb.cluster.actor.MetadataCoordinator.events._
import io.radicalbit.nsdb.cluster.index.Location
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class LocationActorTest
    extends TestKit(ActorSystem("SchemaActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val probe         = TestProbe()
  val metadataActor = system.actorOf(MetadataActor.props("target/test_index/LocationActorTest", null))

  lazy val db        = "db"
  lazy val namespace = "namespaceTest"
  lazy val metric    = "people"

  lazy val locations = Seq(
    Location(metric, "node1", 0, 1),
    Location(metric, "node1", 2, 3),
    Location(metric, "node1", 4, 5),
    Location(metric, "node1", 6, 8)
  )

  before {
    implicit val timeout = Timeout(5 seconds)
    Await.result(metadataActor ? DeleteNamespace(db, namespace), 5 seconds)
    Await.result(metadataActor ? AddLocations(db, namespace, locations), 5 seconds)
  }

  "MetadataActor" should "delete locations for a namespace" in {

    probe.send(metadataActor, DeleteNamespace(db, namespace))

    val deleted = probe.expectMsgType[NamespaceDeleted]
    deleted.namespace shouldBe namespace

    probe.send(metadataActor, GetLocations(db, namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe Seq.empty
  }

  "MetadataActor" should "get locations for metric" in {

    probe.send(metadataActor, GetLocations(db, namespace, "nonexisting"))

    val nonexistingGot = probe.expectMsgType[LocationsGot]
    nonexistingGot.metric shouldBe "nonexisting"
    nonexistingGot.locations shouldBe Seq.empty

    probe.send(metadataActor, GetLocations(db, namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe locations

  }

  "MetadataActor" should "add a new location" in {

    val newLocation = Location(metric, "node2", 10, 11)
    probe.send(metadataActor, AddLocation(db, namespace, newLocation))

    val added = probe.expectMsgType[LocationAdded]
    added.location shouldBe newLocation

    probe.send(metadataActor, GetLocations(db, namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe (locations :+ newLocation)
  }
}
