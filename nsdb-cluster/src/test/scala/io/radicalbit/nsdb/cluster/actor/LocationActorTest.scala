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
  val metadataActor = system.actorOf(MetadataActor.props("target/test_index_metadata_actor", null))

  lazy val namespace = "namespaceTest"
  lazy val metric    = "people"

  lazy val locations = Seq(
    Location(metric, "node1", 0, 1, 1),
    Location(metric, "node1", 2, 3, 3),
    Location(metric, "node1", 4, 5, 5),
    Location(metric, "node1", 6, 8, 7)
  )

  before {
    implicit val timeout = Timeout(5 seconds)
    Await.result(metadataActor ? DeleteNamespace(namespace), 5 seconds)
    Await.result(metadataActor ? AddLocations(namespace, locations), 5 seconds)
  }

  "MetadataActor" should "delete locations for a namespace" in {

    probe.send(metadataActor, DeleteNamespace(namespace))

    val deleted = probe.expectMsgType[NamespaceDeleted]
    deleted.namespace shouldBe namespace

    probe.send(metadataActor, GetLocations(namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe Seq.empty
  }

  "MetadataActor" should "get locations for metric" in {

    probe.send(metadataActor, GetLocations(namespace, "nonexisting"))

    val nonexistingGot = probe.expectMsgType[LocationsGot]
    nonexistingGot.metric shouldBe "nonexisting"
    nonexistingGot.locations shouldBe Seq.empty

    probe.send(metadataActor, GetLocations(namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe locations

    //FIXME see if this has to be removed
//    probe.send(metadataActor, GetLocation(namespace, metric, 3))
//
//    val existingSingleGot = probe.expectMsgType[LocationGot]
//    existingSingleGot.metric shouldBe metric
//    existingSingleGot.location shouldBe Some(locations(1))
  }

  "MetadataActor" should "add a new location" in {

    val newLocation = Location(metric, "node2", 10, 11, 11)
    probe.send(metadataActor, AddLocation(namespace, newLocation))

    val added = probe.expectMsgType[LocationAdded]
    added.location shouldBe newLocation

    probe.send(metadataActor, GetLocations(namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe (locations :+ newLocation)
  }

  "MetadataActor" should "update an existing location" in {

    val oldLocation = locations.last
    probe.send(metadataActor, UpdateLocation(namespace, oldLocation, 8))

    val added = probe.expectMsgType[LocationUpdated]
    added.oldLocation shouldBe oldLocation
    added.newOccupation shouldBe 8

    probe.send(metadataActor, GetLocations(namespace, metric))

    val existingGot = probe.expectMsgType[LocationsGot]
    existingGot.metric shouldBe metric
    existingGot.locations shouldBe (locations.dropRight(1) :+ oldLocation.copy(occupied = 8))
  }
}
