package io.radicalbit.nsdb.actors

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.SchemaCoordinatorActor.commands._
import io.radicalbit.nsdb.actors.SchemaCoordinatorActor.events._
import io.radicalbit.nsdb.index.{BOOLEAN, Schema, VARCHAR}
import io.radicalbit.nsdb.model.{Record, SchemaField}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class NamespaceSchemaActorSpec
    extends TestKit(ActorSystem("SchemaActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val probe       = TestProbe()
  val schemaActor = system.actorOf(SchemaActor.props("target/test_index_schema_actor", "namespace"))

  before {
    implicit val timeout = Timeout(3 seconds)
    Await.result(schemaActor ? DeleteSchema("namespace", "people"), 3 seconds)
    Await.result(
      schemaActor ? UpdateSchema("namespace", "people", Schema("people", Seq(SchemaField("name", VARCHAR())))),
      3 seconds)
  }

  "SchemaActor" should "get schemas" in {

    probe.send(schemaActor, GetSchema("namespace", "nonexisting"))

    val nonexistingGot = probe.expectMsgType[SchemaGot]
    nonexistingGot.metric shouldBe "nonexisting"
    nonexistingGot.schema shouldBe None

    probe.send(schemaActor, GetSchema("namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(Schema("people", Seq(SchemaField("name", VARCHAR()))))
  }

  "SchemaActor" should "update schemas in case of success" in {
    probe.send(schemaActor,
               UpdateSchema("namespace", "people", Schema("people", Seq(SchemaField("surname", VARCHAR())))))

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people", Seq(SchemaField("name", VARCHAR()), SchemaField("surname", VARCHAR())))
    )
  }

  "SchemaActor" should "not update schemas in case of failure" in {
    probe.send(schemaActor, UpdateSchema("namespace", "people", Schema("people", Seq(SchemaField("name", BOOLEAN())))))

    val failed = probe.expectMsgType[UpdateSchemaFailed]
    failed.errors shouldBe List("")

    probe.send(schemaActor, GetSchema("namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people", Seq(SchemaField("name", VARCHAR())))
    )
  }

  "SchemaActor" should "update schemas coming from a record" in {
    probe.send(
      schemaActor,
      UpdateSchemaFromRecord("namespace", "people", Record(0, Map("name" -> "john", "surname" -> "doe"), Map.empty)))

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people", Seq(SchemaField("name", VARCHAR()), SchemaField("surname", VARCHAR())))
    )
  }

  "SchemaActor" should "return the same schema for a new schema included in the old one" in {
    probe.send(
      schemaActor,
      UpdateSchemaFromRecord("namespace", "people", Record(0, Map("name" -> "john", "surname" -> "doe"), Map.empty)))

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people", Seq(SchemaField("name", VARCHAR()), SchemaField("surname", VARCHAR())))
    )

    probe.send(schemaActor, UpdateSchemaFromRecord("namespace", "people", Record(0, Map("name" -> "john"), Map.empty)))
    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("namespace", "people"))

    val newGot = probe.expectMsgType[SchemaGot]
    newGot.metric shouldBe "people"
    newGot.schema shouldBe existingGot.schema
  }
}
