package io.radicalbit.nsdb.actors

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class SchemaActorSpec
    extends TestKit(ActorSystem("SchemaActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val probe       = TestProbe()
  val schemaActor = TestActorRef[SchemaActor](SchemaActor.props("target/test_index/schema_actor", "db", "namespace"))

  before {
    implicit val timeout = Timeout(3 seconds)
    Await.result(schemaActor ? DeleteSchema("db", "namespace", "people"), 3 seconds)

    schemaActor.underlyingActor.schemas.get("people") shouldBe None
    Await
      .result((schemaActor ? GetSchema("db", "namespace", "people")).mapTo[SchemaGot], 3 seconds)
      .schema
      .isDefined shouldBe false

    Await.result(schemaActor ? UpdateSchemaFromRecord("db", "namespace", "people", Bit(0, 1, Map("name" -> "name"))),
                 3 seconds)
  }

  "SchemaActor" should "get schemas" in {

    probe.send(schemaActor, GetSchema("db", "namespace", "nonexisting"))

    val nonexistingGot = probe.expectMsgType[SchemaGot]
    nonexistingGot.metric shouldBe "nonexisting"
    nonexistingGot.schema shouldBe None

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people",
             Set(SchemaField("name", VARCHAR()), SchemaField("timestamp", BIGINT()), SchemaField("value", INT()))))
  }

  "SchemaActor" should "return a failed message when trying to update a schema with an incompatible one" in {
    probe.send(
      schemaActor,
      UpdateSchemaFromRecord("db", "namespace", "people", Bit(0, 23.5, Map("name" -> "john", "surname" -> "doe"))))

    probe.expectMsgType[UpdateSchemaFailed]
  }

  "SchemaActor" should "update schemas coming from a record" in {
    probe.send(
      schemaActor,
      UpdateSchemaFromRecord("db", "namespace", "people", Bit(0, 23, Map("name" -> "john", "surname" -> "doe"))))

    val schema = probe.expectMsgType[SchemaUpdated].schema
    schema.fields.exists(_.name == "timestamp") shouldBe true
    schema.fields.exists(_.name == "value") shouldBe true

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people",
             Set(SchemaField("name", VARCHAR()),
                 SchemaField("timestamp", BIGINT()),
                 SchemaField("value", INT()),
                 SchemaField("surname", VARCHAR())))
    )

    probe.send(schemaActor, UpdateSchemaFromRecord("db", "namespace", "noDimensions", Bit(0, 23.5, Map.empty)))

    probe.expectMsgType[SchemaUpdated]
  }

  "SchemaActor" should "return the same schema for a new schema included in the old one" in {
    probe.send(
      schemaActor,
      UpdateSchemaFromRecord("db", "namespace", "people", Bit(0, 23, Map("name" -> "john", "surname" -> "doe"))))

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people",
             Set(SchemaField("name", VARCHAR()),
                 SchemaField("timestamp", BIGINT()),
                 SchemaField("value", INT()),
                 SchemaField("surname", VARCHAR())))
    )

    probe.send(schemaActor, UpdateSchemaFromRecord("db", "namespace", "people", Bit(0, 2, Map("name" -> "john"))))
    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val newGot = probe.expectMsgType[SchemaGot]
    newGot.metric shouldBe "people"
    newGot.schema shouldBe existingGot.schema
  }

  "SchemaActor" should "drop a schema" in {

    implicit val timeout = Timeout(3 seconds)

    probe.send(
      schemaActor,
      UpdateSchemaFromRecord("db", "namespace", "people", Bit(0, 23, Map("name" -> "john", "surname" -> "doe"))))

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people",
             Set(SchemaField("name", VARCHAR()),
                 SchemaField("timestamp", BIGINT()),
                 SchemaField("value", INT()),
                 SchemaField("surname", VARCHAR())))
    )

    probe.send(
      schemaActor,
      UpdateSchemaFromRecord("db", "namespace", "offices", Bit(0, 23, Map("name" -> "john", "surname" -> "doe"))))

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("db", "namespace", "offices"))

    val schema = probe.expectMsgType[SchemaGot]
    schema.metric shouldBe "offices"
    schema.schema shouldBe Some(
      Schema("offices",
             Set(SchemaField("name", VARCHAR()),
                 SchemaField("surname", VARCHAR()),
                 SchemaField("timestamp", BIGINT()),
                 SchemaField("value", INT())))
    )

    probe.send(
      schemaActor,
      DeleteSchema("db", "namespace", "offices")
    )

    val deletion = probe.expectMsgType[SchemaDeleted]
    deletion.metric shouldBe "offices"

    probe.send(
      schemaActor,
      DeleteSchema("db", "namespace", "offices")
    )

    Await
      .result((schemaActor ? GetSchema("db", "namespace", "offices")).mapTo[SchemaGot], 3 seconds)
      .schema
      .isDefined shouldBe false

    Await
      .result((schemaActor ? GetSchema("db", "namespace", "people")).mapTo[SchemaGot], 3 seconds)
      .schema
      .isDefined shouldBe true

  }
}
