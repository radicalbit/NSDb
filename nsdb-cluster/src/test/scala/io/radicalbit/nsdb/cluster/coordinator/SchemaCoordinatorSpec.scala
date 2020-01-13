/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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

package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class SchemaCoordinatorSpec
    extends TestKit(ActorSystem("SchemaCoordinatorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val probe = TestProbe()
  val schemaCoordinator =
    system.actorOf(
      SchemaCoordinator
        .props(system.actorOf(Props[FakeSchemaCache])))

  val db         = "db"
  val namespace  = "namespace"
  val namespace1 = "namespace1"

  val nameRecord    = Bit(0, 1, Map("name"    -> "name"), Map("city"       -> "milano"))
  val surnameRecord = Bit(0, 1, Map("surname" -> "surname"), Map("country" -> "italy"))

  val baseSchema = Schema(
    "people",
    Map(
      "name"      -> SchemaField("name", DimensionFieldType, VARCHAR()),
      "timestamp" -> SchemaField("timestamp", TimestampFieldType, BIGINT()),
      "value"     -> SchemaField("value", ValueFieldType, INT()),
      "city"      -> SchemaField("city", TagFieldType, VARCHAR())
    )
  )

  val unionSchema = Schema(
    "people",
    Map(
      "timestamp" -> SchemaField("timestamp", TimestampFieldType, BIGINT()),
      "value"     -> SchemaField("value", ValueFieldType, INT()),
      "name"      -> SchemaField("name", DimensionFieldType, VARCHAR()),
      "surname"   -> SchemaField("surname", DimensionFieldType, VARCHAR()),
      "city"      -> SchemaField("city", TagFieldType, VARCHAR()),
      "country"   -> SchemaField("country", TagFieldType, VARCHAR())
    )
  )

  before {
    implicit val timeout = Timeout(10 seconds)

    Await.result(schemaCoordinator ? DeleteNamespace(db, namespace), 10 seconds)
    Await.result(schemaCoordinator ? DeleteNamespace(db, namespace1), 10 seconds)
    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db, namespace, "people", nameRecord), 10 seconds)
    Await.result(schemaCoordinator ? UpdateSchemaFromRecord(db, namespace1, "people", surnameRecord), 10 seconds)

  }

  "schemaCoordinator" should "get schemas" in {

    probe.send(schemaCoordinator, GetSchema("db", "namespace", "nonexisting"))

    val nonexistingGot = probe.expectMsgType[SchemaGot]
    nonexistingGot.metric shouldBe "nonexisting"
    nonexistingGot.schema shouldBe None

    probe.send(schemaCoordinator, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(baseSchema)

  }

  "schemaCoordinator" should "return a failed message when trying to update a schema with an incompatible one" in {
    probe.send(
      schemaCoordinator,
      UpdateSchemaFromRecord(
        "db",
        "namespace",
        "people",
        Bit(0, 23.5, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy")))
    )

    probe.expectMsgType[SchemaUpdateResponse].isInstanceOf[UpdateSchemaFailed] shouldBe true

    probe.expectNoMessage(1 second)

  }

  "schemaCoordinator" should "update schemas coming from a record" in {
    probe.send(
      schemaCoordinator,
      UpdateSchemaFromRecord(
        "db",
        "namespace",
        "people",
        Bit(0, 23, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy")))
    )

    val schema = probe.expectMsgType[SchemaUpdateResponse].asInstanceOf[SchemaUpdated].schema
    schema.fieldsMap.exists(_._1 == "timestamp") shouldBe true
    schema.fieldsMap.exists(_._1 == "value") shouldBe true

    probe.send(schemaCoordinator, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      unionSchema
    )

    probe.send(schemaCoordinator,
               UpdateSchemaFromRecord("db", "namespace", "noDimensions", Bit(0, 23.5, Map.empty, Map.empty)))

    probe.expectMsgType[SchemaUpdateResponse].isInstanceOf[SchemaUpdated] shouldBe true
  }

  "schemaCoordinator" should "return the same schema for a new schema included in the old one" in {
    probe.send(
      schemaCoordinator,
      UpdateSchemaFromRecord(
        "db",
        "namespace",
        "people",
        Bit(0, 23, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy")))
    )

    probe.expectMsgType[SchemaUpdateResponse].isInstanceOf[SchemaUpdated] shouldBe true

    probe.send(schemaCoordinator, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      unionSchema
    )

    probe.send(
      schemaCoordinator,
      UpdateSchemaFromRecord("db", "namespace", "people", Bit(0, 2, Map("name" -> "john"), Map("country" -> "italy"))))

    probe.expectMsgType[SchemaUpdateResponse].isInstanceOf[SchemaUpdated] shouldBe true

    probe.send(schemaCoordinator, GetSchema("db", "namespace", "people"))

    val newGot = probe.expectMsgType[SchemaGot]
    newGot.metric shouldBe "people"
    newGot.schema shouldBe existingGot.schema
  }

  "schemaCoordinator" should "drop a schema" in {

    implicit val timeout = Timeout(3 seconds)

    probe.send(
      schemaCoordinator,
      UpdateSchemaFromRecord(
        "db",
        "namespace",
        "people",
        Bit(0, 23, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy")))
    )

    probe.expectMsgType[SchemaUpdateResponse].isInstanceOf[SchemaUpdated] shouldBe true

    probe.send(schemaCoordinator, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      unionSchema
    )

    probe.send(
      schemaCoordinator,
      UpdateSchemaFromRecord(
        "db",
        "namespace",
        "offices",
        Bit(0, 23, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy")))
    )

    probe.expectMsgType[SchemaUpdateResponse].isInstanceOf[SchemaUpdated] shouldBe true

    probe.send(schemaCoordinator, GetSchema("db", "namespace", "offices"))

    val schema = probe.expectMsgType[SchemaGot]
    schema.metric shouldBe "offices"
    schema.schema shouldBe Some(
      unionSchema.copy(metric = "offices")
    )

    probe.send(
      schemaCoordinator,
      DeleteSchema("db", "namespace", "offices")
    )

    val deletion = probe.expectMsgType[SchemaDeleted]
    deletion.metric shouldBe "offices"

    probe.send(
      schemaCoordinator,
      DeleteSchema("db", "namespace", "offices")
    )

    Await
      .result((schemaCoordinator ? GetSchema("db", "namespace", "offices")).mapTo[SchemaGot], 3 seconds)
      .schema
      .isDefined shouldBe false

    Await
      .result((schemaCoordinator ? GetSchema("db", "namespace", "people")).mapTo[SchemaGot], 3 seconds)
      .schema
      .isDefined shouldBe true
  }

  "schemaCoordinator" should "get schemas from different namespaces" in {

    probe.send(schemaCoordinator, GetSchema(db, namespace, "nonexisting"))

    val nonexistingGot = probe.expectMsgType[SchemaGot]
    nonexistingGot.metric shouldBe "nonexisting"
    nonexistingGot.schema shouldBe None

    probe.send(schemaCoordinator, GetSchema(db, namespace, "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema(
        "people",
        Map(
          "timestamp" -> SchemaField("timestamp", TimestampFieldType, BIGINT()),
          "value"     -> SchemaField("value", ValueFieldType, INT()),
          "name"      -> SchemaField("name", DimensionFieldType, VARCHAR()),
          "city"      -> SchemaField("city", TagFieldType, VARCHAR())
        )
      ))

    probe.send(schemaCoordinator, GetSchema(db, namespace1, "people"))

    val existingGot1 = probe.expectMsgType[SchemaGot]
    existingGot1.metric shouldBe "people"
    existingGot1.schema shouldBe Some(
      Schema(
        "people",
        Map(
          "timestamp" -> SchemaField("timestamp", TimestampFieldType, BIGINT()),
          "value"     -> SchemaField("value", ValueFieldType, INT()),
          "surname"   -> SchemaField("surname", DimensionFieldType, VARCHAR()),
          "country"   -> SchemaField("country", TagFieldType, VARCHAR())
        )
      ))
  }

  "schemaCoordinator" should "update schemas in case of success in different namespaces" in {
    probe.send(schemaCoordinator, UpdateSchemaFromRecord(db, namespace, "people", surnameRecord))

    probe.expectMsgType[SchemaUpdateResponse].isInstanceOf[SchemaUpdated] shouldBe true

    probe.send(schemaCoordinator, GetSchema(db, namespace, "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      unionSchema
    )

    probe.send(schemaCoordinator, UpdateSchemaFromRecord(db, namespace1, "people", nameRecord))

    probe.expectMsgType[SchemaUpdateResponse].isInstanceOf[SchemaUpdated] shouldBe true

    probe.send(schemaCoordinator, GetSchema(db, namespace, "people"))

    val existingGot1 = probe.expectMsgType[SchemaGot]
    existingGot1.metric shouldBe "people"
    existingGot1.schema shouldBe Some(
      unionSchema
    )
  }
}
