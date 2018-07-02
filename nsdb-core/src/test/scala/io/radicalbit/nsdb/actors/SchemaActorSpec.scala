/*
 * Copyright 2018 Radicalbit S.r.l.
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

package io.radicalbit.nsdb.actors

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model._
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

    Await.result(
      schemaActor ? UpdateSchemaFromRecord(
        db = "db",
        namespace = "namespace",
        metric = "people",
        record = Bit(timestamp = 0, value = 1, dimensions = Map("name" -> "name"), tags = Map("city" -> "city"))),
      3 seconds
    )
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
      Schema(
        "people",
        Set(
          SchemaField("name", DimensionFieldType, VARCHAR()),
          SchemaField("timestamp", TimestampFieldType, BIGINT()),
          SchemaField("value", ValueFieldType, INT()),
          SchemaField("city", TagFieldType, VARCHAR())
        )
      ))
  }

  "SchemaActor" should "return a failed message when trying to update a schema with an incompatible one" in {
    probe.send(
      schemaActor,
      UpdateSchemaFromRecord(
        "db",
        "namespace",
        "people",
        Bit(0, 23.5, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy")))
    )

    probe.expectMsgType[UpdateSchemaFailed]
  }

  "SchemaActor" should "update schemas coming from a record" in {
    probe.send(schemaActor,
               UpdateSchemaFromRecord(
                 "db",
                 "namespace",
                 "people",
                 Bit(0, 23, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy"))))

    val schema = probe.expectMsgType[SchemaUpdated].schema
    schema.fields.exists(_.name == "timestamp") shouldBe true
    schema.fields.exists(_.name == "value") shouldBe true

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema(
        "people",
        Set(
          SchemaField("timestamp", TimestampFieldType, BIGINT()),
          SchemaField("value", ValueFieldType, INT()),
          SchemaField("name", DimensionFieldType, VARCHAR()),
          SchemaField("surname", DimensionFieldType, VARCHAR()),
          SchemaField("city", TagFieldType, VARCHAR()),
          SchemaField("country", TagFieldType, VARCHAR())
        )
      )
    )

    probe.send(schemaActor,
               UpdateSchemaFromRecord("db", "namespace", "noDimensions", Bit(0, 23.5, Map.empty, Map.empty)))

    probe.expectMsgType[SchemaUpdated]
  }

  "SchemaActor" should "return the same schema for a new schema included in the old one" in {
    probe.send(schemaActor,
               UpdateSchemaFromRecord(
                 "db",
                 "namespace",
                 "people",
                 Bit(0, 23, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy"))))

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema(
        "people",
        Set(
          SchemaField("timestamp", TimestampFieldType, BIGINT()),
          SchemaField("value", ValueFieldType, INT()),
          SchemaField("name", DimensionFieldType, VARCHAR()),
          SchemaField("surname", DimensionFieldType, VARCHAR()),
          SchemaField("city", TagFieldType, VARCHAR()),
          SchemaField("country", TagFieldType, VARCHAR())
        )
      )
    )

    probe.send(
      schemaActor,
      UpdateSchemaFromRecord("db", "namespace", "people", Bit(0, 2, Map("name" -> "john"), Map("country" -> "italy"))))
    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val newGot = probe.expectMsgType[SchemaGot]
    newGot.metric shouldBe "people"
    newGot.schema shouldBe existingGot.schema
  }

  "SchemaActor" should "drop a schema" in {

    implicit val timeout = Timeout(3 seconds)

    probe.send(schemaActor,
               UpdateSchemaFromRecord(
                 "db",
                 "namespace",
                 "people",
                 Bit(0, 23, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy"))))

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema(
        "people",
        Set(
          SchemaField("timestamp", TimestampFieldType, BIGINT()),
          SchemaField("value", ValueFieldType, INT()),
          SchemaField("name", DimensionFieldType, VARCHAR()),
          SchemaField("surname", DimensionFieldType, VARCHAR()),
          SchemaField("city", TagFieldType, VARCHAR()),
          SchemaField("country", TagFieldType, VARCHAR())
        )
      )
    )

    probe.send(
      schemaActor,
      UpdateSchemaFromRecord(
        "db",
        "namespace",
        "offices",
        Bit(0, 23, Map("name" -> "john", "surname" -> "doe"), Map("city" -> "milano", "country" -> "italy")))
    )

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("db", "namespace", "offices"))

    val schema = probe.expectMsgType[SchemaGot]
    schema.metric shouldBe "offices"
    schema.schema shouldBe Some(
      Schema(
        "offices",
        Set(
          SchemaField("timestamp", TimestampFieldType, BIGINT()),
          SchemaField("value", ValueFieldType, INT()),
          SchemaField("name", DimensionFieldType, VARCHAR()),
          SchemaField("surname", DimensionFieldType, VARCHAR()),
          SchemaField("city", TagFieldType, VARCHAR()),
          SchemaField("country", TagFieldType, VARCHAR())
        )
      )
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
