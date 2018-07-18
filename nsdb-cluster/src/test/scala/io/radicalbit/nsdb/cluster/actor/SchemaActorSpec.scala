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

package io.radicalbit.nsdb.cluster.actor

import akka.actor.{Actor, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol._
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest._

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

class FakeSchemaCache extends Actor {

  val schemas: mutable.Map[(String, String, String), Schema] = mutable.Map.empty

  def receive: Receive = {
    case PutSchemaInCache(db, namespace, metric, value) =>
      schemas += ((db, namespace, metric) -> value)
      sender ! SchemaCached(db, namespace, metric, Some(value))
    case GetSchemaFromCache(db, namespace, metric) =>
      sender ! SchemaCached(db, namespace, metric, schemas.get((db, namespace, metric)))
  }
}

class SchemaActorSpec
    extends TestKit(ActorSystem("SchemaActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val probe       = TestProbe()
  val schemaActor = system.actorOf(SchemaActor.props("target/test_index/schema_actor", TestProbe().ref))

  def baseSchema(metric: String) = Schema(
    metric,
    Set(
      SchemaField("name", DimensionFieldType, VARCHAR()),
      SchemaField("timestamp", TimestampFieldType, BIGINT()),
      SchemaField("value", ValueFieldType, INT()),
      SchemaField("city", TagFieldType, VARCHAR())
    )
  )

  def decimalValueSchema(metric: String) = Schema(
    metric,
    Set(
      SchemaField("name", DimensionFieldType, VARCHAR()),
      SchemaField("timestamp", TimestampFieldType, BIGINT()),
      SchemaField("value", ValueFieldType, DECIMAL()),
      SchemaField("city", TagFieldType, VARCHAR())
    )
  )

  before {
    implicit val timeout = Timeout(3 seconds)
    Await.result(schemaActor ? DeleteSchema("db", "namespace", "people"), 3 seconds)
    Await
      .result((schemaActor ? GetSchema("db", "namespace", "people")).mapTo[SchemaGot], 3 seconds)
      .schema
      .isDefined shouldBe false

    Await.result(
      schemaActor ? UpdateSchema(db = "db",
                                 namespace = "namespace",
                                 metric = "people",
                                 newSchema = baseSchema("people")),
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
    existingGot.schema shouldBe Some(baseSchema("people"))

  }

  "SchemaActor" should "update a schema" in {
    probe.send(schemaActor, UpdateSchema("db", "namespace", "people", decimalValueSchema("people")))

    val schema = probe.expectMsgType[SchemaUpdated].schema
    schema.fields.exists(_.name == "timestamp") shouldBe true
    schema.fields.exists(_.name == "value") shouldBe true

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(decimalValueSchema("people"))
  }

  "SchemaActor" should "drop a schema" in {

    implicit val timeout = Timeout(3 seconds)

    probe.send(schemaActor, UpdateSchema("db", "namespace", "people", baseSchema("people")))

    probe.expectMsgType[SchemaUpdated]

    probe.send(schemaActor, GetSchema("db", "namespace", "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(baseSchema("people"))

    probe.send(
      schemaActor,
      UpdateSchema("db", "namespace", "offices", baseSchema("offices"))
    )

    probe.expectMsg(SchemaUpdated("db", "namespace", "offices", baseSchema("offices")))

    probe.send(schemaActor, GetSchema("db", "namespace", "offices"))

    val schema = probe.expectMsgType[SchemaGot]
    schema.metric shouldBe "offices"
    schema.schema shouldBe Some(baseSchema("offices"))

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
