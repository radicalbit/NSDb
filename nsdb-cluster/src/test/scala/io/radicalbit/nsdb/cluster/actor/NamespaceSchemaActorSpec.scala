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

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.index._
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class NamespaceSchemaActorSpec
    extends TestKit(ActorSystem("NamespaceSchemaActorSpec"))
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with OneInstancePerTest
    with BeforeAndAfter {

  val probe = TestProbe()
  val namespaceSchemaActor =
    TestActorRef[MetricsSchemaActor](MetricsSchemaActor.props("target/test_index/NamespaceSchemaActorSpec"))

  val db         = "db"
  val namespace  = "namespace"
  val namespace1 = "namespace1"

  val nameRecord    = Bit(0, 1, Map("name"    -> "name"), Map.empty)
  val surnameRecord = Bit(0, 1, Map("surname" -> "surname"), Map.empty)

  before {
    implicit val timeout = Timeout(10 seconds)
    Await.result(namespaceSchemaActor ? DeleteNamespace(db, namespace), 10 seconds)
    Await.result(namespaceSchemaActor ? DeleteNamespace(db, namespace1), 10 seconds)
    Await.result(namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace, "people", nameRecord), 10 seconds)
    Await.result(namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace1, "people", surnameRecord), 10 seconds)
  }

  "SchemaActor" should "get schemas from different namespaces" in {

    namespaceSchemaActor.underlyingActor.schemaActors.keys.size shouldBe 2

    probe.send(namespaceSchemaActor, GetSchema(db, namespace, "nonexisting"))

    val nonexistingGot = probe.expectMsgType[SchemaGot]
    nonexistingGot.metric shouldBe "nonexisting"
    nonexistingGot.schema shouldBe None

    probe.send(namespaceSchemaActor, GetSchema(db, namespace, "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people",
             Set(SchemaField("name", VARCHAR()), SchemaField("timestamp", BIGINT()), SchemaField("value", INT()))))

    probe.send(namespaceSchemaActor, GetSchema(db, namespace1, "people"))

    val existingGot1 = probe.expectMsgType[SchemaGot]
    existingGot1.metric shouldBe "people"
    existingGot1.schema shouldBe Some(
      Schema("people",
             Set(SchemaField("surname", VARCHAR()), SchemaField("timestamp", BIGINT()), SchemaField("value", INT()))))
  }

  "SchemaActor" should "update schemas in case of success in different namespaces" in {
    probe.send(namespaceSchemaActor, UpdateSchemaFromRecord(db, namespace, "people", surnameRecord))

    probe.expectMsgType[SchemaUpdated]

    probe.send(namespaceSchemaActor, GetSchema(db, namespace, "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people",
             Set(SchemaField("name", VARCHAR()),
                 SchemaField("timestamp", BIGINT()),
                 SchemaField("value", INT()),
                 SchemaField("surname", VARCHAR())))
    )

    probe.send(namespaceSchemaActor, UpdateSchemaFromRecord(db, namespace1, "people", nameRecord))

    probe.expectMsgType[SchemaUpdated]

    probe.send(namespaceSchemaActor, GetSchema(db, namespace, "people"))

    val existingGot1 = probe.expectMsgType[SchemaGot]
    existingGot1.metric shouldBe "people"
    existingGot1.schema shouldBe Some(
      Schema("people",
             Set(SchemaField("name", VARCHAR()),
                 SchemaField("timestamp", BIGINT()),
                 SchemaField("value", INT()),
                 SchemaField("surname", VARCHAR())))
    )
  }
}
