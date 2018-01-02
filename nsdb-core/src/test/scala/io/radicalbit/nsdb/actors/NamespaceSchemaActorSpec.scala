package io.radicalbit.nsdb.actors

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.index.{Schema, VARCHAR}
import io.radicalbit.nsdb.model.SchemaField
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
    TestActorRef[NamespaceSchemaActor](NamespaceSchemaActor.props("target/test_index_schema_actor"))

  val db         = "db"
  val namespace  = "namespace"
  val namespace1 = "namespace1"

  before {
    implicit val timeout = Timeout(3 seconds)
    Await.result(namespaceSchemaActor ? DeleteNamespace(db, namespace), 3 seconds)
    Await.result(namespaceSchemaActor ? DeleteNamespace(db, namespace1), 3 seconds)
    Await.result(namespaceSchemaActor ? UpdateSchema(db,
                                                     namespace,
                                                     "people",
                                                     Schema("people", Seq(SchemaField("name", VARCHAR())))),
                 3 seconds)
    Await.result(namespaceSchemaActor ? UpdateSchema(db,
                                                     namespace1,
                                                     "people",
                                                     Schema("people", Seq(SchemaField("surname", VARCHAR())))),
                 3 seconds)
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
    existingGot.schema shouldBe Some(Schema("people", Seq(SchemaField("name", VARCHAR()))))

    probe.send(namespaceSchemaActor, GetSchema(db, namespace1, "people"))

    val existingGot1 = probe.expectMsgType[SchemaGot]
    existingGot1.metric shouldBe "people"
    existingGot1.schema shouldBe Some(Schema("people", Seq(SchemaField("surname", VARCHAR()))))
  }

  "SchemaActor" should "update schemas in case of success in different namespaces" in {
    probe.send(namespaceSchemaActor,
               UpdateSchema(db, namespace, "people", Schema("people", Seq(SchemaField("surname", VARCHAR())))))

    probe.expectMsgType[SchemaUpdated]

    probe.send(namespaceSchemaActor, GetSchema(db, namespace, "people"))

    val existingGot = probe.expectMsgType[SchemaGot]
    existingGot.metric shouldBe "people"
    existingGot.schema shouldBe Some(
      Schema("people", Seq(SchemaField("name", VARCHAR()), SchemaField("surname", VARCHAR())))
    )

    probe.send(namespaceSchemaActor,
               UpdateSchema(db, namespace1, "people", Schema("people", Seq(SchemaField("name", VARCHAR())))))

    probe.expectMsgType[SchemaUpdated]

    probe.send(namespaceSchemaActor, GetSchema(db, namespace, "people"))

    val existingGot1 = probe.expectMsgType[SchemaGot]
    existingGot1.metric shouldBe "people"
    existingGot1.schema shouldBe Some(
      Schema("people", Seq(SchemaField("name", VARCHAR()), SchemaField("surname", VARCHAR())))
    )
  }
}
