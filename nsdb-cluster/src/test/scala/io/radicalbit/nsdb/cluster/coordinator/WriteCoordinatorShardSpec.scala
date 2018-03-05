package io.radicalbit.nsdb.cluster.coordinator

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import org.scalatest._

import scala.concurrent.Await

class WriteCoordinatorShardSpec
    extends TestKit(
      ActorSystem(
        "nsdb-test",
        ConfigFactory
          .load()
          .withValue("nsdb.sharding.enabled", ConfigValueFactory.fromAnyRef(true))
          .withValue("nsdb.sharding.interval", ConfigValueFactory.fromAnyRef("5s"))
      ))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfter
    with WriteCoordinatorBehaviour {

  lazy val basePath = "target/test_index/WriteCoordinatorShardSpec"

  val db        = "writeCoordinatorSpecShardDB"
  val namespace = "namespace"

  before {
    import akka.pattern.ask

    import scala.concurrent.duration._

    implicit val timeout = Timeout(3 seconds)

    Await.result(writeCoordinatorActor ? SubscribeNamespaceDataActor(namespaceDataActor, "node1"), 3 seconds)
    Await.result(writeCoordinatorActor ? DeleteNamespace(db, namespace), 3 seconds)
    Await.result(writeCoordinatorActor ? DeleteNamespace(db, "testDelete"), 3 seconds)
    Await.result(namespaceSchemaActor ? UpdateSchemaFromRecord(db, namespace, "testMetric", record1), 3 seconds)
  }


  "WriteCoordinator in shard mode" should behave.like(defaultBehaviour)
}
