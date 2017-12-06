package io.radicalbit.nsdb.cluster.coordinator

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.actors.SchemaActor
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor.AddRecordToLocation
import io.radicalbit.nsdb.cluster.index.Location
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{BIGINT, Schema, VARCHAR}
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class ReadCoordinatorShardSpec
    extends TestKit(
      ActorSystem("nsdb-test",
                  ConfigFactory
                    .load()
                    .withValue("nsdb.sharding.enabled", ConfigValueFactory.fromAnyRef(true))))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val probe                = TestProbe()
  val probeActor           = probe.ref
  private val basePath     = "target/test_index/ReadCoordinatorShardSpec"
  private val db           = "db"
  private val namespace    = "registry"
  val schemaActor          = system.actorOf(SchemaActor.props(basePath, db, namespace))
  val namespaceDataActor   = system.actorOf(NamespaceDataActor.props(basePath))
  val readCoordinatorActor = system actorOf ReadCoordinator.props(null, schemaActor)

  val recordsShard1: Seq[Bit] = Seq(
    Bit(2L, 1L, Map("name" -> "John", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis())),
    Bit(4L, 1L, Map("name" -> "Jon", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis()))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(11L, 1L, Map("name" -> "Bill", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
    Bit(12L, 1L, Map("name" -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis())),
    Bit(13L, 1L, Map("name" -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis()))
  )

  val records = recordsShard1 ++ recordsShard2

  override def beforeAll = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(5 second)

    Await.result(readCoordinatorActor ? SubscribeNamespaceDataActor(namespaceDataActor, Some("node1")), 3 seconds)
    Await.result(namespaceDataActor ? DropMetric(db, namespace, "people"), 3 seconds)

    val interval = FiniteDuration(
      system.settings.config.getDuration("nsdb.write.scheduler.interval", TimeUnit.SECONDS),
      TimeUnit.SECONDS)

    expectNoMessage(interval)

    val schema = Schema(
      "people",
      Seq(SchemaField("name", VARCHAR()), SchemaField("surname", VARCHAR()), SchemaField("creationDate", BIGINT())))
    Await.result(schemaActor ? UpdateSchema(db, namespace, "people", schema), 3 seconds)

    val location1 = Location("people", "node1", 0, 10)
    val location2 = Location("people", "node1", 11, 20)

    recordsShard1.foreach(r =>
      Await.result(namespaceDataActor ? AddRecordToLocation(db, namespace, "people", r, location1), 3 seconds))
    recordsShard2.foreach(r =>
      Await.result(namespaceDataActor ? AddRecordToLocation(db, namespace, "people", r, location2), 3 seconds))

    expectNoMessage(interval)
  }

  "ReadCoordinator in shard mode" when {

    "receive a GetNamespace" should {
      "return it properly" in {
        probe.send(readCoordinatorActor, GetNamespaces(db))

        within(5 seconds) {
          val expected = probe.expectMsgType[NamespacesGot]
          expected.namespaces shouldBe Set(namespace)
        }

      }
    }

    "receive a GetMetrics given a namespace" should {
      "return it properly" in {
        probe.send(readCoordinatorActor, GetMetrics(db, namespace))

        within(5 seconds) {
          val expected = probe.expectMsgType[MetricsGot]
          expected.namespace shouldBe namespace
          expected.metrics shouldBe Set("people")
        }
      }
    }

    "receive a GetSchema given a namespace and a metric" should {
      "return it properly" in {
        probe.send(readCoordinatorActor, GetSchema(db, namespace, "people"))

        within(5 seconds) {
          val expected = probe.expectMsgType[SchemaGot]
          expected.namespace shouldBe namespace
          expected.metric shouldBe "people"
          expected.schema shouldBe Some(
            Schema("people",
                   Seq(SchemaField("name", VARCHAR()),
                       SchemaField("surname", VARCHAR()),
                       SchemaField("creationDate", BIGINT()))))
        }
      }
    }

    "receive a select projecting a wildcard" should {
      "execute it successfully" in {

        probe.send(readCoordinatorActor,
                   ExecuteStatement(
                     SelectSQLStatement(db = db,
                                        namespace = namespace,
                                        metric = "people",
                                        fields = AllFields,
                                        limit = Some(LimitOperator(5)))
                   ))
        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size shouldBe 5

          expected.values.sortBy(_.timestamp) shouldBe records
        }
      }
    }

    "receive a select projecting a wildcard with a limit" should {
      "execute it successfully" in {

        probe.send(readCoordinatorActor,
                   ExecuteStatement(
                     SelectSQLStatement(db = db,
                                        namespace = namespace,
                                        metric = "people",
                                        fields = AllFields,
                                        limit = Some(LimitOperator(2)))
                   ))
        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size shouldBe 2
        }
      }
    }

    "receive a select projecting a wildcard with a limit and a ordering" should {
      "execute it successfully when ordered by timestamp" in {

        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = "people",
                               fields = AllFields,
                               limit = Some(LimitOperator(2)),
                               order = Some(DescOrderOperator("timestamp")))
          )
        )
        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size shouldBe 2
          expected.values shouldBe recordsShard2.tail.reverse
        }
      }

      "execute it successfully when ordered by another dimension" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = "people",
                               fields = AllFields,
                               limit = Some(LimitOperator(2)),
                               order = Some(DescOrderOperator("name")))
          )
        )
        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size shouldBe 2
          expected.values shouldBe recordsShard1.reverse
        }
      }
    }

    "receive a select projecting a list of fields" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("name", None), Field("surname", None))),
              limit = Some(LimitOperator(5))
            )
          )
        )

        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]

          expected.values.sortBy(_.timestamp) shouldBe Seq(
            Bit(2L, 1L, Map("name"  -> "John", "surname"  -> "Doe")),
            Bit(4L, 1L, Map("name"  -> "Jon", "surname"   -> "Doe")),
            Bit(11L, 1L, Map("name" -> "Bill", "surname"  -> "Doe")),
            Bit(12L, 1L, Map("name" -> "Frank", "surname" -> "Doe")),
            Bit(13L, 1L, Map("name" -> "Frank", "surname" -> "Doe"))
          )
        }
      }
    }

    "receive a select containing a range selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]

          expected.values.size should be(2)
        }
      }
    }

    "receive a select containing a GTE selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]

          expected.values.size shouldBe 3
          expected.values shouldBe Seq(
            Bit(11L, 1L, Map("name" -> "Bill")),
            Bit(12L, 1L, Map("name" -> "Frank")),
            Bit(13L, 1L, Map("name" -> "Frank"))
          )
        }
      }
    }

    "receive a select containing a GTE and a NOT selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("name", None))),
              condition = Some(
                Condition(
                  UnaryLogicalExpression(
                    ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L),
                    NotOperator
                  ))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]

          expected.values.size should be(2)
          expected.values shouldBe Seq(
            Bit(2L, 1L, Map("name" -> "John")),
            Bit(4L, 1L, Map("name" -> "Jon"))
          )
        }
      }
    }

    "receive a select containing a GT AND a LTE selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterThanOperator, value = 2L),
                operator = AndOperator,
                expression2 =
                  ComparisonExpression(dimension = "timestamp", comparison = LessOrEqualToOperator, value = 4l)
              ))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]

          expected.values.size should be(1)
        }
      }
    }

    "receive a select containing a GTE OR a LT selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = OrOperator,
                expression2 = ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L)
              ))),
              limit = Some(LimitOperator(5))
            )
          )
        )
        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size should be(5)
        }
      }
    }

    "receive a select containing a = selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(EqualityExpression(dimension = "timestamp", value = 2L))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]

          expected.values.size should be(1)
        }
      }
    }

    "receive a select containing a GTE AND a = selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(expression = TupledLogicalExpression(
                expression1 =
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                operator = AndOperator,
                expression2 = EqualityExpression(dimension = "name", value = "Frank")
              ))),
              limit = Some(LimitOperator(5))
            )
          )
        )
        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size should be(2)
        }
      }
    }

    "receive a select containing a GTE selection and a group by" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("value", Some(SumAggregation)))),
              condition = Some(Condition(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L))),
              groupBy = Some("name")
            )
          )
        )

        within(5 seconds) {
          val expected = probe.expectMsgType[SelectStatementExecuted]
          expected.values.size should be(6)
        }
      }
    }

    "receive a select containing a GTE selection and a group by without any aggregation" should {
      "fail" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = "people",
              fields = ListFields(List(Field("creationDate", None))),
              condition = Some(Condition(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L))),
              groupBy = Some("name")
            )
          )
        )
        within(5 seconds) {
          probe.expectMsgType[SelectStatementFailed]
        }
      }
    }

    "receive a select containing a non existing entity" should {
      "return an error message properly" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = "nonexisting",
                               fields = AllFields,
                               limit = Some(LimitOperator(5)))
          )
        )
        within(5 seconds) {
          probe.expectMsgType[SelectStatementFailed]
        }
      }
    }
  }
}
