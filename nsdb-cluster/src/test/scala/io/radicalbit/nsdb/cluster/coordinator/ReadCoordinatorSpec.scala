package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.cluster.WriteInterval
import io.radicalbit.nsdb.actors.SchemaActor
import io.radicalbit.nsdb.cluster.actor.NamespaceDataActor
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{BIGINT, Schema, VARCHAR}
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest._

import scala.concurrent.Await

class ReadCoordinatorSpec
    extends TestKit(ActorSystem("nsdb-test"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with WriteInterval {

  val probe                = TestProbe()
  val probeActor           = probe.ref
  private val basePath     = "target/test_index"
  private val db           = "db"
  private val namespace    = "registry"
  val schemaActor          = system.actorOf(SchemaActor.props(basePath, db, namespace))
  val namespaceDataActor   = system.actorOf(NamespaceDataActor.props(basePath))
  val readCoordinatorActor = system actorOf ReadCoordinator.props(schemaActor, namespaceDataActor)

  val records: Seq[Bit] = Seq(
    Bit(2L, 1L, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
    Bit(4L, 1L, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
    Bit(6L, 1L, Map("name"  -> "Bill", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis())),
    Bit(8L, 1L, Map("name"  -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis())),
    Bit(10L, 1L, Map("name" -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis()))
  )

  override def beforeAll(): Unit = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(3 second)

    Await.result(namespaceDataActor ? DeleteMetric(db, namespace, "people"), 1 seconds)
    val schema = Schema(
      "people",
      Seq(SchemaField("name", VARCHAR()), SchemaField("surname", VARCHAR()), SchemaField("creationDate", BIGINT())))
    Await.result(schemaActor ? UpdateSchema(db, namespace, "people", schema), 1 seconds)
    namespaceDataActor ! AddRecords(db, namespace, "people", records)

    waitInterval
  }

  "ReadCoordinator" when {

    "receive a GetNamespace" should {
      "return it properly" in {
        probe.send(readCoordinatorActor, GetNamespaces(db))

        val expected = probe.expectMsgType[NamespacesGot]
        expected.namespaces shouldBe Seq(namespace)
      }
    }

    "receive a GetMetrics given a namespace" should {
      "return it properly" in {
        probe.send(readCoordinatorActor, GetMetrics(db, namespace))

        val expected = probe.expectMsgType[MetricsGot]
        expected.namespace shouldBe namespace
        expected.metrics shouldBe Seq("people")
      }
    }

    "receive a GetSchema given a namespace and a metric" should {
      "return it properly" in {
        probe.send(readCoordinatorActor, GetSchema(db, namespace, "people"))

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
        val expected = probe.expectMsgType[SelectStatementExecuted]

        expected.values shouldBe records
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

        val expected = probe.expectMsgType[SelectStatementExecuted]

        expected.values shouldBe Seq(
          Bit(2L, 1L, Map("name"  -> "John", "surname"  -> "Doe")),
          Bit(4L, 1L, Map("name"  -> "John", "surname"  -> "Doe")),
          Bit(6L, 1L, Map("name"  -> "Bill", "surname"  -> "Doe")),
          Bit(8L, 1L, Map("name"  -> "Frank", "surname" -> "Doe")),
          Bit(10L, 1L, Map("name" -> "Frank", "surname" -> "Doe"))
        )
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

        val expected = probe.expectMsgType[SelectStatementExecuted]

        expected.values.size should be(2)
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

        val expected = probe.expectMsgType[SelectStatementExecuted]

        expected.values.size shouldBe 1
        expected.values.head shouldBe Bit(10, 1, Map("name" -> "Frank"))
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

        val expected = probe.expectMsgType[SelectStatementExecuted]

        expected.values.size should be(4)

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

        val expected = probe.expectMsgType[SelectStatementExecuted]

        expected.values.size should be(1)
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
        val expected = probe.expectMsgType[SelectStatementExecuted]
        expected.values.size should be(5)
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

        val expected = probe.expectMsgType[SelectStatementExecuted]

        expected.values.size should be(1)
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
                expression2 = EqualityExpression(dimension = "name", value = "John")
              ))),
              limit = Some(LimitOperator(5))
            )
          )
        )
        val expected = probe.expectMsgType[SelectStatementExecuted]
        expected.values.size should be(2)
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

        val expected = probe.expectMsgType[SelectStatementExecuted]

        expected.values.size should be(3)
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

        probe.expectMsgType[SelectStatementFailed]
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

        probe.expectMsgType[SelectStatementFailed]
      }
    }
  }
}
