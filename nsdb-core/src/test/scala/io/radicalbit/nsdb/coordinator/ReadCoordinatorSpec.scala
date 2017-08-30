package io.radicalbit.nsdb.coordinator

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.radicalbit.nsdb.actors.NamespaceDataActor.commands.{AddRecords, DeleteMetric}
import io.radicalbit.nsdb.actors.NamespaceSchemaActor.commands.UpdateSchema
import io.radicalbit.nsdb.actors.{IndexerActor, SchemaActor}
import io.radicalbit.nsdb.common.protocol.{Record, RecordOut}
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.coordinator.ReadCoordinator._
import io.radicalbit.nsdb.index.{BIGINT, Schema, VARCHAR}
import io.radicalbit.nsdb.model.SchemaField
import org.scalatest._

import scala.concurrent.Await

class ReadCoordinatorSpec
    extends TestKit(ActorSystem("nsdb-test"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val probe                = TestProbe()
  val probeActor           = probe.ref
  private val basePath     = "target/test_index"
  private val namespace    = "namespace"
  val schemaActor          = system.actorOf(SchemaActor.props(basePath, namespace))
  val indexerActor         = system.actorOf(IndexerActor.props(basePath, namespace))
  val readCoordinatorActor = system actorOf ReadCoordinator.props(schemaActor, indexerActor)

  val records: Seq[Record] = Seq(
    Record(2L, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis()), 1L),
    Record(4L, Map("name"  -> "John", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis()), 1L),
    Record(6L, Map("name"  -> "Bill", "surname"  -> "Doe", "creationDate" -> System.currentTimeMillis()), 1L),
    Record(8L, Map("name"  -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis()), 1L),
    Record(10L, Map("name" -> "Frank", "surname" -> "Doe", "creationDate" -> System.currentTimeMillis()), 1L)
  )

  override def beforeAll(): Unit = {
    import scala.concurrent.duration._
    implicit val timeout = Timeout(3 second)

    Await.result(indexerActor ? DeleteMetric(namespace, "people"), 1 seconds)
    val schema = Schema(
      "people",
      Seq(SchemaField("name", VARCHAR()), SchemaField("surname", VARCHAR()), SchemaField("creationDate", BIGINT())))
    Await.result(schemaActor ? UpdateSchema(namespace, "people", schema), 1 seconds)
    indexerActor ! AddRecords(namespace, "people", records)
  }

  "A statement parser instance" when {

    "receive a select projecting a wildcard" should {
      "execute it successfully" in {

        probe.send(readCoordinatorActor,
                   ExecuteStatement(
                     SelectSQLStatement(namespace = "registry",
                                        metric = "people",
                                        fields = AllFields,
                                        limit = Some(LimitOperator(5)))
                   ))
        val expected = probe.expectMsgType[SelectStatementExecuted[RecordOut]]

        expected.values.size should be(5)
      }
    }

    "receive a select projecting a list of fields" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              namespace = "registry",
              metric = "people",
              fields = ListFields(List(Field("name", None), Field("surname", None), Field("creationDate", None))),
              limit = Some(LimitOperator(5))
            )
          )
        )

        val expected = probe.expectMsgType[SelectStatementExecuted[RecordOut]]

        expected.values.size should be(5)
      }
    }

    "receive a select containing a range selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              namespace = "registry",
              metric = "people",
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        val expected = probe.expectMsgType[SelectStatementExecuted[RecordOut]]

        expected.values.size should be(2)
      }
    }

    "receive a select containing a GTE selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              namespace = "registry",
              metric = "people",
              fields = ListFields(List(Field("name", None))),
              condition = Some(Condition(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
              limit = Some(LimitOperator(4))
            )
          )
        )

        val expected = probe.expectMsgType[SelectStatementExecuted[RecordOut]]

        expected.values.size shouldBe 1
        expected.values.head shouldBe RecordOut(records(4))
      }
    }

    "receive a select containing a GTE and a NOT selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              namespace = "registry",
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

        val expected = probe.expectMsgType[SelectStatementExecuted[RecordOut]]

        expected.values.size should be(4)

      }
    }

    "receive a select containing a GT AND a LTE selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              namespace = "registry",
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

        val expected = probe.expectMsgType[SelectStatementExecuted[RecordOut]]

        expected.values.size should be(1)
      }
    }

    "receive a select containing a GTE OR a LT selection" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              namespace = "registry",
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
        val expected = probe.expectMsgType[SelectStatementExecuted[RecordOut]]
        expected.values.size should be(5)
      }
    }

    "receive a select containing a GTE selection and a group by" should {
      "execute it successfully" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              namespace = "registry",
              metric = "people",
              fields = ListFields(List(Field("creationDate", Some(SumAggregation)))),
              condition = Some(Condition(
                ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L))),
              groupBy = Some("name")
            )
          )
        )

        val expected = probe.expectMsgType[SelectStatementExecuted[RecordOut]]

        expected.values.size should be(3)
      }
    }

    "receive a select containing a GTE selection and a group by without any aggregation" should {
      "fail" in {
        probe.send(
          readCoordinatorActor,
          ExecuteStatement(
            SelectSQLStatement(
              namespace = "registry",
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
        probe.send(readCoordinatorActor,
                   ExecuteStatement(
                     SelectSQLStatement(namespace = "registry",
                                        metric = "nonexisting",
                                        fields = AllFields,
                                        limit = Some(LimitOperator(5)))
                   ))

        probe.expectMsgType[SelectStatementFailed]
      }
    }
  }
}
