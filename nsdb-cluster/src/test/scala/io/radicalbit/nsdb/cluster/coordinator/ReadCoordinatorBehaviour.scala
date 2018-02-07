package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.ActorRef
import akka.testkit.{TestKit, TestProbe}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{BIGINT, Schema, VARCHAR}
import io.radicalbit.nsdb.model.SchemaField
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.{ExecuteStatement, GetMetrics, GetNamespaces, GetSchema}
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

trait ReadCoordinatorBehaviour { this: TestKit with WordSpecLike with Matchers =>

  val probe = TestProbe()

  def basePath: String
  def db: String
  def namespace: String

  def readCoordinatorActor: ActorRef

  val recordsShard1: Seq[Bit] = Seq(
    Bit(2L, 1L, Map("name" -> "John", "surname" -> "Doe")),
    Bit(4L, 1L, Map("name" -> "John", "surname" -> "Doe"))
  )

  val recordsShard2: Seq[Bit] = Seq(
    Bit(6L, 1L, Map("name"  -> "Bill", "surname"    -> "Doe")),
    Bit(8L, 1L, Map("name"  -> "Frank", "surname"   -> "Doe")),
    Bit(10L, 1L, Map("name" -> "Frankie", "surname" -> "Doe"))
  )

  val testRecords = recordsShard1 ++ recordsShard2

  def defaultBehaviour {
    "ReadCoordinator" when {

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
              Schema(
                "people",
                Seq(
                  SchemaField("name", VARCHAR()),
                  SchemaField("timestamp", BIGINT()),
                  SchemaField("surname", VARCHAR()),
                  SchemaField("value", BIGINT())
                )
              ))
          }
        }
      }

      "receive a select distinct over a single field" should {
        "execute it successfully" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = true,
                fields = ListFields(List(Field("name", None))),
                limit = Some(LimitOperator(5))
              )
            )
          )
          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            val names    = expected.values.flatMap(_.dimensions.values.map(_.asInstanceOf[String]))
            names.contains("Bill") shouldBe true
            names.contains("Frank") shouldBe true
            names.contains("Frankie") shouldBe true
            names.contains("John") shouldBe true
            names.size shouldBe 4
          }
        }
        "execute successfully with limit over distinct values" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = true,
                fields = ListFields(List(Field("name", None))),
                limit = Some(LimitOperator(2))
              )
            )
          )
          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            val names    = expected.values.flatMap(_.dimensions.values.map(_.asInstanceOf[String]))
            names.size shouldBe 2
          }
        }

        "execute successfully with ascending order" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = true,
                fields = ListFields(List(Field("name", None))),
                order = Some(AscOrderOperator("name")),
                limit = Some(LimitOperator(5))
              )
            )
          )
          within(5 seconds) {

            probe.expectMsgType[SelectStatementExecuted].values shouldBe Seq(
              Bit(0L, 0L, Map("name" -> "Bill")),
              Bit(0L, 0L, Map("name" -> "Frank")),
              Bit(0L, 0L, Map("name" -> "Frankie")),
              Bit(0L, 0L, Map("name" -> "John"))
            )
          }

        }

        "execute successfully with descending order" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = true,
                fields = ListFields(List(Field("name", None))),
                order = Some(DescOrderOperator("name")),
                limit = Some(LimitOperator(5))
              )
            )
          )
          within(5 seconds) {

            probe.expectMsgType[SelectStatementExecuted].values shouldBe Seq(
              Bit(0L, 0L, Map("name" -> "John")),
              Bit(0L, 0L, Map("name" -> "Frankie")),
              Bit(0L, 0L, Map("name" -> "Frank")),
              Bit(0L, 0L, Map("name" -> "Bill"))
            )
          }
        }
      }

      "receive a select projecting a wildcard" should {
        "execute it successfully" in {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(db = db,
                                 namespace = namespace,
                                 metric = "people",
                                 distinct = false,
                                 fields = AllFields,
                                 limit = Some(LimitOperator(5)))
            )
          )
          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.sortBy(_.timestamp) shouldBe testRecords
          }
        }
        "fail if distinct" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(db = db,
                                 namespace = namespace,
                                 metric = "people",
                                 distinct = true,
                                 fields = AllFields,
                                 limit = Some(LimitOperator(5)))
            )
          )
          within(5 seconds) {
            probe.expectMsgType[SelectStatementFailed]
          }
        }
      }

      "receive a select projecting a list of fields" should {
        "execute it successfully with only simple fields" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = false,
                fields = ListFields(List(Field("name", None), Field("surname", None))),
                limit = Some(LimitOperator(5))
              )
            )
          )

          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]

            expected.values.sortBy(_.timestamp) shouldBe testRecords
          }

        }
        "execute it successfully with mixed aggregated and simple" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = false,
                fields = ListFields(List(Field("*", Some(CountAggregation)), Field("name", None))),
                limit = Some(LimitOperator(5))
              )
            )
          )
          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.sortBy(_.timestamp) shouldBe Seq(
              Bit(2L, 1L, Map("name"  -> "John", "count(*)"    -> 5)),
              Bit(4L, 1L, Map("name"  -> "John", "count(*)"    -> 5)),
              Bit(6L, 1L, Map("name"  -> "Bill", "count(*)"    -> 5)),
              Bit(8L, 1L, Map("name"  -> "Frank", "count(*)"   -> 5)),
              Bit(10L, 1L, Map("name" -> "Frankie", "count(*)" -> 5))
            )
          }
        }
        "execute it successfully with only a count" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = false,
                fields = ListFields(
                  List(Field("*", Some(CountAggregation)))
                ),
                limit = Some(LimitOperator(4))
              )
            )
          )
          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0, 4L, Map("count(*)" -> 4))
            )
          }
        }
        "fail when other aggregation than count is provided" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = false,
                fields = ListFields(
                  List(Field("*", Some(CountAggregation)),
                       Field("surname", None),
                       Field("creationDate", Some(SumAggregation)))),
                limit = Some(LimitOperator(4))
              )
            )
          )
          within(5 seconds) {
            probe.expectMsgType[SelectStatementFailed]
          }
        }
        "fail when is select distinct" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = true,
                fields = ListFields(List(Field("name", None), Field("surname", None))),
                limit = Some(LimitOperator(5))
              )
            )
          )

          within(5 seconds) {
            probe.expectMsgType[SelectStatementFailed]
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
                distinct = false,
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
                distinct = false,
                fields = ListFields(List(Field("name", None))),
                condition = Some(Condition(
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
                limit = Some(LimitOperator(4))
              )
            )
          )

          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]

            expected.values.size shouldBe 1
            expected.values.head shouldBe Bit(10, 1, Map("name" -> "Frankie"))
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
                distinct = false,
                fields = ListFields(List(Field("name", None))),
                condition = Some(
                  Condition(UnaryLogicalExpression(
                    ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L),
                    NotOperator
                  ))),
                limit = Some(LimitOperator(4))
              )
            )
          )

          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]

            expected.values.size should be(4)
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
                distinct = false,
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
                distinct = false,
                fields = ListFields(List(Field("name", None))),
                condition = Some(Condition(expression = TupledLogicalExpression(
                  expression1 =
                    ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L),
                  operator = OrOperator,
                  expression2 =
                    ComparisonExpression(dimension = "timestamp", comparison = LessThanOperator, value = 4L)
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
                distinct = false,
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
                distinct = false,
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
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                condition = Some(Condition(
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L))),
                groupBy = Some("name")
              )
            )
          )

          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.size should be(4)
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
                distinct = false,
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
                                 distinct = false,
                                 fields = AllFields,
                                 limit = Some(LimitOperator(5)))
            )
          )
          within(5 seconds) {
            probe.expectMsgType[SelectStatementFailed]
          }
        }
      }

      "receive a select containing a group by" should {
        "execute it successfully with asc ordering over string dimension" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
                groupBy = Some("name"),
                order = Some(AscOrderOperator("name"))
              )
            )
          )

          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0L, 1L, Map("name" -> "Bill")),
              Bit(0L, 1L, Map("name" -> "Frank")),
              Bit(0L, 1L, Map("name" -> "Frankie")),
              Bit(0L, 2L, Map("name" -> "John"))
            )
          }
        }
        "execute it successfully with desc ordering over string dimension" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("name"),
                order = Some(DescOrderOperator("name"))
              )
            )
          )

          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0L, 2L, Map("name" -> "John")),
              Bit(0L, 1L, Map("name" -> "Frankie")),
              Bit(0L, 1L, Map("name" -> "Frank")),
              Bit(0L, 1L, Map("name" -> "Bill"))
            )
          }
        }
        "execute it successfully with desc ordering over numerical dimension" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("name"),
                order = Some(DescOrderOperator("value"))
              )
            )
          )

          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.map(_.value) shouldBe Seq(2l, 1L, 1L, 1l)
          }
        }
        "execute it successfully with asc ordering over numerical dimension" in {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = "people",
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("name"),
                order = Some(AscOrderOperator("value")),
                limit = Some(LimitOperator(2))
              )
            )
          )

          within(5 seconds) {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.map(_.value) shouldBe Seq(1l, 1L)
          }
        }

      }
    }
  }

}
