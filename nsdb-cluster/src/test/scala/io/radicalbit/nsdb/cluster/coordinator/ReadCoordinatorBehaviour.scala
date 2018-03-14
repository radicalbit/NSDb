package io.radicalbit.nsdb.cluster.coordinator

import akka.actor.ActorRef
import akka.testkit.{TestKit, TestProbe}
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.index.{BIGINT, DECIMAL, VARCHAR}
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

  object LongMetric {

    val name = "longMetric"

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
  }

  object DoubleMetric {

    val name = "doubleMetric"

    val recordsShard1: Seq[Bit] = Seq(
      Bit(2L, 1.5, Map("name" -> "John", "surname" -> "Doe")),
      Bit(4L, 1.5, Map("name" -> "John", "surname" -> "Doe"))
    )

    val recordsShard2: Seq[Bit] = Seq(
      Bit(6L, 1.5, Map("name"  -> "Bill", "surname"    -> "Doe")),
      Bit(8L, 1.5, Map("name"  -> "Frank", "surname"   -> "Doe")),
      Bit(10L, 1.5, Map("name" -> "Frankie", "surname" -> "Doe"))
    )

    val testRecords = recordsShard1 ++ recordsShard2
  }

  object AggregationMetric {

    val name = "aggregationMetric"

    val recordsShard1: Seq[Bit] = Seq(
      Bit(2L, 2L, Map("name" -> "John", "surname" -> "Doe", "age" -> 15L, "height" -> 30.5)),
      Bit(4L, 2L, Map("name" -> "John", "surname" -> "Doe", "age" -> 20L, "height" -> 30.5))
    )

    val recordsShard2: Seq[Bit] = Seq(
      Bit(2L, 1L, Map("name"  -> "John", "surname"    -> "Doe", "age" -> 15L, "height" -> 30.5)),
      Bit(6L, 1L, Map("name"  -> "Bill", "surname"    -> "Doe", "age" -> 15L, "height" -> 31.0)),
      Bit(8L, 1L, Map("name"  -> "Frank", "surname"   -> "Doe", "age" -> 15L, "height" -> 32.0)),
      Bit(10L, 1L, Map("name" -> "Frankie", "surname" -> "Doe", "age" -> 15L, "height" -> 32.0))
    )

    val testRecords = recordsShard1 ++ recordsShard2
  }

  def defaultBehaviour {
    "ReadCoordinator" when {

      "receive a GetNamespace" should {
        "return it properly" in within(5.seconds) {
          probe.send(readCoordinatorActor, GetNamespaces(db))

          awaitAssert {
            val expected = probe.expectMsgType[NamespacesGot]
            expected.namespaces shouldBe Set(namespace)
          }

        }
      }

      "receive a GetMetrics given a namespace" should {
        "return it properly" in within(5.seconds) {
          probe.send(readCoordinatorActor, GetMetrics(db, namespace))

          awaitAssert {
            val expected = probe.expectMsgType[MetricsGot]
            expected.namespace shouldBe namespace
            expected.metrics shouldBe Set(LongMetric.name, DoubleMetric.name, AggregationMetric.name)
          }
        }
      }

      "receive a GetSchema given a namespace and a metric" should {
        "return it properly" in within(5.seconds) {
          probe.send(readCoordinatorActor, GetSchema(db, namespace, LongMetric.name))

          awaitAssert {
            val expected = probe.expectMsgType[SchemaGot]
            expected.namespace shouldBe namespace
            expected.metric shouldBe LongMetric.name
            expected.schema shouldBe defined

            expected.schema.get.fields.toSeq.sortBy(_.name) shouldBe
              Seq(
                SchemaField("name", VARCHAR()),
                SchemaField("surname", VARCHAR()),
                SchemaField("timestamp", BIGINT()),
                SchemaField("value", BIGINT())
              )
          }

          probe.send(readCoordinatorActor, GetSchema(db, namespace, DoubleMetric.name))

          awaitAssert {
            val expected = probe.expectMsgType[SchemaGot]
            expected.namespace shouldBe namespace
            expected.metric shouldBe DoubleMetric.name
            expected.schema shouldBe defined

            expected.schema.get.fields.toSeq.sortBy(_.name) shouldBe
              Seq(
                SchemaField("name", VARCHAR()),
                SchemaField("surname", VARCHAR()),
                SchemaField("timestamp", BIGINT()),
                SchemaField("value", DECIMAL())
              )
          }
        }
      }

      "receive a select distinct over a single field" should {
        "execute it successfully" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = true,
                fields = ListFields(List(Field("name", None))),
                limit = Some(LimitOperator(5))
              )
            )
          )
          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            val names    = expected.values.flatMap(_.dimensions.values.map(_.asInstanceOf[String]))
            names.contains("Bill") shouldBe true
            names.contains("Frank") shouldBe true
            names.contains("Frankie") shouldBe true
            names.contains("John") shouldBe true
            names.size shouldBe 4
          }
        }
        "execute successfully with limit over distinct values" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = true,
                fields = ListFields(List(Field("name", None))),
                limit = Some(LimitOperator(2))
              )
            )
          )
          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            val names    = expected.values.flatMap(_.dimensions.values.map(_.asInstanceOf[String]))
            names.size shouldBe 2
          }
        }

        "execute successfully with ascending order" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = true,
                fields = ListFields(List(Field("name", None))),
                order = Some(AscOrderOperator("name")),
                limit = Some(LimitOperator(5))
              )
            )
          )
          awaitAssert {

            probe.expectMsgType[SelectStatementExecuted].values shouldBe Seq(
              Bit(0L, 0L, Map("name" -> "Bill")),
              Bit(0L, 0L, Map("name" -> "Frank")),
              Bit(0L, 0L, Map("name" -> "Frankie")),
              Bit(0L, 0L, Map("name" -> "John"))
            )
          }

        }

        "execute successfully with descending order" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = true,
                fields = ListFields(List(Field("name", None))),
                order = Some(DescOrderOperator("name")),
                limit = Some(LimitOperator(5))
              )
            )
          )
          awaitAssert {

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
        "execute it successfully" in within(5.seconds) {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(db = db,
                                 namespace = namespace,
                                 metric = LongMetric.name,
                                 distinct = false,
                                 fields = AllFields,
                                 limit = Some(LimitOperator(5)))
            )
          )
          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.sortBy(_.timestamp) shouldBe LongMetric.testRecords
          }
        }
        "fail if distinct" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(db = db,
                                 namespace = namespace,
                                 metric = LongMetric.name,
                                 distinct = true,
                                 fields = AllFields,
                                 limit = Some(LimitOperator(5)))
            )
          )
          awaitAssert {
            probe.expectMsgType[SelectStatementFailed]
          }
        }
      }

      "receive a select projecting a list of fields" should {
        "execute it successfully with only simple fields" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("name", None), Field("surname", None))),
                limit = Some(LimitOperator(5))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]

            expected.values.sortBy(_.timestamp) shouldBe LongMetric.testRecords
          }

        }
        "execute it successfully with mixed aggregated and simple" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("*", Some(CountAggregation)), Field("name", None))),
                limit = Some(LimitOperator(5))
              )
            )
          )
          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.sortBy(_.timestamp) shouldBe Seq(
              Bit(2L, 1, Map("name"  -> "John", "count(*)"    -> 5)),
              Bit(4L, 1, Map("name"  -> "John", "count(*)"    -> 5)),
              Bit(6L, 1, Map("name"  -> "Bill", "count(*)"    -> 5)),
              Bit(8L, 1, Map("name"  -> "Frank", "count(*)"   -> 5)),
              Bit(10L, 1, Map("name" -> "Frankie", "count(*)" -> 5))
            )
          }
        }
        "execute it successfully with only a count" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(
                  List(Field("*", Some(CountAggregation)))
                ),
                limit = Some(LimitOperator(4))
              )
            )
          )
          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0, 4L, Map("count(*)" -> 4))
            )
          }
        }
        "fail when other aggregation than count is provided" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(
                  List(Field("*", Some(CountAggregation)),
                       Field("surname", None),
                       Field("creationDate", Some(SumAggregation)))),
                limit = Some(LimitOperator(4))
              )
            )
          )
          awaitAssert {
            probe.expectMsgType[SelectStatementFailed]
          }
        }
        "fail when is select distinct" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = true,
                fields = ListFields(List(Field("name", None), Field("surname", None))),
                limit = Some(LimitOperator(5))
              )
            )
          )

          awaitAssert {
            probe.expectMsgType[SelectStatementFailed]
          }
        }
      }

      "receive a select containing a range selection" should {
        "execute it successfully" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("name", None))),
                condition = Some(Condition(RangeExpression(dimension = "timestamp", value1 = 2L, value2 = 4L))),
                limit = Some(LimitOperator(4))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]

            expected.values.size should be(2)
          }
        }
      }

      "receive a select containing a GTE selection" should {
        "execute it successfully" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("name", None))),
                condition = Some(Condition(
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 10L))),
                limit = Some(LimitOperator(4))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]

            expected.values.size shouldBe 1
            expected.values.head shouldBe Bit(10, 1, Map("name" -> "Frankie"))
          }
        }
      }

      "receive a select containing a GTE and a NOT selection" should {
        "execute it successfully" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
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

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]

            expected.values.size should be(4)
          }
        }
      }

      "receive a select containing a GT AND a LTE selection" should {
        "execute it successfully" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
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

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]

            expected.values.size should be(1)
          }
        }
      }

      "receive a select containing a GTE OR a LT selection" should {
        "execute it successfully" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
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
          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.size should be(5)
          }
        }
      }

      "receive a select containing a = selection" should {
        "execute it successfully" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("name", None))),
                condition = Some(Condition(EqualityExpression(dimension = "timestamp", value = 2L))),
                limit = Some(LimitOperator(4))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]

            expected.values.size should be(1)
          }
        }
      }

      "receive a select containing a GTE AND a = selection" should {
        "execute it successfully" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
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
          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.size should be(2)
          }
        }
      }

      "receive a select containing a GTE selection and a group by" should {
        "execute it successfully" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                condition = Some(Condition(
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L))),
                groupBy = Some("name")
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.size should be(4)
          }
        }
      }

      "receive a select containing a GTE selection and a group by without any aggregation" should {
        "fail" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("creationDate", None))),
                condition = Some(Condition(
                  ComparisonExpression(dimension = "timestamp", comparison = GreaterOrEqualToOperator, value = 2L))),
                groupBy = Some("name")
              )
            )
          )
          awaitAssert {
            probe.expectMsgType[SelectStatementFailed]
          }
        }
      }

      "receive a select containing a non existing entity" should {
        "return an error message properly" in within(5.seconds) {
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
          awaitAssert {
            probe.expectMsgType[SelectStatementFailed]
          }
        }
      }

      "receive a select containing a group by on string dimension " should {
        "execute it successfully with asc ordering over string dimension" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
                groupBy = Some("name"),
                order = Some(AscOrderOperator("name"))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0L, 1L, Map("name" -> "Bill")),
              Bit(0L, 1L, Map("name" -> "Frank")),
              Bit(0L, 1L, Map("name" -> "Frankie")),
              Bit(0L, 2L, Map("name" -> "John"))
            )
          }
        }
        "execute it successfully with desc ordering over string dimension" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("name"),
                order = Some(DescOrderOperator("name"))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0L, 2, Map("name" -> "John")),
              Bit(0L, 1, Map("name" -> "Frankie")),
              Bit(0L, 1, Map("name" -> "Frank")),
              Bit(0L, 1, Map("name" -> "Bill"))
            )
          }

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = DoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("name"),
                order = Some(DescOrderOperator("name"))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0L, 3.0, Map("name" -> "John")),
              Bit(0L, 1.5, Map("name" -> "Frankie")),
              Bit(0L, 1.5, Map("name" -> "Frank")),
              Bit(0L, 1.5, Map("name" -> "Bill"))
            )
          }
        }
        "execute it successfully with desc ordering over numerical dimension" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("name"),
                order = Some(DescOrderOperator("value"))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.map(_.value) shouldBe Seq(2, 1, 1, 1)
          }

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = DoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("name"),
                order = Some(DescOrderOperator("value"))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.map(_.value) shouldBe Seq(3.0, 1.5, 1.5, 1.5)
          }
        }
        "execute it successfully with asc ordering over numerical dimension" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = LongMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("name"),
                order = Some(AscOrderOperator("value")),
                limit = Some(LimitOperator(2))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.map(_.value) shouldBe Seq(1, 1)
          }

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = DoubleMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("name"),
                order = Some(AscOrderOperator("value")),
                limit = Some(LimitOperator(2))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values.map(_.value) shouldBe Seq(1.5, 1.5)
          }
        }

      }

      "receive a select containing a group by on long dimension" should {
        "execute it successfully with count aggregation" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = AggregationMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
                groupBy = Some("age"),
                order = Some(AscOrderOperator("value"))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(Bit(0L, 1, Map("age" -> 20)), Bit(0L, 5, Map("age" -> 15)))
          }
        }
        "execute it successfully with sum aggregation" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = AggregationMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("age"),
                order = Some(AscOrderOperator("age"))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0L, 6L, Map("age" -> 15L)),
              Bit(0L, 2L, Map("age" -> 20L))
            )
          }
        }
      }
      "receive a select containing a group by on double dimension" should {
        "execute it successfully with count aggregation" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = AggregationMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(CountAggregation)))),
                groupBy = Some("height"),
                order = Some(DescOrderOperator("value"))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0L, 3, Map("height" -> 30.5)),
              Bit(0L, 2, Map("height" -> 32.0)),
              Bit(0L, 1, Map("height" -> 31.0))
            )
          }
        }
        "execute it successfully with sum aggregation" in within(5.seconds) {
          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = AggregationMetric.name,
                distinct = false,
                fields = ListFields(List(Field("value", Some(SumAggregation)))),
                groupBy = Some("height"),
                order = Some(AscOrderOperator("height"))
              )
            )
          )

          awaitAssert {
            val expected = probe.expectMsgType[SelectStatementExecuted]
            expected.values shouldBe Seq(
              Bit(0L, 5, Map("height" -> 30.5)),
              Bit(0L, 1, Map("height" -> 31.0)),
              Bit(0L, 2, Map("height" -> 32.0))
            )
          }
        }
      }
    }
  }

}
