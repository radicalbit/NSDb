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

package io.radicalbit.nsdb.actor

import akka.actor.Actor
import io.radicalbit.nsdb.common.protocol.{Bit, DimensionFieldType}
import io.radicalbit.nsdb.common.statement.RangeExpression
import io.radicalbit.nsdb.index.{BIGINT, INT, VARCHAR}
import io.radicalbit.nsdb.model.{Schema, SchemaField}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser

class FakeReadCoordinator extends Actor {
  import FakeReadCoordinator._
  import FakeReadCoordinator.Data._

  override def receive: Receive = {
    case GetDbs =>
      sender() ! DbsGot(dbs)
    case GetNamespaces(db) =>
      sender() ! NamespacesGot(db, namespaces)
    case GetMetrics(db, namespace) =>
      sender ! MetricsGot(db, namespace, metrics)
    case GetSchema(db, namespace, metric) =>
      sender() ! SchemaGot(db, namespace, metric, schemas.get(metric))
    case ValidateStatement(statement) =>
      schemas.get(statement.metric) match {
        case Some(schema) =>
          StatementParser.parseStatement(statement, schema) match {
            case Right(_)  => sender ! SelectStatementValidated(statement)
            case Left(err) => sender ! SelectStatementValidationFailed(statement, err)
          }
        case None =>
          sender ! SelectStatementValidationFailed(statement,
                                                   s"metric ${statement.metric} does not exist",
                                                   MetricNotFound(statement.metric))
      }
    case ExecuteStatement(statement)
        if statement.condition.isDefined && statement.condition.get.expression.isInstanceOf[RangeExpression[Long]] =>
      StatementParser.parseStatement(
        statement,
        Schema("metric", bits.head).getOrElse(Schema("metric", Map.empty[String, SchemaField]))) match {
        case Right(_) =>
          val e = statement.condition.get.expression.asInstanceOf[RangeExpression[Long]]
          sender ! SelectStatementExecuted(statement, bitsParametrized(e.value1.value, e.value2.value))
        case Left(_) => sender ! SelectStatementFailed(statement, "statement not valid")
      }
    case ExecuteStatement(statement) =>
      StatementParser.parseStatement(
        statement,
        Schema("metric", bits.head).getOrElse(Schema("metric", Map.empty[String, SchemaField]))) match {
        case Right(_) =>
          sender ! SelectStatementExecuted(statement, bits)
        case Left(_) => sender ! SelectStatementFailed(statement, "statement not valid")
      }
  }
}

object FakeReadCoordinator {

  object Data {
    val dbs        = Set("db1", "db2")
    val namespaces = Set("namespace1", "namespace2")
    val metrics    = Set("metric1", "metric2")

    val schemas = Map(
      "metric1" -> Schema(
        "metric1",
        Map(
          "dim1" -> SchemaField("dim1", DimensionFieldType, VARCHAR()),
          "dim2" -> SchemaField("dim2", DimensionFieldType, INT()),
          "dim3" -> SchemaField("dim3", DimensionFieldType, BIGINT())
        )
      ),
      "metricWithoutInfo" -> Schema(
        "metricWithoutInfo",
        Map(
          "dim1" -> SchemaField("dim1", DimensionFieldType, VARCHAR()),
          "dim2" -> SchemaField("dim2", DimensionFieldType, INT()),
          "dim3" -> SchemaField("dim3", DimensionFieldType, BIGINT())
        )
      ),
      "metric2" -> Schema(
        "metric2",
        Map(
          "dim1" -> SchemaField("dim1", DimensionFieldType, VARCHAR()),
          "dim2" -> SchemaField("dim2", DimensionFieldType, INT()),
          "dim3" -> SchemaField("dim3", DimensionFieldType, BIGINT())
        )
      )
    )
  }

  def bitsParametrized(from: Long, to: Long): Seq[Bit] =
    Seq(Bit(from, 1, Map("name" -> "name", "number" -> 2), Map("country" -> "country")),
        Bit(to, 3, Map("name"   -> "name", "number" -> 2), Map("country" -> "country")))
  val bits = Seq(Bit(0, 1, Map("name" -> "name", "number" -> 2), Map("country" -> "country")),
                 Bit(2, 3, Map("name" -> "name", "number" -> 2), Map("country" -> "country")))
}
