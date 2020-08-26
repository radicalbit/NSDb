/*
 * Copyright 2018-2020 Radicalbit S.r.l.
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
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.RangeExpression
import io.radicalbit.nsdb.model.{Schema, TimeContext}
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._
import io.radicalbit.nsdb.statement.StatementParser

class FakeReadCoordinator extends Actor {
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
    case ValidateStatement(statement, _) =>
      implicit val timeContext: TimeContext = TimeContext()
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
    case ExecuteStatement(statement, _)
        if statement.condition.isDefined && statement.condition.get.expression.isInstanceOf[RangeExpression] =>
      implicit val timeContext: TimeContext = TimeContext()
      val schema                            = Schema(statement.metric, bits.head)
      StatementParser.parseStatement(statement, schema) match {
        case Right(_) =>
          val e = statement.condition.get.expression.asInstanceOf[RangeExpression]
          val filteredBits = bits.filter(bit =>
            bit.timestamp <= e.value2.value.rawValue.toString.toLong && bit.timestamp >= e.value1.value.rawValue.toString.toLong)
          sender ! SelectStatementExecuted(statement, filteredBits, schema)
        case Left(errorMessage) => sender ! SelectStatementFailed(statement, errorMessage)
      }
    case ExecuteStatement(statement, _) =>
      implicit val timeContext: TimeContext = TimeContext()
      val schema                            = Schema(statement.metric, bits.head)
      StatementParser.parseStatement(statement, schema) match {
        case Right(_) =>
          sender ! SelectStatementExecuted(statement, bits, schema)
        case Left(errorMessage) => sender ! SelectStatementFailed(statement, errorMessage)
      }
  }
}

object FakeReadCoordinator {

  sealed trait FakeReadCoordinatorData {
    def dbs: Set[String]
    def namespaces: Set[String]
    def metrics: Set[String]
    def schemas: Map[String, Schema]

    def bits: Seq[Bit]
  }

  object Data extends FakeReadCoordinatorData {
    val dbs        = Set("db1", "db2")
    val namespaces = Set("namespace1", "namespace2")
    val metrics    = Set("metric1", "metric2")

    val dummyBit = Bit(0, 0, Map("dim1" -> "dime1", "dim2" -> 1, "dim3" -> 1L), Map.empty)

    val schemas = Map(
      "metric1" -> Schema(
        "metric1",
        dummyBit
      ),
      "metricWithoutInfo" -> Schema(
        "metricWithoutInfo",
        dummyBit
      ),
      "metric2" -> Schema(
        "metric2",
        dummyBit
      )
    )

    val bits = Seq(Bit(0, 1, Map("name" -> "name", "number" -> 2), Map("country" -> "country")),
                   Bit(2, 3, Map("name" -> "name", "number" -> 2), Map("country" -> "country")))
  }

}
