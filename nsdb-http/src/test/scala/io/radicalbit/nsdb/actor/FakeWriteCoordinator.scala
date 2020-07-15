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
import io.radicalbit.nsdb.common.statement.DeleteSQLStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands.ExecuteDeleteStatement
import io.radicalbit.nsdb.protocol.MessageProtocol.Events.{
  DeleteStatementExecuted,
  DeleteStatementFailed,
  MetricNotFound
}

class FakeWriteCoordinator extends Actor {

  import FakeWriteCoordinator._

  def receive: Receive = {
    case ExecuteDeleteStatement(DeleteSQLStatement(db, namespace, `notExistingMetric`, _)) =>
      sender() ! DeleteStatementFailed(db,
                                       namespace,
                                       `notExistingMetric`,
                                       "metric does not exist",
                                       MetricNotFound(`notExistingMetric`))
    case ExecuteDeleteStatement(DeleteSQLStatement(db, namespace, `errorMetric`, _)) =>
      sender() ! DeleteStatementFailed(db, namespace, `errorMetric`, "generic error for tests")
    case ExecuteDeleteStatement(statement) =>
      sender() ! DeleteStatementExecuted(statement.db, statement.namespace, statement.metric)
  }
}

object FakeWriteCoordinator {
  val notExistingMetric = "notexisting"
  val errorMetric       = "errormetric"
}
