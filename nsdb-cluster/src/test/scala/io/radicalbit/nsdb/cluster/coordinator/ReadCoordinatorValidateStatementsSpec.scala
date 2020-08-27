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

package io.radicalbit.nsdb.cluster.coordinator

import io.radicalbit.nsdb.cluster.coordinator.mockedData.MockedData._
import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.protocol.MessageProtocol.Commands._
import io.radicalbit.nsdb.protocol.MessageProtocol.Events._

import scala.concurrent.duration._

class ReadCoordinatorValidateStatementsSpec extends AbstractReadCoordinatorSpec {

  "ReadCoordinator" when {

    "receive a validate statement" should {
      "return a success in case the statement is valid" in within(5.seconds) {

        probe.send(
          readCoordinatorActor,
          ValidateStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = AllFields(),
                               limit = Some(LimitOperator(2)))
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementValidated]
        }
      }

      "return an error in case of a select on a non existing field" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ValidateStatement(
            SelectSQLStatement(db = db,
                               namespace = namespace,
                               metric = LongMetric.name,
                               distinct = false,
                               fields = ListFields(List(Field("non existing", None))),
                               limit = Some(LimitOperator(2)))
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementValidationFailed]
        }
      }

      "return an error in case of a wrong statement" in within(5.seconds) {
        probe.send(
          readCoordinatorActor,
          ValidateStatement(
            SelectSQLStatement(
              db = db,
              namespace = namespace,
              metric = LongMetric.name,
              distinct = false,
              fields = AllFields(),
              groupBy = Some(SimpleGroupByAggregation("dimension")),
              limit = Some(LimitOperator(2))
            )
          )
        )

        awaitAssert {
          probe.expectMsgType[SelectStatementValidationFailed]
        }
      }
    }
  }
}
