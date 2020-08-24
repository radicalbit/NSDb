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

class ReadCoordinatorNoReductionSpec extends AbstractTemporalReadCoordinatorSpec {

  "ReadCoordinator" when {

    "receive a select with reduce flag set at false" should {
      "refuse it in case of a plain query" in {

        awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = AllFields(),
                groupBy = None
              )
            )
          )

          probe.expectMsgType[SelectStatementFailed]
        }

      }

      "refuse it in case of a standard group by query" in {
        awaitAssert {

          probe.send(
            readCoordinatorActor,
            ExecuteStatement(
              SelectSQLStatement(
                db = db,
                namespace = namespace,
                metric = TemporalLongMetric.name,
                distinct = false,
                fields = AllFields(),
                groupBy = Some(SimpleGroupByAggregation("name")),
                limit = Some(LimitOperator(2))
              )
            )
          )

          probe.expectMsgType[SelectStatementFailed]
        }
      }

    }
  }
}
