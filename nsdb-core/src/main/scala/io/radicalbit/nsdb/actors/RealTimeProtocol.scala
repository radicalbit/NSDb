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

package io.radicalbit.nsdb.actors

import akka.dispatch.ControlMessage
import io.radicalbit.nsdb.common.protocol.{Bit, NSDbSerializable}

object RealTimeProtocol {

  sealed trait RealTimeOutGoingMessage extends NSDbSerializable

  object Events {
    case class SubscribedByQueryString(db: String,
                                       namespace: String,
                                       metric: String,
                                       queryString: String,
                                       records: Seq[Bit])
        extends ControlMessage
        with RealTimeOutGoingMessage
    case class SubscriptionByQueryStringFailed(db: String,
                                               namespace: String,
                                               metric: String,
                                               queryString: String,
                                               reason: String)
        extends ControlMessage
        with RealTimeOutGoingMessage

    case class RecordsPublished(quid: String, metric: String, records: Seq[Bit]) extends RealTimeOutGoingMessage

    case class ErrorResponse(error: String) extends RealTimeOutGoingMessage
  }

}
