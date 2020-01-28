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

package io.radicalbit.nsdb.util

import akka.actor.{ActorSystem, Scheduler}
import akka.event.{Logging, LoggingAdapter}
import akka.testkit.TestKit
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class RetryFutureUtilitySpec
    extends TestKit(ActorSystem("MySpec"))
    with WordSpecLike
    with Matchers
    with RetryFutureUtility {

  implicit val schedule: Scheduler    = system.scheduler
  implicit val logger: LoggingAdapter = Logging.getLogger(system, this)

  private final val delay: FiniteDuration = 2.seconds
  private final val retries: Int          = 3

  private def future(flag: Boolean) =
    if (flag) Future.successful("Success") else Future.failed(new RuntimeException("Failure"))

  "RetryFutureUtility" must {

    "return success whether, after retries, the future is eventually successful" in {
      Await.result(future(true).retry(delay, retries), Duration.Inf) shouldBe "Success"
    }

    "thrown an Exception whether, after retries, the future eventually returns an Exception" in {
      an[RuntimeException] shouldBe thrownBy(Await.result(future(false).retry(delay, retries), Duration.Inf))
    }
  }
}
