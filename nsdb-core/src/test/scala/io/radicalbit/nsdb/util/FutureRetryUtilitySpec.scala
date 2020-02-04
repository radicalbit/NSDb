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

import akka.actor.{ActorSystem, Scheduler, Status}
import akka.event.{Logging, LoggingAdapter}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{Matchers, WordSpecLike}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class FutureRetryUtilitySpec
    extends TestKit(ActorSystem("MySpec"))
    with WordSpecLike
    with Matchers
    with FutureRetryUtility {

  implicit val schedule: Scheduler    = system.scheduler
  implicit val logger: LoggingAdapter = Logging.getLogger(system, this)

  private final val delay: FiniteDuration = 2.seconds
  private final val retries: Int          = 3

  private def future(flag: Boolean) =
    if (flag) Future.successful("Success") else Future.failed(new RuntimeException("Failure"))

  "retry function in FutureRetryUtility" must {

    "successfully returns whether, after retries, the future is eventually successful" in {
      Await.result(future(true).retry(delay, retries), Duration.Inf) shouldBe "Success"
    }

    "thrown an Exception whether, after retries, the future eventually returns an Exception" in {
      an[RuntimeException] shouldBe thrownBy(Await.result(future(false).retry(delay, retries), Duration.Inf))
    }

    "consider the number of retries" in {
      val q = mutable.Queue(0)
      def future = {
        val nRetries = q.dequeue()
        if (nRetries < 2) { q.enqueue(nRetries + 1); Future.failed(new RuntimeException) } else {
          q.enqueue(nRetries + 1); Future.successful(nRetries)
        }
      }
      Await.result(future.retry(delay, retries), Duration.Inf) shouldBe 2
    }
  }

  "pipeTo function in FutureRetryUtility" must {

    "returns a successful future and send the content of it through pipe" in {
      val testProbe = TestProbe("actor-test")
      future(true).pipeTo(delay, retries, testProbe.testActor)
      testProbe.expectMsg("Success")
    }

    "return a failed future and send a status failure through pipe" in {
      val testProbe = TestProbe("actor-test")
      future(false).pipeTo(delay, retries, testProbe.testActor)
      testProbe.expectMsgAllClassOf(classOf[Status.Failure])
    }
  }
}
