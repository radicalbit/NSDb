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

package io.radicalbit.nsdb.actors.supervision

import akka.actor.SupervisorStrategy.{Restart, Resume, Stop}
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, SupervisorStrategy}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.radicalbit.nsdb.test.NSDbSpecLike
import org.scalatest.{BeforeAndAfter, OneInstancePerTest}

import java.util.UUID
import scala.concurrent.duration._

class OneForOneWithRetriesStrategySpec
    extends TestKit(ActorSystem("OneForOneWithRetriesStrategySpec"))
    with ImplicitSender
    with NSDbSpecLike
    with OneInstancePerTest
    with BeforeAndAfter {

  class TestSupervisorActor() extends Actor with ActorLogging {

    override val supervisorStrategy: SupervisorStrategy = OneForOneWithRetriesStrategy(maxNrOfRetries = 1) {
      case _: IllegalArgumentException =>
        Resume
      case _: IllegalStateException =>
        Restart
      case _ =>
        Stop
    } { (context, child) =>
      context.stop(child)
    }

    override def receive: Receive = {
      case msg => sender() ! msg
    }
  }

  class FailingActor extends Actor {

    val volatileId: String = UUID.randomUUID().toString

    override def receive: Receive = {
      case "ResumeMessage" =>
        throw new IllegalArgumentException()
      case "RestartMessage" =>
        throw new IllegalStateException()
      case _ =>
        throw new RuntimeException()
    }

  }

  val testSupervisor: TestActorRef[TestSupervisorActor] =
    TestActorRef[TestSupervisorActor](Props(new TestSupervisorActor()))

  "OneForOneWithRetriesStrategy" when {
    "An actor throws an exception if maxNrOfRetries is not reached" should {
      "resume it properly" in {
        val failingActor = TestActorRef[FailingActor](Props(new FailingActor()), testSupervisor)

        val volatileId = failingActor.underlyingActor.volatileId

        failingActor ! "ResumeMessage"

        expectNoMessage(1 seconds)

        failingActor.underlyingActor.volatileId shouldBe volatileId
      }

      "restart it properly" in {
        val failingActor = TestActorRef[FailingActor](Props(new FailingActor()), testSupervisor)

        val volatileId = failingActor.underlyingActor.volatileId

        failingActor ! "RestartMessage"

        expectNoMessage(1 seconds)

        failingActor.underlyingActor.volatileId shouldNot be(volatileId)
      }
    }

    "An actor throws an exception if maxNrOfRetries is reached" should {
      "resume it properly" in {

        val failingActor = TestActorRef[FailingActor](Props(new FailingActor()), testSupervisor)

        val probe = TestProbe()
        probe.watch(failingActor)

        failingActor ! "ResumeMessage"
        failingActor ! "ResumeMessage"

        probe.expectTerminated(failingActor)
      }

      "restart it properly" in {

        val failingActor = TestActorRef[FailingActor](Props(new FailingActor()), testSupervisor)

        val probe = TestProbe()
        probe.watch(failingActor)

        failingActor ! "RestartMessage"
        failingActor ! "RestartMessage"

        probe.expectTerminated(failingActor)
      }
    }

  }
}
