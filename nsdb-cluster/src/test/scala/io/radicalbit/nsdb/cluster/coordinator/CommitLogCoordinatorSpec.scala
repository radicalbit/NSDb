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

package io.radicalbit.nsdb.cluster.coordinator

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import io.radicalbit.nsdb.commit_log.CommitLogWriterActor._
import io.radicalbit.nsdb.common.protocol.Bit
import io.radicalbit.nsdb.common.statement.{Condition, DeleteSQLStatement, RangeExpression}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class CommitLogCoordinatorSpec
    extends TestKit(ActorSystem("CommitLogCoordinatorSpec"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  private val commitLogCoordinatorActor = system actorOf CommitLogCoordinator.props

  private val conf = ConfigFactory.load()

  override def beforeAll = {

    val directory = new File(conf.getString("nsdb.commit-log.directory"))
    if (!directory.exists) {
      directory.mkdir
    } else {
      directory.listFiles().foreach(_.delete())
    }
  }

  "CommitLogCoordinator" should {
    "write a insert entry" in within(5.seconds) {
      val bit = Bit(0, 1, Map("dim" -> "v"))
      awaitAssert {
        commitLogCoordinatorActor ! WriteToCommitLog("db", "namespace", "metric", 1L, InsertAction(bit))
        expectMsgType[WriteToCommitLogSucceeded]
      }
    }
    "write a reject entry" in within(5.seconds) {
      val bit = Bit(0, 1, Map("dim" -> "v"))
      awaitAssert {
        commitLogCoordinatorActor ! WriteToCommitLog("db1", "namespace1", "metric1", 1L, RejectAction(bit))
        expectMsgType[WriteToCommitLogSucceeded]
      }
    }
    "write a delete by query" in within(5.seconds) {
      val deleteStatement =
        DeleteSQLStatement("db2", "namespace2", "metric2", Condition(RangeExpression("age", 1L, 2L)))
      awaitAssert {
        commitLogCoordinatorActor ! WriteToCommitLog("db2", "namespace2", "metric2", 1L, DeleteAction(deleteStatement))
        expectMsgType[WriteToCommitLogSucceeded]
      }
    }
    "write a metric deletion" in within(5.seconds) {
      awaitAssert {
        commitLogCoordinatorActor ! WriteToCommitLog("db3", "namespace3", "metric3", 1L, DeleteMetricAction)
        expectMsgType[WriteToCommitLogSucceeded]
      }
    }
    "write a namespace deletion" in within(5.seconds) {
      awaitAssert {
        commitLogCoordinatorActor ! WriteToCommitLog("db4", "namespace4", "", 1L, DeleteNamespaceAction)
        expectMsgType[WriteToCommitLogSucceeded]
      }
    }
  }

}
