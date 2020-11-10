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

package io.radicalbit.nsdb.minicluster.test

import java.time.Duration
import java.util.logging.{Level, Logger}

import io.radicalbit.nsdb.minicluster.NSDbMiniCluster
import org.json4s.DefaultFormats
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}
import org.scalatest.BeforeAndAfterAll

trait MiniClusterSingleNodeSpec extends AnyFunSuite with BeforeAndAfterAll with Eventually with NSDbMiniCluster {

  Logger.getLogger("io.grpc.internal").setLevel(Level.OFF)

  override val nodesNumber: Int         = 1
  override val replicationFactor: Int   = 1
  override val rootFolder: String       = s"target/minicluster/$instanceId"
  override val shardInterval: Duration  = Duration.ofMillis(5)
  override val passivateAfter: Duration = Duration.ofHours(1)

  implicit val formats = DefaultFormats

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(Span(20, Seconds))

  override def beforeAll(): Unit = {
    start(true)
  }

  override def afterAll(): Unit = {
    stop()
  }
}
