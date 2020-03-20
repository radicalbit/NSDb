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

package io.radicalbit.nsdb.test

import java.time.Duration
import java.util.logging.{Level, Logger}

import io.radicalbit.nsdb.minicluster.NsdbMiniCluster
import org.json4s.DefaultFormats
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait MiniClusterSpec extends FunSuite with BeforeAndAfterAll with Eventually with NsdbMiniCluster {

  Logger.getLogger("io.grpc.internal").setLevel(Level.OFF)

  val nodesNumber: Int = 3
  val replicationFactor: Int = 2
  val rootFolder: String = s"target/minicluster/$instanceId/"

  implicit val formats = DefaultFormats

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(Span(20, Seconds))

  def passivateAfter: Duration = Duration.ofHours(1)

  override def beforeAll(): Unit = {
    start(true)
    waitIndexing()
    waitIndexing()
  }

  override def afterAll(): Unit = {
    stop()
  }

  protected lazy val indexingTime: Long =
    nodes.head.system.settings.config.getDuration("nsdb.write.scheduler.interval").toMillis

  protected def waitIndexing(): Unit    = Thread.sleep(indexingTime + 1000)
  protected def waitPassivation(): Unit = Thread.sleep(passivateAfter.toMillis + 1000)
}
