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

import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.cluster.extension.NSDbClusterSnapshot
import io.radicalbit.nsdb.minicluster.{NSDbMiniCluster, NSDbMiniClusterNode}
import org.json4s.DefaultFormats
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.time.Duration
import java.util.logging.{Level, Logger}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Random

trait MiniClusterSpec extends AnyFunSuite with BeforeAndAfterAll with Eventually with NSDbMiniCluster {

  Logger.getLogger("io.grpc.internal").setLevel(Level.OFF)

  override val nodesNumber: Int = 3
  override val replicationFactor: Int = 2
  override val rootFolder: String = s"target/minicluster/$instanceId"
  override val shardInterval: Duration = Duration.ofMillis(5)
  override val passivateAfter: Duration = Duration.ofHours(1)

  implicit val formats: DefaultFormats.type = DefaultFormats

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(Span(20, Seconds))

  override def beforeAll(): Unit = {
    start(true)
    healthCheck()
  }

  override def afterAll(): Unit = {
    leave()
    stop()
  }

  lazy val firstNode: NSDbMiniClusterNode = nodes.head
  lazy val lastNode: NSDbMiniClusterNode = nodes.last

  implicit lazy val executionContext: ExecutionContextExecutor = ExecutionContext.global

  private lazy val connections : Seq[Future[NSDB]] = nodes.map( node => NSDB.connect(host = node.hostname, port = 7817))

  protected def withRandomNodeConnection[A](f : NSDB => Future[A]): Future[A] = connections(Random.nextInt(connections.size)).flatMap(f)

  protected lazy val indexingTime: Long =
    nodes.head.system.settings.config.getDuration("nsdb.write.scheduler.interval").toMillis

  protected def waitIndexing(): Unit    = Thread.sleep(indexingTime + 1000)
  protected def waitPassivation(): Unit = Thread.sleep(passivateAfter.toMillis + 1000)

  def healthCheck(): Seq[Assertion] =
      nodes.map { node =>
        eventually {
          assert(NSDbClusterSnapshot(node.system).nodes.size == nodes.size)
        }
      }

}
