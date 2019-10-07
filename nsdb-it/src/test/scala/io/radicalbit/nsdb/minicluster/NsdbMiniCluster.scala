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

package io.radicalbit.nsdb.minicluster

import java.io.File
import java.time.Duration
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils

import scala.concurrent.Await

trait NsdbMiniCluster extends LazyLogging {

  protected[this] val instanceId = { UUID.randomUUID }

  protected[this] val startingHostname = "127.0.0."
  protected[this] val rootFolder       = s"target/minicluster/$instanceId/"
  protected[this] val clFolder         = s"target/commitLog/$instanceId"

  protected[this] def nodesNumber: Int
  protected[this] def passivateAfter: Duration

  lazy val nodes: Set[NSDbMiniClusterNode] =
    (for {
      i <- 0 until nodesNumber
      _ = Thread.sleep(1000)
    } yield
      new NSDbMiniClusterNode(
        hostname = s"$startingHostname${i + 1}",
        dataDir = s"$rootFolder/data$i",
        commitLogDir = s"$clFolder$i",
        passivateAfter = passivateAfter
      )).toSet

  def start(cleanup: Boolean = false): Unit = {
    if (cleanup)
      FileUtils.deleteDirectory(new File(rootFolder))
    nodes
  }

  def stop(): Unit = {
    import scala.concurrent.duration._
    nodes.foreach(n => Await.result(n.system.terminate(), 10.seconds))
    nodes.foreach(n => Await.result(n.system.whenTerminated, 10.seconds))
  }
}
