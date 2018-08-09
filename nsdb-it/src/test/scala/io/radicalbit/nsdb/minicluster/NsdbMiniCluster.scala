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

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils

import scala.concurrent.Await

trait NsdbMiniCluster extends LazyLogging {

  protected[this] val startingAkkaRemotePort = 2552
  protected[this] val startingHttpPort       = 9010
  protected[this] val startingGrpcPort       = 7817
  protected[this] val rootFolder             = "target/minicluster"

  protected[this] def nodesNumber: Int

  lazy val nodes: Set[NsdbMiniClusterNode] =
    (for {
      i <- 0 until nodesNumber
      _ = Thread.sleep(1000)
    } yield
      new NsdbMiniClusterNode(
        akkaRemotePort = startingAkkaRemotePort + i,
        httpPort = startingHttpPort + i,
        grpcPort = startingGrpcPort + i,
        dataDir = s"$rootFolder/data${startingAkkaRemotePort + i}"
      )).toSet

  def start(cleanup: Boolean = false) = {
    if (cleanup)
      FileUtils.deleteDirectory(new File(rootFolder))
    nodes
    Thread.sleep(5000)
  }
  def stop(): Unit = {
    import scala.concurrent.duration._
    nodes.foreach(n => Await.result(n.system.terminate(), 10.seconds))
    nodes.foreach(n => Await.result(n.system.whenTerminated, 10.seconds))
    Thread.sleep(5000)
  }
}
