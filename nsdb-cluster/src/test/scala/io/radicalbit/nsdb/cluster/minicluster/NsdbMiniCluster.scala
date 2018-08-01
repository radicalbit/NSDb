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

package io.radicalbit.nsdb.cluster.minicluster

trait NsdbMiniCluster {

  protected[this] val startingAkkaRemotePort = 2552
  protected[this] val startingHttpPort       = 9010
  protected[this] val startingGrpcPort       = 7817
  protected[this] val dataFolder             = "data"

  protected[this] def nodesNumber: Int

  protected[this] val nodes: Set[NsdbMiniClusterNode] =
    (for {
      i <- 0 to nodesNumber - 1
      _ = Thread.sleep(1000)
    } yield
      (new NsdbMiniClusterNode(akkaRemotePort = startingAkkaRemotePort + i,
                               httpPort = startingHttpPort + i,
                               grpcPort = startingGrpcPort + i,
                               data = dataFolder + startingAkkaRemotePort + i
      ))).toSet
}
