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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.common.NsdbConfig

trait NsdbMiniClusterConf extends NsdbConfig {

  def akkaRemotePort: Int
  def grpcPort: Int
  def httpPort: Int
  def dataDir: String

  override def config: Config = ConfigFactory.load("minicluster.conf")
    .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(akkaRemotePort))
    .withValue("nsdb.grpc.port", ConfigValueFactory.fromAnyRef(grpcPort))
    .withValue("nsdb.http.port", ConfigValueFactory.fromAnyRef(httpPort))
    .withValue("nsdb.index.base-path", ConfigValueFactory.fromAnyRef(dataDir))
//    .withoutPath("akka.cluster")

}
