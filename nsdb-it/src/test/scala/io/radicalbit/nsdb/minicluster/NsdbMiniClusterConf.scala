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

import java.time.Duration

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.radicalbit.nsdb.common.NsdbConfig

trait NsdbMiniClusterConf extends NsdbConfig {

  def hostname: String
  def dataDir: String
  def commitLogDir: String
  def passivateAfter: Duration

  override def config: Config =
    ConfigFactory
      .load("minicluster.conf")
      .withValue("akka.management.http.hostname", ConfigValueFactory.fromAnyRef(hostname))
      .withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(hostname))
      .withValue("nsdb.grpc.interface", ConfigValueFactory.fromAnyRef(hostname))
      .withValue("nsdb.http.interface", ConfigValueFactory.fromAnyRef(hostname))
      .withValue("nsdb.index.base-path", ConfigValueFactory.fromAnyRef(dataDir))
      .withValue("akka.cluster.distributed-data.durable.lmdb.dir", ConfigValueFactory.fromAnyRef(s"$dataDir/ddata"))
      .withValue("nsdb.commit-log.directory", ConfigValueFactory.fromAnyRef(commitLogDir))
      .withValue("nsdb.sharding.passivate-after", ConfigValueFactory.fromAnyRef(passivateAfter))
}
