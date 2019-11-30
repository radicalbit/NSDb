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
import io.radicalbit.nsdb.common.configuration.NsdbConfigProvider

trait NsdbMiniClusterConfigProvider extends NsdbConfigProvider {

  def hostname: String
  def storageDir: String
  def passivateAfter: Duration

  override lazy val userDefinedConfig: Config =
    ConfigFactory
      .parseResources("nsdb-minicluster.conf")
      .withValue("nsdb.node.hostname", ConfigValueFactory.fromAnyRef(hostname))
      .withValue("nsdb.grpc.interface", ConfigValueFactory.fromAnyRef(hostname))
      .withValue("nsdb.http.interface", ConfigValueFactory.fromAnyRef(hostname))
      .withValue("nsdb.storage.base-path", ConfigValueFactory.fromAnyRef(storageDir))
      .resolve()

  override lazy val lowLevelTemplateConfig: Config =
    mergeConf(userDefinedConfig,
              ConfigFactory.parseResources("application-native.conf"),
              ConfigFactory.parseResources("application-common.conf"))
}
