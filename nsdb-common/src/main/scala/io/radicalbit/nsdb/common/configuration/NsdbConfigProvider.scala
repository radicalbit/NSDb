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

package io.radicalbit.nsdb.common.configuration

import com.typesafe.config.{Config, ConfigFactory}

trait NsdbConfigProvider {

  def highLevelConfig: Config
  def lowLevelConfig: Config

  protected def mergeConf(configs: Config*): Config =
    configs
      .fold(ConfigFactory.empty()) { (acc, e) =>
        acc.withFallback(e)
      }

  private def customize(highLevelConfig: Config, lowLevelConfig: Config) =
    lowLevelConfig
      .withValue("akka.remote.artery.canonical.hostname", highLevelConfig.getValue("nsdb.akka.hostname"))
      .withValue("akka.remote.artery.canonical.port", highLevelConfig.getValue("nsdb.akka.port"))
      .withValue("akka.cluster.distributed-data.durable.lmdb.dir",
                 highLevelConfig.getValue("nsdb.storage.metadata-path"))
      .withValue("akka.management.required-contact-point-nr",
                 highLevelConfig.getValue("nsdb.cluster.required-contact-point-nr"))
      .withValue("akka.discovery.config.services.NSDb.endpoints", highLevelConfig.getValue("nsdb.cluster.endpoints"))
      .resolve()

  final lazy val config: Config = customize(highLevelConfig, lowLevelConfig)

}
