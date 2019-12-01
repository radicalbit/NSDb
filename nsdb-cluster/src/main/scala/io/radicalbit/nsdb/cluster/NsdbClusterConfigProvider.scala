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

package io.radicalbit.nsdb.cluster

import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import io.radicalbit.nsdb.common.configuration.NsdbConfigProvider
import io.radicalbit.nsdb.util.ConfigKeys

/**
  * Creates NSDb user defined configuration looking up the `ConfDir` folder or into the classpath.
  */
trait NsdbClusterConfigProvider extends NsdbConfigProvider {

  override lazy val userDefinedConfig = ConfigFactory
    .parseFile(Paths.get(System.getProperty("confDir"), "nsdb.conf").toFile)
    .withFallback(ConfigFactory.parseResources("nsdb.conf"))
    .resolve()

  override lazy val lowLevelTemplateConfig: Config =
    mergeConf(
      userDefinedConfig,
      ConfigFactory.parseResources(s"application-${userDefinedConfig.getString(ConfigKeys.ClusterMode)}.conf"),
      ConfigFactory.parseResources("application-common.conf")
    )

}
