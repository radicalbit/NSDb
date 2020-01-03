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

package io.radicalbit.nsdb.cluster

import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import kamon.Kamon
import io.radicalbit.nsdb.common.configuration.NSDbConfig.HighLevel._

/**
  * Defines NSDb monitoring system.
  */
trait NSDbMonitoring {

  private def monitoringConfiguration =
    ConfigFactory
      .parseFile(Paths.get(System.getProperty("confDir"), "monitoring.conf").toFile)
      .withFallback(ConfigFactory.parseResources("monitoring.conf"))
      .resolve()

  /**
    * Initializes monitoring system if it is enabled.
    */
  protected def initMonitoring(): Unit =
    if (monitoringConfiguration.getBoolean(MonitoringEnabled))
      Kamon.init(ConfigFactory.load("monitoring.conf"))
}

/**
  * Simply mix in [[NSDbActors]] with [[NSDbMonitoring]] and [[NSDbClusterConfigProvider]]
  */
trait ProductionCluster extends NSDbActors with NSDbMonitoring with NSDbClusterConfigProvider
