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

/**
  * Creates Nsdb configuration looking up the `ConfDir` folder or into the classpath.
  * The retrieved configuration is properly adjusted in case ssl is enabled or not
  */
trait NsdbConfig {
  private lazy val initialConfig: Config = ConfigFactory
    .parseFile(Paths.get(System.getProperty("confDir"), "cluster.conf").toFile)
    .resolve()
    .withFallback(ConfigFactory.load("cluster"))

  lazy val config: Config = if (initialConfig.getBoolean("akka.remote.netty.tcp.enable-ssl")) {
    initialConfig
      .withValue("akka.remote.enabled-transports", initialConfig.getValue("akka.remote.enabled-transports-ssl"))
      .withValue("akka.cluster.seed-nodes", initialConfig.getValue("akka.cluster.seed-nodes-ssl"))
  } else
    initialConfig
}
