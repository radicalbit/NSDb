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
