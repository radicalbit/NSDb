package io.radicalbit.nsdb

import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

abstract class SplitBrainSpecConfig extends MultiNodeConfig {
  commonConfig(ConfigFactory.parseString("""
                                           |akka {
                                           |  loglevel = INFO
                                           |  actor.provider = cluster
                                           |
                                           |  log-dead-letters = off
                                           |
                                           |  coordinated-shutdown.run-by-jvm-shutdown-hook = off
                                           |  coordinated-shutdown.terminate-actor-system = off
                                           |
                                           |  cluster {
                                           |    auto-join = off
                                           |    run-coordinated-shutdown-when-down = off
                                           |  }
                                           |
                                           |  remote {
                                           |    artery {
                                           |      transport = tcp
                                           |      canonical.hostname = "localhost"
                                           |      canonical.port = 0
                                           |    }
                                           |  }
                                           |}
                                           |""".stripMargin))

  testTransport(on = true)
}
