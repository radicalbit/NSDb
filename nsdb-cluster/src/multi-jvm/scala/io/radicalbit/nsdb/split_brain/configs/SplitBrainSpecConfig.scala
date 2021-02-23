package io.radicalbit.nsdb.split_brain.configs

import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

/**
 * Component defining three cluster nodes for test
 */
trait ThreeNodeClusterSpecConfig {
  _: MultiNodeConfig =>

  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")
}

/**
 * Component defining five cluster nodes for test
 */
trait FiveNodeClusterSpecConfig {
  _: MultiNodeConfig =>

  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")
  val node4 = role("node-4")
  val node5 = role("node-5")
}

/**
 * Component defining base akka cluster config for test
 */
trait NSDbBaseSpecConfig {
  this: MultiNodeConfig =>

  protected val baseConfig =
    ConfigFactory.parseString("""
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
                                |""".stripMargin)

  testTransport(on = true)

}

/**
 * Component defining split brain resolver plugin config for test
 */
trait SplitBrainResolutionSpecConfig {

  protected val splitBrainResolverConfig =
    ConfigFactory.parseString("""
        |akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        |
        |akka.cluster.split-brain-resolver {
        |  active-strategy = "keep-majority"
        |  stable-after = 30s
        |}
        |""".stripMargin)

}

/**
 * Component defining serialization config
 */
trait NSDbBaseSpecWithSerializationConfig {

  protected val serializationConfig =
    ConfigFactory.parseString("""
        |akka.actor {
        |  serialization-bindings {
        |    "io.radicalbit.nsdb.common.protocol.NSDbSerializable" = jackson-json
        |  }
        |}
        |""".stripMargin)

}

object SplitBrainThreeNodesResolutionSpecConfig
    extends MultiNodeConfig
    with NSDbBaseSpecConfig
    with SplitBrainResolutionSpecConfig
    with ThreeNodeClusterSpecConfig {
  commonConfig(baseConfig.withFallback(splitBrainResolverConfig))
}

object SplitBrainThreeNodeSpecConfig extends MultiNodeConfig with NSDbBaseSpecConfig with ThreeNodeClusterSpecConfig {
  commonConfig(baseConfig)
}

object SplitBrainFiveNodesSpecConfig extends MultiNodeConfig with NSDbBaseSpecConfig with FiveNodeClusterSpecConfig {
  commonConfig(baseConfig)
}

object SplitBrainFiveNodesResolutionSpecConfig
    extends MultiNodeConfig
    with SplitBrainResolutionSpecConfig
    with NSDbBaseSpecConfig
    with FiveNodeClusterSpecConfig {
  commonConfig(baseConfig.withFallback(splitBrainResolverConfig))
}

object ClusterSingletonWithSplitBrainSpecConfig
    extends MultiNodeConfig
    with NSDbBaseSpecConfig
    with NSDbBaseSpecWithSerializationConfig
    with FiveNodeClusterSpecConfig {
  commonConfig(baseConfig.withFallback(serializationConfig))
}

object ClusterSingletonWithSplitBrainResolutionSpecConfig
  extends MultiNodeConfig
with NSDbBaseSpecConfig
with NSDbBaseSpecWithSerializationConfig
with SplitBrainResolutionSpecConfig
with FiveNodeClusterSpecConfig {
  commonConfig(baseConfig.withFallback(serializationConfig).withFallback(splitBrainResolverConfig))
}


