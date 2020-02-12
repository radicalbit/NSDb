package io.radicalbit.nsdb.split_brain.configs

import com.typesafe.config.ConfigFactory

abstract class SplitBrainThreeNodesSpecConfig extends SplitBrainSpecConfig {

  val node1 = role("node-1")
  val node2 = role("node-2")
  val node3 = role("node-3")

}

object SplitBrainThreeNodeSpecConfig extends SplitBrainThreeNodesSpecConfig

object SplitBrainThreeNodesResolutionSpecConfig extends SplitBrainThreeNodesSpecConfig {

  val otherAkkaConfig =
    ConfigFactory.parseString(
      """
        |akka.cluster.downing-provider-class = "com.swissborg.lithium.DowningProviderImpl"
        |
        |com.swissborg.lithium {
        |  active-strategy = "keep-majority"
        |  stable-after = 30s
        |  keep-majority.role = ""
        |}
        |""".stripMargin).withFallback(akkaConfig)

  commonConfig(otherAkkaConfig)

}
