package io.radicalbit.nsdb.cluster.minicluster

import com.typesafe.config.{Config, ConfigFactory}
import io.radicalbit.nsdb.common.NsdbConfig

trait NsdbMiniClusterConf extends NsdbConfig {

  def akkaRemotePort: Int
  def grpcPort: Int
  def httpPort: Int

  override def config: Config = ConfigFactory.parseString {
    s"""
       |akka {
       |  loglevel = "DEBUG"
       |  loggers = ["akka.event.slf4j.Slf4jLogger"]
       |  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
       |
      |  log-dead-letters = 10
       |  log-dead-letters-during-shutdown = off
       |
      |  actor {
       |    provider = cluster
       |
      |    control-aware-dispatcher {
       |      mailbox-type = "akka.dispatch.UnboundedControlAwareMailbox"
       |
      |    }
       |
      |    debug {
       |      lifecycle = on
       |      receive = off
       |    }
       |  }
       |
      |  remote {
       |    enabled-transports = ["akka.remote.netty.tcp" ]
       |    #In case of SSL enabled in akka.remote.netty.tcp.enable-ssl
       |    enabled-transports-ssl = ["akka.remote.netty.ssl"]
       |    #Uncomment in case of SSL conf
       |    log-remote-lifecycle-events = off
       |    netty.tcp {
       |      hostname = "127.0.0.1"
       |      port = $akkaRemotePort
       |      http.port = $httpPort
       |     grpc.port = $grpcPort
       |      enable-ssl = false
       |    }
       |  }
       |
      |  cluster {
       |    seed-nodes = ["akka.tcp://nsdb@127.0.0.1:2552"]
       |  }
       |
      |  log-dead-letters = 10
       |  log-dead-letters-during-shutdown = on
       |
      |  extensions = ["akka.cluster.client.ClusterClientReceptionist", "akka.cluster.pubsub.DistributedPubSub",
       |                "io.radicalbit.nsdb.cluster.extension.RemoteAddress"]
       |
      |  http.server.idle-timeout = 1 hour
       |}
       |
      |nsdb {
       |
      |  grpc {
       |    port = $grpcPort
       |  }
       |
      |  cluster {
       |    pub-sub{
       |      warm-up-topic = "warm-up"
       |      schema-topic = "schema"
       |      metadata-topic = "metadata"
       |    }
       |  }
       |
      |  http {
       |    interface = "0.0.0.0"
       |    port = $httpPort
       |    api.path = "api"
       |    api.version = "v0.1"
       |  }
       |
      |  index {
       |    base-path= "data/index"
       |  }
       |
      |  commit-log {
       |    enabled = false
       |    serializer = "io.radicalbit.nsdb.commit_log.StandardCommitLogSerializer"
       |    writer = "io.radicalbit.nsdb.commit_log.RollingCommitLogFileWriter"
       |    directory = "/tmp/"
       |    max-size = 50000
       |  }
       |
      |  sharding {
       |    interval = 1d
       |  }
       |
      |  security {
       |    enabled = false
       |    auth-provider-class = ""
       |  }
       |
      |  read {
       |    parallelism {
       |      initial-size = 5
       |      lower-bound= 2
       |      upper-bound = 15
       |    }
       |  }
       |
      |  global.timeout = 30 seconds
       |  http-endpoint.timeout = 60 seconds
       |  rpc-endpoint.timeout = 30 seconds
       |  rpc-akka-endpoint.timeout = 30 seconds
       |
      |  read-coordinator.timeout = 30 seconds
       |  metadata-coordinator.timeout = 30 seconds
       |  write-coordinator.timeout = 30 seconds
       |  namespace-schema.timeout = 30 seconds
       |  namespace-data.timeout = 30 seconds
       |  publisher.timeout = 30 seconds
       |  publisher.scheduler.interval = 5 seconds
       |
      |  write.scheduler.interval = 15 seconds
       |
      |  stream.timeout = 30 seconds
       |
      |  websocket {
       |    // Websocket publish period expressed in milliseconds
       |    refresh-period = 100
       |    //Websocket retention size
       |    retention-size = 10
       |  }
       |}
      """.stripMargin
  }

}
