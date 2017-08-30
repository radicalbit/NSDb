import sbt._

object Dependencies {

  object scalaLang {
    lazy val version   = "2.12.3"
    lazy val namespace = "org.scala-lang"
    lazy val compiler  = namespace % "scala-compiler" % version
  }

  object scalaModules {
    lazy val version           = "1.0.6"
    lazy val namespace         = "org.scala-lang.modules"
    lazy val parserCombinators = namespace %% "scala-parser-combinators" % version
  }

  object cats {
    lazy val version   = "0.9.0"
    lazy val namespace = "org.typelevel"
    lazy val cats      = namespace %% "cats" % version
  }

  object akka {
    lazy val version   = "2.4.20"
    lazy val namespace = "com.typesafe.akka"

    lazy val actor           = namespace %% "akka-actor"            % version
    lazy val testkit         = namespace %% "akka-testkit"          % version
    lazy val stream          = namespace %% "akka-stream"           % version
    lazy val distributedData = namespace %% "akka-distributed-data" % version
    lazy val cluster         = namespace %% "akka-cluster"          % version
    lazy val sharding        = namespace %% "akka-sharding"         % version
    lazy val clusterTools    = namespace %% "akka-cluster-tools"    % version

  }

  object akka_http {
    lazy val version   = "10.0.9"
    lazy val namespace = "com.typesafe.akka"

    lazy val core_http = namespace %% "akka-http-core" % version excludeAll (ExclusionRule(organization =
                                                                                             "com.typesafe.akka",
                                                                                           name = "akka-actor"),
    ExclusionRule(organization = "com.typesafe.akka", name = "akka-testkit"),
    ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream"),
    ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream-testkit"))
    lazy val default = namespace %% "akka-http"         % version
    lazy val testkit = namespace %% "akka-http-testkit" % version % Test
    lazy val jackson = namespace %% "akka-http-jackson" % version
    lazy val xml     = namespace %% "akka-http-xml"     % version

    lazy val core = Seq(core_http, default, testkit, /*spray_json,*/ jackson, xml)
  }

  object javaWebsocket {
    lazy val version       = "1.3.0"
    lazy val namespace     = "org.java-websocket"
    lazy val javaWebsocket = namespace % "Java-WebSocket" % version
  }

  object json4s {
    val version = "3.5.2"
    val native  = "org.json4s" %% "json4s-native" % version
  }

  object lucene {
    lazy val version     = "6.6.0"
    lazy val namespace   = "org.apache.lucene"
    lazy val core        = namespace % "lucene-core" % version
    lazy val queryParser = namespace % "lucene-queryparser" % version
    lazy val grouping    = namespace % "lucene-grouping" % version
  }

  object scalatest {
    lazy val version   = "3.0.0"
    lazy val namespace = "org.scalatest"
    lazy val core      = namespace %% "scalatest" % version
  }

  object junit {
    lazy val version   = "4.12"
    lazy val namespace = "junit"
    lazy val junit     = namespace % "junit" % version
  }

  object junitInterface {
    lazy val version        = "0.11"
    lazy val namespace      = "com.novocode"
    lazy val junitInterface = namespace % "junit-interface" % version
  }

  object logging {
    lazy val `scala-logging` = "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  }

  object flink {
    lazy val version        = "1.3.2"
    lazy val namespace      = "org.apache.flink"
    lazy val core           = namespace % "flink-core" % version
    lazy val streamingScala = namespace % "flink-streaming-scala_2.11" % version
  }

  lazy val asm = "asm" % "asm" % "3.3.1" % Test //import to use ClosureCleaner in test

  object gRPC {
    lazy val version         = "1.4.0"
    lazy val namespace       = "io.grpc"
    lazy val `grpc-netty`    = namespace % "grpc-netty" % version
    lazy val `grpc-protobuf` = namespace % "grpc-protobuf" % version
  }

  object scalaPB {
    lazy val version        = "0.6.1"
    lazy val namespace      = "com.trueaccord.scalapb"
    lazy val `runtime`      = namespace %% "scalapb-runtime" % version % "protobuf"
    lazy val `runtime-grpc` = namespace %% "scalapb-runtime-grpc" % version
  }

  object slf4j {
    lazy val version   = "1.7.25"
    lazy val namespace = "org.slf4j"
    lazy val api       = namespace % "slf4j-api" % version
  }

  object Core {
    val libraries = Seq(
      akka.actor,
      cats.cats,
      lucene.core,
      lucene.queryParser,
      lucene.grouping,
      scalatest.core % Test,
      akka.testkit   % Test
    )
  }

  object Common {
    val libraries = Seq.empty
  }

  object RPC {
    val libraries = Seq(
      slf4j.api,
      gRPC.`grpc-netty`,
      gRPC.`grpc-protobuf`,
      scalaPB.`runtime`,
      scalaPB.`runtime-grpc`
    )
  }

  object AkkaClient {
    val libraries = Seq(
      akka.actor,
      akka.cluster,
      akka.clusterTools
    )
  }

  object Cluster {
    val libraries = Seq(
      akka.cluster,
      akka.clusterTools
    )
  }

  object ScalaAPI {
    val libraries = Seq.empty
  }

  object SQL {
    lazy val libraries = Seq(
      scalaModules.parserCombinators,
      scalatest.core % Test
    )
  }

  object CLI {
    lazy val libraries = Seq(
      scalaLang.compiler
    )
  }

  object FlinkConnector {
    lazy val libraries = Seq(
      flink.streamingScala % Provided
    )
  }

  object Http {
    lazy val libraries = Seq(
      akka.stream,
      akka_http.default,
      json4s.native,
      javaWebsocket.javaWebsocket,
      scalatest.core % Test,
      akka_http.testkit
    )
  }
}
