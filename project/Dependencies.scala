import Dependencies.scalaLang.namespace
import sbt._

object Dependencies {

  object scalaLang {
    lazy val version   = "2.12.4"
    lazy val namespace = "org.scala-lang"
    lazy val compiler  = namespace % "scala-compiler" % version
  }

  object scalaModules {
    lazy val version           = "1.0.6"
    lazy val namespace         = "org.scala-lang.modules"
    lazy val parserCombinators = namespace %% "scala-parser-combinators" % version

    lazy val java8Compatibility = namespace %% "scala-java8-compat" % "0.8.0"
  }

  object scopt {
    lazy val version   = "3.7.0"
    lazy val namespace = "com.github.scopt"
    val scopt          = namespace %% "scopt" % version
  }

  object cats {
    lazy val version   = "0.9.0"
    lazy val namespace = "org.typelevel"
    lazy val cats      = namespace %% "cats" % version
  }

  object spire {
    lazy val version   = "0.14.1"
    lazy val namespace = "org.typelevel"
    lazy val spire     = namespace %% "spire" % version
  }

  object akka {
    lazy val version   = "2.5.6"
    lazy val namespace = "com.typesafe.akka"

    lazy val actor           = namespace %% "akka-actor"              % version
    lazy val testkit         = namespace %% "akka-testkit"            % version
    lazy val stream          = namespace %% "akka-stream"             % version
    lazy val distributedData = namespace %% "akka-distributed-data"   % version
    lazy val cluster         = namespace %% "akka-cluster"            % version
    lazy val sharding        = namespace %% "akka-sharding"           % version
    lazy val slf4j           = namespace %% "akka-slf4j"              % version
    lazy val clusterTools    = namespace %% "akka-cluster-tools"      % version
    lazy val multiNode       = namespace %% "akka-multi-node-testkit" % version
  }

  object scala_logging {
    val scala_logging = "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
  }

  object logback {
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  }

  object akka_http {
    lazy val version   = "10.0.10"
    lazy val namespace = "com.typesafe.akka"

    lazy val core_http = namespace %% "akka-http-core" % version excludeAll (ExclusionRule(organization =
                                                                                             "com.typesafe.akka",
                                                                                           name = "akka-actor"),
    ExclusionRule(organization = "com.typesafe.akka", name = "akka-testkit"),
    ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream"),
    ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream-testkit"))
    lazy val default         = namespace %% "akka-http" % version
    lazy val testkit         = namespace %% "akka-http-testkit" % version % Test
    lazy val akkaHttpJson4s  = "de.heikoseeberger" %% "akka-http-json4s" % "1.18.1"
    lazy val sprayJson       = "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.10"
    lazy val swaggerAkkaHttp = "com.github.swagger-akka-http" %% "swagger-akka-http" % "0.14.0"
    lazy val core            = Seq(core_http, default, testkit)
  }

  object swagger {
    lazy val version     = "1.5.18"
    lazy val namespace   = "io.swagger"
    lazy val coreSwagger = namespace % "swagger-jaxrs" % version
  }

  object javaWebsocket {
    lazy val version       = "1.3.0"
    lazy val namespace     = "org.java-websocket"
    lazy val javaWebsocket = namespace % "Java-WebSocket" % version
  }

  object json4s {
    lazy val version   = "3.5.2"
    lazy val namespace = "org.json4s"
    lazy val jackson   = namespace %% "json4s-jackson" % version
  }

  object lucene {
    lazy val version     = "6.6.0"
    lazy val namespace   = "org.apache.lucene"
    lazy val core        = namespace % "lucene-core" % version
    lazy val queryParser = namespace % "lucene-queryparser" % version
    lazy val grouping    = namespace % "lucene-grouping" % version
    lazy val facet       = namespace % "lucene-facet" % version
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

  object asciitable {
    lazy val version   = "0.3.2"
    lazy val namespace = "de.vandermeer"
    lazy val core      = namespace % "asciitable" % version
  }

  object config {
    lazy val version   = "1.3.1"
    lazy val namespace = "com.typesafe"
    lazy val core      = namespace % "config" % version
  }

  object gatling {
    lazy val version    = "2.2.0"
    lazy val namespace  = "io.gatling"
    lazy val test       = namespace % "gatling-test-framework" % version
    lazy val highcharts = s"$namespace.highcharts" % "gatling-charts-highcharts" % version
  }

  object Core {
    val libraries = Seq(
      akka.actor,
      spire.spire,
      lucene.core,
      lucene.queryParser,
      lucene.grouping,
      lucene.facet,
      scalatest.core % Test,
      akka.testkit   % Test
    )
  }

  object Common {
    val libraries = Seq(
      scala_logging.scala_logging
    )
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

  object Cluster {
    lazy val libraries = Seq(
      akka.cluster,
      akka.clusterTools,
      akka.distributedData,
      scala_logging.scala_logging,
      akka.slf4j,
      logback.logback,
      scalatest.core % Test,
      akka.testkit   % Test,
      akka.multiNode
    )
  }

  object Security {
    lazy val libraries = Seq(
      scala_logging.scala_logging,
      akka.actor,
      akka_http.default
    )
  }

  object ScalaAPI {
    lazy val libraries = Seq.empty
  }

  object JavaAPI {
    val libraries = Seq(
      scalaModules.java8Compatibility
    )
  }

  object SQL {
    lazy val libraries = Seq(
      scalaModules.parserCombinators,
      scalatest.core % Test
    )
  }

  object CLI {
    lazy val libraries = Seq(
      scalaLang.compiler,
      scopt.scopt,
      asciitable.core,
      cats.cats,
      scala_logging.scala_logging,
      akka.slf4j,
      logback.logback,
      scalatest.core % Test
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
      json4s.jackson,
      akka_http.sprayJson,
      javaWebsocket.javaWebsocket,
      scalatest.core % Test,
      akka_http.testkit,
      akka_http.swaggerAkkaHttp,
      swagger.coreSwagger
    )
  }

  object Performance {
    lazy val libraries = Seq(
      config.core,
      gatling.test       % Test,
      gatling.highcharts % Test
    )
  }
}
