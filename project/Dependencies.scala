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

import sbt._

object Dependencies {

  object scalaLang {
    lazy val version   = "2.12.7"
    lazy val namespace = "org.scala-lang"
    lazy val compiler = namespace % "scala-compiler" % version excludeAll (ExclusionRule(organization =
                                                                                           "org.scala-lang.modules",
                                                                                         name = "scala-xml_2.12"))
  }

  object scalaModules {
    lazy val version           = "1.1.1"
    lazy val namespace         = "org.scala-lang.modules"
    lazy val parserCombinators = namespace %% "scala-parser-combinators" % version

    lazy val java8Compatibility = namespace %% "scala-java8-compat" % "0.9.0"
  }

  object scopt {
    lazy val version   = "3.7.0"
    lazy val namespace = "com.github.scopt"
    val scopt          = namespace %% "scopt" % version
  }

  object cats {
    lazy val version   = "1.6.1"
    lazy val namespace = "org.typelevel"
    lazy val cats_core      = namespace %% "cats-core" % version
  }

  object spire {
    lazy val version   = "0.16.2"
    lazy val namespace = "org.typelevel"
    lazy val spire     = namespace %% "spire" % version
  }

  object akka {
    lazy val version   = "2.6.3"
    lazy val namespace = "com.typesafe.akka"

    lazy val actor                  = namespace %% "akka-actor"                  % version
    lazy val testkit                = namespace %% "akka-testkit"                % version
    lazy val stream                 = namespace %% "akka-stream"                 % version
    lazy val stream_testkit         = namespace %% "akka-stream-testkit"         % version
    lazy val distributedData        = namespace %% "akka-distributed-data"       % version
    lazy val cluster                = namespace %% "akka-cluster"                % version
    lazy val sharding               = namespace %% "akka-sharding"               % version
    lazy val jackson_serialization  = namespace %% "akka-serialization-jackson"  % version
    lazy val slf4j                  = namespace %% "akka-slf4j"                  % version
    lazy val clusterTools           = namespace %% "akka-cluster-tools"          % version
    lazy val clusterMetrics         = namespace %% "akka-cluster-metrics"        % version
    lazy val multiNode              = namespace %% "akka-multi-node-testkit"     % version
    lazy val discovery              = namespace %% "akka-discovery"              % version
  }

  object akka_management {
    lazy val version   = "1.0.5"
    lazy val namespace = "com.lightbend.akka.management"
    lazy val cluster_bootstrap = namespace %% "akka-management-cluster-bootstrap" % version excludeAll (ExclusionRule(organization="com.typesafe.akka"))
    lazy val cluster_http = namespace %% "akka-management-cluster-http" % version excludeAll (ExclusionRule(organization="com.typesafe.akka"))
  }

  object akka_discovery {
    lazy val version   = akka_management.version
    lazy val namespace = "com.lightbend.akka.discovery"
    lazy val kubernetes_api =  namespace %% "akka-discovery-kubernetes-api" % version excludeAll (ExclusionRule(organization="com.typesafe.akka"))
  }

  object scala_logging {
    val scala_logging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  }

  object logback {
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  }

  object akka_http {
    lazy val version   = "10.1.11"
    lazy val namespace = "com.typesafe.akka"
    lazy val default         = namespace                      %% "akka-http"            % version
    lazy val testkit         = namespace                      %% "akka-http-testkit"    % version % Test
    lazy val sprayJson       = "com.typesafe.akka"            %% "akka-http-spray-json" % version
    lazy val swaggerAkkaHttp = "com.github.swagger-akka-http" %% "swagger-akka-http"    % "2.0.4"
  }

  object kamon {
    lazy val namespace= "io.kamon"
    lazy val kamonVersion = "2.0.4"
    lazy val kamonPrometheusVersion = "2.0.1"
    lazy val sigarVersion = "1.6.6-rev002"
    lazy val bundle = namespace %% "kamon-bundle" % kamonVersion
    lazy val prometheus = namespace %% "kamon-prometheus" %  kamonPrometheusVersion
    lazy val sigarLoader = namespace % "sigar-loader" % sigarVersion
  }

  object lithium {
    lazy val version   = "0.10.0"
    lazy val namespace = "com.swissborg"
    lazy val core =  namespace %% "lithium" % version excludeAll (ExclusionRule(organization="com.typesafe.akka"))
  }

  object swagger {
    lazy val version     = "1.5.19"
    lazy val namespace   = "io.swagger"
    lazy val coreSwagger = namespace % "swagger-jaxrs" % version
  }

  object javaWebsocket {
    lazy val version       = "1.3.8"
    lazy val namespace     = "org.java-websocket"
    lazy val javaWebsocket = namespace % "Java-WebSocket" % version
  }

  object json4s {
    lazy val version   = "3.6.7"
    lazy val namespace = "org.json4s"
    lazy val jackson   = namespace %% "json4s-jackson" % version
  }

  object lucene {
    lazy val version     = "8.6.1"
    lazy val namespace   = "org.apache.lucene"
    lazy val core        = namespace % "lucene-core" % version
    lazy val queryParser = namespace % "lucene-queryparser" % version
    lazy val grouping    = namespace % "lucene-grouping" % version
    lazy val facet       = namespace % "lucene-facet" % version
    lazy val backwardCodecs = namespace % "lucene-backward-codecs" % version
  }

  object scalatest {
    lazy val version   = "3.0.7"
    lazy val namespace = "org.scalatest"
    lazy val core      = namespace %% "scalatest" % version
  }

  object gRPC {
    lazy val version         = scalapb.compiler.Version.grpcJavaVersion
    lazy val namespace       = "io.grpc"
    lazy val `grpc-netty`    = namespace % "grpc-netty" % version
  }

  object scalaPB {
    lazy val version        = scalapb.compiler.Version.scalapbVersion
    lazy val namespace      = "com.thesamet.scalapb"
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
    lazy val version   = "1.3.3"
    lazy val namespace = "com.typesafe"
    lazy val core      = namespace % "config" % version
  }

  object gatling {
    lazy val version    = "2.3.1"
    lazy val namespace  = "io.gatling"
    lazy val test       = namespace % "gatling-test-framework" % version
    lazy val highcharts = s"$namespace.highcharts" % "gatling-charts-highcharts" % version
  }

  object commonsIo {
    lazy val version   = "2.6"
    lazy val namespace = "commons-io"
    lazy val commonsIo = namespace % "commons-io" % version
  }

  object apacheCommons {
    lazy val version   = "3.11"
    lazy val namespace = "org.apache.commons"
    lazy val commonLang = namespace % "commons-lang3" % version
  }

  object zipUtils {
    lazy val version   = "1.13"
    lazy val namespace = "org.zeroturnaround"
    lazy val zip       = namespace % "zt-zip" % version
  }

  object Core {
    val libraries = Seq(
      akka.actor,
      spire.spire,
      lucene.core,
      lucene.queryParser,
      lucene.grouping,
      lucene.facet,
      lucene.backwardCodecs,
      scalatest.core % Test,
      akka.testkit   % Test,
      commonsIo.commonsIo,
      apacheCommons.commonLang,
      zipUtils.zip
    )
  }

  object Common {
    val libraries = Seq(
      scala_logging.scala_logging,
      config.core,
      akka.jackson_serialization,
      scalatest.core % Test
    )
  }

  object RPC {
    val libraries = Seq(
      slf4j.api,
      gRPC.`grpc-netty`,
      scalaPB.`runtime`,
      scalaPB.`runtime-grpc`
    )
  }

  object Cluster {
    lazy val libraries = Seq(
      akka.cluster,
      akka.clusterTools,
      akka.clusterMetrics,
      akka.discovery,
      akka.jackson_serialization,
      akka_management.cluster_bootstrap,
      akka_management.cluster_http,
      akka_discovery.kubernetes_api,
      akka.distributedData,
      lithium.core,
      scala_logging.scala_logging,
      akka.slf4j,
      kamon.bundle,
      kamon.prometheus,
      kamon.sigarLoader,
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
      akka_http.default,
      akka.stream
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
      cats.cats_core,
      scala_logging.scala_logging,
      akka.slf4j,
      logback.logback,
      scalatest.core % Test
    )
  }

  object Http {
    lazy val libraries = Seq(
      akka.stream,
      akka_http.default,
      akka.stream,
      json4s.jackson,
      akka_http.sprayJson,
      javaWebsocket.javaWebsocket,
      scalatest.core % Test,
      akka.stream_testkit,
      akka_http.testkit,
      akka_http.swaggerAkkaHttp,
      swagger.coreSwagger
    )
  }

  object Minicluster {
    lazy val libraries = Seq(
      scalatest.core
    )
  }

  object It {
    lazy val libraries = Seq(
      scalatest.core % "it,test"
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
