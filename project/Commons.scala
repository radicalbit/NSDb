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

import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.{MergeStrategy, assemblyMergeStrategy}
import sbtassembly.PathList
import scalafix.sbt.ScalafixPlugin.autoImport.scalafixSemanticdb

object Commons {

  val scalaVer = "2.12.7"

  val settings: Seq[Def.Setting[_]] = Seq(
    scalaVersion := scalaVer,
    addCompilerPlugin(scalafixSemanticdb),
    scalacOptions ++= Seq(
      "-encoding",
      "utf8",
      "-Yrangepos",
      "-Ywarn-unused",
      "-deprecation",
      "-language:implicitConversions",
      "-language:higherKinds",
      "-language:existentials",
      "-language:postfixOps",
      "-Ypartial-unification"
    ),
    organization := "io.radicalbit.nsdb",
    resolvers ++= Seq(
      Opts.resolver.mavenLocalFile,
      "Radicalbit Public Releases" at "https://tools.radicalbit.io/artifactory/public-release/",
      "Radicalbit Public Snapshots" at "https://tools.radicalbit.io/artifactory/public-snapshot/",
      "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven",
      Resolver.bintrayRepo("hseeberger", "maven")
    ),
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    concurrentRestrictions in Test += Tags.limitAll(1),
    concurrentRestrictions in IntegrationTest += Tags.limitAll(1),
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case PathList("CHANGELOG.adoc")                           => MergeStrategy.first
      case PathList("CHANGELOG.html")                           => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
}
