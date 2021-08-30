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
import scalafix.sbt.ScalafixPlugin.autoImport.scalafixSemanticdb

object Commons {

  val scalaVer = "2.12.12"

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
      "-language:postfixOps"
    ),
    organization := "io.radicalbit.nsdb",
    resolvers ++= Seq(
      Opts.resolver.mavenLocalFile,
      "Radicalbit Public Releases" at "https://tools.radicalbit.io/artifactory/public-release/",
      "Radicalbit Public Snapshots" at "https://tools.radicalbit.io/artifactory/public-snapshot/",
      Resolver.bintrayRepo("hseeberger", "maven")
    ),
    parallelExecution in IntegrationTest := false,
    parallelExecution in Test := false,
    concurrentRestrictions in IntegrationTest += Tags.limitAll(1)
  )

  val crossScalaVersionSettings = settings :+ (crossScalaVersions := Seq("2.12.12", "2.13.3"))
}
