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
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}
import Path.relativeTo
lazy val root = project
  .in(file("."))
  .settings(
    name := "nsdb",
    publish := {},
    publishLocal := {}
  )
  .aggregate(
    `nsdb-common`,
    `nsdb-core`,
    `nsdb-http`,
    `nsdb-cluster`,
    `nsdb-security`,
    `nsdb-rpc`,
    `nsdb-java-api`,
    `nsdb-scala-api`,
    `nsdb-sql`,
    `nsdb-cli`,
    `nsdb-perf`,
    `nsdb-minicluster`,
    `nsdb-it`
  )
lazy val packageDist   = taskKey[File]("create universal package and move it to package folder")
lazy val packageDeb    = taskKey[File]("create debian package and move it to package folder")
lazy val packageRpm    = taskKey[File]("create RPM package and move it to package folder")
addCommandAlias("fix", "all compile:scalafix test:scalafix")
addCommandAlias("fixCheck", "; compile:scalafix --check ; test:scalafix --check")
addCommandAlias("dist", "packageDist")
addCommandAlias("deb", "packageDeb")
addCommandAlias("rpm", "packageRpm")
addCommandAlias("testAll", "test; multi-jvm:test; it:test")
lazy val `nsdb-common` = project
  .settings(Commons.crossScalaVersionSettings: _*)
  .settings(PublishSettings.settings: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "io.radicalbit.nsdb"
  )
  .settings(LicenseHeader.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Common.libraries)
lazy val `nsdb-core` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Core.libraries)
  .dependsOn(`nsdb-common` % "compile->compile;test->test")
lazy val `nsdb-http` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Http.libraries)
  .dependsOn(`nsdb-core` % "compile->compile;test->test", `nsdb-sql`, `nsdb-security`)
lazy val `nsdb-rpc` = project
  .settings(Commons.crossScalaVersionSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.RPC.libraries)
  .settings(coverageExcludedPackages := "io\\.radicalbit\\.nsdb.*")
  .settings(
    Compile / PB.targets   := Seq(
    scalapb.gen() -> (Compile / sourceManaged).value
  ),
    Test / PB.targets := Seq(
      scalapb.gen() -> (Test / sourceManaged).value
    )
  )
  .settings(LicenseHeader.settings: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(`nsdb-sql`, `nsdb-security`, `nsdb-core`, `nsdb-common` % "compile->compile;test->test")
lazy val `nsdb-cluster` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .enablePlugins(JavaServerAppPackaging, SbtNativePackager)
  .settings(libraryDependencies ++= Dependencies.Cluster.libraries)
  .enablePlugins(MultiJvmPlugin)
  .configs(MultiJvm)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(bashScriptDefines / scriptClasspath += "../ext-lib/*")
  .settings(SbtMultiJvm.multiJvmSettings)
  .configs(MultiJvm)
  .settings(
    /* Docker Settings - to create, run as:
       $ sbt `project nsdb-cluster` docker:publishLocal
       See here for details:
       http://www.scala-sbt.org/sbt-native-packager/formats/docker.html
     */
    Docker / packageName := "nsdb",
    Docker / mappings ++= {
      val confDir = baseDirectory.value / "src/main/resources"
      val confResources = ((confDir ** "*" --- confDir) pair (relativeTo(confDir), false)).filterNot{case (_,name) => name.contains("application")}
      for {
        (file, relativePath) <- confResources
      } yield file -> s"/opt/${(Docker / packageName).value}/conf/$relativePath"
    },
    Docker / mappings ++= {
      val scriptDir = baseDirectory.value / "../docker-scripts"
      for {
        (file, relativePath) <- (scriptDir ** "*" --- scriptDir) pair (relativeTo(scriptDir), false)
      } yield file -> s"/opt/${(Docker / packageName).value}/bin/$relativePath"
    },
    Docker / version := version.value,
    Docker / maintainer := organization.value,
    dockerRepository := Some("weareradicalbit"),
    Docker / defaultLinuxInstallLocation := s"/opt/${(Docker / packageName).value}",
    dockerCommands := Seq(
      Cmd("FROM", "adoptopenjdk/openjdk11:alpine-slim"),
      Cmd("LABEL", s"""MAINTAINER="${organization.value}""""),
      Cmd("RUN", "apk add", "--no-cache", "bash", "udev"),
      Cmd("WORKDIR", s"/opt/${(Docker / packageName).value}"),
      Cmd("RUN", "addgroup", "-S", "nsdb", "&&", "adduser", "-S", "nsdb", "-G", "nsdb"),
      Cmd("ADD", "opt", "/opt"),
      ExecCmd("RUN", "chown", "-R", "nsdb:nsdb", "."),
      Cmd("USER", "nsdb"),
      Cmd("HEALTHCHECK", "--timeout=3s", "CMD", "bin/nsdb-healthcheck"),
      Cmd("CMD", "bin/nsdb-cluster -Dlogback.configurationFile=conf/logback.xml -DconfDir=conf/")
    )
  )
  .settings(
    /* Debian Settings - to create, run as:
       $ sbt `project nsdb-cluster` debian:packageBin
       See here for details:
       http://www.scala-sbt.org/sbt-native-packager/formats/debian.html
     */
    Debian / name := "nsdb",
    Debian / version := version.value,
    Debian / maintainer := "Radicalbit <info@radicalbit.io>",
    Debian / packageSummary := "NSDb is an open source, brand new distributed time series Db, streaming oriented, optimized for the serving layer and completely based on Scala and Akka",
    Debian / packageDescription := "NSDb is an open source, brand new distributed time series Db, streaming oriented, optimized for the serving layer and completely based on Scala and Akka",
    packageDeb := {
      val distFile = (Debian / packageBin).value
      val output   = baseDirectory.value / ".." / "package" / distFile.getName
      IO.move(distFile, output)
      output
    }
  )
  .settings(
    /* RPM Settings - to create, run as:
       $ sbt `project nsdb-cluster` rpm:packageBin
       See here for details:
       http://www.scala-sbt.org/sbt-native-packager/formats/rpm.html
     */
    Rpm / version := version.value,
    Rpm / packageName := "nsdb",
    rpmRelease := "1",
    Rpm / packageSummary := "NSDb is an open source, brand new distributed time series Db, streaming oriented, optimized for the serving layer and completely based on Scala and Akka",
    Rpm / packageDescription := "NSDb is an open source, brand new distributed time series Db, streaming oriented, optimized for the serving layer and completely based on Scala and Akka",
    rpmVendor := "Radicalbit",
    rpmUrl := Some("https://github.com/radicalbit/NSDb"),
    rpmLicense := Some("Apache"),
    packageRpm := {
      val distFile = (Rpm / packageBin).value
      val output   = baseDirectory.value / ".." / "package" / distFile.getName
      IO.move(distFile, output)
      output
    }
  )
  .settings(
    /* Universal Settings - to create, run as:
       $ sbt `project nsdb-cluster` universal:packageBin
       See here for details:
       http://www.scala-sbt.org/sbt-native-packager/formats/universal.html
     */
    Universal / packageName := s"nsdb-${version.value}",
    Universal / mappings ++= {
      val confDir = baseDirectory.value / "src/main/resources"
      val confResources = ((confDir ** "*" --- confDir) pair (relativeTo(confDir), false)).filterNot{case (_,name) => name.contains("application")}
      for {
        (file, relativePath) <- confResources
      } yield file -> s"conf/$relativePath"
    },
    Compile / discoveredMainClasses ++= (`nsdb-cli` / Compile / discoveredMainClasses).value,
    bashScriptDefines ++= Seq(
      """addJava "-DconfDir=${app_home}/../conf"""",
      """addJava "-Dlogback.configurationFile=${app_home}/../conf/logback.xml""""
    ),
    packageDist := {
      val distFile = (Universal / packageBin).value
      val output   = baseDirectory.value / ".." / "package" / distFile.getName
      IO.move(distFile, output)
      output
    }
  )
  .dependsOn(`nsdb-core` % "compile->compile;test->test",  `nsdb-security`, `nsdb-http`, `nsdb-rpc`, `nsdb-cli`)
lazy val `nsdb-security` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(crossPaths := false)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
lazy val `nsdb-sql` = project
  .settings(Commons.crossScalaVersionSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.SQL.libraries)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-common` % "compile->compile;test->test")
lazy val `nsdb-java-api` = project
  .settings(Commons.settings: _*)
  .settings(crossPaths := false)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.JavaAPI.libraries)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-rpc`)
lazy val `nsdb-scala-api` = project
  .settings(Commons.crossScalaVersionSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.ScalaAPI.libraries)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-rpc`)
lazy val `nsdb-cli` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(libraryDependencies ++= Dependencies.CLI.libraries)
  .settings(coverageExcludedPackages := "io\\.radicalbit\\.nsdb.*")
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-rpc`)
  .dependsOn(`nsdb-common` % "compile->compile;test->test")
lazy val `nsdb-perf` = (project in file("nsdb-perf"))
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(libraryDependencies ++= Dependencies.Performance.libraries)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .enablePlugins(GatlingPlugin)
lazy val `nsdb-minicluster` = (project in file("nsdb-minicluster"))
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Minicluster.libraries)
  .dependsOn(`nsdb-cluster`)
  .dependsOn(`nsdb-scala-api`)
lazy val `nsdb-it` = (project in file("nsdb-it"))
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(libraryDependencies ++= Dependencies.It.libraries)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .dependsOn(`nsdb-minicluster`)
ThisBuild / scalafmtOnCompile := true
// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath,  Compile / run / mainClass, Compile / run / runner)