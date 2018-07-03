/*
 * Copyright 2018 Radicalbit S.r.l.
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

lazy val root = project
  .in(file("."))
  .settings(
    name := "nsdb",
    crossScalaVersions := Seq("2.11.11", "2.12.4"),
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
    `nsdb-flink-connector`,
    `nsdb-kafka-connect`,
    `nsdb-perf`,
    `nsdb-web-ui`
  )

addCommandAlias("dist", "universal:packageBin")
addCommandAlias("deb", "debian:packageBin")
addCommandAlias("rpm", "rpm:packageBin")

val uiCompileTask = taskKey[Unit]("build UI")
val copyTask      = taskKey[Unit]("copy UI")

lazy val `nsdb-web-ui` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .enablePlugins(FrontendPlugin)
  .settings(libraryDependencies ++= Dependencies.Http.libraries)
  .settings(
    nodePackageManager := sbtfrontend.NodePackageManager.Yarn,
    FrontendKeys.nodeInstallDirectory := (baseDirectory.value / "app/.frontend"),
    FrontendKeys.nodeWorkingDirectory := (baseDirectory.value / "app"),
    FrontendKeys.nodeVersion := "v8.11.1",
    uiCompileTask := {
      val log = streams.value.log
      log.info("Starting build ui task")
      yarn.toTask(" setup").value
    },
    copyTask := {
      val log = streams.value.log
      uiCompileTask.value
      val to   = (target in Compile).value / s"scala-${scalaVersion.value.split("\\.").take(2).mkString(".")}" / "classes" / "ui"
      val from = baseDirectory.value / "app/build"
      log.info("Deleting previous resources")
      IO.delete(to)
      log.info("Coping ui static resources")
      IO.copyDirectory(from, to)

    },
    (compile in Compile) <<= (compile in Compile) dependsOn copyTask
  )

lazy val `nsdb-common` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Common.libraries)

lazy val `nsdb-core` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Core.libraries)
  .dependsOn(`nsdb-common`)

lazy val `nsdb-http` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Http.libraries)
  .dependsOn(`nsdb-core`, `nsdb-sql`, `nsdb-security`, `nsdb-web-ui`)

lazy val `nsdb-rpc` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.RPC.libraries)
  .settings(coverageExcludedPackages := "io\\.radicalbit\\.nsdb.*")
  .settings(PB.targets in Compile := Seq(
    scalapb.gen() -> (sourceManaged in Compile).value
  ))
  .settings(LicenseHeader.settings: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .dependsOn(`nsdb-common`, `nsdb-sql`)

lazy val `nsdb-cluster` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .enablePlugins(JavaServerAppPackaging, SbtNativePackager)
  .settings(libraryDependencies ++= Dependencies.Cluster.libraries)
  .settings(
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
      case (testResults, multiNodeResults) =>
        val overall =
          if (testResults.overall.id < multiNodeResults.overall.id)
            multiNodeResults.overall
          else
            testResults.overall
        Tests.Output(overall,
                     testResults.events ++ multiNodeResults.events,
                     testResults.summaries ++ multiNodeResults.summaries)
    }
  )
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(scriptClasspath in bashScriptDefines += "../ext-lib/*")
  .settings(SbtMultiJvm.multiJvmSettings)
  .configs(MultiJvm)
  .settings(assemblyJarName in assembly := "nsdb-cluster.jar")
  .settings(
    /* Docker Settings - to create, run as:
       $ sbt `project nsdb-cluster` docker:publishLocal

       See here for details:
       http://www.scala-sbt.org/sbt-native-packager/formats/docker.html
     */
    mappings in Docker ++= {
      val confDir = baseDirectory.value / "src/main/resources"

      for {
        (file, relativePath) <- (confDir.*** --- confDir) x relativeTo(confDir)
      } yield file -> s"/opt/${name.value}/conf/$relativePath"
    },
    mappings in Docker ++= {
      val confDir = baseDirectory.value / "../docker-scripts"

      for {
        (file, relativePath) <- (confDir.*** --- confDir) x relativeTo(confDir)
      } yield file -> s"/opt/${name.value}/bin/$relativePath"
    },
    packageName in Docker := name.value,
    version in Docker := version.value,
    maintainer in Docker := organization.value,
    dockerRepository := Some("tools.radicalbit.io"),
    defaultLinuxInstallLocation in Docker := s"/opt/${name.value}",
    dockerCommands := Seq(
      Cmd("FROM", "tools.radicalbit.io/service-java-base:1.0"),
      Cmd("LABEL", s"""MAINTAINER="${organization.value}""""),
      Cmd("WORKDIR", s"/opt/${name.value}"),
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
    version in Debian := version.value,
    maintainer in Debian := "Radicalbit <info@radicalbit.io>",
    packageSummary in Debian := "NSDb is an open source, brand new distributed time series Db, streaming oriented, optimized for the serving layer and completely based on Scala and Akka",
    packageDescription in Debian := "NSDb is an open source, brand new distributed time series Db, streaming oriented, optimized for the serving layer and completely based on Scala and Akka"
  )
  .settings(
    /* RPM Settings - to create, run as:
       $ sbt `project nsdb-cluster` rpm:packageBin

       See here for details:
       http://www.scala-sbt.org/sbt-native-packager/formats/rpm.html
     */
    version in Rpm := version.value,
    packageName in Rpm := name.value,
    rpmRelease := "1",
    packageSummary in Rpm := "NSDb is an open source, brand new distributed time series Db, streaming oriented, optimized for the serving layer and completely based on Scala and Akka",
    packageDescription in Rpm := "NSDb is an open source, brand new distributed time series Db, streaming oriented, optimized for the serving layer and completely based on Scala and Akka",
    rpmVendor := "Radicalbit",
    rpmUrl := Some("https://github.com/radicalbit/NSDb"),
    rpmLicense := Some("Apache")
  )
  .settings(
    mappings in Universal ++= {
      val confDir = baseDirectory.value / "src/main/resources"

      for {
        (file, relativePath) <- (confDir.*** --- confDir) x relativeTo(confDir)
      } yield file -> s"conf/$relativePath"
    },
    discoveredMainClasses in Compile ++= (discoveredMainClasses in (`nsdb-cli`, Compile)).value,
    bashScriptDefines ++= Seq(
      """addJava "-DconfDir=${app_home}/../conf"""",
      """addJava "-Dlogback.configurationFile=${app_home}/../conf/logback.xml""""
    )
  )
  .dependsOn(`nsdb-security`, `nsdb-http`, `nsdb-rpc`, `nsdb-cli`)

lazy val `nsdb-security` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Security.libraries)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-common`)

lazy val `nsdb-sql` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.SQL.libraries)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-common`)

lazy val `nsdb-java-api` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.JavaAPI.libraries)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-rpc`)

lazy val `nsdb-scala-api` = project
  .settings(Commons.settings: _*)
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
  .settings(assemblyJarName in assembly := "nsdb-cli.jar")
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-rpc`, `nsdb-sql`)

lazy val `nsdb-flink-connector` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.FlinkConnector.libraries)
  .settings(
    // exclude Scala library from assembly
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.google.**"         -> "io.radicalbit.nsdb.shaded.com.google.@1").inAll,
      ShadeRule.rename("org.apache.commons.**" -> "io.radicalbit.nsdb.shaded.org.apache.commons.@1").inAll,
      ShadeRule.rename("io.netty.**"           -> "io.radicalbit.nsdb.shaded.io.netty.@1").inAll
    ),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.filterDistinctLines
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.copy(`classifier` = Some(""))
    },
    addArtifact(artifact in (Compile, assembly), assembly)
  )
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-scala-api`)

lazy val `nsdb-kafka-connect` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.KafkaConnect.libraries)
  .settings(
    // include Scala library in assembly
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true),
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.copy(`classifier` = Some(""))
    },
    addArtifact(artifact in (Compile, assembly), assembly)
  )
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .dependsOn(`nsdb-scala-api`)

lazy val `nsdb-perf` = (project in file("nsdb-perf"))
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(scalaVersion := "2.11.11")
  .settings(libraryDependencies ++= Dependencies.Performance.libraries)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .enablePlugins(GatlingPlugin)

onLoad in Global := (Command.process("scalafmt", _: State)) compose (onLoad in Global).value

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

fork in test := false
