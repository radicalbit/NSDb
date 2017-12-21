import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

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
    `nsdb-rpc`,
    `nsdb-scala-api`,
    `nsdb-sql`,
    `nsdb-cli`,
    `nsdb-flink-connector`,
    `nsdb-perf`
  )

lazy val `nsdb-common` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(libraryDependencies ++= Dependencies.Common.libraries)

lazy val `nsdb-core` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(libraryDependencies ++= Dependencies.Core.libraries)
  .dependsOn(`nsdb-common`)

lazy val `nsdb-http` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(libraryDependencies ++= Dependencies.Http.libraries)
  .dependsOn(`nsdb-core`, `nsdb-sql`)
  .dependsOn(`nsdb-core`)

lazy val `nsdb-rpc` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(libraryDependencies ++= Dependencies.RPC.libraries)
  .settings(
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ))
  .dependsOn(`nsdb-common`, `nsdb-sql`)

lazy val `nsdb-cluster` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
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
  .settings(SbtMultiJvm.multiJvmSettings)
  .configs(MultiJvm)
  .settings(assemblyJarName in assembly := "nsdb-cluster.jar")
  .dependsOn(`nsdb-http`, `nsdb-rpc`)

lazy val `nsdb-sql` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(libraryDependencies ++= Dependencies.SQL.libraries)
  .dependsOn(`nsdb-common`)

lazy val `nsdb-scala-api` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(libraryDependencies ++= Dependencies.ScalaAPI.libraries)
  .dependsOn(`nsdb-rpc`)

lazy val `nsdb-cli` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(libraryDependencies ++= Dependencies.CLI.libraries)
  .settings(assemblyJarName in assembly := "nsdb-cli.jar")
  .dependsOn(`nsdb-rpc`, `nsdb-sql`)

lazy val `nsdb-flink-connector` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.FlinkConnector.libraries)
  .settings(
    // exclude Scala library from assembly
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyShadeRules in assembly := Seq(
      // ShadeRule.rename("com.google.**" -> "shade.com.google.@1").inAll,
      ShadeRule.rename("com.google.**"         -> "io.radicalbit.nsdb.shaded.com.google.@1").inAll,
      ShadeRule.rename("org.apache.commons.**" -> "io.radicalbit.nsdb.shaded.org.apache.commons.@1").inAll,
      // ShadeRule.rename("io.grpc.**" -> "io.radicalbit.nsdb.shaded.io.grpc.@1").inAll,
      ShadeRule.rename("io.netty.**" -> "io.radicalbit.nsdb.shaded.io.netty.@1").inAll
    ),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.filterDistinctLines
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.copy(`classifier` = None)
    },
    addArtifact(artifact in (Compile, assembly), assembly)
  )
  .dependsOn(`nsdb-scala-api`)

lazy val `nsdb-perf` = (project in file("nsdb-perf"))
  .settings(Commons.settings: _*)
  .settings(PublishSettings.dontPublish: _*)
  .settings(scalaVersion := "2.11.11")
  .settings(libraryDependencies ++= Dependencies.Performance.libraries)
  .enablePlugins(GatlingPlugin)

onLoad in Global := (Command.process("scalafmt", _: State)) compose (onLoad in Global).value

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

fork in test := false
