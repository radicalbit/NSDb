lazy val root = project
  .in(file("."))
  .settings(PublishSettings.dontPublish: _*)
  .settings(
    name := "nsdb",
    crossScalaVersions := Seq("2.11.11", "2.12.3")
  )
  .aggregate(`nsdb-common`,
             `nsdb-core`,
             `nsdb-http`,
             `nsdb-cluster`,
             `nsdb-rpc`,
             `nsdb-akka-client`,
             `nsdb-scala-api`,
             `nsdb-sql`,
             `nsdb-cli`,
             `nsdb-flink-connector`)

lazy val `nsdb-common` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Common.libraries)

lazy val `nsdb-core` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Core.libraries)
  .dependsOn(`nsdb-common`)

lazy val `nsdb-http` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Http.libraries)
  .dependsOn(`nsdb-core`, `nsdb-sql`)
  .dependsOn(`nsdb-core`)

lazy val `nsdb-rpc` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.RPC.libraries)
  .settings(
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ))
  .dependsOn(`nsdb-common`)

lazy val `nsdb-akka-client` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.AkkaClient.libraries)
  .dependsOn(`nsdb-common`)

lazy val `nsdb-cluster` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Cluster.libraries)
  .dependsOn(`nsdb-http`, `nsdb-rpc`)

lazy val `nsdb-sql` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.SQL.libraries)
  .dependsOn(`nsdb-common`)

lazy val `nsdb-scala-api` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.ScalaAPI.libraries)
  .dependsOn(`nsdb-rpc`)

lazy val `nsdb-cli` = project
  .settings(Commons.settings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(libraryDependencies ++= Dependencies.CLI.libraries)
  .dependsOn(`nsdb-akka-client`, `nsdb-sql`)

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
    //publishArtifact in makePom := false,
    //    ivyXML :=
    //      <dependencies>
    //        <dependency org="org.apache.flink" name="flink-streaming-scala_2.11" rev="1.3.2" conf="provided->default(compile)"/>
    //      </dependencies>
  )
  .dependsOn(`nsdb-scala-api`)

onLoad in Global := (Command.process("scalafmt", _: State)) compose (onLoad in Global).value

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

fork in test := false
