lazy val root = project
  .in(file("."))
  .settings(
    name := "nsdb",
    crossScalaVersions := Seq("2.10.6", "2.12.2"),
    publish := {},
    publishLocal := {}
  )
  .aggregate(`nsdb-core`, `nsdb-client`, `nsdb-cluster`, `nsdb-scala-api`, `nsdb-sql`, `nsdb-http`, `nsdb-cli`)

lazy val `nsdb-core` = project
  .settings(Commons.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Core.libraries)

lazy val `nsdb-cluster` = project
  .settings(Commons.settings: _*)
  .settings(libraryDependencies ++= Dependencies.Cluster.libraries)
  .dependsOn(`nsdb-core`)

lazy val `nsdb-client` = project
  .settings(Commons.settings: _*)
  .dependsOn(`nsdb-cluster`)
  .settings(libraryDependencies ++= Dependencies.Client.libraries)

lazy val `nsdb-scala-api` = project
  .settings(Commons.settings: _*)
  .settings(libraryDependencies ++= Dependencies.ScalaAPI.libraries)
  .dependsOn(`nsdb-sql`, `nsdb-client`)

lazy val `nsdb-sql` = project
  .settings(Commons.settings: _*)
  .settings(libraryDependencies ++= Dependencies.SQL.libraries)
  .dependsOn(`nsdb-client`)

lazy val `nsdb-http` = project
  .settings(Commons.settings: _*)
  .dependsOn(`nsdb-scala-api`)

lazy val `nsdb-cli` = project
  .settings(Commons.settings: _*)
  .settings(libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.12.2")
  .dependsOn(`nsdb-sql`)

onLoad in Global := (Command.process("scalafmt", _: State)) compose (onLoad in Global).value

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

fork in test := false
