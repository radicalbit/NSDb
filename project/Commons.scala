import sbt._
import Keys._
import sbtassembly.AssemblyKeys._
import Dependencies._

object Commons {

  val scalaVer = "2.12.2"

  val settings: Seq[Def.Setting[_]] = Seq(
    scalaVersion := scalaVer,
    organization := "io.radicalbit",
    resolvers ++= Seq(
      Opts.resolver.mavenLocalFile,
      "Radicalbit Repo" at "https://tools.radicalbit.io/maven/repository/internal/",
      Resolver.bintrayRepo("hseeberger", "maven")
    ),
    libraryDependencies ++= Seq(
      akka.actor,
//      lucene.core,
//      lucene.queryParser,
//      lucene.facet,
      akka.testkit   % "test",
      scalatest.core % "test"
    ), //++ akka_http.core ++ akka_sse.core,
    parallelExecution in Test := false,
    test in assembly := {}
  )
}
