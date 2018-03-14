import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.{MergeStrategy, assemblyMergeStrategy}
import sbtassembly.PathList

object Commons {

  val scalaVer = "2.12.4"

  val settings: Seq[Def.Setting[_]] = Seq(
    scalaVersion := scalaVer,
    organization := "io.radicalbit.nsdb",
    resolvers ++= Seq(
      Opts.resolver.mavenLocalFile,
      "Radicalbit Repo" at "https://tools.radicalbit.io/artifactory/libs-release-local/",
      Resolver.bintrayRepo("hseeberger", "maven"),
      "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven",
      Resolver.bintrayRepo("hseeberger", "maven")
    ),
    parallelExecution in Test := false,
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
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
