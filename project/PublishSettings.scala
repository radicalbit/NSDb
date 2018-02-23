import sbt.Keys._
import sbt._

object PublishSettings {

  lazy val settings: Seq[Def.Setting[_]] = Seq(
    publishArtifact := true,
    publishArtifact in (Compile, packageDoc) := false,
    publishMavenStyle := true,
    publishTo := version { (v: String) =>
      if (v.trim.endsWith("SNAPSHOT"))
        Some(
          "Artifactory Realm" at "https://tools.radicalbit.io/artifactory/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime)
      else
        Some("Artifactory Realm" at "https://tools.radicalbit.io/artifactory/libs-release-local")
    }.value,
    credentials += Credentials(Path.userHome / ".artifactory" / ".credentials")
  )

  lazy val dontPublish: Seq[Def.Setting[_]] = Seq(
    publish := {}
  )
}
