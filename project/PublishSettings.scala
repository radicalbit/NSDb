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

object PublishSettings {

  lazy val settings: Seq[Def.Setting[_]] = Seq(
    publishArtifact := true,
    publishArtifact in (Compile, packageDoc) := false,
    publishMavenStyle := true,
    publishTo := version { v: String =>
      if (v.trim.endsWith("SNAPSHOT"))
        Some(
          "Artifactory Realm" at "https://tools.radicalbit.io/artifactory/public-snapshot;build.timestamp=" + new java.util.Date().getTime)
      else
        Some("Artifactory Realm" at "https://tools.radicalbit.io/artifactory/public-release/")
    }.value,
    credentials += Credentials(Path.userHome / ".artifactory" / ".credentials")
  )

  lazy val dontPublish: Seq[Def.Setting[_]] = Seq(
    publish := {}
  )
}
