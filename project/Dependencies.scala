import sbt._

object Dependencies {

  object akka {
    lazy val version   = "2.5.3"
    lazy val namespace = "com.typesafe.akka"

    lazy val core            = namespace %% "akka-core"             % version
    lazy val stream          = namespace %% "akka-stream"           % version
    lazy val distributedData = namespace %% "akka-distributed-data" % version
    lazy val cluster         = namespace %% "akka-cluster"          % version
    lazy val sharding        = namespace %% "akka-sharding"         % version

  }

  object akka_http {
    lazy val version   = "10.0.5"
    lazy val namespace = "com.typesafe.akka"

    lazy val core_http = namespace %% "akka-http-core" % version excludeAll (ExclusionRule(organization =
                                                                                             "com.typesafe.akka",
                                                                                           name = "akka-actor"),
    ExclusionRule(organization = "com.typesafe.akka", name = "akka-testkit"),
    ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream"),
    ExclusionRule(organization = "com.typesafe.akka", name = "akka-stream-testkit"))
    lazy val default    = namespace %% "akka-http"            % version
    lazy val testkit    = namespace %% "akka-http-testkit"    % version % Test
    lazy val spray_json = namespace %% "akka-http-spray-json" % version
    lazy val jackson    = namespace %% "akka-http-jackson"    % version
    lazy val xml        = namespace %% "akka-http-xml"        % version

    lazy val core = Seq(core_http, default, testkit, spray_json, jackson, xml)
  }

  object akka_sse {
    lazy val sse  = "de.heikoseeberger" %% "akka-sse" % "2.0.0"
    lazy val core = Seq(sse)
  }

  object calcite {
    lazy val version   = "1.12.0"
    lazy val namespace = "org.apache.calcite"
    lazy val core      = namespace % "calcite-core" % version
  }

  object lucene {
    lazy val version     = "6.6.0"
    lazy val namespace   = "org.apache.lucene"
    lazy val core        = namespace % "lucene-core" % version
    lazy val queryParser = "org.apache.lucene" % "lucene-queryparser" % version
    lazy val facet       = "org.apache.lucene" % "lucene-facet" % version

  }

  object scalatest {
    lazy val version   = "3.0.0"
    lazy val namespace = "org.scalatest"
    lazy val core      = namespace %% "scalatest" % version
  }

  object junit {
    lazy val version   = "4.12"
    lazy val namespace = "junit"
    lazy val junit     = namespace % "junit" % version
  }

  object junitInterface {
    lazy val version        = "0.11"
    lazy val namespace      = "com.novocode"
    lazy val junitInterface = namespace % "junit-interface" % version
  }

  object logging {
    lazy val `scala-logging` = "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  }

  lazy val asm = "asm" % "asm" % "3.3.1" % Test //import to use ClosureCleaner in test

  object Core {
    val libraries = Seq(
      lucene.core,
      lucene.queryParser,
      lucene.facet,
      scalatest.core % "test"
    )
  }
}
