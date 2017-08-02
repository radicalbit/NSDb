import sbt._

object Dependencies {

  object Scala {

    lazy val libraries = Seq(
      scalatest.core % Test
    )
  }

  private object scalatest {
    lazy val namespace = "org.scalatest"
    lazy val version = "3.0.1"
    lazy val core = namespace %% "scalatest" % version
  }

  object Core {
    lazy val libraries = Scala.libraries ++ Seq(
    )
  }
}
