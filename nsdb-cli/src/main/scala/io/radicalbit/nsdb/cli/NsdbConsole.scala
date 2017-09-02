package io.radicalbit.nsdb.cli

import io.radicalbit.nsdb.cli.console.NsdbILoop

import scala.tools.nsc.Settings
import scala.util.Try

case class Params(host: Option[String] = None, port: Option[Int] = None)

object NsdbConsole extends App {

  val parser = new scopt.OptionParser[Params]("scopt") {
    head("scopt", "3.x")
    opt[String]('h', "host") action { (x, c) =>
      c.copy(host = Some(x))
    } text "the remote host"
    opt[Int]('p', "port") action { (x, c) =>
      c.copy(port = Some(x))
    } text "the remote port"
  }

  parser.parse(args, Params()) map { params =>
    val settings = new Settings
    settings.usejavacp.value = true
    settings.deprecation.value = true

    new NsdbILoop(params.host, params.port).process(settings)
  }
}
