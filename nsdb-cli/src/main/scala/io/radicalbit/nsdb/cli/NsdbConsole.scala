package io.radicalbit.nsdb.cli

import io.radicalbit.nsdb.cli.console.NsdbILoop

import scala.tools.nsc.Settings

/**
  * Wrapper for CLI parameters
  *
  * @param host rpc server address
  * @param port rpc server port
  * @param db database name
  */
case class Params(host: Option[String] = None, port: Option[Int] = None, db: String, width: Option[Int])

/**
  * Runner for Nsdb CLI.
  * Internally makes use of [[scopt.OptionParser]] to parse CLI connection parameters as [[Params]]
  * It instantiates CLI main class [[NsdbILoop]] with parsed parameters.
  */
object NsdbConsole extends App {

  val parser = new scopt.OptionParser[Params]("scopt") {
    head("scopt", "3.x")
    opt[String]('h', "host") action { (x, c) =>
      c.copy(host = Some(x))
    } text "the remote host"
    opt[Int]('p', "port") action { (x, c) =>
      c.copy(port = Some(x))
    } text "the remote port"
    opt[String]('d', "database").required() action { (x, c) =>
      c.copy(db = x)
    } text "the database to select"
    opt[Int]('w', "width") action { (x, c) =>
      c.copy(port = Some(x))
    } text "table max width"
  }

  parser.parse(args, Params(None, None, "root", None)) map { params =>
    val settings = new Settings
    settings.usejavacp.value = true
    settings.deprecation.value = true

    new NsdbILoop(params.host, params.port, params.db, params.width).process(settings)
  }
}
