/*
 * Copyright 2018 Radicalbit S.r.l.
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

package io.radicalbit.nsdb.cli

import io.radicalbit.nsdb.cli.console.NsdbILoop

import scala.tools.nsc.Settings

/**
  * Runner for Nsdb CLI.
  * Internally makes use of [[scopt.OptionParser]] to parse CLI connection parameters as [[io.radicalbit.nsdb.cli.Cli.Params]].
  * It instantiates CLI main class [[NsdbILoop]] with parsed parameters.
  */
object Cli extends App {

  /**
    * Wrapper for CLI parameters.
    *
    * @param host rpc server address.
    * @param port rpc server port.
    * @param db database name.
    */
  case class Params(host: Option[String] = None, port: Option[Int] = None, db: String, width: Option[Int])

  val parser = new scopt.OptionParser[Params]("scopt") {
    head("scopt", "3.x")
    opt[String]("host") action { (x, c) =>
      c.copy(host = Some(x))
    } text "the remote host"
    opt[Int]("port") action { (x, c) =>
      c.copy(port = Some(x))
    } text "the remote port"
    opt[String]("database").required() action { (x, c) =>
      c.copy(db = x)
    } text "the database to select"
    opt[Int]("width") action { (x, c) =>
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
