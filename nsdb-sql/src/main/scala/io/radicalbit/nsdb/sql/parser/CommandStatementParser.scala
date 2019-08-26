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

package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.exception.InvalidStatementException
import io.radicalbit.nsdb.common.statement._

import scala.util.parsing.combinator.RegexParsers
import scala.util.{Try, Failure => ScalaFailure, Success => ScalaSuccess}

/**
  * Parser combinator for cli commands.
  * Here is the EBNF grammar
  * {{{
  *   "DESCRIBE" literal
  *   "METRICS" literal
  *   "NAMESPACES" literal
  *   "SHOW" literal
  *   "USE" literal
  * }}}
  * @param db the db to execute the command in
  */
class CommandStatementParser(db: String) extends RegexParsers {

  implicit class InsensitiveString(str: String) {
    def ignoreCase: Parser[String] = ("""(?i)\Q""" + str + """\E""").r ^^ { _.toString.toUpperCase }
  }

  private val Describe   = "DESCRIBE" ignoreCase
  private val Metrics    = "METRICS" ignoreCase
  private val Namespaces = "NAMESPACES" ignoreCase
  private val Show       = "SHOW" ignoreCase
  private val Use        = "USE" ignoreCase

  private val namespace = """(^[a-zA-Z][a-zA-Z0-9_]*)""".r
  private val metric    = """(^[a-zA-Z][a-zA-Z0-9_]*)""".r

  private def showNamespaces = Show ~ Namespaces ^^ { _ =>
    ShowNamespaces
  }

  private def useNamespace = Use ~> namespace ^^ { ns =>
    UseNamespace(ns)
  }

  private def showMetrics(namespace: Option[String]) = Show ~ Metrics ^^ {
    case _ if namespace.isDefined => ShowMetrics(db, namespace.get)
    case _                        => sys.error("Please select a valid namespace to list the associated metrics.")
  }

  private def describeMetric(namespace: Option[String]) = Describe ~> metric ^^ {
    case m if namespace.isDefined => DescribeMetric(db, namespace = namespace.get, metric = m)
    case _                        => sys.error("Please select a valid namespace to describe the given metric.")
  }

  private def commands(namespace: Option[String]) =
    showNamespaces | useNamespace | showMetrics(namespace) | describeMetric(namespace)

  def parse(namespace: Option[String], input: String): Try[CommandStatement] =
    Try(parse(commands(namespace), s"$input;")).flatMap {
      case Success(res, _) => ScalaSuccess(res.asInstanceOf[CommandStatement])
      case Error(msg, _)   => ScalaFailure(new InvalidStatementException(msg))
      case Failure(msg, _) => ScalaFailure(new InvalidStatementException(msg))
    }
}
