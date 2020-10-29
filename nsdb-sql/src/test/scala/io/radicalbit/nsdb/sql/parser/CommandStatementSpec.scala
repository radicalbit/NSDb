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

package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.statement._
import io.radicalbit.nsdb.sql.parser.StatementParserResult._
import io.radicalbit.nsdb.test.NSDbSpec

class CommandStatementSpec extends NSDbSpec {

  private val parser = new CommandStatementParser("db")

  "A Command parser instance" when {

    "receive the request to show the namespaces" should {
      "parse it successfully" in {
        val command = "show namespaces"
        parser.parse(None, command) should be(CommandStatementParserSuccess(command, ShowNamespaces))
      }
    }

    "receive the request to use a namespace" should {
      "parse it successfully" in {
        val command = "use registry"
        parser.parse(None, command) should be(CommandStatementParserSuccess(command, UseNamespace("registry")))
      }
    }

    "receive the request to show the metrics" should {
      "not parsing it without specifying a namespace" in {
        parser.parse(None, "show metrics") shouldBe a[CommandStatementParserFailure]
      }

      "parse it successfully specifying a namespace" in {
        val command = "show metrics"
        parser.parse(Some("registry"), command) should be(
          CommandStatementParserSuccess(command, ShowMetrics("db", "registry")))
      }
    }

    "receive the request to describe a metric" should {
      "not parsing it without specifying a namespace" in {
        parser.parse(None, "describe people") shouldBe a[CommandStatementParserFailure]
      }

      "not parsing it without specifying a metric" in {
        parser.parse(Some("registry"), "describe") shouldBe a[CommandStatementParserFailure]
      }

      "not parsing it without specifying a namespace and a metric" in {
        parser.parse(None, "describe") shouldBe a[CommandStatementParserFailure]
      }

      "parse it successfully specifying a namespace and a metric" in {
        val command = "describe people"
        parser.parse(Some("registry"), command) should be(
          CommandStatementParserSuccess(command, DescribeMetric("db", "registry", "people")))
      }
    }
  }
}
