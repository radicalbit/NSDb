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

import io.radicalbit.nsdb.common.statement.{DescribeMetric, ShowMetrics, ShowNamespaces, UseNamespace}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class CommandStatementSpec extends WordSpec with Matchers {

  private val parser = new CommandStatementParser("db")

  "A parser instance" when {

    "receive the request to show the namespaces" should {
      "parse it successfully" in {
        parser.parse(None, "show namespaces") should be(Success(ShowNamespaces))
      }
    }

    "receive the request to use a namespace" should {
      "parse it successfully" in {
        parser.parse(None, "use registry") should be(Success(UseNamespace("registry")))
      }
    }

    "receive the request to show the metrics" should {
      "not parsing it without specifying a namespace" in {
        parser.parse(None, "show metrics") shouldBe 'failure
      }

      "parse it successfully specifying a namespace" in {
        parser.parse(Some("registry"), "show metrics") should be(Success(ShowMetrics("db", "registry")))
      }
    }

    "receive the request to describe a metric" should {
      "not parsing it without specifying a namespace" in {
        parser.parse(None, "describe people") shouldBe 'failure
      }

      "not parsing it without specifying a metric" in {
        parser.parse(Some("registry"), "describe") shouldBe 'failure
      }

      "not parsing it without specifying a namespace and a metric" in {
        parser.parse(None, "describe") shouldBe 'failure
      }

      "parse it successfully specifying a namespace and a metric" in {
        parser.parse(Some("registry"), "describe people") should be(
          Success(DescribeMetric("db", "registry", "people")))
      }
    }
  }
}
