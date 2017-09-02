package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.statement.{DescribeMetric, ShowMetrics, ShowNamespaces, UseNamespace}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class CommandStatementSpec extends WordSpec with Matchers {

  private val parser = new CommandStatementParser()

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
        parser.parse(Some("registry"), "show metrics") should be(Success(ShowMetrics("registry")))
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
        parser.parse(Some("registry"), "describe people") should be(Success(DescribeMetric("registry", "people")))
      }
    }
  }
}
