package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.statement._
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class InsertSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A parser instance for insert statements" when {

    "receive an insert with a single dimension" should {
      "parse it successfully" in {
        parser.parse("INSERT INTO people DIM(name=john) FLD(value=23) ") should be(
          Success(
            InsertSQLStatement(metric = "people",
                               timestamp = None,
                               dimensions = ListAssignment(Map("name" -> "john")),
                               fields = ListAssignment(Map("value"    -> 23))))
        )
      }
    }

    "receive an insert with multiple dimensions" should {
      "parse it successfully" in {
        parser.parse("INSERT INTO people DIM(name=john, surname=doe) FLD(value=23) ") should be(
          Success(
            InsertSQLStatement(metric = "people",
                               timestamp = None,
                               dimensions = ListAssignment(Map("name" -> "john", "surname" -> "doe")),
                               fields = ListAssignment(Map("value"    -> 23))))
        )
      }
    }

    "receive an insert with multiple dimensions and a timestamp" should {
      "parse it successfully" in {
        parser.parse("INSERT INTO people TS=123456 DIM(name=john, surname=doe) FLD(value=23) ") should be(
          Success(
            InsertSQLStatement(metric = "people",
                               timestamp = Some(123456),
                               dimensions = ListAssignment(Map("name" -> "john", "surname" -> "doe")),
                               fields = ListAssignment(Map("value"    -> 23))))
        )
      }
    }

    "receive a wrong metric without dimensions" should {
      "fail" in {
        parser.parse("INSERT INTO people FLD(value=23) ") shouldBe 'failure
      }
    }

    "receive a wrong metric without fields" should {
      "fail" in {
        parser.parse("INSERT INTO people DIM(name=john, surname=doe) ") shouldBe 'failure
      }
    }
  }
}
