package io.radicalbit.nsdb.sql.parser

import io.radicalbit.nsdb.common.statement.{InsertSQLStatement, ListAssignment}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class InsertSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A parser instance for insert statements" when {

    "receive an insert with a single dimension" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry", input = "INSERT INTO people DIM(name=john) FLD(value=23) ") should be(
          Success(
            InsertSQLStatement(namespace = "registry",
                               metric = "people",
                               timestamp = None,
                               dimensions = ListAssignment(Map("name" -> "john")),
                               fields = ListAssignment(Map("value"    -> 23))))
        )
      }
    }

    "receive an insert with multiple dimensions" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry", input = "INSERT INTO people DIM(name=john, surname=doe) FLD(value=23) ") should be(
          Success(
            InsertSQLStatement(
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = ListAssignment(Map("name" -> "john", "surname" -> "doe")),
              fields = ListAssignment(Map("value"    -> 23))
            ))
        )
      }
    }

    "receive an insert with multiple dimensions and a timestamp" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry",
                     input = "INSERT INTO people TS=123456 DIM(name=john, surname=doe) FLD(value=23) ") should be(
          Success(
            InsertSQLStatement(
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = ListAssignment(Map("name" -> "john", "surname" -> "doe")),
              fields = ListAssignment(Map("value"    -> 23))
            ))
        )
      }
    }

    "receive an insert with multiple dimensions in int and float format and a timestamp" should {
      "parse it successfully" in {
        parser.parse(namespace = "registry",
          input = "INSERT INTO people TS=123456 DIM(x=1, y=1.5) FLD(value=23) ") should be(
          Success(
            InsertSQLStatement(
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = ListAssignment(Map("x" -> 1, "y" -> 1.5)),
              fields = ListAssignment(Map("value"    -> 23))
            ))
        )
      }
    }

    "receive a wrong metric without dimensions" should {
      "fail" in {
        parser.parse(namespace = "registry", input = "INSERT INTO people FLD(value=23) ") shouldBe 'failure
      }
    }
  }
}
