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

import io.radicalbit.nsdb.common.statement.{InsertSQLStatement, ListAssignment}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class InsertSQLStatementSpec extends WordSpec with Matchers {

  private val parser = new SQLStatementParser

  "A parser instance for insert statements" when {

    "receive an insert with a single dimension" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "INSERT INTO people DIM(name=john) VAL=23 ") should be(
          Success(
            InsertSQLStatement(db = "db",
                               namespace = "registry",
                               metric = "people",
                               timestamp = None,
                               dimensions = Some(ListAssignment(Map("name" -> "john"))),
                               value = 23))
        )
      }
    }

    "receive an insert with multiple dimensions" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "INSERT INTO people DIM(name=john, surname=doe) VAL=23 ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = None,
              dimensions = Some(ListAssignment(Map("name" -> "john", "surname" -> "doe"))),
              value = 23
            ))
        )
      }
    }

    "receive an insert with multiple dimensions and a timestamp" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "INSERT INTO people TS=123456 DIM(name=john, surname=doe) VAL=23 ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = Some(ListAssignment(Map("name" -> "john", "surname" -> "doe"))),
              value = 23
            ))
        )
      }
    }

    "receive an insert with multiple dimensions in int and float format and a timestamp" should {
      "parse it successfully" in {
        parser.parse(db = "db", namespace = "registry", input = "INSERT INTO people TS=123456 DIM(x=1, y=1.5) VAL=23 ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = Some(ListAssignment(Map("x" -> 1, "y" -> 1.5))),
              value = 23
            ))
        )
      }
    }

    "receive an insert with multiple dimensions in int and float format, a timestamp and a float value" should {
      "parse it successfully" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "INSERT INTO people TS=123456 DIM(x=1, y=1.5) VAL=23.5 ") should be(
          Success(
            InsertSQLStatement(
              db = "db",
              namespace = "registry",
              metric = "people",
              timestamp = Some(123456),
              dimensions = Some(ListAssignment(Map("x" -> 1, "y" -> 1.5))),
              value = 23.5
            ))
        )
      }
    }

    "receive a insert metric without dimensions" should {
      "succeed" in {
        parser.parse(db = "db", namespace = "registry", input = "INSERT INTO people val=23) ") should be(
          Success(
            InsertSQLStatement(db = "db", "registry", "people", None, None, 23)
          )
        )
      }
    }

    "receive a insert metric with dimension string with spaces" should {
      "succeed" in {
        parser.parse(db = "db",
                     namespace = "registry",
                     input = "INSERT INTO people DIM(name = 'spaced string')val=23) ") should be(
          Success(
            InsertSQLStatement(db = "db",
                               "registry",
                               "people",
                               None,
                               Some(ListAssignment(Map("name" -> "spaced string"))),
                               23)
          )
        )
      }
    }
  }
}
